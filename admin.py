from __future__ import annotations

import os
import sys
from typing import Any, Dict

# Ensure local folder is importable even if run from another working directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from bson import ObjectId

import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession

import phonenumbers


def _emoji_from_region(region: str) -> str:
    """Convert country code (IN) -> flag emoji (🇮🇳)."""
    if not region or len(region) != 2:
        return ""
    region = region.upper()
    return chr(127397 + ord(region[0])) + chr(127397 + ord(region[1]))


def detect_country_from_phone(phone_e164: str) -> tuple[str | None, str | None, bool]:
    """Return (country_code, emoji, needs_us_ca_choice).

    Uses phonenumbers to detect region. If region can't be determined for +1,
    we ask admin to choose US/CA.
    """
    try:
        num = phonenumbers.parse(phone_e164, None)
        region = phonenumbers.region_code_for_number(num)
    except Exception:
        region = None

    digits = "".join(ch for ch in phone_e164 if ch.isdigit())

    # Special handling for +1 if unresolved
    if digits.startswith("1") and (region is None or region not in {"US", "CA"}):
        return None, None, True

    if not region:
        return None, None, False

    return region, _emoji_from_region(region), False

try:
    from telegram import (
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        KeyboardButton,
        ReplyKeyboardMarkup,
        ReplyKeyboardRemove,
        Update,
    )
    from telegram.constants import ParseMode
    from telegram.ext import ContextTypes
except ImportError as e:  # pragma: no cover
    raise RuntimeError(
        "Wrong 'telegram' package installed. This project requires 'python-telegram-bot'.\n\n"
        "Fix (recommended):\n"
        "  pip uninstall -y telegram\n"
        "  pip uninstall -y python-telegram-bot telegram-bot\n"
        "  pip install -U python-telegram-bot\n\n"
        "Then restart the bot. Original import error: "
        + str(e)
    )



async def _notify_referral_award(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    repo: Repo,
    referred_user_id: int,
    deposit_amount: int,
    admin_id: int,
    deposit_id: str | None = None,
) -> None:
    """Credit referral earning to referrer (forever) and notify them.

    Note: This is called after a deposit is approved.
    """
    # Find referral mapping
    ref = await repo.db.referrals.find_one({"referred_user_id": int(referred_user_id)})
    if not ref:
        return

    referrer_id = int(ref.get("referrer_user_id") or 0)
    if not referrer_id:
        return

    reward = int(round((int(deposit_amount) * float(REFERRAL_PERCENT)) / 100.0))
    if reward <= 0:
        return

    user = await repo.add_referral_earning(
        referrer_user_id=referrer_id,
        referred_user_id=int(referred_user_id),
        amount=float(reward),
        by_admin=int(admin_id),
        deposit_id=str(deposit_id) if deposit_id else None,
        deposit_amount=int(deposit_amount),
    )

    referred_un = (ref.get("referred_username") or "").strip()
    ref_line = f"@{referred_un}" if referred_un else "N/A"

    try:
        await context.bot.send_message(
            chat_id=referrer_id,
            text=(
                "🎉 Referral Reward Added!\n"
                f"• From user: {referred_user_id} {ref_line}\n"
                f"• Deposit: ₹{int(deposit_amount)}\n"
                f"• Reward: +₹{reward} credits ({REFERRAL_PERCENT:.1f}%)\n"
                f"• New Balance: {int((user or {}).get('credits', 0))} credits"
            ),
        )
    except Exception:
        pass


async def safe_edit(
    message,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode=ParseMode.MARKDOWN,
):
    """Edit a message safely.

    - If the message is a photo/document with caption, use edit_caption.
    - Else use edit_text.
    """
    try:
        if getattr(message, "photo", None) and (getattr(message, "text", None) in (None, "")):
            return await message.edit_caption(caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
        return await message.edit_text(text=text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception:
        return await message.edit_text(text=text, parse_mode=parse_mode, reply_markup=reply_markup)



def deposits_keyboard(
      filter_key: str,
      page: int,
      has_prev: bool,
      has_next: bool,
      deposits: list[dict] | None = None,
  ) -> InlineKeyboardMarkup:
      """Deposits list keyboard.

      Adds per-deposit 'View' buttons so admins can open/resend screenshot even if initial DM failed.
      """
      # filter_key: all|pending|approved
      rows: list[list[InlineKeyboardButton]] = [
          [
              InlineKeyboardButton("🟡 Pending", callback_data="admin:deposits:pending:0"),
              InlineKeyboardButton("🟢 Confirmed", callback_data="admin:deposits:approved:0"),
              InlineKeyboardButton("📋 All", callback_data="admin:deposits:all:0"),
          ]
      ]

      # Per deposit view buttons (page sized, so safe)
      if deposits:
          for d in deposits:
              dep_id = str(d.get("_id"))
              amt = d.get("amount")
              uid = d.get("user_id")
              rows.append([InlineKeyboardButton(f"🔎 View {amt} | {uid}", callback_data=f"admin:dep:view:{dep_id}")])

      nav: list[InlineKeyboardButton] = []
      if has_prev:
          nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:deposits:{filter_key}:{page-1}"))
      if has_next:
          nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin:deposits:{filter_key}:{page+1}"))
      if nav:
          rows.append(nav)

      rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")])
      return kb(rows)

from config import ADMIN_USER_IDS, TELEGRAM_API_ID, TELEGRAM_API_HASH, BOT_USERNAME

# Backward-compatible: if REFERRAL_PERCENT is not present in config.py yet
try:
    from config import REFERRAL_PERCENT  # type: ignore
except Exception:
    import os

    REFERRAL_PERCENT = float(os.getenv("REFERRAL_PERCENT", "3.0"))
from database import Repo, get_db


def is_admin(user_id: int) -> bool:
    return int(user_id) in set(ADMIN_USER_IDS)


def kb(rows: list[list[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(rows)


def cancel_reply_kb() -> ReplyKeyboardMarkup:
    # bottom keyboard for admin text flows
    return ReplyKeyboardMarkup([[KeyboardButton("Cancel")]], resize_keyboard=True)


async def restore_main_reply_menu(message) -> None:
    """Ensure the normal bottom reply keyboard is visible (after back navigation).

    Telegram only applies ReplyKeyboardMarkup when sending a message, so we send
    a zero-width character to keep it visually silent.
    """
    try:
        await message.reply_text("\u200b", reply_markup=main_reply_menu(True))
    except Exception:
        return


def main_reply_menu(is_admin_user: bool = True) -> ReplyKeyboardMarkup:
    # Restore the normal bottom menu after cancelling admin flows
    rows = [
        [KeyboardButton("🛒 Buy"), KeyboardButton("💳 Deposit")],
        [KeyboardButton("💰 Balance"), KeyboardButton("📜 History")],
        [KeyboardButton("🆘 Support")],
    ]
    if is_admin_user:
        rows.append([KeyboardButton("🛠 Admin")])

    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True, selective=False)


def admin_menu_keyboard() -> InlineKeyboardMarkup:
    # Most rows: 3 buttons per row (clean grid)
    # Last row: Stats + Menu (2 per row)
    return kb(
        [
            [
                InlineKeyboardButton("➕ Add Account", callback_data="admin:addaccount"),
                InlineKeyboardButton("👤 Credits", callback_data="admin:credits"),
                InlineKeyboardButton("📦 Accounts", callback_data="admin:accounts"),
            ],
            [
                InlineKeyboardButton("💳 Deposits", callback_data="admin:deposits"),
                InlineKeyboardButton("💰 Active Credits", callback_data="admin:activecredits:0"),
            ],
            [
                InlineKeyboardButton("💠 QRs", callback_data="admin:qrs"),
                InlineKeyboardButton("🎁 Referrals", callback_data="admin:referrals:0"),
            ],
            [
                InlineKeyboardButton("🚫 Ban System", callback_data="admin:banmenu"),
                InlineKeyboardButton("📊 Stats", callback_data="admin:stats"),
                InlineKeyboardButton("🏠 Menu", callback_data="menu:home"),
            ],
        ]
    )


def accounts_menu_keyboard() -> InlineKeyboardMarkup:
    return kb(
        [
            [
                InlineKeyboardButton("✅ Available Accounts", callback_data="admin:accounts:available:0"),
                InlineKeyboardButton("💸 Sold Accounts", callback_data="admin:accounts:sold:0"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")],
        ]
    )


def accounts_list_keyboard(
    accounts: list[dict],
    *,
    filter_key: str,
    page: int,
    has_prev: bool,
    has_next: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    # list items
    for a in accounts:
        acc_id = str(a.get("_id"))
        emoji = a.get("country_emoji") or ""
        country = a.get("country") or ""
        year = a.get("year")
        status = "sold" if a.get("status") == "assigned" else a.get("status")
        sold_to = ""
        if status == "sold":
            su = (a.get("sold_to_username") or "").strip()
            sid = a.get("sold_to_user_id") or a.get("assigned_to")
            sold_to = f" -> @{su}" if su else (f" -> {sid}" if sid else "")

        price = a.get("price")
        price_txt = f"{price}c" if price is not None else "default"
        rows.append(
            [
                InlineKeyboardButton(
                    f"{emoji}+{a.get('phone')} | {country} | {year} | {status}{sold_to} | {price_txt}",
                    callback_data=f"admin:account:view:{acc_id}",
                )
            ]
        )

    # pagination
    nav: list[InlineKeyboardButton] = []
    if has_prev:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:accounts:{filter_key}:{page-1}"))
    if has_next:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin:accounts:{filter_key}:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin:accounts")])
    return kb(rows)


def account_detail_keyboard(account_id: str) -> InlineKeyboardMarkup:
    return kb(
        [
            [
                InlineKeyboardButton("✏️ Edit", callback_data=f"admin:account:edit:{account_id}"),
                InlineKeyboardButton("🗑 Delete", callback_data=f"admin:account:delete:{account_id}"),
            ],
            [
                InlineKeyboardButton("🛠️ Manage Devices", callback_data=f"dev:menu:{account_id}"),
                InlineKeyboardButton("📱 Get OTP", callback_data=f"admin:account:getotp:{account_id}"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin:accounts")],
        ]
    )


def account_delete_confirm_keyboard(account_id: str) -> InlineKeyboardMarkup:
    return kb(
        [
            [InlineKeyboardButton("✅ Yes, delete", callback_data=f"admin:account:delete_confirm:{account_id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"admin:account:view:{account_id}")],
        ]
    )


def active_credits_keyboard(page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    nav: list[InlineKeyboardButton] = []
    if has_prev:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:activecredits:{page-1}"))
    if has_next:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin:activecredits:{page+1}"))

    rows: list[list[InlineKeyboardButton]] = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")])
    return kb(rows)


# Sessions helper functions removed


async def handle_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, state: Dict[int, Dict[str, Any]]) -> bool:
    """Handle admin:* callbacks. Returns True if handled."""
    query = update.callback_query
    if not query:
        return False

    data = query.data or ""
    if not data.startswith("admin:"):
        return False

    uid = update.effective_user.id
    if not is_admin(uid):
        await query.answer("❌ Access denied.", show_alert=True)
        return True

    repo: Repo = context.application.bot_data["repo"]

    if data == "admin:menu":
        await restore_main_reply_menu(query.message)
        await safe_edit(query.message, "🛠 Admin Panel", reply_markup=admin_menu_keyboard(), parse_mode=None)
        return True

    if data == "admin:addaccount":
        # Use fixed API creds; ask only for phone
        state[uid] = {"flow": "admin_add_account", "step": "phone", "api_id": TELEGRAM_API_ID, "api_hash": TELEGRAM_API_HASH}
        await query.message.reply_text(
            "➕ Add Account\n\nSend phone with + (example: +923001234567):",
            reply_markup=cancel_reply_kb(),
        )
        return True

    if data in {"admin:addaccount:cc:us", "admin:addaccount:cc:ca"}:
        await query.answer(cache_time=0)
        st = state.get(uid) or {}
        if st.get("flow") != "admin_add_account" or st.get("step") != "pick_usca":
            return True
        if data.endswith(":us"):
            st["country"] = "US"
            st["country_emoji"] = "🇺🇸"
        else:
            st["country"] = "CA"
            st["country_emoji"] = "🇨🇦"
        st["step"] = "year"
        state[uid] = st
        await query.message.reply_text("Send account year (example 2023) or type 'skip':")
        return True

    if data == "admin:credits":
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        await safe_edit(
            query.message,
            "👤 *Credits Manager*\n\nChoose action:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb(
                [
                    [
                        InlineKeyboardButton("➕ Add", callback_data="admin:credits:add"),
                        InlineKeyboardButton("➖ Remove", callback_data="admin:credits:remove"),
                    ],
                    [InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")],
                ]
            ),
        )
        return True

    if data in {"admin:credits:add", "admin:credits:remove"}:
        await query.answer(cache_time=0)
        mode = "add" if data.endswith(":add") else "remove"
        # Store UI message for inline updates and prompt for input
        state[uid] = {
            "flow": "admin_credits_inline",
            "step": "input",
            "mode": mode,
            "ui_chat_id": query.message.chat_id,
            "ui_message_id": query.message.message_id,
        }
        await safe_edit(
            query.message,
            f"👤 *Credits ({mode})*\n\nSend in one line:\n`<user_id> <amount>`\n\nOr press Cancel.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="admin:credits")]]),
        )
        # show bottom cancel keyboard too
        await query.message.reply_text("Press Cancel to stop.", reply_markup=cancel_reply_kb())
        return True

    if data == "admin:accounts":
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        await safe_edit(query.message, "📦 Accounts\n\nChoose list:", reply_markup=accounts_menu_keyboard(), parse_mode=None)
        return True

    if data.startswith("admin:accounts:"):
        # admin:accounts:<available|sold>:<page>
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        parts = data.split(":")
        filter_key = parts[2] if len(parts) > 2 else "available"
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0

        status = "available" if filter_key == "available" else "assigned"  # sold
        total_rows = await repo.count_accounts(status=status)
        page_size = 5
        max_page = max(0, (total_rows - 1) // page_size)
        if page > max_page:
            page = max_page

        accounts = await repo.list_accounts_page(status=status, page=page, page_size=page_size)
        has_prev = page > 0
        has_next = page < max_page

        title = "✅ Available Accounts" if filter_key == "available" else "💸 Sold Accounts"
        header = f"{title}\n\nPage: {page+1}/{max_page+1 if total_rows else 1}"

        if not accounts:
            await safe_edit(
                query.message,
                header + "\n\nNo accounts found.",
                reply_markup=accounts_list_keyboard([], filter_key=filter_key, page=page, has_prev=False, has_next=False),
                parse_mode=None,
            )
            return True

        await safe_edit(
            query.message,
            header + "\n\nSelect an account:",
            reply_markup=accounts_list_keyboard(accounts, filter_key=filter_key, page=page, has_prev=has_prev, has_next=has_next),
            parse_mode=None,
        )
        return True

    # Sessions handlers removed (all admin:sessions:* callbacks now ignored)

    if data == "admin:qrs":
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        flags = await repo.get_inr_qr_flags()
        qr1 = "ON ✅" if flags.get("qr1") else "OFF ❌"
        qr2 = "ON ✅" if flags.get("qr2") else "OFF ❌"
        qr3 = "ON ✅" if flags.get("qr3") else "OFF ❌"
        text = f"💠 INR QRs\n\nQR 1: {qr1}\nQR 2: {qr2}\nQR 3: {qr3}"
        await safe_edit(
            query.message,
            text,
            parse_mode=None,
            reply_markup=kb(
                [
                    [
                        InlineKeyboardButton("Toggle QR 1", callback_data="admin:qrs:toggle:qr1"),
                        InlineKeyboardButton("Toggle QR 2", callback_data="admin:qrs:toggle:qr2"),
                    ],
                    [InlineKeyboardButton("Toggle QR 3", callback_data="admin:qrs:toggle:qr3")],
                    [InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")],
                ]
            ),
        )
        return True

    if data.startswith("admin:qrs:toggle:"):
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        qr_key = data.split(":", 3)[3]
        flags = await repo.get_inr_qr_flags()
        new_enabled = not bool(flags.get(qr_key))
        flags = await repo.set_inr_qr_flag(qr_key=qr_key, enabled=new_enabled)
        qr1 = "ON ✅" if flags.get("qr1") else "OFF ❌"
        qr2 = "ON ✅" if flags.get("qr2") else "OFF ❌"
        qr3 = "ON ✅" if flags.get("qr3") else "OFF ❌"
        text = f"💠 INR QRs\n\nQR 1: {qr1}\nQR 2: {qr2}\nQR 3: {qr3}"
        await safe_edit(
            query.message,
            text,
            parse_mode=None,
            reply_markup=kb(
                [
                    [
                        InlineKeyboardButton("Toggle QR 1", callback_data="admin:qrs:toggle:qr1"),
                        InlineKeyboardButton("Toggle QR 2", callback_data="admin:qrs:toggle:qr2"),
                    ],
                    [InlineKeyboardButton("Toggle QR 3", callback_data="admin:qrs:toggle:qr3")],
                    [InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")],
                ]
            ),
        )
        return True

    if data == "admin:banmenu":
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        await safe_edit(
            query.message,
            "🚫 Ban System\n\nChoose action:",
            parse_mode=None,
            reply_markup=kb(
                [
                    [
                        InlineKeyboardButton("🚫 Ban", callback_data="admin:banmenu:ban"),
                        InlineKeyboardButton("✅ Unban", callback_data="admin:banmenu:unban"),
                    ],
                    [InlineKeyboardButton("📋 Check Ban List", callback_data="admin:banlist:0")],
                    [
                    ],
                    [InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")],
                ]
            ),
        )
        return True

    if data.startswith("admin:banlist:"):
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        page = int(data.split(":", 2)[2]) if data.split(":", 2)[2].isdigit() else 0
        db = get_db()
        page_size = 5
        total = await db.banned_users.count_documents({})
        max_page = max(0, (total - 1) // page_size) if total else 0
        if page > max_page:
            page = max_page

        cur = (
            db.banned_users.find({})
            .sort("created_at", -1)
            .skip(page * page_size)
            .limit(page_size)
        )
        items = await cur.to_list(length=page_size)

        lines: list[str] = ["🚫 Banned Users", ""]
        if not items:
            lines.append("No banned users.")
        else:
            for it in items:
                uid2 = it.get("user_id")
                un = (it.get("username") or "").strip()
                uline = f"@{un}" if un else "N/A"
                lines.append(f"• {uid2} | {uline}")

        nav: list[list[InlineKeyboardButton]] = []
        btns: list[InlineKeyboardButton] = []
        if page > 0:
            btns.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:banlist:{page-1}"))
        if page < max_page:
            btns.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin:banlist:{page+1}"))
        if btns:
            nav.append(btns)
        nav.append([InlineKeyboardButton("⬅️ Back", callback_data="admin:banmenu")])

        await safe_edit(query.message, "\n".join(lines), parse_mode=None, reply_markup=kb(nav))
        return True

    if data in {"admin:banmenu:ban", "admin:banmenu:unban"}:
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        mode = "ban" if data.endswith(":ban") else "unban"
        state[uid] = {"flow": "admin_ban", "step": "input", "mode": mode}
        await query.message.reply_text(
            f"🚫 Ban System ({mode})\n\nSend user id:\nExample: 6670166083\n\nType Cancel to stop.",
            reply_markup=cancel_reply_kb(),
        )
        return True

    # (Edit Tokens removed)

    if data.startswith("admin:referrals:"):
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        page = int(data.split(":", 2)[2]) if data.split(":", 2)[2].isdigit() else 0

        db = get_db()
        page_size = 10

        pipeline = [
            {"$group": {"_id": "$referrer_user_id", "count": {"$sum": 1}, "username": {"$first": "$referrer_username"}}},
            {"$sort": {"count": -1}},
            {"$skip": page * page_size},
            {"$limit": page_size},
        ]

        rows = [r async for r in db.referrals.aggregate(pipeline)]
        total_referrers = await db.referrals.distinct("referrer_user_id")
        total = len(total_referrers)
        max_page = max(0, (total - 1) // page_size) if total else 0

        lines: list[str] = ["🎁 Referrals (Top referrers)", ""]
        if not rows:
            lines.append("No referrals yet.")
        else:
            for r in rows:
                rid = int(r.get("_id") or 0)
                uname = (r.get("username") or "").strip()
                ref_count = int(r.get("count") or 0)
                udoc = await db.users.find_one({"user_id": rid})
                earned = float((udoc or {}).get("ref_earned_total", 0.0) or 0.0)
                uline = f"@{uname}" if uname else "N/A"
                lines.append(f"• {rid} | {uline} | refs: {ref_count} | earned: ₹{earned:.2f}")

        nav: list[list[InlineKeyboardButton]] = []
        btns: list[InlineKeyboardButton] = []
        if page > 0:
            btns.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:referrals:{page-1}"))
        if page < max_page:
            btns.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin:referrals:{page+1}"))
        if btns:
            nav.append(btns)
        nav.append([InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")])

        await safe_edit(query.message, "\n".join(lines), parse_mode=None, reply_markup=kb(nav))
        return True

    # admin:sessions:purge_confirm removed

    if data.startswith("admin:activecredits:"):
        # admin:activecredits:<page>
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        page = int(data.split(":", 2)[2]) if data.split(":", 2)[2].isdigit() else 0

        db = get_db()
        page_size = 15
        total_rows = await db.users.count_documents({"credits": {"$gt": 0}})
        max_page = max(0, (total_rows - 1) // page_size)
        if page > max_page:
            page = max_page

        cursor = (
            db.users.find({"credits": {"$gt": 0}})
            .sort("credits", -1)
            .skip(page * page_size)
            .limit(page_size)
        )
        users = await cursor.to_list(length=page_size)

        lines: list[str] = [
            "💰 Active Credits",
            "",
            f"Page: {page + 1}/{max_page + 1}",
            "",
        ]

        if not users:
            lines.append("No users with credits > 0.")
        else:
            for u in users:
                uid2 = u.get("user_id") or u.get("_id")
                username = (u.get("username") or "").strip()
                # Fallback for older records: try to read last known username from deposits
                if not username and uid2:
                    last_dep = await db.deposits.find_one({"user_id": int(uid2)}, sort=[("created_at", -1)])
                    if last_dep:
                        username = (last_dep.get("username") or "").strip()
                credits = u.get("credits", 0)
                uname = f"@{username}" if username else "N/A"
                lines.append(f"• {uid2} | {uname} | credits: {credits}")

        has_prev = page > 0
        has_next = page < max_page
        await safe_edit(
            query.message,
            "\n".join(lines),
            parse_mode=None,
            reply_markup=active_credits_keyboard(page, has_prev, has_next),
        )
        return True

    if data == "admin:stats":
        await query.answer(cache_time=0)
        await restore_main_reply_menu(query.message)
        db = get_db()
        total_users = await db.users.count_documents({})
        total_accounts = await db.accounts.count_documents({})
        available = await db.accounts.count_documents({"status": "available"})
        sold = await db.accounts.count_documents({"status": "assigned"})

        text = (
            "📊 *Statistics*\n\n"
            f"👥 Users: *{total_users}*\n"
            f"📦 Accounts: *{total_accounts}*\n"
            f"✅ Available: *{available}*\n"
            f"💸 Sold: *{sold}*\n"
        )

        await safe_edit(query.message, text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")]]))
        return True

    if data == "admin:deposits":
        await restore_main_reply_menu(query.message)
        # default view: pending page 0
        data = "admin:deposits:pending:0"

    if data.startswith("admin:deposits:"):
        await restore_main_reply_menu(query.message)
        # admin:deposits:<filter>:<page>
        parts = data.split(":")
        filter_key = parts[2] if len(parts) > 2 else "pending"
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0

        status = None
        if filter_key == "pending":
            status = "pending"
        elif filter_key == "approved":
            status = "approved"
        else:
            status = None  # all

        totals = await repo.deposit_totals()
        # hide rejected in UI
        pending_count = totals.get("pending_count", 0)
        pending_amount = totals.get("pending_amount", 0)
        approved_count = totals.get("approved_count", 0)
        approved_amount = totals.get("approved_amount", 0)
        total_count = pending_count + approved_count
        total_amount = pending_amount + approved_amount

        total_rows = await repo.count_deposits(status=status)
        page_size = 8
        max_page = max(0, (total_rows - 1) // page_size)
        if page > max_page:
            page = max_page

        deps = await repo.list_deposits_page(status=status, page=page, page_size=page_size)

        header = [
            "💳 Deposits",
            "",
            f"🟡 Pending: {pending_count} (amount {pending_amount})",
            f"🟢 Confirmed: {approved_count} (amount {approved_amount})",
            f"⭐ Total: {total_count} (amount {total_amount})",
            "",
        ]

        lines: list[str] = []
        for d in deps:
            stt = d.get("status")
            icon = "🟡" if stt == "pending" else "🟢" if stt == "approved" else "⚪"
            amount = d.get("amount")
            user_id = d.get("user_id")
            username = d.get("username") or ""
            dep_id = str(d.get("_id"))
            lines.append(f"{icon} {amount} | {user_id} @{username} | {dep_id}")

        body = "\n".join(header + (lines or ["No records."]))
        has_prev = page > 0
        has_next = page < max_page

        # Use plain text to avoid Telegram Markdown parse errors from dynamic content.
        await safe_edit(
            query.message,
            body,
            reply_markup=deposits_keyboard(filter_key, page, has_prev, has_next, deps),
            parse_mode=None,
        )
        return True

    if data.startswith("admin:account:view:"):
        acc_id = data.split(":", 3)[3]

        # Stop OTP monitoring if this admin was monitoring this account
        try:
            account_manager: AccountManager = context.application.bot_data["account_manager"]
            oid = ObjectId(acc_id)
            if account_manager.get_admin_monitor(oid) == int(uid):
                account_manager.stop_admin_monitor(oid)
        except Exception:
            pass

        acc = await repo.get_account(ObjectId(acc_id))
        if not acc:
            await query.answer("❌ Account not found.", show_alert=True)
            return True

        status = "sold" if acc.get("status") == "assigned" else acc.get("status")
        emoji = acc.get("country_emoji") or ""
        country = acc.get("country") or ""
        year = acc.get("year")
        price = acc.get("price")
        twofa = acc.get("twofa_password")

        sold_to_line = ""
        if status == "sold":
            su = (acc.get("sold_to_username") or "").strip()
            sid = acc.get("sold_to_user_id") or acc.get("assigned_to")
            if su:
                sold_to_line = f"Sold to: *@{su}*\n"
            elif sid:
                sold_to_line = f"Sold to: *{sid}*\n"

        text = (
            "*Account Details*\n\n"
            f"ID: `{acc_id}`\n"
            f"Phone: `{emoji} +{acc.get('phone')}`\n"
            f"Country: *{country}*\n"
            f"Year: *{year if year is not None else '-'}*\n"
            f"Status: *{status}*\n"
            + sold_to_line
            + f"Price: *{price if price is not None else 'default'}*\n"
            + f"2FA: *{'set' if twofa else 'not set'}*\n"
        )
        await safe_edit(query.message, text, reply_markup=account_detail_keyboard(acc_id), parse_mode=ParseMode.MARKDOWN)
        return True

    if data.startswith("admin:account:delete:"):
        acc_id = data.split(":", 3)[3]
        await safe_edit(
            query.message,
            "⚠️ Delete this account? This cannot be undone.",
            reply_markup=account_delete_confirm_keyboard(acc_id),
            parse_mode=None,
        )
        return True

    if data.startswith("admin:account:delete_confirm:"):
        acc_id = data.split(":", 3)[3]
        ok = await repo.delete_account(ObjectId(acc_id))
        await safe_edit(query.message, "✅ Deleted." if ok else "Account not found.", reply_markup=None, parse_mode=None)
        return True

    if data.startswith("admin:account:edit:"):
        acc_id = data.split(":", 3)[3]
        state[uid] = {"flow": "admin_edit_account", "step": "field", "account_id": acc_id}
        await query.message.reply_text(
            "✏️ Edit Account\n\n"
            "Type which field to edit: `country`, `emoji`, `year`, `twofa`, `price`\n"
            "Or type `cancel`.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return True

    if data.startswith("admin:account:getotp:"):
        await query.answer(cache_time=0)
        acc_id_s = data.split(":", 3)[3]
        try:
            account_id = ObjectId(acc_id_s)
        except Exception:
            await query.answer("Invalid account ID.", show_alert=True)
            return True

        acc = await repo.get_account(account_id)
        if not acc:
            await query.answer("Account not found.", show_alert=True)
            return True

        phone = acc.get("phone") or ""
        twofa = (acc.get("twofa_password") or "").strip()
        twofa_line = f"\n🔒 2FA: `{twofa}`" if twofa else "\n🔒 2FA: Not set"

        # Start monitoring (replace any existing monitor)
        account_manager: AccountManager = context.application.bot_data["account_manager"]
        account_manager.start_admin_monitor(account_id, uid)

        # Ensure session connected (admin monitor mode: do NOT mark admin as buyer)
        try:
            await account_manager.ensure_connected_for_admin_monitor(account_id, acc)
        except Exception as e:
            await query.answer(f"Failed to connect session: {e}", show_alert=True)
            account_manager.stop_admin_monitor(account_id)
            return True

        await safe_edit(
            query.message,
            f"📱 Get OTP\n\n"
            f"Phone: +{phone}{twofa_line}\n\n"
            f"✅ OTP forwarding started.\n"
            f"I will forward any OTP received in this chat.",

            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb(
                [
                    [InlineKeyboardButton("🔄 Retry OTP", callback_data=f"admin:account:getotp:{acc_id_s}")],
                    [InlineKeyboardButton("⬅️ Back", callback_data=f"admin:account:view:{acc_id_s}")],
                ]
            ),
        )
        return True

    # Cancel removed; use Retry button or Back (stops monitor automatically)
    if data.startswith("admin:account:getotp:cancel:"):
        await query.answer(cache_time=0)
        return True

    # Deposit details / resend screenshot
    if data.startswith("admin:dep:view:"):
        dep_id = data.split(":", 3)[3]
        dep = await repo.get_deposit(dep_id)
        if not dep:
            await query.answer("❌ Deposit not found.", show_alert=True)
            return True

        stt = dep.get("status")
        method = (dep.get("method") or "").upper()
        network = (dep.get("network") or "").upper()
        amount = dep.get("amount")
        amount_text = dep.get("amount_text")
        user_id = dep.get("user_id")
        username = dep.get("username") or ""

        # Build action buttons (same as bot)
        if dep.get("method") == "crypto":
            action_markup = kb(
                [
                    [
                        InlineKeyboardButton("✅ Set Credits & Approve", callback_data=f"admin:dep:setcredits:{dep_id}"),
                        InlineKeyboardButton("❌ Reject", callback_data=f"admin:dep:reject:{dep_id}"),
                    ],
                    [InlineKeyboardButton("⬅️ Back", callback_data="admin:deposits")],
                ]
            )
        else:
            action_markup = kb(
                [
                    [
                        InlineKeyboardButton("✅ Approve", callback_data=f"admin:dep:approve:{dep_id}"),
                        InlineKeyboardButton("❌ Reject", callback_data=f"admin:dep:reject:{dep_id}"),
                    ],
                    [InlineKeyboardButton("⬅️ Back", callback_data="admin:deposits")],
                ]
            )

        # Diagnostics: last notify failures
        notify = dep.get("admin_notify") or []
        fails = [n for n in notify if not n.get("ok")]
        fail_lines = []
        for n in fails[-5:]:
            fail_lines.append(f"• admin {n.get('admin_id')}: {n.get('error')}")
        diag = ("\n\n⚠️ Last notify errors:\n" + "\n".join(fail_lines)) if fail_lines else ""

        caption = (
            "💳 Deposit Request\n\n"
            f"Status: {stt}\n"
            f"User: {user_id} @{username if username else 'N/A'}\n"
            f"Method: {method}" + (f" ({network})" if network else "") + "\n"
            + (f"Paid: {amount_text}\n" if amount_text else "")
            + f"Amount: {amount}\n"
            + f"Deposit ID: {dep_id}"
            + diag
        )

        sc = dep.get("screenshot")
        if sc and sc.get("file_id"):
            try:
                if sc.get("kind") == "photo":
                    await context.bot.send_photo(
                        chat_id=uid,
                        photo=sc["file_id"],
                        caption=caption,
                        parse_mode=None,
                        reply_markup=action_markup,
                    )
                else:
                    await context.bot.send_document(
                        chat_id=uid,
                        document=sc["file_id"],
                        caption=caption,
                        parse_mode=None,
                        reply_markup=action_markup,
                    )
                await query.answer("✅ Sent deposit details.", show_alert=True)
            except Exception as e:
                await query.answer(f"❌ Failed to send screenshot: {e}", show_alert=True)
        else:
            # No screenshot saved (old deposits)
            await safe_edit(query.message, caption + "\n\n❌ Screenshot not stored.", parse_mode=None, reply_markup=action_markup)
        return True

    # Deposit approvals
    if data.startswith("admin:dep:setcredits:"):
        dep_id = data.split(":", 3)[3]
        dep = await repo.get_deposit(dep_id)
        if not dep or dep.get("status") != "pending":
            await query.answer("❌ Deposit not found or already processed.", show_alert=True)
            return True
        state[uid] = {"flow": "admin_dep_setcredits", "step": "credits", "dep_id": dep_id}
        await query.message.reply_text(
            "Send how many credits to add for this crypto payment (example: 1 USDT = 70 credits => send 70):"
        )
        return True

    if data.startswith("admin:dep:approve:"):
        # INR deposit approve: credits = amount
        dep_id = data.split(":", 3)[3]
        dep = await repo.get_deposit(dep_id)
        if not dep or dep.get("status") != "pending":
            await query.answer("❌ Deposit not found or already processed.", show_alert=True)
            return True

        base = int(dep.get("amount", 0))
        bonus = int(round((base * float(REFERRAL_PERCENT)) / 100.0))
        credits = base + bonus

        dep2 = await repo.mark_deposit(dep_id, "approved", admin_id=uid, credits_added=credits)
        if not dep2:
            await query.answer("❌ Deposit not found or already processed.", show_alert=True)
            return True

        await repo.add_credits(dep["user_id"], credits, by_admin=uid)
        await query.answer("✅ Approved and credits added.", show_alert=True)

        # notify depositor with bonus
        try:
            udoc = await repo.db.users.find_one({"user_id": int(dep["user_id"])})
            bal = int((udoc or {}).get("credits", 0))
            await context.bot.send_message(
                chat_id=int(dep["user_id"]),
                text=(
                    "✅ Deposit approved!\n"
                    f"• Deposit: ₹{base}\n"
                    f"• Bonus: +₹{bonus} ({REFERRAL_PERCENT:.1f}%)\n"
                    f"• Total Credited: ₹{credits}\n"
                    f"• Balance: {bal} credits"
                ),
            )
        except Exception:
            pass

        # Referral earning (3%) to referrer if user was referred
        try:
            await _notify_referral_award(
                context=context,
                repo=repo,
                referred_user_id=int(dep["user_id"]),
                deposit_amount=int(base),
                admin_id=int(uid),
                deposit_id=str(dep_id),
            )
        except Exception:
            pass

        return True

    if data.startswith("admin:dep:reject:"):
        dep_id = data.split(":", 3)[3]
        dep = await repo.mark_deposit(dep_id, "rejected", admin_id=uid)
        if not dep:
            await query.answer("❌ Deposit not found or already processed.", show_alert=True)
            return True
        await query.answer("❌ Rejected.", show_alert=True)
        try:
            await context.bot.send_message(
                chat_id=int(dep["user_id"]),
                text="❌ Payment rejected. Contact admin if this is a mistake.",
            )
        except Exception:
            pass
        return True

    return True


async def handle_admin_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    state: Dict[int, Dict[str, Any]],
    account_manager,
) -> bool:
    """Handle admin guided text flows. Returns True if handled."""
    if not update.message:
        return False

    uid = update.effective_user.id
    if uid not in state:
        return False

    st = state[uid]
    flow = st.get("flow")
    step = st.get("step")

    if flow not in {
        "admin_add_account",
        "admin_credits",
        "admin_credits_inline",
        "admin_edit_account",
        "admin_dep_setcredits",
        "admin_tokenedit",
        "admin_ban",
    }:
        return False

    if not is_admin(uid):
        state.pop(uid, None)
        return True

    repo: Repo = context.application.bot_data["repo"]
    text = update.message.text.strip()

    # Global cancel for admin text flows (via bottom reply keyboard or typed text)
    if text.lower() == "cancel":
        state.pop(uid, None)
        await update.message.reply_text("Cancelled.", reply_markup=main_reply_menu(True))
        return True

    # ----- add account -----
    if flow == "admin_add_account":
        # API credentials are fixed (see FIXED_API_ID/FIXED_API_HASH). Start from phone.
        if step == "phone":
            # ensure fixed API creds are present
            st.setdefault("api_id", TELEGRAM_API_ID)
            st.setdefault("api_hash", TELEGRAM_API_HASH)

            phone_e164 = text.replace(" ", "")
            if not phone_e164.startswith("+"):
                await update.message.reply_text("Phone must start with +. Send again:")
                return True
            st["phone_e164"] = phone_e164
            st["phone"] = phone_e164.lstrip("+")

            # Auto detect country + emoji from calling code
            cc, em, needs_choice = detect_country_from_phone(phone_e164)
            if needs_choice:
                st["step"] = "pick_usca"
                state[uid] = st
                await update.message.reply_text(
                    "Detected +1 number. Choose country:",
                    reply_markup=ReplyKeyboardRemove(),
                )
                await update.message.reply_text(
                    "Select:",
                    reply_markup=kb(
                        [
                            [
                                InlineKeyboardButton("🇺🇸 USA", callback_data="admin:addaccount:cc:us"),
                                InlineKeyboardButton("🇨🇦 Canada", callback_data="admin:addaccount:cc:ca"),
                            ]
                        ]
                    ),
                )
                return True

            if cc and em:
                st["country"] = cc
                st["country_emoji"] = em
            else:
                # No manual country/emoji input anymore
                await update.message.reply_text(
                    "❌ Could not detect country from this number. Please send a valid international number with correct country code.",
                    reply_markup=cancel_reply_kb(),
                )
                st["step"] = "phone"
                return True

            st["step"] = "year"
            await update.message.reply_text("Send account year (example 2023) or type 'skip':")
            return True

        if step == "year":
            t = text.strip().lower()
            if t == "skip":
                st["year"] = None
                st["premium_months"] = None
                st["step"] = "price"
                await update.message.reply_text("Send price in credits for this account (example 75):")
                return True

            if t == "premium":
                st["year"] = "premium"
                st["step"] = "premium_months"
                await update.message.reply_text("⭐ Premium selected. Send premium months (number, e.g. 1):")
                return True

            if not t.isdigit() or len(t) != 4:
                await update.message.reply_text("Year must be 4 digits like 2023, or type 'premium', or 'skip':")
                return True
            st["year"] = int(t)
            st["premium_months"] = None

            st["step"] = "price"
            await update.message.reply_text("Send price in credits for this account (example 75):")
            return True

        if step == "premium_months":
            if not text.isdigit() or int(text) <= 0:
                await update.message.reply_text("Send premium months as a number (e.g. 1):")
                return True
            st["premium_months"] = int(text)
            st["step"] = "price"
            await update.message.reply_text("Send price in credits for this account (example 75):")
            return True

        if step == "price":
            if not text.isdigit():
                await update.message.reply_text("Price must be numeric. Send again:")
                return True
            st["price"] = int(text)

            st["step"] = "send_code"
            await update.message.reply_text("Sending Telegram login code to this phone...")
            await account_manager.admin_begin_login(uid, st["api_id"], st["api_hash"], st["phone_e164"])
            await update.message.reply_text("Now send the OTP code. If 2FA enabled, I will ask password.")
            return True

        if step == "send_code":
            code = text.replace(" ", "")
            doc, status = await account_manager.admin_complete_code(uid, code)
            if status == "need_password":
                st["step"] = "tg_password"
                await update.message.reply_text("Telegram 2FA required. Send Telegram 2FA password:")
                return True

            if status == "invalid_code":
                # allow retry
                st["step"] = "send_code"
                await update.message.reply_text(
                    "❌ Invalid OTP code. Send the OTP again (or press Cancel).",
                    reply_markup=cancel_reply_kb(),
                )
                return True

            if status == "code_expired":
                # must restart login (resend code)
                await account_manager.admin_cancel_login(uid)
                state.pop(uid, None)
                await update.message.reply_text(
                    "⚠️ OTP expired. Please start again and request a new code.",
                    reply_markup=main_reply_menu(True),
                )
                return True

            if status != "ok" or not doc:
                await account_manager.admin_cancel_login(uid)
                state.pop(uid, None)
                await update.message.reply_text("Failed to login. Cancelled.", reply_markup=main_reply_menu(True))
                return True

            await repo.create_account(
                phone=doc["phone"],
                api_id=doc["api_id"],
                api_hash=doc["api_hash"],
                session_string=doc["session_string"],
                added_by=uid,
                year=st.get("year"),
                premium_months=st.get("premium_months"),
                country=st.get("country"),
                country_emoji=st.get("country_emoji"),
                twofa_password=st.get("twofa_password"),
                price=st.get("price"),
            )
            state.pop(uid, None)
            await update.message.reply_text("✅ Account saved and added to stock.", reply_markup=main_reply_menu(True))
            return True

        if step == "tg_password":
            pwd = text.strip()
            doc, status = await account_manager.admin_complete_password(uid, pwd)
            if status in {"invalid_password"} or status != "ok" or not doc:
                # Don't cancel the whole flow; allow retry
                st["step"] = "tg_password"
                await update.message.reply_text(
                    "❌ Wrong 2FA password. Send again (or press Cancel).",
                    reply_markup=cancel_reply_kb(),
                )
                return True

            # Save ONLY the correct 2FA password
            st["twofa_password"] = pwd

            await repo.create_account(
                phone=doc["phone"],
                api_id=doc["api_id"],
                api_hash=doc["api_hash"],
                session_string=doc["session_string"],
                added_by=uid,
                year=st.get("year"),
                premium_months=st.get("premium_months"),
                country=st.get("country"),
                country_emoji=st.get("country_emoji"),
                twofa_password=st.get("twofa_password"),
                price=st.get("price"),
            )
            state.pop(uid, None)
            await update.message.reply_text("✅ Account saved and added to stock.", reply_markup=main_reply_menu(True))
            return True

    # ----- edit account -----
    if flow == "admin_edit_account":
        if text.lower() == "cancel":
            state.pop(uid, None)
            await update.message.reply_text("Cancelled.")
            return True

        acc_id = st.get("account_id")
        if not acc_id:
            state.pop(uid, None)
            return True

        if step == "field":
            field = text.lower().strip()
            if field not in {"country", "emoji", "year", "twofa", "price"}:
                await update.message.reply_text("Choose: country / emoji / year / twofa / price (or cancel)")
                return True
            st["field"] = field
            st["step"] = "value"
            await update.message.reply_text("Send new value (or 'skip' to clear):")
            return True

        if step == "value":
            field = st.get("field")
            val_raw = text

            fields: dict[str, Any] = {}
            if val_raw.lower() == "skip":
                mapping = {
                    "country": "country",
                    "emoji": "country_emoji",
                    "year": "year",
                    "twofa": "twofa_password",
                    "price": "price",
                }
                fields[mapping[field]] = None
            else:
                if field == "country":
                    fields["country"] = val_raw.upper()
                elif field == "emoji":
                    fields["country_emoji"] = val_raw
                elif field == "year":
                    if not val_raw.isdigit() or len(val_raw) != 4:
                        await update.message.reply_text("Year must be 4 digits (example 2023)")
                        return True
                    fields["year"] = int(val_raw)
                elif field == "twofa":
                    fields["twofa_password"] = val_raw
                elif field == "price":
                    if not val_raw.isdigit():
                        await update.message.reply_text("Price must be numeric")
                        return True
                    fields["price"] = int(val_raw)

            ok = await repo.update_account_fields(ObjectId(acc_id), fields)
            state.pop(uid, None)
            await update.message.reply_text("✅ Updated." if ok else "No changes saved.")
            return True

    # ----- crypto deposit: set credits then approve -----
    if flow == "admin_dep_setcredits":
        if step == "credits":
            if not text.isdigit() or int(text) <= 0:
                await update.message.reply_text("Send numeric credits (example 70):")
                return True

            dep_id = st.get("dep_id")
            base = int(text)
            bonus = int(round((base * float(REFERRAL_PERCENT)) / 100.0))
            credits = base + bonus

            dep = await repo.mark_deposit(dep_id, "approved", admin_id=uid, credits_added=credits)
            if not dep:
                state.pop(uid, None)
                await update.message.reply_text("Deposit not found or already processed.")
                return True

            await repo.add_credits(dep["user_id"], credits, by_admin=uid)
            state.pop(uid, None)
            await update.message.reply_text("✅ Approved and credits added.")
            try:
                udoc = await repo.db.users.find_one({"user_id": int(dep["user_id"])})
                bal = int((udoc or {}).get("credits", 0))
                await context.bot.send_message(
                    chat_id=int(dep["user_id"]),
                    text=(
                        "✅ Deposit approved!\n"
                        f"• Deposit: ₹{base}\n"
                        f"• Bonus: +₹{bonus} ({REFERRAL_PERCENT:.1f}%)\n"
                        f"• Total Credited: ₹{credits}\n"
                        f"• Balance: {bal} credits"
                    ),
                )
            except Exception:
                pass

            # Referral earning to referrer if user was referred
            try:
                await _notify_referral_award(
                    context=context,
                    repo=repo,
                    referred_user_id=int(dep["user_id"]),
                    deposit_amount=int(base),
                    admin_id=int(uid),
                    deposit_id=str(dep_id),
                )
            except Exception:
                pass

            return True

    # ----- credits inline (no new messages) -----
    if flow == "admin_credits_inline":
        if step == "input":
            parts = text.split()
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].lstrip('-').isdigit():
                # keep inline prompt
                chat_id = st.get("ui_chat_id")
                msg_id = st.get("ui_message_id")
                if chat_id and msg_id:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=msg_id,
                        text="❌ Invalid format. Use: `<user_id> <amount>`",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="admin:credits")]]),
                    )
                return True

            target = int(parts[0])
            amt = int(parts[1])
            if st.get("mode") == "remove":
                amt = -abs(amt)

            user = await repo.add_credits(target, amt, by_admin=uid)
            chat_id = st.get("ui_chat_id")
            msg_id = st.get("ui_message_id")
            state.pop(uid, None)

            if chat_id and msg_id:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=f"✅ Updated user `{target}` credits: *{user.get('credits', 0)}*",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="admin:menu")]]),
                )
            return True

    # ----- credits -----
    if flow == "admin_ban":
        if step == "input":
            if not text.isdigit():
                await update.message.reply_text("Send numeric user id:")
                return True
            target = int(text)
            mode = st.get("mode")
            if mode == "ban":
                # capture username if known
                uname = ""
                try:
                    ch = await context.bot.get_chat(target)
                    uname = getattr(ch, "username", "") or ""
                except Exception:
                    uname = ""
                await repo.ban_user(user_id=target, by_admin=uid, username=uname)
                state.pop(uid, None)
                await update.message.reply_text(
                    f"✅ Banned user: {target}",
                    reply_markup=main_reply_menu(True),
                )
                return True
            else:
                ok = await repo.unban_user(user_id=target)
                state.pop(uid, None)
                await update.message.reply_text(
                    f"✅ Unbanned user: {target}" if ok else f"User not banned: {target}",
                    reply_markup=main_reply_menu(True),
                )
                return True

    if flow == "admin_tokenedit":
        if step == "input":
            parts = text.split()
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                await update.message.reply_text("Format: <user_id> <count>")
                return True
            target = int(parts[0])
            count = int(parts[1])
            mode = st.get("mode")
            if mode == "remove":
                count = -abs(count)
            else:
                count = abs(count)

            new_tokens = await repo.add_tokens(target, count)
            state.pop(uid, None)
            await update.message.reply_text(
                f"✅ Tokens updated for {target}: {new_tokens}",
                reply_markup=main_reply_menu(True),
            )
            return True

    if flow == "admin_credits":
        if step == "user_id":
            if not text.isdigit():
                await update.message.reply_text("Send numeric user id:")
                return True
            st["target_user"] = int(text)
            st["step"] = "mode"
            await update.message.reply_text("Type 'add' to add credits or 'set' to set credits:")
            return True

        if step == "mode":
            m = text.lower()
            if m not in {"add", "set"}:
                await update.message.reply_text("Type 'add' or 'set':")
                return True
            st["mode"] = m
            st["step"] = "amount"
            await update.message.reply_text("Send amount (number):")
            return True

        if step == "amount":
            if not text.lstrip("-").isdigit():
                await update.message.reply_text("Send numeric amount:")
                return True
            amt = int(text)
            target = int(st["target_user"])
            if st.get("mode") == "add":
                user = await repo.add_credits(target, amt, by_admin=uid)
            else:
                user = await repo.set_credits(target, amt, by_admin=uid)
            state.pop(uid, None)
            await update.message.reply_text(
                f"✅ Updated user {target} credits: {user.get('credits', 0)}",
                reply_markup=main_reply_menu(True),
            )
            return True

    return False
