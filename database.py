from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

# Use certifi CA bundle for TLS connections to MongoDB Atlas (helps on Windows/Python)
try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None  # type: ignore
from pymongo import ReturnDocument

from config import DB_NAME, MONGO_URI

_client: Optional[AsyncIOMotorClient] = None
_db: Optional[AsyncIOMotorDatabase] = None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_client() -> AsyncIOMotorClient:
    global _client
    if _client is None:
        kwargs: dict[str, Any] = {}
        if certifi is not None:
            kwargs["tlsCAFile"] = certifi.where()
        # keep default timeouts unless overridden by URI
        _client = AsyncIOMotorClient(MONGO_URI, **kwargs)
    return _client


def get_db() -> AsyncIOMotorDatabase:
    global _db
    if _db is None:
        _db = get_client()[DB_NAME]
    return _db


async def init_indexes() -> None:
    db = get_db()

    await db.users.create_index("user_id", unique=True)
    await db.credit_logs.create_index([("user_id", 1), ("created_at", -1)])

    await db.accounts.create_index("phone", unique=True)
    await db.accounts.create_index("status")
    await db.accounts.create_index("country")
    await db.accounts.create_index("year")
    await db.accounts.create_index([("country", 1), ("year", 1), ("status", 1)])
    await db.accounts.create_index([("assigned_to", 1), ("status", 1)])

    await db.purchases.create_index([("user_id", 1), ("created_at", -1)])
    await db.purchases.create_index([("account_id", 1), ("created_at", -1)])

    await db.deposits.create_index([("user_id", 1), ("created_at", -1)])
    await db.deposits.create_index([("status", 1), ("created_at", -1)])

    # referrals
    await db.referrals.create_index([("referred_user_id", 1)], unique=True)
    await db.referrals.create_index([("referrer_user_id", 1), ("created_at", -1)])

    # referral earnings log (optional, for audit)
    await db.ref_earn_logs.create_index([("referrer_user_id", 1), ("created_at", -1)])
    await db.ref_earn_logs.create_index([("referred_user_id", 1), ("created_at", -1)])
    await db.ref_earn_logs.create_index([("deposit_id", 1)], unique=True, sparse=True)

    # legacy tokens (kept for compatibility; no longer used in UI)
    await db.ref_tokens.create_index([("user_id", 1)], unique=True)

    # bans
    await db.banned_users.create_index([("user_id", 1)], unique=True)

    # qr settings
    await db.qr_settings.create_index([("key", 1)], unique=True)

    # admin settings (bulk discount etc.)
    await db.admin_settings.create_index([("key", 1)], unique=True)


class Repo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    # -------- Admin Settings (Bulk Discount) --------
    async def get_bulk_discount(self) -> dict[str, Any]:
        """Return {'enabled': bool, 'percent': int}."""
        doc = await self.db.admin_settings.find_one({"key": "bulk_discount"})
        if not doc:
            return {"enabled": False, "percent": 0}
        return {"enabled": bool(doc.get("enabled", False)), "percent": int(doc.get("percent", 0) or 0)}

    async def set_bulk_discount(self, *, enabled: bool, percent: int) -> dict[str, Any]:
        percent_i = int(percent)
        await self.db.admin_settings.update_one(
            {"key": "bulk_discount"},
            {"$set": {"key": "bulk_discount", "enabled": bool(enabled), "percent": percent_i, "updated_at": utcnow()}},
            upsert=True,
        )
        return await self.get_bulk_discount()

    async def apply_bulk_discount(self, *, percent: int) -> dict[str, Any]:
        """Apply discount percent to ALL AVAILABLE accounts.

        Stores original base price in 'base_price' (only once) so it can be reset.
        """
        p = max(0, min(95, int(percent)))

        # Ensure base_price is saved (only if missing)
        await self.db.accounts.update_many(
            {"status": "available", "price": {"$ne": None}, "base_price": {"$exists": False}},
            [{"$set": {"base_price": "$price"}}],
        )

        # Apply discount based on base_price
        await self.db.accounts.update_many(
            {"status": "available", "base_price": {"$ne": None}},
            [
                {
                    "$set": {
                        "price": {
                            "$toInt": {
                                "$round": [
                                    {
                                        "$multiply": [
                                            "$base_price",
                                            {"$divide": [{"$subtract": [100, p]}, 100]},
                                        ]
                                    },
                                    0,
                                ]
                            }
                        },
                        "updated_at": "$$NOW",
                    }
                }
            ],
        )

        return await self.set_bulk_discount(enabled=True, percent=p)

    async def disable_bulk_discount(self) -> dict[str, Any]:
        """Disable discount and restore price from base_price (keeps base_price)."""
        await self.db.accounts.update_many(
            {"status": "available", "base_price": {"$ne": None}},
            [{"$set": {"price": "$base_price", "updated_at": "$$NOW"}}],
        )
        st = await self.get_bulk_discount()
        return await self.set_bulk_discount(enabled=False, percent=int(st.get("percent", 0) or 0))

    async def reset_bulk_discount(self) -> dict[str, Any]:
        """Reset to normal (restore price and remove base_price field)."""
        await self.db.accounts.update_many(
            {"status": "available", "base_price": {"$exists": True}},
            [{"$set": {"price": "$base_price", "updated_at": "$$NOW"}}],
        )
        await self.db.accounts.update_many(
            {"status": "available", "base_price": {"$exists": True}},
            {"$unset": {"base_price": ""}},
        )
        return await self.set_bulk_discount(enabled=False, percent=0)

    # -------- Payment toggles --------
    async def get_crypto_enabled(self) -> bool:
        doc = await self.db.admin_settings.find_one({"key": "crypto_enabled"})
        if not doc:
            return True
        return bool(doc.get("enabled", True))

    async def set_crypto_enabled(self, *, enabled: bool) -> bool:
        await self.db.admin_settings.update_one(
            {"key": "crypto_enabled"},
            {"$set": {"key": "crypto_enabled", "enabled": bool(enabled), "updated_at": utcnow()}},
            upsert=True,
        )
        return await self.get_crypto_enabled()

    # ----------------------------
    # Referral / earnings
    # ----------------------------
    async def is_new_user(self, user_id: int) -> bool:
        doc = await self.db.users.find_one({"user_id": int(user_id)})
        return doc is None

    async def save_referral_if_new(
        self,
        *,
        referred_user_id: int,
        referred_username: str | None,
        referrer_user_id: int,
        referrer_username: str | None,
    ) -> bool:
        """Save referral for a NEW user. Returns True if saved."""
        if referred_user_id == referrer_user_id:
            return False

        # only for new users
        if not await self.is_new_user(referred_user_id):
            return False

        now = utcnow()
        try:
            await self.db.referrals.insert_one(
                {
                    "referrer_user_id": int(referrer_user_id),
                    "referrer_username": (referrer_username or ""),
                    "referred_user_id": int(referred_user_id),
                    "referred_username": (referred_username or ""),
                    "created_at": now,
                    "credited": False,
                }
            )
        except Exception:
            return False

        # store on user document too
        await self.db.users.update_one(
            {"user_id": int(referred_user_id)},
            {
                "$setOnInsert": {"user_id": int(referred_user_id), "credits": 0, "created_at": now},
                "$set": {
                    "referrer_user_id": int(referrer_user_id),
                    "referrer_username": (referrer_username or ""),
                    "referrer_set_at": now,
                    "updated_at": now,
                },
            },
            upsert=True,
        )
        return True

    async def get_tokens(self, user_id: int) -> int:
        # legacy (discount tokens) - no longer used by referral UI
        doc = await self.db.ref_tokens.find_one({"user_id": int(user_id)})
        return int((doc or {}).get("tokens", 0))

    async def get_referral_stats(self, user_id: int) -> dict[str, Any]:
        """Return {'referrals': int, 'total_earned': float} for a referrer."""
        referrals = await self.db.referrals.count_documents({"referrer_user_id": int(user_id)})
        u = await self.db.users.find_one({"user_id": int(user_id)})
        total_earned = float((u or {}).get("ref_earned_total", 0.0) or 0.0)
        return {"referrals": int(referrals), "total_earned": float(total_earned)}

    async def add_referral_earning(
        self,
        *,
        referrer_user_id: int,
        referred_user_id: int,
        amount: float,
        by_admin: int,
        deposit_id: str | None = None,
        deposit_amount: int | None = None,
    ) -> dict[str, Any]:
        """Credit referral earning to referrer (adds to credits + tracks total earned).

        Returns updated user doc for referrer.
        """
        now = utcnow()
        amt_i = int(amount)
        # add to credits as integer credits, and track total earned as float
        user = await self.db.users.find_one_and_update(
            {"user_id": int(referrer_user_id)},
            {
                "$inc": {"credits": amt_i, "ref_earned_total": float(amount)},
                "$setOnInsert": {"user_id": int(referrer_user_id), "created_at": now},
                "$set": {"updated_at": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

        # best-effort audit log
        try:
            await self.db.ref_earn_logs.insert_one(
                {
                    "referrer_user_id": int(referrer_user_id),
                    "referred_user_id": int(referred_user_id),
                    "amount": float(amount),
                    "amount_int": amt_i,
                    "deposit_id": deposit_id,
                    "deposit_amount": int(deposit_amount) if deposit_amount is not None else None,
                    "by_admin": int(by_admin),
                    "created_at": now,
                }
            )
        except Exception:
            pass

        return user or {}

    async def _reserve_token(self, user_id: int) -> bool:
        """Reserve one token for a purchase (decrement immediately)."""
        doc = await self.db.ref_tokens.find_one_and_update(
            {"user_id": int(user_id), "tokens": {"$gte": 1}},
            {"$inc": {"tokens": -1}, "$set": {"updated_at": utcnow()}},
            return_document=ReturnDocument.AFTER,
        )
        return doc is not None

    async def _release_token(self, user_id: int) -> None:
        """Rollback a reserved token."""
        await self.db.ref_tokens.update_one(
            {"user_id": int(user_id)},
            {"$inc": {"tokens": 1}, "$set": {"updated_at": utcnow()}},
            upsert=True,
        )

    async def add_tokens(self, user_id: int, delta: int) -> int:
        now = utcnow()
        doc = await self.db.ref_tokens.find_one_and_update(
            {"user_id": int(user_id)},
            {"$inc": {"tokens": int(delta)}, "$set": {"updated_at": now}, "$setOnInsert": {"user_id": int(user_id)}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int((doc or {}).get("tokens", 0))

    async def set_tokens(self, user_id: int, tokens: int) -> int:
        now = utcnow()
        doc = await self.db.ref_tokens.find_one_and_update(
            {"user_id": int(user_id)},
            {"$set": {"tokens": int(tokens), "updated_at": now, "user_id": int(user_id)}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int((doc or {}).get("tokens", 0))

    # ----------------------------
    # Bans
    # ----------------------------
    async def is_banned(self, user_id: int) -> bool:
        doc = await self.db.banned_users.find_one({"user_id": int(user_id)})
        return doc is not None

    async def ban_user(self, *, user_id: int, by_admin: int, username: str | None = None) -> None:
        await self.db.banned_users.update_one(
            {"user_id": int(user_id)},
            {
                "$set": {
                    "user_id": int(user_id),
                    "username": (username or ""),
                    "by_admin": int(by_admin),
                    "created_at": utcnow(),
                }
            },
            upsert=True,
        )

    async def unban_user(self, *, user_id: int) -> bool:
        res = await self.db.banned_users.delete_one({"user_id": int(user_id)})
        return res.deleted_count == 1

    # ----------------------------
    # QR settings
    # ----------------------------
    async def get_inr_qr_flags(self) -> dict[str, bool]:
        """Return {'qr1': bool, 'qr2': bool}. Defaults to both True."""
        doc = await self.db.qr_settings.find_one({"key": "inr"})
        if not doc:
            return {"qr1": True, "qr2": True}
        return {
            "qr1": bool(doc.get("qr1", True)),
            "qr2": bool(doc.get("qr2", True)),
        }

    async def set_inr_qr_flag(self, *, qr_key: str, enabled: bool) -> dict[str, bool]:
        if qr_key not in {"qr1", "qr2"}:
            return await self.get_inr_qr_flags()
        await self.db.qr_settings.update_one(
            {"key": "inr"},
            {"$set": {"key": "inr", qr_key: bool(enabled), "updated_at": utcnow()}},
            upsert=True,
        )
        return await self.get_inr_qr_flags()

    async def try_credit_referral_on_first_approved_deposit(self, *, referred_user_id: int) -> dict[str, Any] | None:
        """If referred_user has a referral and this is their first approved deposit, grant +1 token.

        Returns info dict if credited, else None.
        """
        ref = await self.db.referrals.find_one({"referred_user_id": int(referred_user_id)})
        if not ref or ref.get("credited") is True:
            return None

        approved_count = await self.db.deposits.count_documents({"user_id": int(referred_user_id), "status": "approved"})
        if approved_count != 1:
            return None

        # mark credited atomically
        res = await self.db.referrals.update_one(
            {"_id": ref.get("_id"), "credited": False},
            {"$set": {"credited": True, "credited_at": utcnow()}},
        )
        if res.modified_count != 1:
            return None

        referrer_id = int(ref.get("referrer_user_id"))
        new_tokens = await self.add_tokens(referrer_id, 1)
        return {
            "referrer_user_id": referrer_id,
            "referrer_username": (ref.get("referrer_username") or ""),
            "referred_user_id": int(ref.get("referred_user_id")),
            "referred_username": (ref.get("referred_username") or ""),
            "tokens_now": new_tokens,
        }

    # -------- Deposits --------
    async def create_deposit_request(
        self,
        *,
        user_id: int,
        username: str,
        amount: int,
        method: str,
        network: str | None = None,
        amount_text: str | None = None,
    ) -> str:
        now = utcnow()
        res = await self.db.deposits.insert_one(
            {
                "user_id": int(user_id),
                "username": username,
                "amount": int(amount),
                "amount_text": amount_text,
                "method": method,  # inr|crypto
                "network": network,  # trc20|bep20|sol|ton|binance
                "credits_added": None,
                "status": "pending",  # pending|approved|rejected
                # Screenshot info (Telegram file_id). Stored so admin can fetch/resend later.
                "screenshot": None,  # {kind: photo|document, file_id: str}
                # Delivery attempts to admins (best-effort). Stored for diagnostics.
                "admin_notify": [],  # list[{admin_id:int, ok:bool, error:str|None, at:datetime}]
                "created_at": now,
                "updated_at": now,
            }
        )
        return str(res.inserted_id)

    async def get_deposit(self, deposit_id: str) -> Optional[dict[str, Any]]:
        try:
            oid = ObjectId(deposit_id)
        except Exception:
            return None
        return await self.db.deposits.find_one({"_id": oid})

    async def mark_deposit(
        self, deposit_id: str, status: str, *, admin_id: int, credits_added: int | None = None
    ) -> Optional[dict[str, Any]]:
        try:
            oid = ObjectId(deposit_id)
        except Exception:
            return None
        now = utcnow()
        set_doc: dict[str, Any] = {"status": status, "admin_id": int(admin_id), "updated_at": now}
        if credits_added is not None:
            set_doc["credits_added"] = int(credits_added)
        return await self.db.deposits.find_one_and_update(
            {"_id": oid, "status": "pending"},
            {"$set": set_doc},
            return_document=ReturnDocument.AFTER,
        )

    async def attach_deposit_screenshot(self, deposit_id: str, *, kind: str, file_id: str) -> bool:
        """Persist screenshot file reference on a deposit. Returns True if updated."""
        try:
            oid = ObjectId(deposit_id)
        except Exception:
            return False
        now = utcnow()
        res = await self.db.deposits.update_one(
            {"_id": oid},
            {"$set": {"screenshot": {"kind": kind, "file_id": str(file_id)}, "updated_at": now}},
        )
        return res.modified_count == 1

    async def add_deposit_admin_notify(
        self, deposit_id: str, *, admin_id: int, ok: bool, error: str | None
    ) -> None:
        """Append an admin notification result for diagnostics."""
        try:
            oid = ObjectId(deposit_id)
        except Exception:
            return
        now = utcnow()
        await self.db.deposits.update_one(
            {"_id": oid},
            {
                "$push": {
                    "admin_notify": {
                        "admin_id": int(admin_id),
                        "ok": bool(ok),
                        "error": (str(error) if error else None),
                        "at": now,
                    }
                },
                "$set": {"updated_at": now},
            },
        )

    async def deposit_totals(self) -> dict[str, Any]:
        """Totals for pending/approved/rejected + overall."""
        pipeline = [
            {"$group": {"_id": "$status", "count": {"$sum": 1}, "amount": {"$sum": "$amount"}}}
        ]
        out = {
            "pending_count": 0,
            "pending_amount": 0,
            "approved_count": 0,
            "approved_amount": 0,
            "rejected_count": 0,
            "rejected_amount": 0,
        }
        async for row in self.db.deposits.aggregate(pipeline):
            status = row.get("_id")
            if status not in {"pending", "approved", "rejected"}:
                continue
            out[f"{status}_count"] = int(row.get("count", 0))
            out[f"{status}_amount"] = int(row.get("amount", 0))
        return out

    async def list_deposits_page(self, *, status: str | None, page: int, page_size: int = 8) -> list[dict[str, Any]]:
        q: dict[str, Any] = {}
        if status:
            q["status"] = status
        cur = (
            self.db.deposits.find(q)
            .sort("created_at", -1)
            .skip(max(0, int(page)) * int(page_size))
            .limit(int(page_size))
        )
        return [x async for x in cur]

    async def count_deposits(self, *, status: str | None) -> int:
        q: dict[str, Any] = {}
        if status:
            q["status"] = status
        return await self.db.deposits.count_documents(q)

    async def list_deposits_for_user(self, user_id: int, *, limit: int = 20) -> list[dict[str, Any]]:
        cur = self.db.deposits.find({"user_id": int(user_id)}).sort("created_at", -1).limit(int(limit))
        return [x async for x in cur]


    # -------- Users --------
    async def ensure_user(self, user_id: int, *, username: str | None = None) -> dict[str, Any]:
        now = utcnow()
        set_doc: dict[str, Any] = {"updated_at": now}
        if username is not None:
            set_doc["username"] = str(username)

        await self.db.users.update_one(
            {"user_id": int(user_id)},
            {
                "$setOnInsert": {"user_id": int(user_id), "credits": 0, "created_at": now},
                "$set": set_doc,
            },
            upsert=True,
        )
        return await self.db.users.find_one({"user_id": int(user_id)})

    async def add_credits(self, user_id: int, amount: int, *, by_admin: int) -> dict[str, Any]:
        now = utcnow()
        await self.ensure_user(user_id)
        user = await self.db.users.find_one_and_update(
            {"user_id": int(user_id)},
            {"$inc": {"credits": int(amount)}, "$set": {"updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        await self.db.credit_logs.insert_one(
            {"user_id": int(user_id), "amount": int(amount), "by_admin": int(by_admin), "created_at": now}
        )
        return user

    async def set_credits(self, user_id: int, credits: int, *, by_admin: int) -> dict[str, Any]:
        now = utcnow()
        await self.ensure_user(user_id)
        user = await self.db.users.find_one_and_update(
            {"user_id": int(user_id)},
            {"$set": {"credits": int(credits), "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        await self.db.credit_logs.insert_one(
            {
                "user_id": int(user_id),
                "amount": int(credits),
                "by_admin": int(by_admin),
                "mode": "set",
                "created_at": now,
            }
        )
        return user

    # -------- Accounts --------
    async def create_account(
        self,
        *,
        phone: str,
        api_id: int,
        api_hash: str,
        session_string: str,
        added_by: int,
        year: int | str | None = None,
        premium_months: int | None = None,
        country: str | None = None,
        country_emoji: str | None = None,
        twofa_password: str | None = None,
        price: int | None = None,
    ) -> ObjectId:
        now = utcnow()
        doc = {
            "phone": str(phone),
            "api_id": int(api_id),
            "api_hash": str(api_hash),
            "session_string": str(session_string),
            "twofa_password": twofa_password,
            "country": country,
            "country_emoji": country_emoji,
            "year": int(year) if isinstance(year, int) else (str(year) if year is not None else None),
            "premium_months": int(premium_months) if premium_months is not None else None,
            "price": int(price) if price is not None else None,
            "status": "available",  # available|assigned
            "assigned_to": None,
            "assigned_at": None,
            "created_at": now,
            "updated_at": now,
            "added_by": int(added_by),
        }
        res = await self.db.accounts.insert_one(doc)
        return res.inserted_id

    async def list_accounts(self, limit: int = 20) -> list[dict[str, Any]]:
        cur = self.db.accounts.find({}).sort("created_at", -1).limit(int(limit))
        return [x async for x in cur]

    async def count_accounts(self, *, status: str | None = None) -> int:
        q: dict[str, Any] = {}
        if status is not None:
            q["status"] = status
        return await self.db.accounts.count_documents(q)

    async def list_accounts_page(
        self, *, status: str | None = None, page: int, page_size: int = 5
    ) -> list[dict[str, Any]]:
        q: dict[str, Any] = {}
        if status is not None:
            q["status"] = status
        cur = (
            self.db.accounts.find(q)
            .sort("created_at", -1)
            .skip(max(0, int(page)) * int(page_size))
            .limit(int(page_size))
        )
        return [x async for x in cur]

    async def get_account(self, account_id: ObjectId) -> Optional[dict[str, Any]]:
        return await self.db.accounts.find_one({"_id": account_id})

    async def delete_account(self, account_id: ObjectId) -> bool:
        res = await self.db.accounts.delete_one({"_id": account_id})
        return res.deleted_count == 1

    async def update_account_fields(self, account_id: ObjectId, fields: dict[str, Any]) -> bool:
        if not fields:
            return False
        fields = dict(fields)
        fields["updated_at"] = utcnow()
        res = await self.db.accounts.update_one({"_id": account_id}, {"$set": fields})
        return res.modified_count == 1

    async def count_available_accounts(self) -> int:
        return await self.db.accounts.count_documents({"status": "available"})

    async def list_available_countries(self) -> list[dict[str, Any]]:
        pipeline = [
            {"$match": {"status": "available"}},
            {"$group": {"_id": {"country": "$country", "emoji": "$country_emoji"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        out: list[dict[str, Any]] = []
        async for row in self.db.accounts.aggregate(pipeline):
            out.append(
                {
                    "country": (row.get("_id") or {}).get("country"),
                    "country_emoji": (row.get("_id") or {}).get("emoji"),
                    "count": row.get("count", 0),
                }
            )
        return out

    async def list_available_years_for_country(self, country: str) -> list[dict[str, Any]]:
        pipeline = [
            {"$match": {"status": "available", "country": country}},
            {"$group": {"_id": "$year", "count": {"$sum": 1}}},
            {"$sort": {"_id": -1}},
        ]
        out: list[dict[str, Any]] = []
        async for row in self.db.accounts.aggregate(pipeline):
            out.append({"year": row.get("_id"), "count": row.get("count", 0)})
        return out

    async def available_price_range(self, *, country: str, year: Any) -> dict[str, Any]:
        """Return {min_price, max_price, count} for available accounts in selection."""
        match: dict[str, Any] = {"status": "available", "country": country, "price": {"$ne": None}}
        if year is not None:
            match["year"] = year

        pipeline = [
            {"$match": match},
            {
                "$group": {
                    "_id": None,
                    "min_price": {"$min": "$price"},
                    "max_price": {"$max": "$price"},
                    "count": {"$sum": 1},
                }
            },
        ]

        row = None
        async for r in self.db.accounts.aggregate(pipeline):
            row = r
            break

        if not row:
            return {"min_price": None, "max_price": None, "count": 0}

        return {
            "min_price": row.get("min_price"),
            "max_price": row.get("max_price"),
            "count": row.get("count", 0),
        }

    async def list_purchases_page(self, *, user_id: int, page: int, page_size: int = 6) -> list[dict[str, Any]]:
        cur = (
            self.db.purchases.find({"user_id": int(user_id)})
            .sort("created_at", -1)
            .skip(max(0, int(page)) * int(page_size))
            .limit(int(page_size))
        )
        return [x async for x in cur]

    async def count_purchases(self, *, user_id: int) -> int:
        return await self.db.purchases.count_documents({"user_id": int(user_id)})

    async def buy_account_filtered(
        self, *, user_id: int, username: str | None, country: str, year: Any
    ) -> tuple[Optional[dict[str, Any]], str]:
        """Buy one account for (country, year) that the user can afford.

        This intentionally picks the cheapest available account within the user's budget.
        If the user has discount tokens, it will first try using a token (allows buying
        up to credits+5 because charge = price-5).
        """
        now = utcnow()
        user = await self.ensure_user(user_id)
        credits = int((user or {}).get("credits", 0) or 0)

        base_q: dict[str, Any] = {"status": "available", "country": country, "price": {"$ne": None}}
        if year is not None:
            base_q["year"] = year

        # Try with token first (if available), then without token.
        for want_token in (True, False):
            token_used = False
            max_price = credits

            if want_token:
                token_used = await self._reserve_token(user_id)
                if not token_used:
                    continue
                max_price = credits + 5

            q = dict(base_q)
            q["price"] = {"$ne": None, "$lte": int(max_price)}

            account = await self.db.accounts.find_one_and_update(
                q,
                {
                    "$set": {
                        "status": "assigned",
                        "assigned_to": int(user_id),
                        "sold_to_user_id": int(user_id),
                        "sold_to_username": (username or ""),
                        "assigned_at": now,
                        "updated_at": now,
                    }
                },
                # cheapest first
                sort=[("price", 1), ("created_at", 1)],
                return_document=ReturnDocument.AFTER,
            )

            if not account:
                if token_used:
                    await self._release_token(user_id)
                continue

            original_price = int(account.get("price") or 0)
            charge = max(0, original_price - 5) if token_used else original_price

            dec = await self.db.users.update_one(
                {"user_id": int(user_id), "credits": {"$gte": int(charge)}},
                {"$inc": {"credits": -int(charge)}, "$set": {"updated_at": now}},
            )
            if dec.modified_count != 1:
                if token_used:
                    await self._release_token(user_id)
                await self.db.accounts.update_one(
                    {"_id": account["_id"]},
                    {"$set": {"status": "available", "assigned_to": None, "assigned_at": None, "updated_at": now}},
                )
                return None, "insufficient_credits"

            account["_original_price"] = original_price
            account["_final_price"] = charge
            account["_discount_used"] = token_used

            await self.db.purchases.insert_one(
                {
                    "user_id": int(user_id),
                    "account_id": account["_id"],
                    "price": int(charge),
                    "original_price": int(original_price),
                    "discount_used": bool(token_used),
                    "phone": account.get("phone"),
                    "country": account.get("country"),
                    "year": account.get("year"),
                    "created_at": now,
                }
            )
            return account, "ok"

        return None, "no_affordable"

    async def count_available_under_price(self, *, max_price: int) -> int:
        return await self.db.accounts.count_documents(
            {"status": "available", "price": {"$ne": None, "$lte": int(max_price)}}
        )

    async def count_groups_under_price(self, *, max_price: int) -> int:
        pipeline = [
            {"$match": {"status": "available", "price": {"$ne": None, "$lte": int(max_price)}}},
            {
                "$group": {
                    "_id": {
                        "country": "$country",
                        "country_emoji": "$country_emoji",
                        "year": "$year",
                        "price": "$price",
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$count": "n"},
        ]
        row = None
        async for r in self.db.accounts.aggregate(pipeline):
            row = r
            break
        return int((row or {}).get("n", 0))

    async def list_groups_under_price_page(
        self, *, max_price: int, page: int, page_size: int = 10
    ) -> list[dict[str, Any]]:
        pipeline = [
            {"$match": {"status": "available", "price": {"$ne": None, "$lte": int(max_price)}}},
            {
                "$group": {
                    "_id": {
                        "country": "$country",
                        "country_emoji": "$country_emoji",
                        "year": "$year",
                        "price": "$price",
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id.price": 1, "_id.country": 1, "_id.year": -1}},
            {"$skip": max(0, int(page)) * int(page_size)},
            {"$limit": int(page_size)},
        ]
        out: list[dict[str, Any]] = []
        async for row in self.db.accounts.aggregate(pipeline):
            _id = row.get("_id") or {}
            out.append(
                {
                    "country": _id.get("country"),
                    "country_emoji": _id.get("country_emoji"),
                    "year": _id.get("year"),
                    "premium_months": _id.get("premium_months"),
                    "price": _id.get("price"),
                    "count": row.get("count", 0),
                }
            )
        return out

    async def buy_account_by_group(
        self,
        *,
        user_id: int,
        username: str | None,
        country: str,
        year: Any,
        price: int,
    ) -> tuple[Optional[dict[str, Any]], str]:
        """Buy one available account from a (country, year, price) group."""
        now = utcnow()
        await self.ensure_user(user_id)

        q: dict[str, Any] = {
            "status": "available",
            "country": country,
            "price": int(price),
        }
        if year is None:
            q["year"] = None
        else:
            q["year"] = year

        account = await self.db.accounts.find_one_and_update(
            q,
            {
                "$set": {
                    "status": "assigned",
                    "assigned_to": int(user_id),
                    "sold_to_user_id": int(user_id),
                    "sold_to_username": (username or ""),
                    "assigned_at": now,
                    "updated_at": now,
                }
            },
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not account:
            return None, "not_available"

        original_price = int(account.get("price") or 0)

        token_used = await self._reserve_token(user_id)
        charge = max(0, original_price - 5) if token_used else original_price

        dec = await self.db.users.update_one(
            {"user_id": int(user_id), "credits": {"$gte": int(charge)}},
            {"$inc": {"credits": -int(charge)}, "$set": {"updated_at": now}},
        )
        if dec.modified_count != 1:
            if token_used:
                await self._release_token(user_id)
            await self.db.accounts.update_one(
                {"_id": account["_id"]},
                {"$set": {"status": "available", "assigned_to": None, "assigned_at": None, "updated_at": now}},
            )
            return None, "insufficient_credits"

        account["_original_price"] = original_price
        account["_final_price"] = charge
        account["_discount_used"] = token_used

        await self.db.purchases.insert_one(
            {
                "user_id": int(user_id),
                "account_id": account["_id"],
                "price": int(charge),
                "original_price": int(original_price),
                "discount_used": bool(token_used),
                "phone": account.get("phone"),
                "country": account.get("country"),
                "year": account.get("year"),
                "created_at": now,
            }
        )
        return account, "ok"

    async def buy_account_by_id(
        self, *, user_id: int, username: str | None, account_id: str
    ) -> tuple[Optional[dict[str, Any]], str]:
        """Atomically assign a specific account id to user, then decrement credits."""
        now = utcnow()
        await self.ensure_user(user_id)
        try:
            oid = ObjectId(account_id)
        except Exception:
            return None, "invalid_id"

        account = await self.db.accounts.find_one_and_update(
            {"_id": oid, "status": "available"},
            {
                "$set": {
                    "status": "assigned",
                    "assigned_to": int(user_id),
                    "sold_to_user_id": int(user_id),
                    "sold_to_username": (username or ""),
                    "assigned_at": now,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if not account:
            return None, "not_available"

        if account.get("price") is None:
            await self.db.accounts.update_one(
                {"_id": oid},
                {"$set": {"status": "available", "assigned_to": None, "assigned_at": None, "updated_at": now}},
            )
            return None, "no_price"

        original_price = int(account["price"])
        token_used = await self._reserve_token(user_id)
        charge = max(0, original_price - 5) if token_used else original_price

        dec = await self.db.users.update_one(
            {"user_id": int(user_id), "credits": {"$gte": int(charge)}},
            {"$inc": {"credits": -int(charge)}, "$set": {"updated_at": now}},
        )
        if dec.modified_count != 1:
            if token_used:
                await self._release_token(user_id)

            await self.db.accounts.update_one(
                {"_id": oid},
                {"$set": {"status": "available", "assigned_to": None, "assigned_at": None, "updated_at": now}},
            )
            return None, "insufficient_credits"

        # annotate returned account with discount info for UI
        account["_original_price"] = original_price
        account["_final_price"] = charge
        account["_discount_used"] = token_used

        await self.db.purchases.insert_one(
            {
                "user_id": int(user_id),
                "account_id": oid,
                "price": int(charge),
                "original_price": int(original_price),
                "discount_used": bool(token_used),
                "phone": account.get("phone"),
                "country": account.get("country"),
                "year": account.get("year"),
                "created_at": now,
            }
        )
        return account, "ok"
