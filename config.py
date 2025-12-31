import os
from typing import Dict, List, Optional, TypedDict

# Telegram Bot token
BOT_TOKEN = os.getenv("BOT_TOKEN", "8486930233:AAFgfpSxCxeE1vUZ4L1WkK-vx83OtKTaYHY")

# Mongo
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://aztech:ayazahmed1122@cluster0.mhuaw3q.mongodb.net/idstore_db?retryWrites=true&w=majority",
)
DB_NAME = os.getenv("DB_NAME", "idstore_db")

# Admin Telegram user IDs (comma-separated)
ADMIN_USER_IDS: List[int] = [
    int(x)
    for x in os.getenv("ADMIN_USER_IDS", "6670166083,6193229528").split(",")
    if x.strip().isdigit()
]

# Start screen image
START_IMAGE = "https://i.postimg.cc/zD73Wn61/photo-2025-12-28-18-55-27.jpg"

# Bot username (without @) for referral links
BOT_USERNAME = os.getenv("BOT_USERNAME", "DreamAccountbot")

# Channel join requirement
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "DreamAccountSup")  # without @

# Report channel (bot must be admin there). Without @
REPORT_CHANNEL_USERNAME = os.getenv("REPORT_CHANNEL_USERNAME", "dreamAccreport")

# Fixed Telegram API credentials used for adding accounts (admin flow)
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "31538472"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "fa66f05927f292f94ce8d0034ca57242")


# ----------------------------
# Payment configuration


class PaymentMethodCfg(TypedDict, total=False):
    # Human label shown to user
    label: str

    # For INR: UPI + payee name
    upi_id: str
    payee_name: str
    notes: str

    # Optional: direct image URL (preferred)
    image_url: str



class CryptoNetworkCfg(TypedDict, total=False):
    label: str
    address: str
    image_url: str


# INR payment (UPI) - two QR options
INR_QRS: Dict[str, PaymentMethodCfg] = {
    "qr1": {
        "label": "INR QR 1",
        "payee_name": "HRIDAY NARAIN JAISWAL",
        "upi_id": "BHARATPE09914513765@yesbankltd",
        "notes": "REGARDS :- @BrutodGIFT\n\nCHECK USERNAME BEFORE DEAL",
        "image_url": "https://i.postimg.cc/FsDM6Sw-T/inr.jpg",
    },
    "qr2": {
        "label": "INR QR 2",
        "payee_name": "MANSI JAISWAL",
        "upi_id": "paytmqr13l2j6z2w1@paytm",
        "notes": "REGARDS :- @BrutodGIFT\n\nCHECK USERNAME BEFORE DEAL",
        "image_url": "https://i.postimg.cc/JnLgPsf3/inr2.jpg",
    },
}

# Backwards compatibility (some code may still reference INR_PAYMENT)
INR_PAYMENT: PaymentMethodCfg = INR_QRS["qr1"]


# Crypto networks
CRYPTO_NETWORKS: Dict[str, CryptoNetworkCfg] = {
    # key must match bot callback data (dep:net:<key>)
    "trc20": {
        "label": "TRC20",
        "address": "TBfXjQ6MXiYSzT4bASdzkW8nPJKUuD3kdw",
        "image_url": "https://i.postimg.cc/V6XyZVcG/trc20.jpg",
    },
    "bep20": {
        "label": "BEP20",
        "address": "0xf481b60dd4500db39c47465c2080bf5539f8352a",
        "image_url": "https://i.postimg.cc/BnwGjFgj/bep20.jpg",
    },
    "sol": {
        "label": "SOL",
        "address": "EXr3M7Ffh7kr9msyazywvgFTrie5ikfsgjaZ8Jqy8kMQ",
        "image_url": "https://i.postimg.cc/qB801zJB/sol.jpg",
    },
    "ton": {
        "label": "TON",
        "address": "UQCv2BteDQuu5BN4b6fnhNmgSVRB_Huglb4evOTmGdCMHV5j",
        "image_url": "https://i.postimg.cc/43qZScHR/ton.jpg",
    },
    "binance": {
        "label": "Binance ID",
        "address": "1182131729",
        "image_url": "https://i.postimg.cc/VL8sw-XGf/binance.jpg",
    },
}

