import logging
import re
import threading
import time
import random
import sys
import os
from datetime import datetime, timedelta
from bson import ObjectId
import asyncio

# Event loop — always create fresh one, auto-recreate if closed
def _ensure_event_loop():
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Loop closed")
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

asyncio.set_event_loop(asyncio.new_event_loop())
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
import telebot.types

@classmethod
def _disable_story(cls, obj):
    # Telegram stories completely ignored
    return None

telebot.types.Story.de_json = _disable_story
from pymongo import MongoClient
import os
import requests
from pyrogram import Client
from pyrogram.errors import (
    ApiIdInvalid, PhoneNumberInvalid, PhoneCodeInvalid,
    PhoneCodeExpired, SessionPasswordNeeded, PasswordHashInvalid,
    FloodWait, PhoneCodeEmpty
)

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

BOT_TOKEN = os.getenv('BOT_TOKEN', '')
ADMIN_ID = int(os.getenv('ADMIN_ID', '8358951104').split(',')[0].strip())
MONGO_URL = os.getenv('MONGO_URL', '')
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')

# UPI PAYMENT CONFIG
UPI_ID = os.getenv('UPI_ID', '')
QR_IMAGE_URL = os.getenv('QR_IMAGE_URL', '')

# MUST JOIN CHANNELS - TWO CHANNELS
MUST_JOIN_CHANNEL_1 = "@II_LEGEND_OTP_SELLER_UPDATES_II"
MUST_JOIN_CHANNEL_2 = "@Mystric_seller"
# LOG CHANNEL
LOG_CHANNEL_ID = "-1003659930873"

# Referral commission percentage
REFERRAL_COMMISSION = 1.7

# Primary API Credentials for Pyrogram Login
GLOBAL_API_ID = 37242432
GLOBAL_API_HASH = "d481340f928d3072a2ec01a7b6b597e0"

# Backup API Credentials
SERVER2_API_ID = 37751241
SERVER2_API_HASH = "2e90f273e745d4c080fdfab24fa98494"

# Successfully Purchase Group Link
PURCHASE_SUCCESS_LINK = "https://t.me/+QXhmkIm6m0YzMDI0"

# ---------------------------------------------------------------------
# INIT
# ---------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(BOT_TOKEN)

# Gemini AI Setup — using new google-genai SDK
try:
    from google import genai as _genai
    from google.genai import types as _genai_types
    GEMINI_MODEL_NAME = "gemini-2.0-flash"
    GEMINI_FALLBACK_MODELS = ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-flash-8b"]
    gemini_model = True  # flag: initialized
    _genai_client = None  # initialized lazily using get_ai_key()
    logger.info(f"✅ Gemini AI ready (model: {GEMINI_MODEL_NAME})")
except Exception as _ge:
    _genai_client = None
    gemini_model = None
    logger.error(f"❌ Gemini init failed: {_ge}")

# Gemini chat history per user  (list of {"role": ..., "parts": [...]})
gemini_chat_sessions = {}

def get_ai_key():
    """Return current Gemini API key — checks MongoDB first, then falls back to hardcoded."""
    try:
        cfg = db['bot_config'].find_one({"key": "gemini_api_key"})
        if cfg and cfg.get("value"):
            return cfg["value"]
    except Exception:
        pass
    return GEMINI_API_KEY

def get_genai_client():
    """Get a fresh Gemini client with the latest API key."""
    try:
        return _genai.Client(api_key=get_ai_key())
    except Exception:
        return None

# MongoDB Setup — with retry and reconnect support for Railway
def _connect_mongo():
    global client, db, users_col, accounts_col, orders_col, wallets_col
    global recharges_col, otp_sessions_col, referrals_col, countries_col
    global banned_users_col, transactions_col, coupons_col, admins_col, privacy_warns_col
    try:
        client = MongoClient(
            MONGO_URL,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=30000,
            retryWrites=True,
            maxPoolSize=10,
        )
        client.admin.command('ping')  # Verify connection
        db = client['otp_bot']
        users_col = db['users']
        accounts_col = db['accounts']
        orders_col = db['orders']
        wallets_col = db['wallets']
        recharges_col = db['recharges']
        otp_sessions_col = db['otp_sessions']
        referrals_col = db['referrals']
        countries_col = db['countries']
        banned_users_col = db['banned_users']
        transactions_col = db['transactions']
        coupons_col = db['coupons']
        admins_col = db['admins']
        privacy_warns_col = db['privacy_warns']
        logger.info("✅ MongoDB connected successfully")
        return True
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        return False

_connect_mongo()

_BSON_MAX_BYTES = 16 * 1024 * 1024  # 16 MB MongoDB document limit

def check_doc_size(doc: dict, label: str = "document") -> bool:
    """Return True if document is within BSON 16 MB limit, False otherwise."""
    try:
        import bson
        size = len(bson.encode(doc))
        if size > _BSON_MAX_BYTES:
            logger.error(
                f"BSON size check FAILED for {label}: {size} bytes "
                f"(limit {_BSON_MAX_BYTES}). Insert skipped."
            )
            return False
        return True
    except Exception as _e:
        logger.warning(f"BSON size check error for {label}: {_e}")
        return True  # allow insert if size check itself fails

def safe_db_op(fn, *args, default=None, **kwargs):
    """Wrap any MongoDB call — auto-reconnect on connection error, never crash."""
    for _attempt in range(3):
        try:
            return fn(*args, **kwargs)
        except Exception as _e:
            err = str(_e).lower()
            if any(k in err for k in ("connection", "timeout", "network", "reset", "closed")):
                logger.warning(f"MongoDB connection issue, reconnecting... ({_e})")
                try:
                    _connect_mongo()
                except Exception:
                    pass
                time.sleep(1)
            elif any(k in err for k in ("bson", "document too large", "object size", "invaliddocument", "exceeds")):
                logger.error(f"MongoDB BSON size error (document too large): {_e}")
                return default
            else:
                logger.error(f"MongoDB op error: {_e}")
                return default
    return default

def safe_insert_one(col, doc: dict, label: str = "document"):
    """Safe insert_one — checks BSON size, catches all errors, never crashes."""
    try:
        import bson as _bson
        _size = len(_bson.encode(doc))
        if _size > _BSON_MAX_BYTES:
            logger.error(f"BSON insert BLOCKED for [{label}]: {_size} bytes > 16MB limit")
            return None
    except Exception as _se:
        logger.warning(f"BSON pre-check skipped for [{label}]: {_se}")
    try:
        return col.insert_one(doc)
    except Exception as _e:
        err = str(_e).lower()
        if any(k in err for k in ("bson", "document too large", "object size", "invaliddocument", "exceeds", "get_object_size")):
            logger.error(f"BSON size error on insert [{label}]: {_e}")
        elif any(k in err for k in ("connection", "timeout", "network")):
            logger.warning(f"DB connection error on insert [{label}]: {_e}")
        else:
            logger.error(f"DB insert error [{label}]: {_e}")
        return None

def safe_obj_id(val):
    """Safely convert any value to ObjectId — returns None on failure."""
    if val is None:
        return None
    if isinstance(val, ObjectId):
        return val
    try:
        return ObjectId(str(val))
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────────────────────
# 🧹 AI RESPONSE CLEANER — strip markdown/special chars Gemini adds
# ─────────────────────────────────────────────────────────────────────────────
import re as _re

def clean_ai_response(text):
    """Remove markdown formatting chars so response looks like plain Gemini chat."""
    if not text:
        return text
    # Remove bold/italic markdown
    text = _re.sub(r'\*{1,3}(.*?)\*{1,3}', r'\1', text, flags=_re.DOTALL)
    # Remove headings (### ## #)
    text = _re.sub(r'^#{1,6}\s+', '', text, flags=_re.MULTILINE)
    # Remove underline/strikethrough
    text = _re.sub(r'_{1,2}(.*?)_{1,2}', r'\1', text, flags=_re.DOTALL)
    text = _re.sub(r'~~(.*?)~~', r'\1', text, flags=_re.DOTALL)
    # Remove inline code backticks (keep content)
    text = _re.sub(r'`{1,3}(.*?)`{1,3}', r'\1', text, flags=_re.DOTALL)
    # Remove horizontal rules
    text = _re.sub(r'^[-*_]{3,}\s*$', '', text, flags=_re.MULTILINE)
    # Clean up multiple blank lines
    text = _re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

# ─────────────────────────────────────────────────────────────────────────────
# 🚨 PRIVACY WARN SYSTEM — warn users asking private bot info
# ─────────────────────────────────────────────────────────────────────────────

# Keywords that suggest someone is trying to extract private bot info
_PRIVACY_KEYWORDS = [
    "bot token", "api id", "api hash", "api key", "mongo", "database url",
    "admin id", "upi id", "secret", "source code", "bot ka code",
    "owner ka number", "owner kaun", "owner ka phone", "admin kaun hai",
    "admin ka number", "malik kaun", "bot banane wala", "bot ka malik",
    "bot creator", "owner number", "admin number", "server ip",
    "railway url", "webhook url", "gemini key", "openai key",
    "bot ki detail", "bot ki info", "private info", "config",
    "password kya hai", "database password", "mongo password",
]

def get_privacy_warn_count(user_id):
    try:
        doc = privacy_warns_col.find_one({"user_id": user_id})
        return doc.get("warns", 0) if doc else 0
    except Exception:
        return 0

def add_privacy_warn(user_id):
    """Add 1 warn. Returns new warn count."""
    try:
        privacy_warns_col.update_one(
            {"user_id": user_id},
            {"$inc": {"warns": 1}, "$set": {"updated_at": datetime.utcnow()}},
            upsert=True
        )
        return get_privacy_warn_count(user_id)
    except Exception:
        return 0

def remove_privacy_warn(user_id):
    """Remove all warns for user. Owner only."""
    try:
        privacy_warns_col.delete_one({"user_id": user_id})
        return True
    except Exception:
        return False

def is_privacy_question(text):
    """Returns True if user seems to be asking about private bot info."""
    t = text.lower()
    return any(kw in t for kw in _PRIVACY_KEYWORDS)

# ─────────────────────────────────────────────────────────────────────────────
# 🎬 ANIMATION SYSTEM — Telegram-native animated messages
# ─────────────────────────────────────────────────────────────────────────────

def _typing(chat_id):
    """Send 'typing' action silently."""
    try:
        bot.send_chat_action(chat_id, "typing")
    except:
        pass

def _upload_action(chat_id):
    """Send 'upload_document' action silently."""
    try:
        bot.send_chat_action(chat_id, "upload_document")
    except:
        pass

class AnimLoader:
    """
    Send an animated loading message that cycles through frames,
    then call .finish(text, markup) to replace it with the final content.
    Usage:
        anim = AnimLoader(chat_id, AnimLoader.PURCHASE_FRAMES)
        # ... do work ...
        anim.finish("✅ Done!", markup=markup)
    """

    LOADING_FRAMES = [
        "⠋ Loading...",
        "⠙ Loading...",
        "⠹ Loading...",
        "⠸ Loading...",
        "⠼ Loading...",
        "⠴ Loading...",
        "⠦ Loading...",
        "⠧ Loading...",
        "⠇ Loading...",
        "⠏ Loading...",
    ]

    PURCHASE_FRAMES = [
        "🔄 <b>Processing your order...</b>\n\n▱▱▱▱▱▱▱▱▱▱  0%",
        "🔄 <b>Verifying account...</b>\n\n▰▰▱▱▱▱▱▱▱▱  20%",
        "🔄 <b>Checking balance...</b>\n\n▰▰▰▰▱▱▱▱▱▱  40%",
        "🔄 <b>Securing session...</b>\n\n▰▰▰▰▰▰▱▱▱▱  60%",
        "🔄 <b>Activating account...</b>\n\n▰▰▰▰▰▰▰▰▱▱  80%",
        "✨ <b>Finalizing order...</b>\n\n▰▰▰▰▰▰▰▰▰▰  100%",
    ]

    FETCH_FRAMES = [
        "🔍 Fetching data...",
        "🔍 Fetching data..",
        "🔍 Fetching data.",
        "🔍 Please wait...",
    ]

    SCAN_FRAMES = [
        "📡 Scanning servers...",
        "📡 Scanning servers..",
        "📡 Scanning servers.",
        "📡 Checking stock...",
        "📡 Checking stock..",
        "📡 Checking stock.",
    ]

    RECHARGE_FRAMES = [
        "💳 <b>Submitting request...</b>\n\n⏳ Please wait...",
        "💳 <b>Verifying payment...</b>\n\n⏳ Processing...",
        "💳 <b>Almost done...</b>\n\n⏳ Finalizing...",
    ]

    PROFILE_FRAMES = [
        "🪪 Loading your profile...",
        "🪪 Loading your profile..",
        "🪪 Loading your profile.",
        "📊 Fetching stats...",
    ]

    HISTORY_FRAMES = [
        "🛒 Fetching your orders...",
        "🛒 Fetching your orders..",
        "🛒 Scanning database...",
    ]

    PRICE_FRAMES = [
        "📋 Loading live prices...",
        "📋 Checking stock...",
        "📋 Almost ready...",
    ]

    def __init__(self, chat_id, frames, interval=0.45, parse_mode=None):
        self.chat_id = chat_id
        self.frames = frames
        self.interval = interval
        self.parse_mode = parse_mode
        self._stop = threading.Event()
        self._msg_id = None
        self._thread = None
        try:
            kw = {}
            if parse_mode:
                kw["parse_mode"] = parse_mode
            m = bot.send_message(chat_id, frames[0], **kw)
            self._msg_id = m.message_id
        except:
            return
        self._start()

    def _start(self):
        def _run():
            i = 0
            while not self._stop.is_set():
                i = (i + 1) % len(self.frames)
                try:
                    kw = {}
                    if self.parse_mode:
                        kw["parse_mode"] = self.parse_mode
                    bot.edit_message_text(self.frames[i], self.chat_id, self._msg_id, **kw)
                except:
                    pass
                self._stop.wait(self.interval)
        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def finish(self, text, markup=None, parse_mode="HTML"):
        """Stop animation and replace with final message."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.5)
        try:
            bot.edit_message_text(
                text, self.chat_id, self._msg_id,
                parse_mode=parse_mode, reply_markup=markup
            )
            return self._msg_id
        except:
            try:
                m = bot.send_message(self.chat_id, text, parse_mode=parse_mode, reply_markup=markup)
                return m.message_id
            except:
                return self._msg_id

    def delete(self):
        """Stop animation and delete the message."""
        self._stop.set()
        try:
            bot.delete_message(self.chat_id, self._msg_id)
        except:
            pass


# ─────────────────────────────────────────────────────────────────────────────

# Store temporary data
user_states = {}
pending_messages = {}
active_chats = {}
user_stage = {}
user_last_message = {}
user_orders = {}
order_messages = {}
cancellation_trackers = {}
order_timers = {}
change_number_requests = {}
whatsapp_number_timers = {}
payment_orders = {}
admin_deduct_state = {}
referral_data = {}
broadcast_data = {}
edit_price_state = {}
coupon_state = {}
recharge_method_state = {}
upi_payment_states = {}
admin_add_state = {}  # For /addadmin flow
admin_remove_state = {}  # For /removeadmin flow

# add this line for bordcast 
IS_BROADCASTING = False

# Pyrogram login states
login_states = {}

# BULK ADD STATES
bulk_add_states = {}

# Recharge approval tracking
recharge_approvals = {}  # Track who approved/rejected which recharge

# ── 180+ WORLD COUNTRIES (flag, name, dial code) ─────────────────────
WORLD_COUNTRIES = [
    {"name":"Afghanistan","flag":"🇦🇫","code":"+93"},
    {"name":"Albania","flag":"🇦🇱","code":"+355"},
    {"name":"Algeria","flag":"🇩🇿","code":"+213"},
    {"name":"Andorra","flag":"🇦🇩","code":"+376"},
    {"name":"Angola","flag":"🇦🇴","code":"+244"},
    {"name":"Antigua and Barbuda","flag":"🇦🇬","code":"+1268"},
    {"name":"Argentina","flag":"🇦🇷","code":"+54"},
    {"name":"Armenia","flag":"🇦🇲","code":"+374"},
    {"name":"Australia","flag":"🇦🇺","code":"+61"},
    {"name":"Austria","flag":"🇦🇹","code":"+43"},
    {"name":"Azerbaijan","flag":"🇦🇿","code":"+994"},
    {"name":"Bahamas","flag":"🇧🇸","code":"+1242"},
    {"name":"Bahrain","flag":"🇧🇭","code":"+973"},
    {"name":"Bangladesh","flag":"🇧🇩","code":"+880"},
    {"name":"Barbados","flag":"🇧🇧","code":"+1246"},
    {"name":"Belarus","flag":"🇧🇾","code":"+375"},
    {"name":"Belgium","flag":"🇧🇪","code":"+32"},
    {"name":"Belize","flag":"🇧🇿","code":"+501"},
    {"name":"Benin","flag":"🇧🇯","code":"+229"},
    {"name":"Bhutan","flag":"🇧🇹","code":"+975"},
    {"name":"Bolivia","flag":"🇧🇴","code":"+591"},
    {"name":"Bosnia and Herzegovina","flag":"🇧🇦","code":"+387"},
    {"name":"Botswana","flag":"🇧🇼","code":"+267"},
    {"name":"Brazil","flag":"🇧🇷","code":"+55"},
    {"name":"Brunei","flag":"🇧🇳","code":"+673"},
    {"name":"Bulgaria","flag":"🇧🇬","code":"+359"},
    {"name":"Burkina Faso","flag":"🇧🇫","code":"+226"},
    {"name":"Burundi","flag":"🇧🇮","code":"+257"},
    {"name":"Cabo Verde","flag":"🇨🇻","code":"+238"},
    {"name":"Cambodia","flag":"🇰🇭","code":"+855"},
    {"name":"Cameroon","flag":"🇨🇲","code":"+237"},
    {"name":"Canada","flag":"🇨🇦","code":"+1"},
    {"name":"Central African Republic","flag":"🇨🇫","code":"+236"},
    {"name":"Chad","flag":"🇹🇩","code":"+235"},
    {"name":"Chile","flag":"🇨🇱","code":"+56"},
    {"name":"China","flag":"🇨🇳","code":"+86"},
    {"name":"Colombia","flag":"🇨🇴","code":"+57"},
    {"name":"Comoros","flag":"🇰🇲","code":"+269"},
    {"name":"Congo","flag":"🇨🇬","code":"+242"},
    {"name":"Congo DR","flag":"🇨🇩","code":"+243"},
    {"name":"Costa Rica","flag":"🇨🇷","code":"+506"},
    {"name":"Croatia","flag":"🇭🇷","code":"+385"},
    {"name":"Cuba","flag":"🇨🇺","code":"+53"},
    {"name":"Cyprus","flag":"🇨🇾","code":"+357"},
    {"name":"Czech Republic","flag":"🇨🇿","code":"+420"},
    {"name":"Denmark","flag":"🇩🇰","code":"+45"},
    {"name":"Djibouti","flag":"🇩🇯","code":"+253"},
    {"name":"Dominica","flag":"🇩🇲","code":"+1767"},
    {"name":"Dominican Republic","flag":"🇩🇴","code":"+1809"},
    {"name":"Ecuador","flag":"🇪🇨","code":"+593"},
    {"name":"Egypt","flag":"🇪🇬","code":"+20"},
    {"name":"El Salvador","flag":"🇸🇻","code":"+503"},
    {"name":"Equatorial Guinea","flag":"🇬🇶","code":"+240"},
    {"name":"Eritrea","flag":"🇪🇷","code":"+291"},
    {"name":"Estonia","flag":"🇪🇪","code":"+372"},
    {"name":"Eswatini","flag":"🇸🇿","code":"+268"},
    {"name":"Ethiopia","flag":"🇪🇹","code":"+251"},
    {"name":"Fiji","flag":"🇫🇯","code":"+679"},
    {"name":"Finland","flag":"🇫🇮","code":"+358"},
    {"name":"France","flag":"🇫🇷","code":"+33"},
    {"name":"Gabon","flag":"🇬🇦","code":"+241"},
    {"name":"Gambia","flag":"🇬🇲","code":"+220"},
    {"name":"Georgia","flag":"🇬🇪","code":"+995"},
    {"name":"Germany","flag":"🇩🇪","code":"+49"},
    {"name":"Ghana","flag":"🇬🇭","code":"+233"},
    {"name":"Greece","flag":"🇬🇷","code":"+30"},
    {"name":"Greenland","flag":"🇬🇱","code":"+299"},
    {"name":"Grenada","flag":"🇬🇩","code":"+1473"},
    {"name":"Guatemala","flag":"🇬🇹","code":"+502"},
    {"name":"Guinea","flag":"🇬🇳","code":"+224"},
    {"name":"Guinea-Bissau","flag":"🇬🇼","code":"+245"},
    {"name":"Guyana","flag":"🇬🇾","code":"+592"},
    {"name":"Haiti","flag":"🇭🇹","code":"+509"},
    {"name":"Honduras","flag":"🇭🇳","code":"+504"},
    {"name":"Hong Kong","flag":"🇭🇰","code":"+852"},
    {"name":"Hungary","flag":"🇭🇺","code":"+36"},
    {"name":"Iceland","flag":"🇮🇸","code":"+354"},
    {"name":"India","flag":"🇮🇳","code":"+91"},
    {"name":"Indonesia","flag":"🇮🇩","code":"+62"},
    {"name":"Iran","flag":"🇮🇷","code":"+98"},
    {"name":"Iraq","flag":"🇮🇶","code":"+964"},
    {"name":"Ireland","flag":"🇮🇪","code":"+353"},
    {"name":"Israel","flag":"🇮🇱","code":"+972"},
    {"name":"Italy","flag":"🇮🇹","code":"+39"},
    {"name":"Jamaica","flag":"🇯🇲","code":"+1876"},
    {"name":"Japan","flag":"🇯🇵","code":"+81"},
    {"name":"Jordan","flag":"🇯🇴","code":"+962"},
    {"name":"Kazakhstan","flag":"🇰🇿","code":"+7"},
    {"name":"Kenya","flag":"🇰🇪","code":"+254"},
    {"name":"Kiribati","flag":"🇰🇮","code":"+686"},
    {"name":"Kosovo","flag":"🇽🇰","code":"+383"},
    {"name":"Kuwait","flag":"🇰🇼","code":"+965"},
    {"name":"Kyrgyzstan","flag":"🇰🇬","code":"+996"},
    {"name":"Laos","flag":"🇱🇦","code":"+856"},
    {"name":"Latvia","flag":"🇱🇻","code":"+371"},
    {"name":"Lebanon","flag":"🇱🇧","code":"+961"},
    {"name":"Lesotho","flag":"🇱🇸","code":"+266"},
    {"name":"Liberia","flag":"🇱🇷","code":"+231"},
    {"name":"Libya","flag":"🇱🇾","code":"+218"},
    {"name":"Liechtenstein","flag":"🇱🇮","code":"+423"},
    {"name":"Lithuania","flag":"🇱🇹","code":"+370"},
    {"name":"Luxembourg","flag":"🇱🇺","code":"+352"},
    {"name":"Macau","flag":"🇲🇴","code":"+853"},
    {"name":"Madagascar","flag":"🇲🇬","code":"+261"},
    {"name":"Malawi","flag":"🇲🇼","code":"+265"},
    {"name":"Malaysia","flag":"🇲🇾","code":"+60"},
    {"name":"Maldives","flag":"🇲🇻","code":"+960"},
    {"name":"Mali","flag":"🇲🇱","code":"+223"},
    {"name":"Malta","flag":"🇲🇹","code":"+356"},
    {"name":"Marshall Islands","flag":"🇲🇭","code":"+692"},
    {"name":"Mauritania","flag":"🇲🇷","code":"+222"},
    {"name":"Mauritius","flag":"🇲🇺","code":"+230"},
    {"name":"Mexico","flag":"🇲🇽","code":"+52"},
    {"name":"Micronesia","flag":"🇫🇲","code":"+691"},
    {"name":"Moldova","flag":"🇲🇩","code":"+373"},
    {"name":"Monaco","flag":"🇲🇨","code":"+377"},
    {"name":"Mongolia","flag":"🇲🇳","code":"+976"},
    {"name":"Montenegro","flag":"🇲🇪","code":"+382"},
    {"name":"Morocco","flag":"🇲🇦","code":"+212"},
    {"name":"Mozambique","flag":"🇲🇿","code":"+258"},
    {"name":"Myanmar","flag":"🇲🇲","code":"+95"},
    {"name":"Namibia","flag":"🇳🇦","code":"+264"},
    {"name":"Nauru","flag":"🇳🇷","code":"+674"},
    {"name":"Nepal","flag":"🇳🇵","code":"+977"},
    {"name":"Netherlands","flag":"🇳🇱","code":"+31"},
    {"name":"New Zealand","flag":"🇳🇿","code":"+64"},
    {"name":"Nicaragua","flag":"🇳🇮","code":"+505"},
    {"name":"Niger","flag":"🇳🇪","code":"+227"},
    {"name":"Nigeria","flag":"🇳🇬","code":"+234"},
    {"name":"North Korea","flag":"🇰🇵","code":"+850"},
    {"name":"North Macedonia","flag":"🇲🇰","code":"+389"},
    {"name":"Norway","flag":"🇳🇴","code":"+47"},
    {"name":"Oman","flag":"🇴🇲","code":"+968"},
    {"name":"Pakistan","flag":"🇵🇰","code":"+92"},
    {"name":"Palau","flag":"🇵🇼","code":"+680"},
    {"name":"Palestine","flag":"🇵🇸","code":"+970"},
    {"name":"Panama","flag":"🇵🇦","code":"+507"},
    {"name":"Papua New Guinea","flag":"🇵🇬","code":"+675"},
    {"name":"Paraguay","flag":"🇵🇾","code":"+595"},
    {"name":"Peru","flag":"🇵🇪","code":"+51"},
    {"name":"Philippines","flag":"🇵🇭","code":"+63"},
    {"name":"Poland","flag":"🇵🇱","code":"+48"},
    {"name":"Portugal","flag":"🇵🇹","code":"+351"},
    {"name":"Puerto Rico","flag":"🇵🇷","code":"+1787"},
    {"name":"Qatar","flag":"🇶🇦","code":"+974"},
    {"name":"Romania","flag":"🇷🇴","code":"+40"},
    {"name":"Russia","flag":"🇷🇺","code":"+7"},
    {"name":"Rwanda","flag":"🇷🇼","code":"+250"},
    {"name":"Saint Kitts and Nevis","flag":"🇰🇳","code":"+1869"},
    {"name":"Saint Lucia","flag":"🇱🇨","code":"+1758"},
    {"name":"Saint Vincent","flag":"🇻🇨","code":"+1784"},
    {"name":"Samoa","flag":"🇼🇸","code":"+685"},
    {"name":"San Marino","flag":"🇸🇲","code":"+378"},
    {"name":"Sao Tome and Principe","flag":"🇸🇹","code":"+239"},
    {"name":"Saudi Arabia","flag":"🇸🇦","code":"+966"},
    {"name":"Senegal","flag":"🇸🇳","code":"+221"},
    {"name":"Serbia","flag":"🇷🇸","code":"+381"},
    {"name":"Seychelles","flag":"🇸🇨","code":"+248"},
    {"name":"Sierra Leone","flag":"🇸🇱","code":"+232"},
    {"name":"Singapore","flag":"🇸🇬","code":"+65"},
    {"name":"Slovakia","flag":"🇸🇰","code":"+421"},
    {"name":"Slovenia","flag":"🇸🇮","code":"+386"},
    {"name":"Solomon Islands","flag":"🇸🇧","code":"+677"},
    {"name":"Somalia","flag":"🇸🇴","code":"+252"},
    {"name":"South Africa","flag":"🇿🇦","code":"+27"},
    {"name":"South Korea","flag":"🇰🇷","code":"+82"},
    {"name":"South Sudan","flag":"🇸🇸","code":"+211"},
    {"name":"Spain","flag":"🇪🇸","code":"+34"},
    {"name":"Sri Lanka","flag":"🇱🇰","code":"+94"},
    {"name":"Sudan","flag":"🇸🇩","code":"+249"},
    {"name":"Suriname","flag":"🇸🇷","code":"+597"},
    {"name":"Sweden","flag":"🇸🇪","code":"+46"},
    {"name":"Switzerland","flag":"🇨🇭","code":"+41"},
    {"name":"Syria","flag":"🇸🇾","code":"+963"},
    {"name":"Taiwan","flag":"🇹🇼","code":"+886"},
    {"name":"Tajikistan","flag":"🇹🇯","code":"+992"},
    {"name":"Tanzania","flag":"🇹🇿","code":"+255"},
    {"name":"Thailand","flag":"🇹🇭","code":"+66"},
    {"name":"Timor-Leste","flag":"🇹🇱","code":"+670"},
    {"name":"Togo","flag":"🇹🇬","code":"+228"},
    {"name":"Tonga","flag":"🇹🇴","code":"+676"},
    {"name":"Trinidad and Tobago","flag":"🇹🇹","code":"+1868"},
    {"name":"Tunisia","flag":"🇹🇳","code":"+216"},
    {"name":"Turkey","flag":"🇹🇷","code":"+90"},
    {"name":"Turkmenistan","flag":"🇹🇲","code":"+993"},
    {"name":"Tuvalu","flag":"🇹🇻","code":"+688"},
    {"name":"Uganda","flag":"🇺🇬","code":"+256"},
    {"name":"Ukraine","flag":"🇺🇦","code":"+380"},
    {"name":"United Arab Emirates","flag":"🇦🇪","code":"+971"},
    {"name":"United Kingdom","flag":"🇬🇧","code":"+44"},
    {"name":"United States","flag":"🇺🇸","code":"+1"},
    {"name":"Uruguay","flag":"🇺🇾","code":"+598"},
    {"name":"Uzbekistan","flag":"🇺🇿","code":"+998"},
    {"name":"Vanuatu","flag":"🇻🇺","code":"+678"},
    {"name":"Vatican","flag":"🇻🇦","code":"+379"},
    {"name":"Venezuela","flag":"🇻🇪","code":"+58"},
    {"name":"Vietnam","flag":"🇻🇳","code":"+84"},
    {"name":"Yemen","flag":"🇾🇪","code":"+967"},
    {"name":"Zambia","flag":"🇿🇲","code":"+260"},
    {"name":"Zimbabwe","flag":"🇿🇼","code":"+263"},
]

WORLD_COUNTRIES_MAP = {c["name"].lower(): c for c in WORLD_COUNTRIES}

def get_country_flag(name):
    """Return flag emoji for a country name, fallback to 🌍"""
    return WORLD_COUNTRIES_MAP.get((name or "").lower(), {}).get("flag", "🌍")

def get_country_code(name):
    """Return dial code for a country name"""
    return WORLD_COUNTRIES_MAP.get((name or "").lower(), {}).get("code", "")

WC_PER_PAGE = 10  # Countries per page in world picker

# Import account management
try:
    from account import AccountManager
    account_manager = AccountManager(GLOBAL_API_ID, GLOBAL_API_HASH)
    logger.info("✅ Account manager loaded successfully")
except ImportError as e:
    logger.error(f"❌ Failed to load account module: {e}")
    account_manager = None

# Import logging module
try:
    from logs import init_logger, log_purchase_async, log_otp_received_async, log_recharge_approved_async
    init_logger(BOT_TOKEN, LOG_CHANNEL_ID)
    logger.info(f"✅ Telegram logger initialized for channel: {LOG_CHANNEL_ID}")
except ImportError as e:
    logger.error(f"❌ Failed to load logging module: {e}")

# Async manager for background tasks
async_manager = None
if account_manager:
    async_manager = account_manager.async_manager

# Initialize admin in database
def init_admin():
    """Initialize the first admin in database"""
    try:
        # Check if admins collection exists and has any admins
        if 'admins' not in db.list_collection_names():
            db.create_collection('admins')
        
        admin_count = admins_col.count_documents({})
        if admin_count == 0:
            # Add the main admin
            admin_data = {
                "user_id": ADMIN_ID,
                "added_by": "SYSTEM",
                "added_at": datetime.utcnow(),
                "is_super_admin": True
            }
            safe_insert_one(admins_col, admin_data, "admin_init")
            logger.info(f"✅ Main admin {ADMIN_ID} added to database")
    except Exception as e:
        logger.error(f"❌ Failed to initialize admin: {e}")

# Call init_admin
init_admin()

# ---------------------------------------------------------------------
# ADMIN MANAGEMENT FUNCTIONS
# ---------------------------------------------------------------------
def get_admin_info(user_id):
    """Get admin info by user ID"""
    try:
        # Check if it's main admin
        if str(user_id) == str(ADMIN_ID):
            user = users_col.find_one({"user_id": user_id})
            return {
                "user_id": user_id,
                "is_super_admin": True,
                "name": user.get("name", "Main Admin") if user else "Main Admin"
            }
        
        # Check in admins collection
        admin = admins_col.find_one({"user_id": user_id})
        if admin:
            user = users_col.find_one({"user_id": user_id})
            admin["name"] = user.get("name", "Admin") if user else "Admin"
            return admin
        return None
    except Exception as e:
        logger.error(f"Error in get_admin_info: {e}")
        return None
        
def is_admin(user_id):
    """Check if user is an admin"""
    try:
        # Check if it's the main admin
        if str(user_id) == str(ADMIN_ID):
            return True
        
        # Check in admins collection
        admin = admins_col.find_one({"user_id": user_id})
        return admin is not None
    except:
        return False

def is_super_admin(user_id):
    """Check if user is the main super admin"""
    return str(user_id) == str(ADMIN_ID)

def add_admin(user_id, added_by):
    """Add a new admin (max 5 admins)"""
    try:
        # Check if already admin
        if is_admin(user_id):
            return False, "User is already an admin"
        
        # Count current admins (excluding super admin if counting separately)
        admin_count = admins_col.count_documents({})
        if admin_count >= 5:
            return False, "Maximum 5 admins reached"
        
        # Add new admin
        admin_data = {
            "user_id": user_id,
            "added_by": added_by,
            "added_at": datetime.utcnow(),
            "is_super_admin": False
        }
        safe_insert_one(admins_col, admin_data, "admin")
        
        # Get user info
        user = users_col.find_one({"user_id": user_id})
        username = user.get("username", "No username") if user else "Unknown"
        
        return True, f"✅ Admin added successfully!"
    except Exception as e:
        logger.error(f"Error adding admin: {e}")
        return False, f"Error: {str(e)}"

def remove_admin(user_id, removed_by):
    """Remove an admin"""
    try:
        # Check if user is admin
        admin = admins_col.find_one({"user_id": user_id})
        if not admin:
            return False, "User is not an admin"
        
        # Check if trying to remove super admin
        if str(user_id) == str(ADMIN_ID):
            return False, "Cannot remove main admin"
        
        # Remove admin
        result = admins_col.delete_one({"user_id": user_id})
        
        if result.deleted_count > 0:
            return True, f"✅ Admin removed successfully!"
        else:
            return False, "Failed to remove admin"
    except Exception as e:
        logger.error(f"Error removing admin: {e}")
        return False, f"Error: {str(e)}"

def get_all_admins():
    """Get list of all admins"""
    try:
        admins = list(admins_col.find({}))
        # Also include main admin if not in collection
        main_admin_exists = any(str(a.get("user_id")) == str(ADMIN_ID) for a in admins)
        
        admin_list = []
        
        # Add main admin first
        if not main_admin_exists:
            admin_list.append({
                "user_id": ADMIN_ID,
                "username": "Main Admin",
                "name": "Main Admin",
                "added_at": datetime.utcnow(),
                "added_by": "SYSTEM",
                "is_super_admin": True
            })
        
        # Add other admins
        for admin in admins:
            user_id = admin["user_id"]
            user = users_col.find_one({"user_id": user_id})
            username = user.get("username", "No username") if user else "Unknown"
            name = user.get("name", "Unknown") if user else "Unknown"
            
            admin_list.append({
                "user_id": user_id,
                "username": username,
                "name": name,
                "added_at": admin.get("added_at"),
                "added_by": admin.get("added_by"),
                "is_super_admin": admin.get("is_super_admin", False)
            })
        return admin_list
    except Exception as e:
        logger.error(f"Error getting admins: {e}")
        return []

def get_admin_count():
    """Get total number of admins"""
    try:
        return admins_col.count_documents({}) + 1  # +1 for main admin
    except:
        return 1

# ---------------------------------------------------------------------
# ADMIN COMMAND HANDLERS
# ---------------------------------------------------------------------

@bot.message_handler(commands=['addadmin'])
def add_admin_command(msg):
    """Add a new admin - Only main admin can use"""
    user_id = msg.from_user.id
    
    # Only main admin can add admins
    if not is_super_admin(user_id):
        bot.reply_to(msg, "❌ Sirf main admin hi addadmin use kar sakta hai!")
        return
    
    # Start the add admin flow
    admin_add_state[user_id] = {"step": "waiting_user_id"}
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_add_admin"))
    
    bot.reply_to(
        msg,
        "👤 **Add New Admin**\n\n"
        "Please enter the User ID of the person you want to make admin:\n\n"
        "📝 User ID milne ke liye:\n"
        "• User ko /start karna hoga bot mein\n"
        "• Ya admin panel se user search karo\n\n"
        "Example: `123456789`",
        parse_mode="Markdown",
        reply_markup=markup
    )

@bot.message_handler(commands=['removeadmin'])
def remove_admin_command(msg):
    """Remove an admin - Only main admin can use"""
    user_id = msg.from_user.id
    
    # Only main admin can remove admins
    if not is_super_admin(user_id):
        bot.reply_to(msg, "❌ Sirf main admin hi removeadmin use kar sakta hai!")
        return
    
    # Get list of admins
    admins = get_all_admins()
    
    if len(admins) <= 1:  # Only main admin
        bot.reply_to(
            msg,
            "📋 **Admin List**\n\n"
            "Koi aur admin nahi hai remove karne ke liye.\n\n"
            f"👑 Main Admin: `{ADMIN_ID}`",
            parse_mode="Markdown"
        )
        return
    
    # Show list of admins
    admin_list_text = "📋 **Existing Admins:**\n\n"
    for admin in admins:
        if not admin.get("is_super_admin", False):
            admin_list_text += f"• `{admin['user_id']}` - {admin['name']}\n"
    
    admin_list_text += "\nPlease enter the User ID of the admin you want to remove:"
    
    admin_remove_state[user_id] = {"step": "waiting_user_id"}
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_remove_admin"))
    
    bot.reply_to(
        msg,
        admin_list_text,
        parse_mode="Markdown",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data in ["cancel_add_admin", "cancel_remove_admin"])
def handle_cancel_admin(call):
    user_id = call.from_user.id
    
    if call.data == "cancel_add_admin":
        if user_id in admin_add_state:
            del admin_add_state[user_id]
        bot.edit_message_text(
            "❌ Add admin cancelled.",
            call.message.chat.id,
            call.message.message_id
        )
    elif call.data == "cancel_remove_admin":
        if user_id in admin_remove_state:
            del admin_remove_state[user_id]
        bot.edit_message_text(
            "❌ Remove admin cancelled.",
            call.message.chat.id,
            call.message.message_id
        )

@bot.message_handler(func=lambda m: m.from_user.id in admin_add_state and admin_add_state[m.from_user.id]["step"] == "waiting_user_id")
def handle_add_admin_userid(msg):
    user_id = msg.from_user.id
    
    try:
        target_user_id = int(msg.text.strip())
        
        # Check if trying to add self
        if target_user_id == user_id:
            bot.reply_to(msg, "❌ Aap khudko admin nahi bana sakte! Aap already main admin ho.")
            del admin_add_state[user_id]
            return
        
        # Check if user exists
        user = users_col.find_one({"user_id": target_user_id})
        if not user:
            bot.reply_to(
                msg,
                f"❌ User `{target_user_id}` database mein nahi mila.\n\n"
                f"Pehle user ko /start karwaiye bot mein.",
                parse_mode="Markdown"
            )
            del admin_add_state[user_id]
            return
        
        # Check if already admin
        if is_admin(target_user_id):
            bot.reply_to(
                msg,
                f"⚠️ User `{target_user_id}` already admin hai!",
                parse_mode="Markdown"
            )
            del admin_add_state[user_id]
            return
        
        # Check max admins
        admin_count = admins_col.count_documents({})
        if admin_count >= 5:
            bot.reply_to(
                msg,
                "❌ Maximum 5 admins ho chuke hain. Pehle kisi admin ko remove karo.",
                parse_mode="Markdown"
            )
            del admin_add_state[user_id]
            return
        
        # Add admin
        success, message = add_admin(target_user_id, user_id)
        
        if success:
            # Get updated admin count
            new_count = admins_col.count_documents({})
            
            bot.reply_to(
                msg,
                f"✅ **Admin Added Successfully!**\n\n"
                f"👤 User ID: `{target_user_id}`\n"
                f"👤 Name: {user.get('name', 'Unknown')}\n"
                f"📊 Total Admins: {new_count + 1}/6 (Main Admin + {new_count})\n\n"
                f"Ab ye admin panel access kar sakte hain!",
                parse_mode="Markdown"
            )
            
            # Notify new admin
            try:
                bot.send_message(
                    target_user_id,
                    f"🎉 **Congratulations! You've Been Promoted to Admin!**\n\n"
                    f"Ab aap admin panel use kar sakte hain:\n"
                    f"• Recharge Approve/Reject\n"
                    f"• Add/Remove Countries\n"
                    f"• Add Accounts\n"
                    f"• Broadcast Messages\n"
                    f"• And more!\n\n"
                    f"Admin panel ke liye /start karo.",
                    parse_mode="Markdown"
                )
            except:
                bot.reply_to(msg, "⚠️ New admin ko notification nahi bhej sakte (unhone bot block kar diya hai)")
        else:
            bot.reply_to(msg, f"❌ {message}")
        
        del admin_add_state[user_id]
        
    except ValueError:
        bot.reply_to(msg, "❌ Invalid User ID. Sirf numbers daalo.")
    except Exception as e:
        logger.error(f"Add admin error: {e}")
        bot.reply_to(msg, f"❌ Error: {str(e)}")
        del admin_add_state[user_id]

@bot.message_handler(func=lambda m: m.from_user.id in admin_remove_state and admin_remove_state[m.from_user.id]["step"] == "waiting_user_id")
def handle_remove_admin_userid(msg):
    user_id = msg.from_user.id
    
    try:
        target_user_id = int(msg.text.strip())
        
        # Check if trying to remove self
        if target_user_id == user_id:
            bot.reply_to(msg, "❌ Aap khudko remove nahi kar sakte! Aap main admin ho.")
            del admin_remove_state[user_id]
            return
        
        # Check if user is admin
        if not is_admin(target_user_id):
            bot.reply_to(
                msg,
                f"❌ User `{target_user_id}` admin nahi hai!",
                parse_mode="Markdown"
            )
            del admin_remove_state[user_id]
            return
        
        # Remove admin
        success, message = remove_admin(target_user_id, user_id)
        
        if success:
            # Get user info
            user = users_col.find_one({"user_id": target_user_id})
            name = user.get('name', 'Unknown') if user else 'Unknown'
            
            # Get updated admin count
            new_count = admins_col.count_documents({})
            
            bot.reply_to(
                msg,
                f"✅ **Admin Removed Successfully!**\n\n"
                f"👤 User ID: `{target_user_id}`\n"
                f"👤 Name: {name}\n"
                f"📊 Remaining Admins: {new_count + 1}/6 (Main Admin + {new_count})\n\n"
                f"Ab ye admin nahi rahe.",
                parse_mode="Markdown"
            )
            
            # Notify removed admin
            try:
                bot.send_message(
                    target_user_id,
                    f"⚠️ **Your Admin Access Has Been Removed**\n\n"
                    f"Aap ab admin nahi rahe. Bot use karne ke liye /start karo.",
                    parse_mode="Markdown"
                )
            except:
                pass
        else:
            bot.reply_to(msg, f"❌ {message}")
        
        del admin_remove_state[user_id]
        
    except ValueError:
        bot.reply_to(msg, "❌ Invalid User ID. Sirf numbers daalo.")
    except Exception as e:
        logger.error(f"Remove admin error: {e}")
        bot.reply_to(msg, f"❌ Error: {str(e)}")
        del admin_remove_state[user_id]

# ---------------------------------------------------------------------
# UTILITY FUNCTIONS - UPDATED FOR TWO CHANNELS
# ---------------------------------------------------------------------

def ensure_user_exists(user_id, user_name=None, username=None, referred_by=None):
    user = users_col.find_one({"user_id": user_id})
    if not user:
        user_data = {
            "user_id": user_id,
            "name": user_name or "Unknown",
            "username": username,
            "referred_by": referred_by,
            "referral_code": f"REF{user_id}",
            "total_commission_earned": 0.0,
            "total_referrals": 0,
            "created_at": datetime.utcnow()
        }
        safe_insert_one(users_col, user_data, "user")
        
        if referred_by:
            referral_record = {
                "referrer_id": referred_by,
                "referred_id": user_id,
                "referral_code": user_data['referral_code'],
                "status": "pending",
                "created_at": datetime.utcnow()
            }
            safe_insert_one(referrals_col, referral_record, "referral")
            users_col.update_one(
                {"user_id": referred_by},
                {"$inc": {"total_referrals": 1}}
            )
            logger.info(f"Referral recorded: {referred_by} -> {user_id}")
    
    wallets_col.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {"user_id": user_id, "balance": 0.0}},
        upsert=True
    )

def get_balance(user_id):
    rec = wallets_col.find_one({"user_id": user_id})
    return float(rec.get("balance", 0.0)) if rec else 0.0

def add_balance(user_id, amount):
    wallets_col.update_one(
        {"user_id": user_id},
        {"$inc": {"balance": float(amount)}},
        upsert=True
    )

def deduct_balance(user_id, amount):
    wallets_col.update_one(
        {"user_id": user_id},
        {"$inc": {"balance": -float(amount)}},
        upsert=True
    )

def format_currency(x):
    try:
        x = float(x)
        if x.is_integer():
            return f"₹{int(x)}"
        return f"₹{x:.2f}"
    except:
        return "₹0"

def get_available_accounts_count(country):
    return accounts_col.count_documents({
        "country": country,
        "used": {"$ne": True},
        "$or": [{"status": "active"}, {"status": {"$exists": False}}]
    })

def is_user_banned(user_id):
    banned = banned_users_col.find_one({"user_id": user_id, "status": "active"})
    return banned is not None

def get_all_countries():
    return list(countries_col.find({"status": "active"}))

def get_country_by_name(country_name):
    return countries_col.find_one({
        "name": {"$regex": f"^{country_name}$", "$options": "i"},
        "status": "active"
    })

def add_referral_commission(referrer_id, recharge_amount, recharge_id):
    try:
        commission = (recharge_amount * REFERRAL_COMMISSION) / 100
        add_balance(referrer_id, commission)
        
        transaction_id = f"COM{referrer_id}{int(time.time())}"
        transaction_record = {
            "transaction_id": transaction_id,
            "user_id": referrer_id,
            "amount": commission,
            "type": "referral_commission",
            "description": f"Referral commission from recharge #{recharge_id}",
            "timestamp": datetime.utcnow(),
            "recharge_id": str(recharge_id)
        }
        safe_insert_one(transactions_col, transaction_record, "transaction_referral")
        
        users_col.update_one(
            {"user_id": referrer_id},
            {"$inc": {"total_commission_earned": commission}}
        )
        
        referrals_col.update_one(
            {"referred_id": recharge_id.get("user_id"), "referrer_id": referrer_id},
            {"$set": {"status": "completed", "commission": commission, "completed_at": datetime.utcnow()}}
        )
        
        try:
            bot.send_message(
                referrer_id,
                f"💰 **Referral Commission Earned!**\n\n"
                f"✅ You earned {format_currency(commission)} commission!\n"
                f"📊 From: {format_currency(recharge_amount)} recharge\n"
                f"📈 Commission Rate: {REFERRAL_COMMISSION}%\n"
                f"💳 New Balance: {format_currency(get_balance(referrer_id))}\n\n"
                f"Keep referring to earn more! 🎉"
            )
        except:
            pass
        
        logger.info(f"Referral commission added: {referrer_id} - {format_currency(commission)}")
    except Exception as e:
        logger.error(f"Error adding referral commission: {e}")

# ---------------------------------------------------------------------
# UPDATED: CHECK BOTH CHANNELS MEMBERSHIP
# ---------------------------------------------------------------------

def _check_single_channel(user_id, channel):
    """
    Returns True if the user has joined the channel, or if the channel
    cannot be verified (bad config / bot not admin). Returns False only
    when the user is definitively NOT a member.
    """
    try:
        member = bot.get_chat_member(channel, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        err = str(e).lower()
        # If the channel is misconfigured or bot has no access, skip the check
        if 'chat not found' in err or 'user not found' in err or 'bot is not a member' in err:
            logger.warning(f"Channel check skipped for {channel}: {e}")
            return True  # Don't punish users for admin misconfiguration
        logger.error(f"Error checking channel membership for {channel}: {e}")
        return True  # Fail open so buttons still work

def has_user_joined_channels(user_id):
    """Check if user has joined both mandatory channels"""
    return (
        _check_single_channel(user_id, MUST_JOIN_CHANNEL_1) and
        _check_single_channel(user_id, MUST_JOIN_CHANNEL_2)
    )

def get_missing_channels(user_id):
    """Get list of channels user hasn't definitively joined yet"""
    missing = []
    for channel in [MUST_JOIN_CHANNEL_1, MUST_JOIN_CHANNEL_2]:
        try:
            member = bot.get_chat_member(channel, user_id)
            if member.status not in ['member', 'administrator', 'creator']:
                missing.append(channel)
        except Exception as e:
            err = str(e).lower()
            if 'chat not found' in err or 'bot is not a member' in err:
                # Channel misconfigured — don't show it as missing
                logger.warning(f"Skipping missing-channel display for {channel}: {e}")
            else:
                missing.append(channel)
    return missing

# ---------------------------------------------------------------------
# COUPON UTILITY FUNCTIONS
# ---------------------------------------------------------------------

def get_coupon(code):
    return coupons_col.find_one({"coupon_code": code})

def is_coupon_claimed_by_user(coupon_code, user_id):
    coupon = get_coupon(coupon_code)
    if not coupon:
        return False
    claimed_users = coupon.get("claimed_users", [])
    return user_id in claimed_users

def claim_coupon(coupon_code, user_id):
    try:
        coupon = get_coupon(coupon_code)
        if not coupon:
            return False, "Coupon not found"
        
        if user_id in coupon.get("claimed_users", []):
            return False, "Already claimed"
        
        if coupon.get("status") != "active":
            status = coupon.get("status", "inactive")
            return False, f"Coupon {status}"
        
        total_claimed = coupon.get("total_claimed_count", 0)
        max_users = coupon.get("max_users", 0)
        if total_claimed >= max_users:
            coupons_col.update_one(
                {"coupon_code": coupon_code},
                {"$set": {"status": "expired"}}
            )
            return False, "Fully claimed"
        
        result = coupons_col.update_one(
            {
                "coupon_code": coupon_code,
                "status": "active",
                "total_claimed_count": {"$lt": max_users}
            },
            {
                "$inc": {"total_claimed_count": 1},
                "$push": {"claimed_users": user_id},
                "$set": {
                    "last_claimed_at": datetime.utcnow(),
                    "last_claimed_by": user_id
                }
            }
        )
        
        if result.modified_count == 0:
            return False, "Coupon no longer available"
        
        amount = coupon.get("amount", 0)
        add_balance(user_id, amount)
        
        transaction_id = f"CPN{user_id}{int(time.time())}"
        transaction_record = {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "amount": amount,
            "type": "coupon_redeem",
            "description": f"Coupou redeem: {coupon_code}",
            "coupon_code": coupon_code,
            "timestamp": datetime.utcnow()
        }
        safe_insert_one(transactions_col, transaction_record, "transaction_coupon")
        
        updated_coupon = get_coupon(coupon_code)
        if updated_coupon and updated_coupon.get("total_claimed_count", 0) >= max_users:
            coupons_col.update_one(
                {"coupon_code": coupon_code},
                {"$set": {"status": "expired"}}
            )
        
        return True, amount
    except Exception as e:
        logger.error(f"Error claiming coupon: {e}")
        return False, "Error processing coupon"

def create_coupon(code, amount, max_users, created_by):
    try:
        if amount < 1:
            return False, "Amount must be at least ₹1"
        if max_users < 1:
            return False, "Max users must be at least 1"
        
        existing = get_coupon(code)
        if existing:
            return False, "Coupon code already exists"
        
        coupon_data = {
            "coupon_code": code,
            "amount": float(amount),
            "max_users": int(max_users),
            "total_claimed_count": 0,
            "claimed_users": [],
            "status": "active",
            "created_at": datetime.utcnow(),
            "created_by": created_by
        }
        safe_insert_one(coupons_col, coupon_data, "coupon")
        return True, "Coupon created successfully"
    except Exception as e:
        logger.error(f"Error creating coupon: {e}")
        return False, f"Error: {str(e)}"

def remove_coupon(code, removed_by):
    try:
        coupon = get_coupon(code)
        if not coupon:
            return False, "Coupon not found"
        
        result = coupons_col.update_one(
            {"coupon_code": code},
            {"$set": {
                "status": "removed",
                "removed_at": datetime.utcnow(),
                "removed_by": removed_by
            }}
        )
        
        if result.modified_count == 0:
            return False, "Failed to remove coupon"
        return True, "Coupon removed successfully"
    except Exception as e:
        logger.error(f"Error removing coupon: {e}")
        return False, f"Error: {str(e)}"

def get_coupon_status(code):
    coupon = get_coupon(code)
    if not coupon:
        return None
    
    claimed = coupon.get("total_claimed_count", 0)
    max_users = coupon.get("max_users", 0)
    remaining = max(0, max_users - claimed)
    
    return {
        "code": coupon.get("coupon_code"),
        "amount": coupon.get("amount", 0),
        "max_users": max_users,
        "claimed": claimed,
        "remaining": remaining,
        "status": coupon.get("status", "unknown"),
        "created_at": coupon.get("created_at"),
        "created_by": coupon.get("created_by"),
        "claimed_users": coupon.get("claimed_users", [])[:10]
    }

# ---------------------------------------------------------------------
# ENHANCED RECHARGE APPROVAL FUNCTIONS
# ---------------------------------------------------------------------

def process_recharge_approval(admin_id, req_id, action):
    """Process recharge approval/rejection with tracking"""
    try:
        # Get recharge request
        req = recharges_col.find_one({"req_id": req_id})
        if not req:
            return False, "Request not found", None
        
        # Check if already processed
        if req.get("status") != "pending":
            return False, f"Request already {req.get('status')}", None
        
        # Get admin info
        admin_info = get_admin_info(admin_id)
        admin_name = f"Admin {admin_id}"
        if admin_info:
            user = users_col.find_one({"user_id": admin_id})
            if user:
                admin_name = user.get("name", f"Admin {admin_id}")
        
        user_target = req.get("user_id")
        amount = float(req.get("amount", 0))
        
        # Track this approval
        approval_key = f"{req_id}_{action}"
        
        # Check if another admin already processed this (via tracking)
        if approval_key in recharge_approvals:
            prev_admin = recharge_approvals[approval_key]
            return False, f"Already {action}ed by {prev_admin['admin_name']}", None
        
        if action == "approve":
            # Add balance to user
            add_balance(user_target, amount)
            
            # Update recharge status
            recharges_col.update_one(
                {"req_id": req_id},
                {"$set": {
                    "status": "approved", 
                    "processed_at": datetime.utcnow(), 
                    "processed_by": admin_id,
                    "processed_by_name": admin_name
                }}
            )
            
            # Log approval
            try:
                from logs import log_recharge_approved_async
                log_recharge_approved_async(
                    user_id=user_target,
                    amount=amount,
                    method=req.get("method", "UPI"),
                    utr=req.get("utr")
                )
            except:
                pass
            
            # Add referral commission if applicable
            user_data = users_col.find_one({"user_id": user_target})
            if user_data and user_data.get("referred_by"):
                add_referral_commission(user_data["referred_by"], amount, req)
            
            # Mark this approval in tracking
            recharge_approvals[approval_key] = {
                "admin_id": admin_id,
                "admin_name": admin_name,
                "timestamp": datetime.utcnow()
            }

            # Notify user — approved ✅
            try:
                new_balance = get_balance(user_target)
                bot.send_message(
                    user_target,
                    f"╔══════════════════╗\n"
                    f"  𝐋𝐄𝐆𝐄𝐍𝐃𝐀𝐑𝐘 𝐗 𝐎𝐓𝐏\n"
                    f"╚══════════════════╝\n\n"
                    f"✅ <b>Recharge Approved!</b>\n\n"
                    f"💰 Amount Added: <b>₹{amount:,.0f}</b>\n"
                    f"💳 New Balance: <b>{format_currency(new_balance)}</b>\n"
                    f"👤 Approved By: <b>{admin_name}</b>\n"
                    f"🆔 Request ID: <code>{req_id}</code>\n"
                    f"⏰ Time: {datetime.utcnow().strftime('%d %b %Y, %H:%M')} UTC\n\n"
                    f"Thank you for recharging! 🎉\n"
                    f"Use /menu to buy accounts.",
                    parse_mode="HTML"
                )
            except Exception:
                pass

            return True, f"✅ Approved ₹{amount:,.0f} for user {user_target}", {
                "admin_name": admin_name,
                "admin_id": admin_id,
                "action": "approved",
                "user_id": user_target,
                "amount": amount
            }

        else:  # cancel/reject
            # Update recharge status
            recharges_col.update_one(
                {"req_id": req_id},
                {"$set": {
                    "status": "cancelled",
                    "processed_at": datetime.utcnow(),
                    "processed_by": admin_id,
                    "processed_by_name": admin_name
                }}
            )

            # Mark this rejection in tracking
            recharge_approvals[approval_key] = {
                "admin_id": admin_id,
                "admin_name": admin_name,
                "timestamp": datetime.utcnow()
            }

            # Notify user — rejected ❌
            try:
                bot.send_message(
                    user_target,
                    f"╔══════════════════╗\n"
                    f"  𝐋𝐄𝐆𝐄𝐍𝐃𝐀𝐑𝐘 𝐗 𝐎𝐓𝐏\n"
                    f"╚══════════════════╝\n\n"
                    f"❌ <b>Recharge Rejected</b>\n\n"
                    f"💰 Amount: <b>₹{amount:,.0f}</b>\n"
                    f"👤 Rejected By: <b>{admin_name}</b>\n"
                    f"🆔 Request ID: <code>{req_id}</code>\n"
                    f"⏰ Time: {datetime.utcnow().strftime('%d %b %Y, %H:%M')} UTC\n\n"
                    f"❓ Payment not verified. Please contact support:\n"
                    f"👉 @rchiex",
                    parse_mode="HTML"
                )
            except Exception:
                pass

            return True, f"❌ Rejected ₹{amount:,.0f} for user {user_target}", {
                "admin_name": admin_name,
                "admin_id": admin_id,
                "action": "rejected",
                "user_id": user_target,
                "amount": amount
            }
            
    except Exception as e:
        logger.error(f"Error in recharge approval: {e}")
        return False, f"Error: {str(e)}", None

# ---------------------------------------------------------------------
# UI HELPER FUNCTIONS - FIXED
# ---------------------------------------------------------------------

def edit_or_resend(chat_id, message_id, text, markup=None, parse_mode=None, photo_url=None):
    """Edit message if possible, otherwise delete and send new"""
    try:
        if photo_url:
            # For photos, we need to send new message
            try:
                bot.delete_message(chat_id, message_id)
            except:
                pass
            return bot.send_photo(chat_id, photo_url, caption=text, parse_mode=parse_mode, reply_markup=markup)
        else:
            # For text messages, try to edit first
            try:
                return bot.edit_message_text(
                    text,
                    chat_id=chat_id,
                    message_id=message_id,
                    parse_mode=parse_mode,
                    reply_markup=markup
                )
            except Exception as e:
                # If edit fails, delete and send new
                try:
                    bot.delete_message(chat_id, message_id)
                except:
                    pass
                return bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=markup)
    except Exception as e:
        logger.error(f"Error in edit_or_resend: {e}")
        return bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=markup)

def clean_ui_and_send_menu(chat_id, user_id, text=None, markup=None):
    """Clean UI and send main menu - FIXED: Always deletes old message"""
    try:
        # ALWAYS try to delete the previous message
        if user_id in user_last_message:
            try:
                bot.delete_message(chat_id, user_last_message[user_id])
            except:
                pass

        # Show sequence of messages with deletion
        def show_sequence():
            try:
                # Premium start animation
                anim_msg = bot.send_message(chat_id, "✨ HLO SIR....", parse_mode="HTML")
                time.sleep(0.3)
                try:
                    bot.edit_message_text(
                        "🏓 <b>PING  PONG ....</b>",
                        chat_id, anim_msg.message_id, parse_mode="HTML"
                    )
                except: pass
                time.sleep(0.3)
                try:
                    bot.edit_message_text(
                        "⚡ <b>STARTING ....</b>\n<i>Loading your dashboard...</i>",
                        chat_id, anim_msg.message_id, parse_mode="HTML"
                    )
                except: pass
                time.sleep(0.35)
                try:
                    bot.edit_message_text(
                        "🚀 <b>OPENING MAIN MENU</b> 🚀\n\n"
                        "╔══════════════════╗\n"
                        "  𝐋𝐄𝐆𝐄𝐍𝐃𝐀𝐑𝐘 𝐗 𝐎𝐓𝐏  \n"
                        "╚══════════════════╝",
                        chat_id, anim_msg.message_id, parse_mode="HTML"
                    )
                except: pass
                time.sleep(0.3)
                try:
                    bot.delete_message(chat_id, anim_msg.message_id)
                except: pass
            except Exception as e:
                logger.error(f"Error in sequence: {e}")

        # Run sequence in background thread
        thread = threading.Thread(target=show_sequence, daemon=True)
        thread.start()
        thread.join()

        # Main menu caption with expandable blockquotes
        caption = (
            "🥂 <b>Welcome to ˹ 𝐋ᴇɢᴇɴᴅᴀʀʏ ꭙ 𝐎ᴛᴘ 𝐒ᴇʟʟᴇʀ [ 𝐁ᴏᴛ ] ❤️‍🔥 By Darklord$🇮🇳</b> 🥂\n"
            "<blockquote expandable>\n"
            "- Automatic OTPs 📍\n"
            "- Easy to Use 🥂🥂\n"
            "- 24/7 Support 👨‍🔧\n"
            "- Instant Payment Approvals 🧾\n"
            "</blockquote>\n"
            "<blockquote expandable>\n"
            "🚀 <b>How to use Bot :</b>\n"
            "1️⃣ Recharge\n"
            "2️⃣ Select Country\n"
            "3️⃣ Buy Account\n"
            "4️⃣ Get Number & Login through Telegram / Telegram X / Turbotel\n"
            "5️⃣ Receive OTP & You're Done ✅\n"
            "</blockquote>\n"
            "🚀 <b>Enjoy Fast Account Buying Experience!</b>"
        )

        if markup is None:
            markup = InlineKeyboardMarkup(row_width=2)
            # Row 1: Buy + Balance
            markup.add(
                InlineKeyboardButton("🛒 Buy Account", callback_data="buy_account"),
                InlineKeyboardButton("💰 Balance", callback_data="balance")
            )
            # Row 2: Recharge
            markup.add(
                InlineKeyboardButton("💳 Recharge", callback_data="recharge")
            )
            # Row 3: Refer + Redeem
            markup.add(
                InlineKeyboardButton("👥 Refer Friends", callback_data="refer_friends"),
                InlineKeyboardButton("🎁 Redeem", callback_data="redeem_coupon")
            )
            # Row 4: AI Chat + Support
            markup.add(
                InlineKeyboardButton("🤖 AI Chat", callback_data="ai_chat"),
                InlineKeyboardButton("🛠️ Support", callback_data="support")
            )
            # Row 5: Admin Panel (only for admin)
            if is_admin(user_id):
                markup.add(InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel"))

        # Send new message (TEXT ONLY - NO PHOTO)
        sent_msg = bot.send_message(
            chat_id,
            text or caption,
            parse_mode="HTML",
            reply_markup=markup,
            disable_web_page_preview=True
        )
        user_last_message[user_id] = sent_msg.message_id
        return sent_msg
    except Exception as e:
        logger.error(f"Error in clean_ui_and_send_menu: {e}")
        # Fallback
        try:
            sent_msg = bot.send_message(chat_id, text or caption, parse_mode="HTML", reply_markup=markup)
            user_last_message[user_id] = sent_msg.message_id
            return sent_msg
        except:
            pass

# ---------------------------------------------------------------------
# BALANCE TRANSFER FUNCTIONS
# ---------------------------------------------------------------------

def transfer_balance(sender_id, receiver_id, amount):
    """Balance transfer function"""
    try:
        # Sender ka balance check
        sender_balance = get_balance(sender_id)
        
        if sender_balance < amount:
            return False, "Insufficient balance"
        
        if amount <= 0:
            return False, "Amount must be greater than 0"
        
        if sender_id == receiver_id:
            return False, "Cannot send to yourself"
        
        # Check if receiver exists
        receiver = users_col.find_one({"user_id": receiver_id})
        if not receiver:
            return False, "Receiver user not found"
        
        # Transfer balance
        deduct_balance(sender_id, amount)
        add_balance(receiver_id, amount)
        
        # Transaction record
        transaction_id = f"TRF{int(time.time())}{sender_id}"
        transaction_record = {
            "transaction_id": transaction_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "amount": amount,
            "type": "transfer",
            "timestamp": datetime.utcnow()
        }
        safe_insert_one(transactions_col, transaction_record, "transaction_transfer")
        
        return True, f"✅ {format_currency(amount)} transferred successfully!"
        
    except Exception as e:
        logger.error(f"Transfer error: {e}")
        return False, f"Error: {str(e)}"

# ---------------------------------------------------------------------
# BOT HANDLERS - UPDATED WITH TWO CHANNELS
# ---------------------------------------------------------------------

@bot.message_handler(commands=['start'])
def start(msg):
    user_id = msg.from_user.id
    logger.info(f"Start command from user {user_id}")
    
    if is_user_banned(user_id):
        try:
            bot.delete_message(msg.chat.id, msg.message_id)
        except:
            pass
        return
    
    # Check if user has joined BOTH channels
    if not has_user_joined_channels(user_id):
        missing_channels = get_missing_channels(user_id)
        
        caption = """<b>🚀 Join Both Channels First!</b> 

📢 To use this bot, you must join our official channels.

👉 Get updates, new features & support from our channels.

Click the buttons below to join both channels, then press VERIFY ✅"""
        
        markup = InlineKeyboardMarkup(row_width=2)
        
        # Add buttons for both channels
        for channel in missing_channels:
            markup.add(InlineKeyboardButton(
                f"📢 Join {channel}",
                url=f"https://t.me/{channel[1:]}"
            ))
        
        markup.add(InlineKeyboardButton("✅ Verify Join", callback_data="verify_join"))
        
        try:
            bot.send_message(
                user_id,
                caption,
                parse_mode="HTML",
                reply_markup=markup
            )
        except Exception as e:
            logger.error(f"Error sending join message: {e}")
        return
    
    referred_by = None
    if len(msg.text.split()) > 1:
        referral_code = msg.text.split()[1]
        if referral_code.startswith('REF'):
            try:
                referrer_id = int(referral_code[3:])
                referrer = users_col.find_one({"user_id": referrer_id})
                if referrer:
                    referred_by = referrer_id
                    logger.info(f"Referral detected: {referrer_id} -> {user_id}")
            except:
                pass
    
    ensure_user_exists(user_id, msg.from_user.first_name, msg.from_user.username, referred_by)
    clean_ui_and_send_menu(user_id, user_id)

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "🚫 Your account is banned", show_alert=True)
        return
    
    logger.info(f"Callback received: {data} from user {user_id}")
    
    try:
        if data == "verify_join":
            # Check if user has joined BOTH channels
            if has_user_joined_channels(user_id):
                try:
                    bot.delete_message(call.message.chat.id, call.message.message_id)
                except:
                    pass
                clean_ui_and_send_menu(call.message.chat.id, user_id)
                bot.answer_callback_query(call.id, "✅ Verified! Welcome to the bot.", show_alert=True)
            else:
                missing_channels = get_missing_channels(user_id)
                
                caption = """<b>🚀 Join Both Channels First!</b> 

📢 To use this bot, you must join our official channels.

👉 Get updates, new features & support from our channels.

Click the buttons below to join both channels, then press VERIFY ✅"""
                
                markup = InlineKeyboardMarkup(row_width=2)
                
                # Add buttons for both channels
                for channel in missing_channels:
                    markup.add(InlineKeyboardButton(
                        f"📢 Join {channel}",
                        url=f"https://t.me/{channel[1:]}"
                    ))
                
                markup.add(InlineKeyboardButton("✅ Verify Join", callback_data="verify_join"))
                
                try:
                    bot.edit_message_text(
                        caption,
                        call.message.chat.id,
                        call.message.message_id,
                        parse_mode="HTML",
                        reply_markup=markup
                    )
                except:
                    pass
                
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join these channels first:\n{missing_list}", 
                    show_alert=True
                )
        
        elif data == "buy_account":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            show_countries(call.message.chat.id)
        
        elif data == "balance":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            balance = get_balance(user_id)
            user_data = users_col.find_one({"user_id": user_id}) or {}
            commission_earned = user_data.get("total_commission_earned", 0)
            
            message = f"💰 **Your Balance:** {format_currency(balance)}\n\n"
            message += f"📊 **Referral Stats:**\n"
            message += f"• Total Commission Earned: {format_currency(commission_earned)}\n"
            message += f"• Total Referrals: {user_data.get('total_referrals', 0)}\n"
            message += f"• Commission Rate: {REFERRAL_COMMISSION}%\n\n"
            message += f"Your Referral Code: `{user_data.get('referral_code', 'REF' + str(user_id))}`"
            
            # Sirf Send Balance aur Back button
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("📤 Send Balance", callback_data="send_balance_menu")
            )
            markup.add(
                InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")
            )
            
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            
            sent_msg = bot.send_message(
                call.message.chat.id,
                message,
                parse_mode="Markdown",
                reply_markup=markup
            )
            user_last_message[user_id] = sent_msg.message_id
        
        elif data == "send_balance_menu":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            balance = get_balance(user_id)
            
            message = f"📤 **Send Balance - Step 1/2**\n\n"
            message += f"💰 Your Current Balance: {format_currency(balance)}\n\n"
            message += f"Please enter the **Receiver's User ID**:\n"
            message += f"_(Only numeric ID, e.g., 123456789)_"
            
            # Sirf Back button
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("⬅️ Back to Balance", callback_data="balance"))
            
            edit_or_resend(
                call.message.chat.id,
                call.message.message_id,
                message,
                markup=markup,
                parse_mode="Markdown"
            )
            
            # Set user state for user ID input
            user_stage[user_id] = "waiting_receiver_id"
        
        elif data == "transfer_confirm":
            # Transfer confirmation screen
            transfer_data = user_states.get(user_id, {})
            if not transfer_data or "receiver_id" not in transfer_data or "amount" not in transfer_data:
                bot.answer_callback_query(call.id, "❌ Session expired", show_alert=True)
                clean_ui_and_send_menu(call.message.chat.id, user_id)
                return
            
            receiver_id = transfer_data["receiver_id"]
            receiver_name = transfer_data.get("receiver_name", f"ID: {receiver_id}")
            amount = transfer_data["amount"]
            sender_balance = get_balance(user_id)
            
            message = f"📤 **Confirm Transfer**\n\n"
            message += f"👤 Receiver: {receiver_name}\n"
            message += f"🆔 Receiver ID: `{receiver_id}`\n"
            message += f"💰 Amount: {format_currency(amount)}\n"
            message += f"💳 Your Balance: {format_currency(sender_balance)}\n\n"
            message += f"Are you sure you want to proceed?"
            
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("✅ Confirm", callback_data="transfer_execute"),
                InlineKeyboardButton("❌ Cancel", callback_data="balance")
            )
            
            edit_or_resend(
                call.message.chat.id,
                call.message.message_id,
                message,
                markup=markup,
                parse_mode="Markdown"
            )
        
        elif data == "transfer_execute":
            # Execute transfer
            transfer_data = user_states.get(user_id, {})
            if not transfer_data or "receiver_id" not in transfer_data or "amount" not in transfer_data:
                bot.answer_callback_query(call.id, "❌ Session expired", show_alert=True)
                clean_ui_and_send_menu(call.message.chat.id, user_id)
                return
            
            receiver_id = transfer_data["receiver_id"]
            receiver_name = transfer_data.get("receiver_name", f"ID: {receiver_id}")
            amount = transfer_data["amount"]
            
            success, message_text = transfer_balance(user_id, receiver_id, amount)
            
            if success:
                # Get updated balances
                sender_new_balance = get_balance(user_id)
                receiver_new_balance = get_balance(receiver_id)
                
                # Message for sender
                sender_message = f"✅ **Transfer Successful!**\n\n"
                sender_message += f"👤 Sent to: {receiver_name}\n"
                sender_message += f"🆔 Receiver ID: `{receiver_id}`\n"
                sender_message += f"💰 Amount Sent: {format_currency(amount)}\n"
                sender_message += f"💳 Your New Balance: {format_currency(sender_new_balance)}\n\n"
                
                # Sirf Back to Balance button
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("⬅️ Back to Balance", callback_data="balance"))
                
                edit_or_resend(
                    call.message.chat.id,
                    call.message.message_id,
                    sender_message,
                    markup=markup,
                    parse_mode="Markdown"
                )
                
                # Send notification to receiver
                try:
                    # Get sender name
                    sender = users_col.find_one({"user_id": user_id})
                    sender_name = sender.get("name", "Unknown") if sender else "Unknown"
                    
                    receiver_message = f"📥 **Balance Received!**\n\n"
                    receiver_message += f"👤 From: {sender_name}\n"
                    receiver_message += f"🆔 Sender ID: `{user_id}`\n"
                    receiver_message += f"💰 Amount Received: {format_currency(amount)}\n"
                    receiver_message += f"💳 Your New Balance: {format_currency(receiver_new_balance)}\n\n"
                    
                    # Sirf Close button for receiver
                    receiver_markup = InlineKeyboardMarkup()
                    receiver_markup.add(InlineKeyboardButton("❌ Close", callback_data="back_to_menu"))
                    
                    bot.send_message(
                        receiver_id,
                        receiver_message,
                        parse_mode="Markdown",
                        reply_markup=receiver_markup
                    )
                except Exception as e:
                    logger.warning(f"Could not notify receiver {receiver_id}: {e}")
                
            else:
                # Transfer failed
                markup = InlineKeyboardMarkup()
                markup.add(
                    InlineKeyboardButton("🔄 Try Again", callback_data="send_balance_menu"),
                    InlineKeyboardButton("⬅️ Back to Balance", callback_data="balance")
                )
                
                edit_or_resend(
                    call.message.chat.id,
                    call.message.message_id,
                    f"❌ **Transfer Failed!**\n\n{message_text}",
                    markup=markup,
                    parse_mode="Markdown"
                )
            
            # Clear transfer state
            if user_id in user_states:
                user_states.pop(user_id, None)
            if user_id in user_stage:
                user_stage.pop(user_id, None)
        
        elif data == "redeem_coupon":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            msg_text = "🎟 **Redeem Coupon**\n\nEnter your coupon code:"
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu"))
            
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            
            sent_msg = bot.send_message(
                call.message.chat.id,
                msg_text,
                parse_mode="Markdown",
                reply_markup=markup
            )
            user_last_message[user_id] = sent_msg.message_id
            user_stage[user_id] = "waiting_coupon"
        
        elif data == "recharge":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            show_recharge_methods(call.message.chat.id, call.message.message_id, user_id)
        
        elif data == "refer_friends":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            show_referral_info(user_id, call.message.chat.id)
        
        elif data == "support":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            msg_text = "🛠️ Support: @rchiex"
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu"))
            
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            
            sent_msg = bot.send_message(
                call.message.chat.id,
                msg_text,
                reply_markup=markup
            )
            user_last_message[user_id] = sent_msg.message_id
        
        elif data == "admin_panel":
            if is_admin(user_id):
                try:
                    bot.delete_message(call.message.chat.id, call.message.message_id)
                except:
                    pass
                show_admin_panel(call.message.chat.id)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data.startswith("bulk_account_"):
            if not is_admin(user_id):
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
                return
            
            country_name = data.replace("bulk_account_", "")
            
            bulk_add_states[user_id] = {
                "mode": "bulk",
                "country": country_name,
                "account_age": login_states.get(user_id, {}).get("account_age", "Fresh"),
                "phone_numbers": [],
                "current_index": 0,
                "total_numbers": 0,
                "success_count": 0,
                "failed_count": 0,
                "failed_numbers": [],
                "current_client": None,
                "current_phone_code_hash": None,
                "current_phone": None,
                "current_manager": None,
                "password_attempts": 0,
                "message_id": call.message.message_id,
                "step": "waiting_numbers",
                "chat_id": call.message.chat.id,
                "is_processing": False
            }
            
            edit_or_resend(
                call.message.chat.id,
                call.message.message_id,
                f"📦 **Bulk Account Addition**\n\n"
                f"🌍 Country: {country_name}\n\n"
                "📱 Enter phone numbers (one per line):\n"
                "Format:\n"
                "+91XXXXXXXXXX\n"
                "+91828XXXXXXX\n"
                "+91999XXXXXXX\n\n"
                "⚠️ Max 50 numbers at once\n"
                "⚠️ Include country code\n"
                "⚠️ One number per line",
                markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk")
                )
            )
        
        elif data.startswith("single_account_"):
            country_name = data.replace("single_account_", "")
            login_states[user_id]["country"] = country_name
            login_states[user_id]["step"] = "phone"
            login_states[user_id]["mode"] = "single"
            
            edit_or_resend(
                call.message.chat.id,
                call.message.message_id,
                f"🌍 Country: {country_name}\n\n"
                "📱 Enter phone number with country code:\n"
                "Example: +919876543210",
                markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")
                )
            )
        
        elif data == "start_bulk_add":
            if not is_admin(user_id):
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
                return
            
            if user_id not in bulk_add_states:
                bot.answer_callback_query(call.id, "❌ Session expired", show_alert=True)
                return
            
            state = bulk_add_states[user_id]
            if not state.get("phone_numbers"):
                bot.answer_callback_query(call.id, "❌ No phone numbers to process", show_alert=True)
                return
            
            bot.answer_callback_query(call.id, "🚀 Starting bulk account addition...")
            start_bulk_processing(user_id)
        
        elif data == "cancel_bulk":
            handle_cancel_bulk(call)

        elif data == "edit_bulk_numbers":
            if not is_admin(user_id):
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
                return
            if user_id not in bulk_add_states:
                bot.answer_callback_query(call.id, "❌ Session expired. Restart bulk add.", show_alert=True)
                return
            state = bulk_add_states[user_id]
            state["step"] = "waiting_numbers"
            state.pop("phone_numbers", None)
            bot.answer_callback_query(call.id, "✏️ Send new phone numbers")
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            sent = bot.send_message(
                call.message.chat.id,
                "✏️ <b>Edit Numbers</b>\n\nSend the phone numbers again (one per line):\n\nExample:\n+8801700000000\n+8801800000000",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk")
                )
            )
            user_last_message[user_id] = sent.message_id
        
        elif data == "pause_bulk":
            if user_id in bulk_add_states:
                bulk_add_states[user_id]["is_processing"] = False
                bot.answer_callback_query(call.id, "⏸️ Processing paused", show_alert=True)
        
        elif data == "resume_bulk":
            if user_id in bulk_add_states:
                bulk_add_states[user_id]["is_processing"] = True
                bot.answer_callback_query(call.id, "▶️ Processing resumed", show_alert=True)
                process_next_bulk_number(user_id)
        
        elif data == "skip_bulk_number":
            if user_id in bulk_add_states:
                state = bulk_add_states[user_id]
                state["failed_count"] += 1
                state["failed_numbers"].append({
                    "number": state.get("current_phone", "Unknown"),
                    "reason": "Skipped by admin"
                })
                
                if state.get("current_client") and account_manager:
                    try:
                        asyncio.run(account_manager.pyrogram_manager.safe_disconnect(state["current_client"]))
                    except:
                        pass
                
                state["current_index"] += 1
                state["password_attempts"] = 0
                bot.answer_callback_query(call.id, "⏭️ Number skipped", show_alert=True)
                process_next_bulk_number(user_id)
        
        elif data.startswith("country_raw_"):
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            country_name = data.replace("country_raw_", "")
            show_country_details(user_id, country_name, call.message.chat.id, call.message.message_id, call.id)
        
        elif data.startswith("buy_"):
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            # Correctly strip the "buy_account_" prefix (12 chars)
            account_id = data[len("buy_account_"):]
            _account = None
            _oid = safe_obj_id(account_id)
            if _oid:
                try:
                    _account = accounts_col.find_one({"_id": _oid})
                except Exception:
                    pass
            if not _account:
                try:
                    _account = accounts_col.find_one({"_id": account_id})
                except Exception:
                    pass
            if not _account:
                bot.answer_callback_query(call.id, "❌ Account not available", show_alert=True)
            else:
                process_purchase(user_id, _account, call.message.chat.id, call.message.message_id, call.id)
        
        elif data.startswith("logout_session_"):
            session_id = data.split("_", 2)[2]
            handle_logout_session(user_id, session_id, call.message.chat.id, call.message.message_id, call.id)
        
        elif data.startswith("get_otp_"):
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            session_id = data.split("_", 2)[2]
            get_latest_otp(user_id, session_id, call.message.chat.id, call.message.message_id, call.id)
        
        elif data == "back_to_countries":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id,
                    f"❌ Please join:\n{missing_list}",
                    show_alert=True
                )
                start(call.message)
                return
            bot.answer_callback_query(call.id, "")
            show_countries(call.message.chat.id, page=0, message_id=call.message.message_id)

        elif data.startswith("countries_pg_"):
            if not has_user_joined_channels(user_id):
                bot.answer_callback_query(call.id, "❌ Join required channels first!", show_alert=True)
                return
            try:
                page = int(data.split("_")[-1])
            except:
                page = 0
            bot.answer_callback_query(call.id, "")
            show_countries(call.message.chat.id, page=page, message_id=call.message.message_id)
        
        elif data == "back_to_menu":
            clean_ui_and_send_menu(call.message.chat.id, user_id)
        
        elif data == "recharge_upi":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            recharge_method_state[user_id] = "upi"
            edit_or_resend(
                call.message.chat.id,
                call.message.message_id,
                "💳 Enter recharge amount for UPI (minimum ₹3):",
                markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("❌ Cancel", callback_data="back_to_menu")
                )
            )
            bot.register_next_step_handler(call.message, process_recharge_amount)
        
        elif data == "recharge_crypto":
            if not has_user_joined_channels(user_id):
                missing_channels = get_missing_channels(user_id)
                missing_list = "\n".join([f"• {ch}" for ch in missing_channels])
                bot.answer_callback_query(
                    call.id, 
                    f"❌ Please join:\n{missing_list}", 
                    show_alert=True
                )
                start(call.message)
                return
            
            recharge_method_state[user_id] = "crypto"
            edit_or_resend(
                call.message.chat.id,
                call.message.message_id,
                "💳 Enter recharge amount in INR for Crypto (minimum ₹10):",
                markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("❌ Cancel", callback_data="back_to_menu")
                )
            )
            bot.register_next_step_handler(call.message, process_recharge_amount)
        
        elif data == "upi_deposited":
            user_id = call.from_user.id
            amount = upi_payment_states.get(user_id, {}).get("amount", 0)
            if amount <= 0:
                bot.answer_callback_query(call.id, "❌ Invalid amount", show_alert=True)
                return
            
            bot.answer_callback_query(call.id, "📝 Please send your 12-digit UTR number", show_alert=False)
            
            upi_payment_states[user_id] = {
                "step": "waiting_utr",
                "amount": amount,
                "chat_id": call.message.chat.id
            }
            
            bot.send_message(
                call.message.chat.id,
                "📝 **Step 1: Enter UTR**\n\n"
                "Please send your 12-digit UTR number:\n"
                "_(Sent by your bank after payment)_"
            )
        
        elif data.startswith("approve_rech|") or data.startswith("cancel_rech|"):
            if is_admin(user_id):
                parts = data.split("|")
                action = parts[0]
                req_id = parts[1] if len(parts) > 1 else None
                
                # Process approval/rejection
                success, message, admin_info = process_recharge_approval(user_id, req_id, 
                                                                        "approve" if action == "approve_rech" else "reject")
                
                if success:
                    bot.answer_callback_query(call.id, message, show_alert=True)

                    # Delete the original recharge request message
                    try:
                        bot.delete_message(call.message.chat.id, call.message.message_id)
                    except:
                        pass

                    action_done = admin_info['action']  # "approved" or "rejected"
                    emoji = "✅" if action_done == "approved" else "❌"
                    target_uid = admin_info.get("user_id", "?")
                    amt = admin_info.get("amount", 0)

                    # Admin confirmation message
                    admin_action_msg = (
                        f"╔══════════════════╗\n"
                        f"  𝐋𝐄𝐆𝐄𝐍𝐃𝐀𝐑𝐘 𝐗 𝐎𝐓𝐏\n"
                        f"╚══════════════════╝\n\n"
                        f"{emoji} <b>Recharge {action_done.upper()}</b>\n\n"
                        f"👤 User ID: <code>{target_uid}</code>\n"
                        f"💰 Amount: <b>₹{amt:,.0f}</b>\n"
                        f"🛡 Processed by: <b>{admin_info['admin_name']}</b>\n"
                        f"🆔 Admin ID: <code>{admin_info['admin_id']}</code>\n"
                        f"📋 Req ID: <code>{req_id}</code>\n"
                        f"⏰ {datetime.utcnow().strftime('%d %b %Y, %H:%M')} UTC"
                    )
                    bot.send_message(
                        call.message.chat.id,
                        admin_action_msg,
                        parse_mode="HTML"
                    )

                    # Also notify all other admins about who processed it
                    try:
                        all_admins = get_all_admins()
                        for adm in all_admins:
                            if adm["user_id"] != admin_id:
                                try:
                                    bot.send_message(
                                        adm["user_id"],
                                        f"{emoji} <b>Recharge {action_done.upper()}</b> by <b>{admin_info['admin_name']}</b>\n"
                                        f"👤 User: <code>{target_uid}</code>  💰 ₹{amt:,.0f}\n"
                                        f"📋 Req: <code>{req_id}</code>",
                                        parse_mode="HTML"
                                    )
                                except:
                                    pass
                    except:
                        pass
                else:
                    bot.answer_callback_query(call.id, f"❌ {message}", show_alert=True)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "add_account":
            logger.info(f"Add account button clicked by user {user_id}")
            if not is_admin(user_id):
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
                return
            
            login_states[user_id] = {
                "step": "select_country",
                "message_id": call.message.message_id,
                "chat_id": call.message.chat.id
            }
            
            countries = get_all_countries()
            if not countries:
                bot.answer_callback_query(call.id, "❌ No countries available. Add a country first.", show_alert=True)
                return
            
            markup = InlineKeyboardMarkup(row_width=2)
            for country in countries:
                markup.add(InlineKeyboardButton(
                    country['name'],
                    callback_data=f"login_country_{country['name']}"
                ))
            markup.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_login"))
            
            edit_or_resend(
                call.message.chat.id,
                call.message.message_id,
                "🌍 **Select Country for Account**\n\nChoose country:",
                markup=markup
            )
        
        elif data.startswith("login_country_"):
            handle_login_country_selection(call)

        elif data.startswith("acc_age_"):
            if not is_admin(user_id):
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
                return
            if user_id not in login_states:
                bot.answer_callback_query(call.id, "❌ Session expired. Click Add Account again.", show_alert=True)
                return
            age_key = data.replace("acc_age_", "")
            account_age = age_key.replace("_", " ")
            login_states[user_id]["account_age"] = account_age
            country_name = login_states[user_id].get("country", "Unknown")
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("➕ Single Account", callback_data=f"single_account_{country_name}"),
                InlineKeyboardButton("📦 Bulk Accounts", callback_data=f"bulk_account_{country_name}")
            )
            markup.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_login"))
            edit_or_resend(
                call.message.chat.id,
                call.message.message_id,
                f"🌍 <b>Country:</b> {country_name}\n"
                f"🗓️ <b>Age:</b> {account_age}\n\n"
                f"📱 <b>Select account adding mode:</b>",
                markup=markup,
                parse_mode="HTML"
            )
        
        elif data == "cancel_login":
            handle_cancel_login(call)
        
        elif data == "out_of_stock":
            bot.answer_callback_query(call.id, "❌ Out of Stock! No accounts available.", show_alert=True)
        
        elif data == "edit_price":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Processing...")
                show_edit_price_country_selection(call.message.chat.id, call.message.message_id)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data.startswith("edit_price_country_"):
            if is_admin(user_id):
                country_name = data.replace("edit_price_country_", "")
                show_edit_price_details(call.message.chat.id, call.message.message_id, country_name)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data.startswith("edit_price_confirm_"):
            if is_admin(user_id):
                country_name = data.replace("edit_price_confirm_", "")
                edit_price_state[user_id] = {"country": country_name, "step": "waiting_price"}
                try:
                    country = get_country_by_name(country_name)
                    if country:
                        current_price = country.get("price", 0)
                        edit_or_resend(
                            call.message.chat.id,
                            call.message.message_id,
                            f"🌍 Country: {country_name}\n💰 Current Price: {format_currency(current_price)}\n\n"
                            f"Enter new price for {country_name}:",
                            markup=InlineKeyboardMarkup().add(
                                InlineKeyboardButton("❌ Cancel", callback_data="manage_countries")
                            )
                        )
                    else:
                        bot.answer_callback_query(call.id, "❌ Country not found", show_alert=True)
                except:
                    pass
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "cancel_edit_price":
            if is_admin(user_id):
                show_country_management(call.message.chat.id)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "admin_coupon_menu":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "🎟 Coupon Management")
                show_coupon_management(call.message.chat.id, call.message.message_id)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "admin_create_coupon":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Creating coupon...")
                coupon_state[user_id] = {"step": "ask_code"}
                edit_or_resend(
                    call.message.chat.id,
                    call.message.message_id,
                    "🎟 **Create Coupon**\n\nEnter coupon code:",
                    markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("❌ Cancel", callback_data="admin_coupon_menu")
                    ),
                    parse_mode="Markdown"
                )
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "admin_remove_coupon":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Removing coupon...")
                coupon_state[user_id] = {"step": "ask_remove_code"}
                edit_or_resend(
                    call.message.chat.id,
                    call.message.message_id,
                    "🗑 **Remove Coupon**\n\nEnter coupon code to remove:",
                    markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("❌ Cancel", callback_data="admin_coupon_menu")
                    ),
                    parse_mode="Markdown"
                )
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "admin_coupon_status":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Checking coupon status...")
                coupon_state[user_id] = {"step": "ask_status_code"}
                edit_or_resend(
                    call.message.chat.id,
                    call.message.message_id,
                    "📊 **Coupon Status**\n\nEnter coupon code to check:",
                    markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("❌ Cancel", callback_data="admin_coupon_menu")
                    ),
                    parse_mode="Markdown"
                )
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "broadcast_menu":
            if is_admin(user_id):
                global IS_BROADCASTING
                bot.answer_callback_query(call.id, "📢 Broadcast Panel")
                status_txt = "🔴 BUSY (another broadcast running)" if IS_BROADCASTING else "🟢 Ready"
                total_users = users_col.count_documents({})
                broadcast_msg = (
                    "📢 **Broadcast Panel**\n\n"
                    f"📡 Status: {status_txt}\n"
                    f"👥 Total Users: {total_users}\n\n"
                    "**How to broadcast:**\n"
                    "1️⃣ Send or forward any message in this chat\n"
                    "2️⃣ Reply to that message with `/sendbroadcast`\n\n"
                    "📌 **Options:**\n"
                    "• `/sendbroadcast` — Send to all users\n"
                    "• `/sendbroadcast -pin` — Send + auto-pin (silent)\n"
                    "• `/sendbroadcast -pinloud` — Send + pin with notification\n\n"
                    "⚠️ If stuck, use `/resetbroadcast`"
                )
                bot.send_message(call.message.chat.id, broadcast_msg, parse_mode="Markdown")
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "refund_start":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Processing...")
                msg = bot.send_message(call.message.chat.id, "💸 Enter user ID for refund:")
                bot.register_next_step_handler(msg, ask_refund_user)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "ranking":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "📊 Generating ranking...")
                show_user_ranking(call.message.chat.id)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "message_user":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "👤 Enter user ID to send message:")
                msg = bot.send_message(call.message.chat.id, "👤 Enter user ID to send message:")
                bot.register_next_step_handler(msg, ask_message_content)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "admin_deduct_start":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Processing...")
                admin_deduct_state[user_id] = {"step": "ask_user_id"}
                msg = bot.send_message(call.message.chat.id, "👤 Enter User ID whose balance you want to deduct:")
                if user_id in broadcast_data:
                    del broadcast_data[user_id]
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "ban_user":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Processing...")
                msg = bot.send_message(call.message.chat.id, "🚫 Enter User ID to ban:")
                bot.register_next_step_handler(msg, ask_ban_user)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "unban_user":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Processing...")
                msg = bot.send_message(call.message.chat.id, "✅ Enter User ID to unban:")
                bot.register_next_step_handler(msg, ask_unban_user)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "manage_countries":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Processing...")
                show_country_management(call.message.chat.id)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "add_country":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "🌍 Select Country")
                show_world_country_picker(call.message.chat.id, call.message.message_id, page=0)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)

        elif data.startswith("wc_pg_"):
            if is_admin(user_id):
                try:
                    page = int(data.split("_")[-1])
                except:
                    page = 0
                bot.answer_callback_query(call.id)
                show_world_country_picker(call.message.chat.id, call.message.message_id, page=page)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)

        elif data.startswith("wc_sel_"):
            if is_admin(user_id):
                country_name = data[7:]
                bot.answer_callback_query(call.id, f"Selected: {country_name}")
                flag = get_country_flag(country_name)
                dial = get_country_code(country_name)
                user_states[user_id] = {"step": "ask_country_price", "country_name": country_name}
                try:
                    bot.edit_message_text(
                        f"🌍 <b>Adding Country</b>\n\n"
                        f"{flag} <b>{country_name}</b> {dial}\n\n"
                        f"💰 Enter price for this country (e.g. 150):",
                        call.message.chat.id, call.message.message_id,
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup().add(
                            InlineKeyboardButton("⬅️ Back", callback_data="add_country")
                        )
                    )
                except:
                    pass
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)

        elif data == "wc_custom":
            if is_admin(user_id):
                bot.answer_callback_query(call.id)
                try:
                    bot.edit_message_text(
                        "✏️ <b>Custom Country Name</b>\n\nType the country name:",
                        call.message.chat.id, call.message.message_id,
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup().add(
                            InlineKeyboardButton("⬅️ Back", callback_data="add_country")
                        )
                    )
                except:
                    pass
                msg = bot.send_message(call.message.chat.id, "🌍 Enter custom country name:")
                bot.register_next_step_handler(msg, ask_country_name)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data == "remove_country":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "Processing...")
                show_country_removal(call.message.chat.id)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        
        elif data.startswith("remove_country_"):
            if is_admin(user_id):
                country_name = data.split("_", 2)[2]
                result = remove_country(country_name, call.message.chat.id, call.message.message_id)
                bot.answer_callback_query(call.id, result, show_alert=True)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)

        # ── AI Chat ──────────────────────────────────────────────────
        elif data == "ai_chat":
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🚪 Exit AI Chat", callback_data="exit_ai_chat"))
            sent = bot.send_message(
                call.message.chat.id,
                "╔══════════════════════╗\n"
                "  ✨ <b>˹ 𝐋ᴇɢᴇɴᴅᴀʀʏ 𝐀𝐈 𝐀𝐬𝐬𝐢𝐬𝐭𝐚𝐧𝐭 ˺</b> ✨\n"
                "╚══════════════════════╝\n\n"
                "🟢 <b>Online &amp; Ready!</b>\n\n"
                "💬 Mujhse kuch bhi pucho:\n"
                "  • Math, Science, Coding\n"
                "  • Writing, Translation\n"
                "  • Bot support, OTP help\n"
                "  • Ya kuch bhi!\n\n"
                "⚡ <i>Bas message bhejo — main hoon yahan!</i>",
                parse_mode="HTML",
                reply_markup=markup
            )
            user_stage[user_id] = "ai_chat"
            user_last_message[user_id] = sent.message_id

        elif data == "exit_ai_chat":
            user_stage.pop(user_id, None)
            gemini_chat_sessions.pop(user_id, None)
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            clean_ui_and_send_menu(call.message.chat.id, user_id)

        # ── Direct Buy Now ────────────────────────────────────────────
        elif data.startswith("buy_now_"):
            country_name = data[8:]
            bot.answer_callback_query(call.id, "⏳ Processing...")
            logger.info(f"buy_now: country='{country_name}' user={user_id}")
            # Match same query as get_available_accounts_count — status active OR missing
            account = accounts_col.find_one({
                "country": country_name,
                "used": {"$ne": True},
                "$or": [{"status": "active"}, {"status": {"$exists": False}}]
            })
            logger.info(f"buy_now: query1 result={'found' if account else 'None'} for country='{country_name}'")
            if not account:
                account = accounts_col.find_one({"country": country_name, "used": {"$ne": True}})
                logger.info(f"buy_now: query2 result={'found' if account else 'None'}")
            if not account:
                account = accounts_col.find_one({
                    "country": {"$regex": f"^{re.escape(country_name)}$", "$options": "i"},
                    "used": {"$ne": True}
                })
                logger.info(f"buy_now: query3 result={'found' if account else 'None'}")
            if not account:
                total = accounts_col.count_documents({"country": country_name})
                used = accounts_col.count_documents({"country": country_name, "used": True})
                logger.warning(f"buy_now: No account found for country='{country_name}'. Total={total} Used={used}")
                bot.answer_callback_query(call.id, "❌ Out of Stock! No accounts available right now.", show_alert=True)
                return
            logger.info(f"buy_now: account found _id={account.get('_id')} status={account.get('status')} used={account.get('used')}")
            process_purchase(user_id, account, call.message.chat.id, call.message.message_id, call.id)

        # ── Legacy srv1/srv2 kept for backwards compat ────────────────
        elif data.startswith("srv1_") or data.startswith("srv2_"):
            country_name = data[5:]
            bot.answer_callback_query(call.id, "⏳ Processing...")
            account = accounts_col.find_one({"country": country_name, "status": "active", "used": {"$ne": True}})
            if not account:
                account = accounts_col.find_one({"country": country_name, "used": {"$ne": True}})
            if not account:
                bot.answer_callback_query(call.id, "❌ No accounts available right now!", show_alert=True)
                return
            process_purchase(user_id, account, call.message.chat.id, call.message.message_id, call.id)

        # ── Manage Admins Panel — OWNER ONLY ─────────────────────────
        elif data == "manage_admins_panel":
            if is_super_admin(user_id):
                bot.answer_callback_query(call.id, "👥 Admin Management")
                show_manage_admins_panel(call.message.chat.id, call.message.message_id)
            else:
                bot.answer_callback_query(call.id, "❌ Only the owner can manage admins!", show_alert=True)

        elif data == "admin_add_new":
            if is_super_admin(user_id):
                bot.answer_callback_query(call.id, "")
                admin_add_state[user_id] = {"step": "waiting_user_id"}
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("❌ Cancel", callback_data="manage_admins_panel"))
                try:
                    bot.edit_message_text(
                        "👤 <b>Add New Admin</b>\n\nEnter the User ID of the person to make admin:",
                        call.message.chat.id, call.message.message_id,
                        parse_mode="HTML", reply_markup=markup
                    )
                except:
                    bot.send_message(call.message.chat.id, "👤 Enter User ID to make admin:", reply_markup=markup)
            else:
                bot.answer_callback_query(call.id, "❌ Only main admin can add admins!", show_alert=True)

        elif data == "admin_remove_existing":
            if is_super_admin(user_id):
                bot.answer_callback_query(call.id, "")
                admins = get_all_admins()
                non_super = [a for a in admins if not a.get("is_super_admin", False)]
                if not non_super:
                    bot.answer_callback_query(call.id, "No sub-admins to remove.", show_alert=True)
                    return
                markup = InlineKeyboardMarkup(row_width=1)
                for adm in non_super:
                    markup.add(InlineKeyboardButton(
                        f"❌ Remove {adm['user_id']} — {adm.get('name','?')}",
                        callback_data=f"confirm_remove_admin_{adm['user_id']}"
                    ))
                markup.add(InlineKeyboardButton("⬅️ Back", callback_data="manage_admins_panel"))
                try:
                    bot.edit_message_text(
                        "🗑 <b>Remove Admin</b>\n\nSelect admin to remove:",
                        call.message.chat.id, call.message.message_id,
                        parse_mode="HTML", reply_markup=markup
                    )
                except:
                    pass
            else:
                bot.answer_callback_query(call.id, "❌ Only main admin!", show_alert=True)

        elif data.startswith("confirm_remove_admin_"):
            if is_super_admin(user_id):
                try:
                    target_id = int(data.split("_")[-1])
                except (ValueError, IndexError):
                    bot.answer_callback_query(call.id, "❌ Invalid admin ID", show_alert=True)
                    return
                success, msg_text = remove_admin(target_id, user_id)
                bot.answer_callback_query(call.id, msg_text, show_alert=True)
                try:
                    bot.send_message(target_id, "⚠️ <b>Your admin access has been removed.</b>", parse_mode="HTML")
                except:
                    pass
                show_manage_admins_panel(call.message.chat.id, call.message.message_id)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)

        # ── Pending Recharges List ────────────────────────────────────
        elif data == "admin_permissions":
            if is_super_admin(user_id):
                bot.answer_callback_query(call.id, "🔐 Admin Permissions")
                show_admin_permissions_panel(call.message.chat.id, call.message.message_id)
            else:
                bot.answer_callback_query(call.id, "❌ Only owner can manage permissions!", show_alert=True)

        elif data.startswith("toggle_perm_"):
            if not is_super_admin(user_id):
                bot.answer_callback_query(call.id, "❌ Only owner can change permissions!", show_alert=True)
                return
            parts = data.split("_", 3)  # toggle_perm_USERID_PERMNAME
            if len(parts) < 4:
                bot.answer_callback_query(call.id, "❌ Invalid action", show_alert=True)
                return
            target_uid = int(parts[2])
            perm_name = parts[3]
            admin_doc = admins_col.find_one({"user_id": target_uid})
            if not admin_doc:
                bot.answer_callback_query(call.id, "❌ Admin not found", show_alert=True)
                return
            perms = admin_doc.get("permissions", {})
            current = perms.get(perm_name, True)
            perms[perm_name] = not current
            admins_col.update_one({"user_id": target_uid}, {"$set": {"permissions": perms}})
            status = "✅ ON" if not current else "❌ OFF"
            bot.answer_callback_query(call.id, f"{perm_name}: {status}", show_alert=False)
            show_admin_permissions_panel(call.message.chat.id, call.message.message_id)

        elif data.startswith("view_perms_"):
            if is_super_admin(user_id):
                try:
                    target_uid = int(data.split("_")[-1])
                except (ValueError, IndexError):
                    bot.answer_callback_query(call.id, "❌ Invalid ID", show_alert=True)
                    return
                show_single_admin_perms(call.message.chat.id, call.message.message_id, target_uid)
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)

        elif data == "pending_recharges_list":
            if is_admin(user_id):
                bot.answer_callback_query(call.id, "💳 Pending Recharges")
                pending = list(recharges_col.find({"status": "pending"}).sort("created_at", -1).limit(10))
                if not pending:
                    bot.send_message(call.message.chat.id, "✅ No pending recharges right now!")
                    return
                for r in pending:
                    req_id = r.get("req_id", str(r["_id"]))
                    txt = (
                        f"💳 <b>Pending Recharge</b>\n\n"
                        f"👤 User: <code>{r['user_id']}</code>\n"
                        f"💰 Amount: {format_currency(r['amount'])}\n"
                        f"🔢 UTR: {r.get('utr','N/A')}\n"
                        f"🆔 Req ID: <code>{req_id}</code>"
                    )
                    markup = InlineKeyboardMarkup(row_width=2)
                    markup.add(
                        InlineKeyboardButton("✅ Approve", callback_data=f"approve_rech|{req_id}"),
                        InlineKeyboardButton("❌ Reject", callback_data=f"cancel_rech|{req_id}")
                    )
                    try:
                        if r.get("screenshot"):
                            bot.send_photo(call.message.chat.id, r["screenshot"], caption=txt, parse_mode="HTML", reply_markup=markup)
                        else:
                            bot.send_message(call.message.chat.id, txt, parse_mode="HTML", reply_markup=markup)
                    except:
                        pass
            else:
                bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)

        else:
            bot.answer_callback_query(call.id, "❌ Unknown action", show_alert=True)
    
    except Exception as e:
        logger.error(f"Callback error: {e}")
        try:
            bot.answer_callback_query(call.id, "❌ Error occurred", show_alert=True)
            if is_admin(user_id):
                bot.send_message(call.message.chat.id, f"Callback handler error:\n{e}")
        except:
            pass

# ---------------------------------------------------------------------
# BULK ACCOUNT FUNCTIONS
# ---------------------------------------------------------------------

def handle_cancel_bulk(call):
    user_id = call.from_user.id
    
    if user_id in bulk_add_states:
        state = bulk_add_states[user_id]
        
        if state.get("current_client") and account_manager:
            try:
                def _disc():
                    import asyncio as _aio
                    loop = _aio.new_event_loop()
                    _aio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(account_manager.pyrogram_manager.safe_disconnect(state["current_client"]))
                    finally:
                        loop.close()
                threading.Thread(target=_disc, daemon=True).start()
            except:
                pass
        
        del bulk_add_states[user_id]
    
    edit_or_resend(
        call.message.chat.id,
        call.message.message_id,
        "❌ Bulk account addition cancelled.",
        markup=None
    )
    show_admin_panel(call.message.chat.id)

@bot.message_handler(func=lambda m: bulk_add_states.get(m.from_user.id, {}).get("step") == "waiting_numbers")
def handle_bulk_numbers_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    if state.get("step") != "waiting_numbers":
        return
    
    text = msg.text.strip()
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    valid_numbers = []
    invalid_numbers = []
    
    for line in lines[:100]:
        cleaned = line.strip()
        # Remove spaces/dashes from number
        cleaned_digits = re.sub(r'[\s\-\(\)]', '', cleaned)
        if cleaned_digits.startswith('+') and re.match(r'^\+\d{6,15}$', cleaned_digits):
            valid_numbers.append(cleaned_digits)
        elif re.match(r'^\d{6,15}$', cleaned_digits):
            valid_numbers.append('+' + cleaned_digits)
        else:
            invalid_numbers.append(cleaned)
    
    if not valid_numbers:
        bot.send_message(
            msg.chat.id,
            "❌ No valid phone numbers found.\n"
            "Please enter numbers with country code (one per line).\n"
            "Example: +79123456789 or +8613800138000"
        )
        return
    
    state["phone_numbers"] = valid_numbers
    state["total_numbers"] = len(valid_numbers)
    state["step"] = "confirm_numbers"
    
    message = f"📦 **Bulk Account Addition**\n\n"
    message += f"🌍 Country: {state['country']}\n"
    message += f"📱 Total Numbers: {len(valid_numbers)}\n"
    
    if invalid_numbers:
        message += f"⚠️ Invalid (skipped): {len(invalid_numbers)}\n"
    
    message += f"\n**First 5 numbers:**\n"
    for i, num in enumerate(valid_numbers[:5], 1):
        message += f"{i}. `{num}`\n"
    
    if len(valid_numbers) > 5:
        message += f"... and {len(valid_numbers) - 5} more\n"
    
    message += f"\nClick below to start adding accounts:"
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("▶️ Start Adding Accounts", callback_data="start_bulk_add"),
        InlineKeyboardButton("✏️ Edit Numbers", callback_data="edit_bulk_numbers")
    )
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk"))
    
    sent_msg = bot.send_message(msg.chat.id, message, parse_mode="Markdown", reply_markup=markup)
    state["message_id"] = sent_msg.message_id
    user_last_message[user_id] = sent_msg.message_id

def start_bulk_processing(user_id):
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    state["is_processing"] = True
    
    edit_or_resend(
        state["chat_id"],
        state["message_id"],
        f"🚀 **Bulk Processing Started**\n\n"
        f"🌍 Country: {state['country']}\n"
        f"📱 Total: {state['total_numbers']} numbers\n"
        f"⏳ Processing first number...",
        markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("⏸️ Pause", callback_data="pause_bulk"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk")
        )
    )
    
    process_next_bulk_number(user_id)

def process_next_bulk_number(user_id):
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    
    if not state.get("is_processing", True):
        return
    
    if state["current_index"] >= state["total_numbers"]:
        show_bulk_summary(user_id)
        return
    
    phone_number = state["phone_numbers"][state["current_index"]]
    state["current_phone"] = phone_number
    state["password_attempts"] = 0
    
    progress = state["current_index"] + 1
    total = state["total_numbers"]
    percentage = (progress / total) * 100
    
    edit_or_resend(
        state["chat_id"],
        state["message_id"],
        f"🔄 **Processing Number {progress}/{total}**\n\n"
        f"📱 Phone: `{phone_number}`\n"
        f"📊 Progress: {progress}/{total} ({percentage:.1f}%)\n"
        f"✅ Success: {state['success_count']}\n"
        f"❌ Failed: {state['failed_count']}\n\n"
        f"⏳ Sending OTP...",
        markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("⏸️ Pause", callback_data="pause_bulk"),
            InlineKeyboardButton("⏭️ Skip", callback_data="skip_bulk_number"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk")
        )
    )
    
    send_bulk_otp(user_id, phone_number)

def send_bulk_otp(user_id, phone_number):
    try:
        if not account_manager:
            bulk_number_failed(user_id, "Account module not loaded")
            return
        
        state = bulk_add_states[user_id]
        
        result = account_manager.bulk_send_code_sync(phone_number)
        
        if result.get("success"):
            state["current_client"] = result["client"]
            state["current_phone_code_hash"] = result["phone_code_hash"]
            state["current_manager"] = result["manager"]
            state["step"] = "waiting_bulk_otp"
            
            edit_or_resend(
                state["chat_id"],
                state["message_id"],
                f"📱 Phone: `{phone_number}`\n\n"
                f"✅ OTP sent!\n"
                f"Please enter the OTP received for this number:\n\n"
                f"_(Type 'skip' to skip this number)_",
                markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("⏭️ Skip This Number", callback_data="skip_bulk_number"),
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk")
                )
            )
        else:
            error_msg = result.get("error", "Unknown error")
            bulk_number_failed(user_id, f"Failed to send OTP: {error_msg}")
    
    except Exception as e:
        logger.error(f"Bulk send OTP error: {e}")
        bulk_number_failed(user_id, f"Error: {str(e)}")

def bulk_number_failed(user_id, reason):
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    state["failed_count"] += 1
    state["failed_numbers"].append({
        "number": state.get("current_phone", "Unknown"),
        "reason": reason
    })
    
    if state.get("current_client") and account_manager:
        try:
            asyncio.run(account_manager.pyrogram_manager.safe_disconnect(state["current_client"]))
        except:
            pass
    
    state["current_index"] += 1
    state["password_attempts"] = 0
    process_next_bulk_number(user_id)

def bulk_number_success(user_id):
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    state["success_count"] += 1
    
    if state.get("current_client") and account_manager:
        try:
            asyncio.run(account_manager.pyrogram_manager.safe_disconnect(state["current_client"]))
        except:
            pass
    
    state["current_index"] += 1
    state["password_attempts"] = 0
    process_next_bulk_number(user_id)

@bot.message_handler(func=lambda m: bulk_add_states.get(m.from_user.id, {}).get("step") == "waiting_bulk_otp")
def handle_bulk_otp_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    if state.get("step") != "waiting_bulk_otp":
        return
    
    otp_code = msg.text.strip()
    
    if otp_code.lower() == 'skip':
        bulk_number_failed(user_id, "Skipped by admin")
        return
    
    if not otp_code.isdigit() or len(otp_code) != 5:
        bot.send_message(
            msg.chat.id,
            "❌ Invalid OTP format. Please enter 5-digit OTP or type 'skip' to skip:"
        )
        return
    
    try:
        result = account_manager.bulk_verify_otp_sync(
            state["current_client"],
            state["current_phone"],
            state["current_phone_code_hash"],
            otp_code,
            state["current_manager"]
        )
        
        if result.get("success"):
            save_bulk_account(user_id)
        
        elif result.get("status") == "password_required":
            state["step"] = "waiting_bulk_password"
            state["password_attempts"] = 0
            
            edit_or_resend(
                state["chat_id"],
                state["message_id"],
                f"📱 Phone: `{state['current_phone']}`\n\n"
                f"🔐 2FA Password required!\n"
                f"Enter your 2-step verification password:\n\n"
                f"_(Type 'skip' to skip this number)_",
                markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("⏭️ Skip This Number", callback_data="skip_bulk_number"),
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk")
                )
            )
        
        else:
            error_msg = result.get("error", "OTP verification failed")
            bulk_number_failed(user_id, f"OTP error: {error_msg}")
    
    except Exception as e:
        logger.error(f"Bulk OTP verification error: {e}")
        bulk_number_failed(user_id, f"OTP error: {str(e)}")

@bot.message_handler(func=lambda m: bulk_add_states.get(m.from_user.id, {}).get("step") == "waiting_bulk_password")
def handle_bulk_password_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    if state.get("step") != "waiting_bulk_password":
        return
    
    password = msg.text.strip()
    
    if password.lower() == 'skip':
        bulk_number_failed(user_id, "Skipped by admin")
        return
    
    if not password:
        bot.send_message(
            msg.chat.id,
            "❌ Password cannot be empty. Enter 2FA password or type 'skip' to skip:"
        )
        return
    
    state["password_attempts"] = state.get("password_attempts", 0) + 1
    
    if state["password_attempts"] > 2:
        bulk_number_failed(user_id, "Max password attempts exceeded")
        return
    
    try:
        result = account_manager.bulk_verify_password_sync(
            state["current_client"],
            password,
            state["current_manager"]
        )
        
        if result.get("success"):
            save_bulk_account(user_id, password)
        else:
            error_msg = result.get("error", "Incorrect password")
            
            if state["password_attempts"] >= 2:
                bulk_number_failed(user_id, f"Password error: {error_msg}")
            else:
                attempts_left = 2 - state["password_attempts"]
                bot.send_message(
                    msg.chat.id,
                    f"❌ Incorrect password. {attempts_left} attempt(s) left.\n"
                    f"Enter password again or type 'skip' to skip:"
                )
    
    except Exception as e:
        logger.error(f"Bulk password verification error: {e}")
        bulk_number_failed(user_id, f"Password error: {str(e)}")

def save_bulk_account(user_id, password=None):
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    
    try:
        success, message = account_manager.bulk_save_account_sync(
            state["current_client"],
            state["current_phone"],
            state["country"],
            user_id,
            state["current_manager"],
            accounts_col,
            password,
            state.get("account_age", "Fresh")
        )
        
        if success:
            progress = state["current_index"] + 1
            total = state["total_numbers"]
            
            edit_or_resend(
                state["chat_id"],
                state["message_id"],
                f"✅ **Number {progress}/{total} Added Successfully!**\n\n"
                f"📱 Phone: `{state['current_phone']}`\n"
                f"🌍 Country: {state['country']}\n"
                f"🔐 2FA: {'✅ Enabled' if password else '❌ Disabled'}\n\n"
                f"📊 Progress: {progress}/{total}\n"
                f"✅ Success: {state['success_count'] + 1}\n"
                f"❌ Failed: {state['failed_count']}\n\n"
                f"⏳ Moving to next number...",
                markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("⏸️ Pause", callback_data="pause_bulk"),
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk")
                )
            )
            
            bulk_number_success(user_id)
        
        else:
            bulk_number_failed(user_id, f"Save error: {message}")
    
    except Exception as e:
        logger.error(f"Bulk save account error: {e}")
        bulk_number_failed(user_id, f"Save error: {str(e)}")

def show_bulk_summary(user_id):
    if user_id not in bulk_add_states:
        return
    
    state = bulk_add_states[user_id]
    
    summary = f"📊 **Bulk Processing Complete!**\n\n"
    summary += f"🌍 Country: {state['country']}\n"
    summary += f"📱 Total Numbers: {state['total_numbers']}\n"
    summary += f"✅ Successfully Added: {state['success_count']}\n"
    summary += f"❌ Failed/Skipped: {state['failed_count']}\n\n"
    
    if state['failed_numbers']:
        summary += f"**Failed Numbers:**\n"
        for i, failed in enumerate(state['failed_numbers'][:10], 1):
            summary += f"{i}. {failed['number']} - {failed['reason']}\n"
        
        if len(state['failed_numbers']) > 10:
            summary += f"... and {len(state['failed_numbers']) - 10} more\n"
    
    summary += f"\n⏰ Completed at: {datetime.utcnow().strftime('%H:%M:%S')}"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🏠 Admin Panel", callback_data="admin_panel"))
    
    edit_or_resend(
        state["chat_id"],
        state["message_id"],
        summary,
        markup=markup
    )
    
    del bulk_add_states[user_id]

# ---------------------------------------------------------------------
# EXISTING FUNCTIONS
# ---------------------------------------------------------------------

def handle_login_country_selection(call):
    user_id = call.from_user.id
    
    if user_id not in login_states:
        bot.answer_callback_query(call.id, "❌ Session expired", show_alert=True)
        return
    
    country_name = call.data.replace("login_country_", "")
    login_states[user_id]["country"] = country_name

    # Show account age selection first
    markup = InlineKeyboardMarkup(row_width=2)
    age_options = [
        ("🆕 Fresh", "Fresh"),
        ("📅 2 yr old", "2_yr_old"),
        ("📅 3 yr old", "3_yr_old"),
        ("📅 4 yr old", "4_yr_old"),
        ("📅 5 yr old", "5_yr_old"),
        ("📅 6 yr old", "6_yr_old"),
        ("📅 7 yr old", "7_yr_old"),
    ]
    # Add all buttons at once so row_width=2 puts them 2 per row
    age_buttons = [InlineKeyboardButton(label, callback_data=f"acc_age_{key}") for label, key in age_options]
    markup.add(*age_buttons)
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_login"))

    edit_or_resend(
        call.message.chat.id,
        call.message.message_id,
        f"🌍 <b>Country:</b> {country_name}\n\n"
        f"🗓️ <b>Select Account Age:</b>",
        markup=markup,
        parse_mode="HTML"
    )

def handle_cancel_login(call):
    user_id = call.from_user.id
    
    if user_id in login_states:
        state = login_states[user_id]
        if "client" in state:
            try:
                if account_manager and account_manager.pyrogram_manager:
                    _client = state["client"]
                    def _disc_login():
                        import asyncio as _aio
                        loop = _aio.new_event_loop()
                        _aio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(account_manager.pyrogram_manager.safe_disconnect(_client))
                        finally:
                            loop.close()
                    threading.Thread(target=_disc_login, daemon=True).start()
            except:
                pass
        login_states.pop(user_id, None)
    
    edit_or_resend(
        call.message.chat.id,
        call.message.message_id,
        "❌ Login cancelled.",
        markup=None
    )
    show_admin_panel(call.message.chat.id)

def handle_logout_session(user_id, session_id, chat_id, message_id, callback_id):
    try:
        if not account_manager:
            bot.answer_callback_query(callback_id, "❌ Account module not loaded", show_alert=True)
            return
        
        bot.answer_callback_query(callback_id, "🔄 Logging out...", show_alert=False)
        success, message = account_manager.logout_session_sync(
            session_id, user_id, otp_sessions_col, accounts_col, orders_col
        )
        
        if success:
            try:
                bot.delete_message(chat_id, message_id)
            except:
                pass
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_menu"))
            
            sent_msg = bot.send_message(
                chat_id,
                "✅ **Logged Out Successfully!**\n\n"
                "You have been logged out from this session.\n"
                "Order marked as completed.\n\n"
                "Thank you for using our service!",
                reply_markup=markup
            )
            user_last_message[user_id] = sent_msg.message_id
        else:
            bot.answer_callback_query(callback_id, f"❌ {message}", show_alert=True)
    except Exception as e:
        logger.error(f"Logout handler error: {e}")
        bot.answer_callback_query(callback_id, "❌ Error logging out", show_alert=True)

def get_latest_otp(user_id, session_id, chat_id, message_id, callback_id):
    try:
        session_data = otp_sessions_col.find_one({"session_id": session_id})
        if not session_data:
            bot.answer_callback_query(callback_id, "❌ Session not found", show_alert=True)
            return
        
        # ALWAYS fetch fresh OTP, don't use cached
        bot.answer_callback_query(callback_id, "🔍 Searching for latest OTP...", show_alert=False)
        
        session_string = session_data.get("session_string")
        if not session_string:
            bot.answer_callback_query(callback_id, "❌ No session string found", show_alert=True)
            return
        
        # Always fetch new OTP
        otp_code = account_manager.get_latest_otp_sync(session_string)
        
        if not otp_code:
            bot.answer_callback_query(callback_id, "❌ No OTP received yet. Please wait...", show_alert=True)
            return
        
        # Update database with the new OTP
        otp_sessions_col.update_one(
            {"session_id": session_id},
            {"$set": {
                "has_otp": True,
                "last_otp": otp_code,
                "last_otp_time": datetime.utcnow(),
                "status": "otp_received"
            }}
        )
        
        try:
            from logs import log_otp_received_async
            order = orders_col.find_one({"session_id": session_id})
            if order:
                log_otp_received_async(
                    user_id=user_id,
                    phone=session_data.get('phone', 'N/A'),
                    otp_code=otp_code,
                    country=order.get('country', 'Unknown'),
                    price=order.get('price', 0)
                )
        except:
            pass
        
        account_id = session_data.get("account_id")
        account = None
        two_step_password = ""
        if account_id:
            try:
                _aoid = safe_obj_id(account_id)
                if _aoid:
                    account = accounts_col.find_one({"_id": _aoid})
                if account:
                    two_step_password = account.get("two_step_password", "")
            except:
                pass
        
        message = (
            "✅ <b>Latest OTP Received!</b>\n\n"
            f"📱 Phone: <code>{session_data.get('phone', 'N/A')}</code>\n"
            f"🔢 OTP Code: <code>{otp_code}</code>\n"
        )
        if two_step_password:
            message += f"🔐 2FA Password: <code>{two_step_password}</code>\n"
        elif account and account.get("two_step_password"):
            message += f"🔐 2FA Password: <code>{account.get('two_step_password')}</code>\n"
        message += (
            f"\n⏰ Time: <b>{datetime.utcnow().strftime('%H:%M:%S')} UTC</b>\n"
            f"\n💡 <i>Tap any value above to copy it.</i>\n"
            f"📲 Enter OTP in Telegram X / Turbotel app."
        )
        
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("🔄 Get OTP Again", callback_data=f"get_otp_{session_id}"),
            InlineKeyboardButton("🚪 Logout", callback_data=f"logout_session_{session_id}")
        )
        
        try:
            bot.edit_message_text(
                message,
                chat_id,
                message_id,
                parse_mode="HTML",
                reply_markup=markup
            )
        except:
            sent_msg = bot.send_message(
                chat_id,
                message,
                parse_mode="HTML",
                reply_markup=markup
            )
            user_last_message[user_id] = sent_msg.message_id
        
        bot.answer_callback_query(callback_id, "✅ Latest OTP fetched!", show_alert=False)
    except Exception as e:
        logger.error(f"Get OTP error: {e}")
        bot.answer_callback_query(callback_id, "❌ Error getting OTP", show_alert=True)

# ---------------------------------------------------------------------
# COUPON MANAGEMENT FUNCTIONS
# ---------------------------------------------------------------------

def show_coupon_management(chat_id, message_id=None):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "❌ Unauthorized access")
        return
    
    text = "🎟 **Coupon Management**\n\nChoose an option:"
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("➕ Add Coupon", callback_data="admin_create_coupon"),
        InlineKeyboardButton("❌ Remove Coupon", callback_data="admin_remove_coupon")
    )
    markup.add(
        InlineKeyboardButton("📊 Coupon Status", callback_data="admin_coupon_status"),
        InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_panel")
    )
    
    if message_id:
        edit_or_resend(
            chat_id,
            message_id,
            text,
            markup=markup,
            parse_mode="Markdown"
        )
    else:
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode="Markdown")

# ---------------------------------------------------------------------
# COUPON MESSAGE HANDLERS
# ---------------------------------------------------------------------

@bot.message_handler(func=lambda m: user_stage.get(m.from_user.id) == "waiting_coupon")
def handle_coupon_input(msg):
    user_id = msg.from_user.id
    
    if user_stage.get(user_id) != "waiting_coupon":
        return
    
    coupon_code = msg.text.strip().upper()
    user_stage.pop(user_id, None)
    
    success, result = claim_coupon(coupon_code, user_id)
    
    if success:
        amount = result
        new_balance = get_balance(user_id)
        text = f"✅ **Coupon Redeemed Successfully!**\n\n"
        text += f"🎟 Coupon Code: `{coupon_code}`\n"
        text += f"💰 Amount Added: {format_currency(amount)}\n"
        text += f"💳 New Balance: {format_currency(new_balance)}\n\n"
        text += f"Thank you for using our service! 🎉"
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_menu"))
        
        sent_msg = bot.send_message(
            msg.chat.id,
            text,
            parse_mode="Markdown",
            reply_markup=markup
        )
        user_last_message[user_id] = sent_msg.message_id
    else:
        error_msg = result
        if error_msg == "Coupon not found":
            response = "❌ **Invalid Coupon Code**\n\n"
            response += "The coupon code you entered does not exist.\n"
            response += "Please check the code and try again."
        elif error_msg == "Already claimed":
            response = "⚠️ **Coupon Already Claimed**\n\n"
            response += "You have already claimed this coupon code.\n"
            response += "Each coupon can only be claimed once per user."
        elif error_msg == "Fully claimed":
            response = "🚫 **Coupon Fully Claimed**\n\n"
            response += "This coupon has been claimed by all eligible users.\n"
            response += "No more claims are available."
        elif error_msg in ["removed", "expired"]:
            response = f"🚫 **Coupon {error_msg.capitalize()}**\n\n"
            response += "This coupon is no longer valid for redemption.\n"
            response += "It may have been removed or expired."
        else:
            response = f"❌ **Error:** {error_msg}"
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu"))
        
        sent_msg = bot.send_message(
            msg.chat.id,
            response,
            parse_mode="Markdown",
            reply_markup=markup
        )
        user_last_message[user_id] = sent_msg.message_id

@bot.message_handler(func=lambda m: coupon_state.get(m.from_user.id, {}).get("step") == "ask_code")
def handle_coupon_code_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in coupon_state or coupon_state[user_id]["step"] != "ask_code":
        return
    
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Unauthorized access")
        coupon_state.pop(user_id, None)
        return
    
    code = msg.text.strip().upper()
    if not code:
        bot.send_message(msg.chat.id, "❌ Coupon code cannot be empty. Enter coupon code:")
        return
    
    existing = get_coupon(code)
    if existing:
        bot.send_message(
            msg.chat.id,
            f"❌ Coupon code `{code}` already exists.\n\nEnter a different coupon code:"
        )
        return
    
    coupon_state[user_id] = {
        "step": "ask_amount",
        "code": code
    }
    
    bot.send_message(
        msg.chat.id,
        f"🎟 Coupon Code: `{code}`\n\n"
        f"💰 Enter coupon amount (minimum ₹1):"
    )

@bot.message_handler(func=lambda m: coupon_state.get(m.from_user.id, {}).get("step") == "ask_amount")
def handle_coupon_amount_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in coupon_state or coupon_state[user_id]["step"] != "ask_amount":
        return
    
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Unauthorized access")
        coupon_state.pop(user_id, None)
        return
    
    try:
        amount = float(msg.text.strip())
        if amount < 1:
            bot.send_message(msg.chat.id, "❌ Amount must be at least ₹1. Enter amount:")
            return
        
        coupon_state[user_id] = {
            "step": "ask_max_users",
            "code": coupon_state[user_id]["code"],
            "amount": amount
        }
        
        bot.send_message(
            msg.chat.id,
            f"🎟 Coupon Code: `{coupon_state[user_id]['code']}`\n"
            f"💰 Amount: {format_currency(amount)}\n\n"
            f"👥 Enter number of users who can claim this coupon (minimum 1):"
        )
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid amount. Enter numbers only (e.g., 100):")

@bot.message_handler(func=lambda m: coupon_state.get(m.from_user.id, {}).get("step") == "ask_max_users")
def handle_coupon_max_users_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in coupon_state or coupon_state[user_id]["step"] != "ask_max_users":
        return
    
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Unauthorized access")
        coupon_state.pop(user_id, None)
        return
    
    try:
        max_users = int(msg.text.strip())
        if max_users < 1:
            bot.send_message(msg.chat.id, "❌ Must be at least 1 user. Enter number:")
            return
        
        code = coupon_state[user_id]["code"]
        amount = coupon_state[user_id]["amount"]
        
        success, message = create_coupon(code, amount, max_users, user_id)
        
        if success:
            text = f"✅ **Coupon Created Successfully!**\n\n"
            text += f"🎟 Code: `{code}`\n"
            text += f"💰 Amount: {format_currency(amount)}\n"
            text += f"👥 Max Users: {max_users}\n\n"
            text += f"Coupon is now active and ready for users to redeem."
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🎟 Coupon Management", callback_data="admin_coupon_menu"))
            
            bot.send_message(
                msg.chat.id,
                text,
                parse_mode="Markdown",
                reply_markup=markup
            )
        else:
            bot.send_message(
                msg.chat.id,
                f"❌ Failed to create coupon: {message}\n\n"
                f"Try again or contact support."
            )
        
        coupon_state.pop(user_id, None)
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid number. Enter whole numbers only (e.g., 100):")

@bot.message_handler(func=lambda m: coupon_state.get(m.from_user.id, {}).get("step") == "ask_remove_code")
def handle_coupon_remove_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in coupon_state or coupon_state[user_id]["step"] != "ask_remove_code":
        return
    
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Unauthorized access")
        coupon_state.pop(user_id, None)
        return
    
    code = msg.text.strip().upper()
    
    success, message = remove_coupon(code, user_id)
    
    if success:
        text = f"✅ **Coupon Removed Successfully!**\n\n"
        text += f"🎟 Code: `{code}`\n"
        text += f"🚫 Status: Removed\n\n"
        text += f"This coupon can no longer be claimed by users."
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🎟 Coupon Management", callback_data="admin_coupon_menu"))
        
        bot.send_message(
            msg.chat.id,
            text,
            parse_mode="Markdown",
            reply_markup=markup
        )
    else:
        if message == "Coupon not found":
            response = f"❌ **Coupon Not Found**\n\n"
            response += f"Coupon code `{code}` does not exist.\n"
            response += f"Please check the code and try again."
        else:
            response = f"❌ **Error:** {message}"
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🎟 Coupon Management", callback_data="admin_coupon_menu"))
        
        bot.send_message(
            msg.chat.id,
            response,
            parse_mode="Markdown",
            reply_markup=markup
        )
    
    coupon_state.pop(user_id, None)

@bot.message_handler(func=lambda m: coupon_state.get(m.from_user.id, {}).get("step") == "ask_status_code")
def handle_coupon_status_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in coupon_state or coupon_state[user_id]["step"] != "ask_status_code":
        return
    
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Unauthorized access")
        coupon_state.pop(user_id, None)
        return
    
    code = msg.text.strip().upper()
    
    status = get_coupon_status(code)
    
    if not status:
        text = f"❌ **Coupon Not Found**\n\n"
        text += f"Coupon code `{code}` does not exist.\n"
        text += f"Please check the code and try again."
    else:
        status_text = status["status"].capitalize()
        if status["status"] == "active":
            status_text = "🟢 Active"
        elif status["status"] == "expired":
            status_text = "🔴 Expired"
        elif status["status"] == "removed":
            status_text = "⚫ Removed"
        
        text = f"📊 **Coupon Details**\n\n"
        text += f"🎟 Code: `{status['code']}`\n"
        text += f"💰 Amount: {format_currency(status['amount'])}\n"
        text += f"👥 Max Users: {status['max_users']}\n"
        text += f"✅ Claimed: {status['claimed']}\n"
        text += f"🔄 Remaining: {status['remaining']}\n"
        text += f"📊 Status: {status_text}\n"
        text += f"📅 Created: {status['created_at'].strftime('%Y-%m-%d %H:%M') if status['created_at'] else 'N/A'}\n"
        
        if status['claimed'] > 0:
            text += f"\n👤 Recent Users (first 10):\n"
            for i, uid in enumerate(status['claimed_users'][:10], 1):
                text += f"{i}. User ID: {uid}\n"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🎟 Coupon Management", callback_data="admin_coupon_menu"))
    
    bot.send_message(
        msg.chat.id,
        text,
        parse_mode="Markdown",
        reply_markup=markup
    )
    
    coupon_state.pop(user_id, None)

# ---------------------------------------------------------------------
# RECHARGE METHODS FUNCTIONS - UPDATED WITH TOTAL AND TODAY RECHARGE
# ---------------------------------------------------------------------

def show_recharge_methods(chat_id, message_id, user_id):
    # Calculate total recharge and today's recharge for this user
    total_recharge = 0
    today_recharge = 0
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Get all approved recharges for this user
    user_recharges = recharges_col.find({
        "user_id": user_id,
        "status": "approved"
    })
    
    for recharge in user_recharges:
        amount = float(recharge.get("amount", 0))
        total_recharge += amount
        
        # Check if recharge was done today
        recharge_date = recharge.get("created_at") or recharge.get("submitted_at")
        if recharge_date and recharge_date >= today_start:
            today_recharge += amount
    
    text = f"💳 **Recharge**\n\n"
    text += f"💰 **Total Recharge:** {format_currency(total_recharge)}\n"
    text += f"📅 **Today's Recharge:** {format_currency(today_recharge)}\n\n"
    text += f"⬇️ **Select Payment Method:**"
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("📱 UPI Payment", callback_data="recharge_upi")
    )
    markup.add(InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu"))
    
    edit_or_resend(
        chat_id,
        message_id,
        text,
        markup=markup,
        parse_mode="Markdown"
    )

# ---------------------------------------------------------------------
# PROCESS RECHARGE AMOUNT FUNCTION - FIXED DATABASE ISSUE
# ---------------------------------------------------------------------

def process_recharge_amount(msg):
    try:
        amount = float(msg.text)
        if amount < 1:
            bot.send_message(msg.chat.id, "❌ Minimum recharge is ₹1. Enter amount again:")
            bot.register_next_step_handler(msg, process_recharge_amount)
            return
        
        user_id = msg.from_user.id
        
        caption = f"""<blockquote>💳 <b>UPI Payment Details</b> 

💰 Amount: {format_currency(amount)}
📱 UPI ID: {UPI_ID}

📋 Instructions:
1. Scan QR code OR send {format_currency(amount)} to above UPI
2. After payment, click <b>Deposited ✅</b> button
3. Follow the steps to submit proof

</blockquote>"""
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("💰 Deposited ✅", callback_data="upi_deposited"))
        
        upi_payment_states[user_id] = {
            "amount": amount,
            "step": "qr_shown"
        }
        
        bot.send_photo(
            msg.chat.id,
            QR_IMAGE_URL,
            caption=caption,
            parse_mode="HTML",
            reply_markup=markup
        )
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid amount. Enter numbers only:")
        bot.register_next_step_handler(msg, process_recharge_amount)

# FIXED UTR HANDLER - Now properly checks and stores in database
@bot.message_handler(func=lambda m: upi_payment_states.get(m.from_user.id, {}).get("step") == "waiting_utr")
def handle_utr_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in upi_payment_states or upi_payment_states[user_id]["step"] != "waiting_utr":
        return
    
    utr = msg.text.strip()
    
    if not utr.isdigit() or len(utr) != 12:
        bot.send_message(msg.chat.id, "❌ Invalid UTR. Please enter a valid 12-digit UTR number:")
        return
    
    # Store UTR and move to screenshot step
    upi_payment_states[user_id]["utr"] = utr
    upi_payment_states[user_id]["step"] = "waiting_screenshot"
    
    bot.send_message(
        msg.chat.id,
        "✅ UTR Received!\n\n"
        "📸 Step 2: Send Screenshot\n\n"
        "Now please send the payment screenshot from your bank app:\n"
        "_(Make sure screenshot shows amount, date, and UTR)_"
    )

# FIXED SCREENSHOT HANDLER - Now properly saves to database
@bot.message_handler(content_types=['photo'], func=lambda m: upi_payment_states.get(m.from_user.id, {}).get("step") == "waiting_screenshot")
def handle_screenshot_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in upi_payment_states or upi_payment_states[user_id]["step"] != "waiting_screenshot":
        return
    
    try:
        screenshot_file_id = msg.photo[-1].file_id
        
        amount = upi_payment_states[user_id]["amount"]
        utr = upi_payment_states[user_id].get("utr", "")
        
        # Generate unique request ID
        req_id = f"R{int(time.time())}{user_id}"
        
        # Save to database with proper fields
        recharge_data = {
            "user_id": user_id,
            "amount": amount,
            "status": "pending",
            "created_at": datetime.utcnow(),
            "method": "upi",
            "utr": utr,
            "screenshot": screenshot_file_id,
            "submitted_at": datetime.utcnow(),
            "req_id": req_id
        }
        
        _recharge_res = safe_insert_one(recharges_col, recharge_data, "recharge")
        recharge_id = _recharge_res.inserted_id if _recharge_res else None
        
        # Update with req_id (recharge_id is already an ObjectId from insert_one)
        recharges_col.update_one(
            {"_id": recharge_id},
            {"$set": {"req_id": req_id}}
        )
        
        # Get all admins to send notification
        all_admins = get_all_admins()
        
        admin_caption = f"""📋 **UPI Payment Request** 

👤 User: {user_id}
💰 Amount: {format_currency(amount)}
🔢 UTR: {utr}
📅 Submitted: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}
🆔 Request ID: {req_id}

✅ Both UTR and Screenshot received."""

        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_rech|{req_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"cancel_rech|{req_id}")
        )
        
        # Send to all admins
        for admin in all_admins:
            admin_user_id = admin["user_id"]
            try:
                bot.send_photo(
                    admin_user_id,
                    screenshot_file_id,
                    caption=admin_caption,
                    parse_mode="HTML",
                    reply_markup=markup
                )
            except Exception as e:
                logger.error(f"Failed to send recharge notification to admin {admin_user_id}: {e}")
        
        bot.send_message(
            msg.chat.id,
            f"✅ **Payment Proof Submitted Successfully!**\n\n"
            f"📋 **Details:**\n"
            f"💰 Amount: {format_currency(amount)}\n"
            f"🔢 UTR: {utr}\n"
            f"📸 Screenshot: ✅ Received\n\n"
            f"⏳ **Status:** Admin verification pending\n"
            f"🆔 Request ID: `{req_id}`\n\n"
            f"Admin will review and approve soon. Thank you! 🎉"
        )
        
        # Clear state after successful submission
        upi_payment_states.pop(user_id, None)
        
    except Exception as e:
        logger.error(f"Screenshot handler error: {e}")
        bot.send_message(msg.chat.id, f"❌ Error submitting payment: {str(e)}")

# =============================================================
# RECEIVER ID INPUT HANDLER - FIXED NAME DISPLAY
# =============================================================

@bot.message_handler(func=lambda m: user_stage.get(m.from_user.id) == "waiting_receiver_id")
def handle_receiver_id(msg):
    user_id = msg.from_user.id
    
    if user_stage.get(user_id) != "waiting_receiver_id":
        return
    
    try:
        receiver_id = int(msg.text.strip())
        
        # Check if receiver exists in database
        receiver = users_col.find_one({"user_id": receiver_id})
        if not receiver:
            bot.send_message(
                msg.chat.id,
                f"❌ User ID `{receiver_id}` not found in database!\n\nPlease enter a valid User ID:",
                parse_mode="Markdown"
            )
            return
        
        # Get receiver's name - properly formatted
        receiver_name = receiver.get("name", "Unknown")
        receiver_username = receiver.get("username", "")
        
        if receiver_username:
            receiver_display = f"{receiver_name} (@{receiver_username})"
        else:
            receiver_display = receiver_name
        
        # Store receiver info in user_states
        user_states[user_id] = {
            "receiver_id": receiver_id,
            "receiver_name": receiver_display
        }
        
        # Move to amount input
        user_stage[user_id] = "waiting_transfer_amount"
        
        balance = get_balance(user_id)
        
        message = f"📤 **Send Balance - Step 2/2**\n\n"
        message += f"👤 Receiver: {receiver_display}\n"
        message += f"🆔 Receiver ID: `{receiver_id}`\n"
        message += f"💰 Your Balance: {format_currency(balance)}\n\n"
        message += f"Please enter the **Amount** to send:"
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("⬅️ Back", callback_data="send_balance_menu"))
        
        bot.send_message(
            msg.chat.id,
            message,
            parse_mode="Markdown",
            reply_markup=markup
        )
        
    except ValueError:
        bot.send_message(
            msg.chat.id,
            "❌ Invalid User ID! Please enter a numeric ID only:\nExample: `123456789`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Receiver ID error: {e}")
        bot.send_message(msg.chat.id, f"❌ Error: {str(e)}")

# =============================================================
# TRANSFER AMOUNT INPUT HANDLER
# =============================================================

@bot.message_handler(func=lambda m: user_stage.get(m.from_user.id) == "waiting_transfer_amount")
def handle_transfer_amount(msg):
    user_id = msg.from_user.id
    
    if user_stage.get(user_id) != "waiting_transfer_amount":
        return
    
    try:
        amount = float(msg.text.strip())
        
        # Get stored data
        transfer_data = user_states.get(user_id, {})
        receiver_id = transfer_data.get("receiver_id")
        receiver_name = transfer_data.get("receiver_name", f"ID: {receiver_id}")
        
        if not receiver_id:
            bot.send_message(msg.chat.id, "❌ Session expired! Please start again.")
            user_stage.pop(user_id, None)
            user_states.pop(user_id, None)
            return
        
        # Validate amount
        if amount <= 0:
            bot.send_message(msg.chat.id, "❌ Amount must be greater than 0!\nPlease enter valid amount:")
            return
        
        sender_balance = get_balance(user_id)
        if amount > sender_balance:
            bot.send_message(
                msg.chat.id, 
                f"❌ Insufficient balance! You have {format_currency(sender_balance)}\nPlease enter smaller amount:"
            )
            return
        
        # Update transfer data with amount
        transfer_data["amount"] = amount
        user_states[user_id] = transfer_data
        
        # Show confirmation
        confirm_message = f"📤 **Confirm Transfer**\n\n"
        confirm_message += f"👤 Receiver: {receiver_name}\n"
        confirm_message += f"🆔 Receiver ID: `{receiver_id}`\n"
        confirm_message += f"💰 Amount to Send: {format_currency(amount)}\n"
        confirm_message += f"💳 Your Balance: {format_currency(sender_balance)}\n"
        confirm_message += f"💳 Balance After: {format_currency(sender_balance - amount)}\n\n"
        confirm_message += f"Are you sure you want to proceed?"
        
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("✅ Confirm Transfer", callback_data="transfer_confirm"),
            InlineKeyboardButton("❌ Cancel", callback_data="balance")
        )
        
        bot.send_message(
            msg.chat.id,
            confirm_message,
            parse_mode="Markdown",
            reply_markup=markup
        )
        
        user_stage.pop(user_id, None)
        
    except ValueError:
        bot.send_message(
            msg.chat.id,
            "❌ Invalid amount! Please enter numbers only:\nExample: `100`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Transfer amount error: {e}")
        bot.send_message(msg.chat.id, f"❌ Error: {str(e)}")

# ---------------------------------------------------------------------
# EDIT PRICE FUNCTIONS
# ---------------------------------------------------------------------

def show_edit_price_country_selection(chat_id, message_id=None):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "❌ Unauthorized access")
        return
    
    countries = get_all_countries()
    if not countries:
        text = "❌ No countries available to edit."
        if message_id:
            edit_or_resend(
                chat_id,
                message_id,
                text,
                markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("⬅️ Back", callback_data="manage_countries")
                )
            )
        else:
            bot.send_message(chat_id, text)
        return
    
    text = "✏️ **Edit Country Price**\n\nSelect a country to edit its price:"
    markup = InlineKeyboardMarkup(row_width=2)
    for country in countries:
        markup.add(InlineKeyboardButton(
            f"{country['name']} - {format_currency(country['price'])}",
            callback_data=f"edit_price_country_{country['name']}"
        ))
    markup.add(InlineKeyboardButton("⬅️ Back", callback_data="manage_countries"))
    
    if message_id:
        edit_or_resend(
            chat_id,
            message_id,
            text,
            markup=markup,
            parse_mode="Markdown"
        )
    else:
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode="Markdown")

def show_edit_price_details(chat_id, message_id, country_name):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "❌ Unauthorized access")
        return
    
    country = get_country_by_name(country_name)
    if not country:
        edit_or_resend(
            chat_id,
            message_id,
            f"❌ Country '{country_name}' not found.",
            markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("⬅️ Back", callback_data="edit_price")
            )
        )
        return
    
    text = f"✏️ **Edit Price for {country_name}**\n\n"
    text += f"🌍 Country: {country_name}\n"
    text += f"💰 Current Price: {format_currency(country['price'])}\n"
    text += f"📊 Available Accounts: {get_available_accounts_count(country_name)}\n\n"
    text += f"Click below to edit the price:"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(
        "✏️ Edit Price",
        callback_data=f"edit_price_confirm_{country_name}"
    ))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_edit_price"))
    
    edit_or_resend(
        chat_id,
        message_id,
        text,
        markup=markup,
        parse_mode="Markdown"
    )

# ---------------------------------------------------------------------
# MESSAGE HANDLER FOR LOGIN FLOW
# ---------------------------------------------------------------------

@bot.message_handler(func=lambda m: login_states.get(m.from_user.id, {}).get("step") in ["phone", "waiting_otp", "waiting_password"])
def handle_login_flow_messages(msg):
    user_id = msg.from_user.id
    
    if user_id not in login_states:
        return
    
    state = login_states[user_id]
    step = state["step"]
    chat_id = state["chat_id"]
    message_id = state["message_id"]
    
    if step == "phone":
        phone = msg.text.strip()
        if not phone.startswith('+'):
            phone = '+' + phone
        if len(phone) < 7:
            bot.send_message(chat_id, "❌ Invalid phone number. Please enter with country code:\nExample: +919876543210 or +79123456789")
            return
        
        if not account_manager:
            try:
                bot.edit_message_text(
                    "❌ Account module not loaded. Please contact admin.",
                    chat_id, message_id
                )
            except:
                pass
            login_states.pop(user_id, None)
            return
        
        try:
            success, message = account_manager.pyrogram_login_flow_sync(
                login_states, accounts_col, user_id, phone, chat_id, message_id, state["country"]
            )
            
            if success:
                try:
                    bot.edit_message_text(
                        f"📱 Phone: {phone}\n\n"
                        "📩 OTP sent! Enter the OTP you received:",
                        chat_id, message_id,
                        reply_markup=InlineKeyboardMarkup().add(
                            InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")
                        )
                    )
                except:
                    pass
            else:
                try:
                    bot.edit_message_text(
                        f"❌ Failed to send OTP: {message}\n\nPlease try again.",
                        chat_id, message_id
                    )
                except:
                    pass
                login_states.pop(user_id, None)
        
        except Exception as e:
            logger.error(f"Login flow error: {e}")
            try:
                bot.edit_message_text(
                    f"❌ Error: {str(e)}\n\nPlease try again.",
                    chat_id, message_id
                )
            except:
                pass
            login_states.pop(user_id, None)
    
    elif step == "waiting_otp":
        otp = msg.text.strip()
        if not otp.isdigit() or len(otp) != 5:
            bot.send_message(chat_id, "❌ Invalid OTP format. Please enter 5-digit OTP:")
            return
        
        if not account_manager:
            try:
                bot.edit_message_text(
                    "❌ Account module not loaded. Please contact admin.",
                    chat_id, message_id
                )
            except:
                pass
            login_states.pop(user_id, None)
            return
        
        try:
            success, message = account_manager.verify_otp_and_save_sync(
                login_states, accounts_col, user_id, otp
            )
            
            if success:
                country = state["country"]
                phone = state["phone"]
                try:
                    bot.edit_message_text(
                        f"✅ **Account Added Successfully!**\n\n"
                        f"🌍 Country: {country}\n"
                        f"📱 Phone: {phone}\n"
                        f"🔐 Session: Generated\n\n"
                        f"Account is now available for purchase!",
                        chat_id, message_id
                    )
                except:
                    pass
                login_states.pop(user_id, None)
            
            elif message == "password_required":
                try:
                    bot.edit_message_text(
                        f"📱 Phone: {state['phone']}\n\n"
                        "🔐 2FA Password required!\n"
                        "Enter your 2-step verification password:",
                        chat_id, message_id,
                        reply_markup=InlineKeyboardMarkup().add(
                            InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")
                        )
                    )
                except:
                    pass
            
            else:
                try:
                    bot.edit_message_text(
                        f"❌ OTP verification failed: {message}\n\nPlease try again.",
                        chat_id, message_id
                    )
                except:
                    pass
                login_states.pop(user_id, None)
        
        except Exception as e:
            logger.error(f"OTP verification error: {e}")
            try:
                bot.edit_message_text(
                    f"❌ Error: {str(e)}\n\nPlease try again.",
                    chat_id, message_id
                )
            except:
                pass
            login_states.pop(user_id, None)
    
    elif step == "waiting_password":
        password = msg.text.strip()
        if not password:
            bot.send_message(chat_id, "❌ Password cannot be empty. Enter 2FA password:")
            return
        
        if not account_manager:
            try:
                bot.edit_message_text(
                    "❌ Account module not loaded. Please contact admin.",
                    chat_id, message_id
                )
            except:
                pass
            login_states.pop(user_id, None)
            return
        
        try:
            success, message = account_manager.verify_2fa_password_sync(
                login_states, accounts_col, user_id, password
            )
            
            if success:
                country = state["country"]
                phone = state["phone"]
                try:
                    bot.edit_message_text(
                        f"✅ **Account Added Successfully!**\n\n"
                        f"🌍 Country: {country}\n"
                        f"📱 Phone: {phone}\n"
                        f"🔐 2FA: Enabled\n"
                        f"🔐 Session: Generated\n\n"
                        f"Account is now available for purchase!",
                        chat_id, message_id
                    )
                except:
                    pass
                login_states.pop(user_id, None)
            
            else:
                try:
                    bot.edit_message_text(
                        f"❌ 2FA password failed: {message}\n\nPlease try again.",
                        chat_id, message_id
                    )
                except:
                    pass
                login_states.pop(user_id, None)
        
        except Exception as e:
            logger.error(f"2FA verification error: {e}")
            try:
                bot.edit_message_text(
                    f"❌ Error: {str(e)}\n\nPlease try again.",
                    chat_id, message_id
                )
            except:
                pass
            login_states.pop(user_id, None)

# ---------------------------------------------------------------------
# EDIT PRICE MESSAGE HANDLER
# ---------------------------------------------------------------------

@bot.message_handler(func=lambda m: edit_price_state.get(m.from_user.id, {}).get("step") == "waiting_price")
def handle_edit_price_input(msg):
    user_id = msg.from_user.id
    
    if user_id not in edit_price_state or edit_price_state[user_id]["step"] != "waiting_price":
        return
    
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Unauthorized access")
        edit_price_state.pop(user_id, None)
        return
    
    try:
        new_price = float(msg.text.strip())
        if new_price <= 0:
            bot.send_message(msg.chat.id, "❌ Price must be greater than 0. Enter valid price:")
            return
        
        country_name = edit_price_state[user_id]["country"]
        
        result = countries_col.update_one(
            {"name": country_name, "status": "active"},
            {"$set": {"price": new_price, "updated_at": datetime.utcnow(), "updated_by": user_id}}
        )
        
        if result.modified_count > 0:
            bot.send_message(
                msg.chat.id,
                f"✅ Price updated successfully!\n\n"
                f"🌍 Country: {country_name}\n"
                f"💰 New Price: {format_currency(new_price)}\n\n"
                f"Price has been updated for all users."
            )
        else:
            bot.send_message(
                msg.chat.id,
                f"❌ Failed to update price. Country '{country_name}' not found or already has same price."
            )
        
        edit_price_state.pop(user_id, None)
        show_country_management(msg.chat.id)
    
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid price format. Enter numbers only (e.g., 99.99):")

# ---------------------------------------------------------------------
# REFERRAL SYSTEM FUNCTIONS
# ---------------------------------------------------------------------

def show_referral_info(user_id, chat_id):
    user_data = users_col.find_one({"user_id": user_id}) or {}
    referral_code = user_data.get('referral_code', f'REF{user_id}')
    total_commission = user_data.get('total_commission_earned', 0)
    total_referrals = user_data.get('total_referrals', 0)
    
    referral_link = f"https://t.me/{bot.get_me().username}?start={referral_code}"
    
    message = f"👥 **Refer & Earn {REFERRAL_COMMISSION}% Commission!**\n\n"
    message += f"📊 **Your Stats:**\n"
    message += f"• Total Referrals: {total_referrals}\n"
    message += f"• Total Commission Earned: {format_currency(total_commission)}\n"
    message += f"• Commission Rate: {REFERRAL_COMMISSION}% per recharge\n\n"
    message += f"🔗 **Your Referral Link:**\n`{referral_link}`\n\n"
    message += f"📝 **How it works:**\n"
    message += f"1. Share your referral link with friends\n"
    message += f"2. When they join using your link\n"
    message += f"3. You earn {REFERRAL_COMMISSION}% of EVERY recharge they make!\n"
    message += f"4. Commission credited instantly\n\n"
    message += f"💰 **Example:** If a friend recharges ₹1000, you earn ₹{1000 * REFERRAL_COMMISSION / 100}!\n\n"
    message += f"Start sharing and earning today! 🎉"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📤 Share Link", url=f"https://t.me/share/url?url={referral_link}&text=Join%20this%20awesome%20OTP%20bot%20to%20buy%20Telegram%20accounts!"))
    markup.add(InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu"))
    
    sent_msg = bot.send_message(chat_id, message, parse_mode="Markdown", reply_markup=markup)
    user_last_message[user_id] = sent_msg.message_id

# ---------------------------------------------------------------------
# ADMIN MANAGEMENT FUNCTIONS
# ---------------------------------------------------------------------

def show_admin_panel(chat_id):
    user_id = chat_id
    
    if not is_admin(user_id):
        bot.send_message(chat_id, "❌ Unauthorized access")
        return
    
    total_accounts = accounts_col.count_documents({})
    active_accounts = accounts_col.count_documents({"status": "active", "used": {"$ne": True}})
    total_users = users_col.count_documents({})
    total_orders = orders_col.count_documents({})
    banned_users = banned_users_col.count_documents({"status": "active"})
    active_countries = countries_col.count_documents({"status": "active"})
    total_admins = get_admin_count()
    
    text = (
        f"👑 **Admin Panel**\n\n"
        f"📊 **Statistics:**\n"
        f"• Total Accounts: {total_accounts}\n"
        f"• Active Accounts: {active_accounts}\n"
        f"• Total Users: {total_users}\n"
        f"• Total Orders: {total_orders}\n"
        f"• Banned Users: {banned_users}\n"
        f"• Active Countries: {active_countries}\n"
        f"• Total Admins: {total_admins}/6\n\n"
        f"🛠️ **Management Tools:**"
    )
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("➕ Add Account", callback_data="add_account"),
        InlineKeyboardButton("📢 Broadcast", callback_data="broadcast_menu")
    )
    markup.add(
        InlineKeyboardButton("💸 Refund", callback_data="refund_start"),
        InlineKeyboardButton("📊 Ranking", callback_data="ranking")
    )
    markup.add(
        InlineKeyboardButton("💬 Message User", callback_data="message_user"),
        InlineKeyboardButton("💳 Deduct Balance", callback_data="admin_deduct_start")
    )
    markup.add(
        InlineKeyboardButton("🚫 Ban User", callback_data="ban_user"),
        InlineKeyboardButton("✅ Unban User", callback_data="unban_user")
    )
    markup.add(
        InlineKeyboardButton("🌍 Manage Countries", callback_data="manage_countries"),
        InlineKeyboardButton("🎟 Coupon Management", callback_data="admin_coupon_menu")
    )
    if is_super_admin(user_id):
        markup.add(
            InlineKeyboardButton("👥 Manage Admins 👑", callback_data="manage_admins_panel"),
            InlineKeyboardButton("🔐 Admin Permissions", callback_data="admin_permissions")
        )
    
    # Pending recharges count
    pending_recharges = recharges_col.count_documents({"status": "pending"})
    if pending_recharges > 0:
        markup.add(InlineKeyboardButton(f"💳 Pending Recharges ({pending_recharges})", callback_data="pending_recharges_list"))
    
    # Show admin list for main admin
    if is_super_admin(user_id):
        admins = get_all_admins()
        admin_text = "\n\n👥 **Current Admins:**\n"
        for admin in admins:
            if admin.get("is_super_admin", False):
                admin_text += f"👑 Main: `{admin['user_id']}`\n"
            else:
                admin_text += f"👤 Admin: `{admin['user_id']}`\n"
        text += admin_text
    
    sent_msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="Markdown")
    user_last_message[user_id] = sent_msg.message_id

def show_country_management(chat_id):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "❌ Unauthorized access")
        return
    
    countries = get_all_countries()
    if not countries:
        text = "🌍 **Country Management**\n\nNo countries available. Add a country first."
    else:
        text = "🌍 **Country Management**\n\n**Available Countries:**\n"
        for country in countries:
            accounts_count = get_available_accounts_count(country['name'])
            text += f"• {country['name']} - Price: {format_currency(country['price'])} - Accounts: {accounts_count}\n"
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("➕ Add Country", callback_data="add_country"),
        InlineKeyboardButton("✏️ Edit Price", callback_data="edit_price")
    )
    markup.add(
        InlineKeyboardButton("➖ Remove Country", callback_data="remove_country")
    )
    markup.add(InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_panel"))
    
    sent_msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="Markdown")
    user_last_message[chat_id] = sent_msg.message_id

def ask_country_name(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "❌ Unauthorized access")
        return
    
    country_name = message.text.strip()
    user_states[message.chat.id] = {
        "step": "ask_country_price",
        "country_name": country_name
    }
    bot.send_message(message.chat.id, f"💰 Enter price for {country_name}:")

@bot.message_handler(func=lambda message: user_states.get(message.chat.id, {}).get("step") == "ask_country_price")
def ask_country_price(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "❌ Unauthorized access")
        return
    
    try:
        price = float(message.text.strip())
        user_data = user_states.get(message.chat.id)
        country_name = user_data.get("country_name")
        
        country_data = {
            "name": country_name,
            "price": price,
            "status": "active",
            "created_at": datetime.utcnow(),
            "created_by": message.from_user.id
        }
        safe_insert_one(countries_col, country_data, "country")
        
        del user_states[message.chat.id]
        bot.send_message(
            message.chat.id,
            f"✅ **Country Added Successfully!**\n\n"
            f"🌍 Country: {country_name}\n"
            f"💰 Price: {format_currency(price)}\n\n"
            f"Country is now available for users to purchase accounts."
        )
        show_country_management(message.chat.id)
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid price. Please enter a number:")

def show_world_country_picker(chat_id, message_id=None, page=0):
    """Paginated 180+ world country picker for admin Add Country flow"""
    total = len(WORLD_COUNTRIES)
    total_pages = max(1, (total + WC_PER_PAGE - 1) // WC_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * WC_PER_PAGE
    end_idx = start_idx + WC_PER_PAGE
    page_countries = WORLD_COUNTRIES[start_idx:end_idx]

    text = (
        f"🌍 <b>Select Country to Add</b>\n"
        f"<i>Page {page+1}/{total_pages}  •  {total} countries supported</i>\n\n"
        f"Choose from the list below or enter a custom name:"
    )
    markup = InlineKeyboardMarkup(row_width=2)
    row = []
    for c in page_countries:
        # Check if already in DB
        exists = countries_col.find_one({"name": {"$regex": f"^{re.escape(c['name'])}$", "$options": "i"}, "status": "active"})
        label = f"{c['flag']} {c['name']}" + (" ✅" if exists else "")
        row.append(InlineKeyboardButton(label, callback_data=f"wc_sel_{c['name']}"))
        if len(row) == 2:
            markup.add(*row)
            row = []
    if row:
        markup.add(*row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"wc_pg_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"wc_pg_{page+1}"))
    if nav:
        markup.add(*nav)

    markup.add(InlineKeyboardButton("✏️ Custom Country Name", callback_data="wc_custom"))
    markup.add(InlineKeyboardButton("⬅️ Back to Manage", callback_data="manage_countries"))

    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
            return
        except:
            pass
    sent = bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    user_last_message[chat_id] = sent.message_id

def show_country_removal(chat_id):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "❌ Unauthorized access")
        return
    
    countries = get_all_countries()
    if not countries:
        bot.send_message(chat_id, "❌ No countries available to remove.")
        return
    
    markup = InlineKeyboardMarkup(row_width=2)
    for country in countries:
        markup.add(InlineKeyboardButton(
            f"❌ {country['name']}",
            callback_data=f"remove_country_{country['name']}"
        ))
    markup.add(InlineKeyboardButton("⬅️ Back", callback_data="manage_countries"))
    
    sent_msg = bot.send_message(
        chat_id,
        "🗑️ **Remove Country**\n\nSelect a country to remove:",
        reply_markup=markup,
        parse_mode="Markdown"
    )
    user_last_message[chat_id] = sent_msg.message_id

def remove_country(country_name, chat_id, message_id=None):
    if not is_admin(chat_id):
        return "❌ Unauthorized access"
    
    try:
        result = countries_col.update_one(
            {"name": country_name, "status": "active"},
            {"$set": {"status": "inactive", "removed_at": datetime.utcnow()}}
        )
        
        if result.modified_count > 0:
            accounts_col.delete_many({"country": country_name})
            
            if message_id:
                try:
                    bot.delete_message(chat_id, message_id)
                except:
                    pass
            
            bot.send_message(chat_id, f"✅ Country '{country_name}' and all its accounts have been removed.")
            show_country_management(chat_id)
            return f"✅ {country_name} removed successfully"
        else:
            return f"❌ Country '{country_name}' not found or already removed"
    except Exception as e:
        logger.error(f"Error removing country: {e}")
        return f"❌ Error removing country: {str(e)}"

def ask_ban_user(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "❌ Unauthorized access")
        return
    
    try:
        user_id_to_ban = int(message.text.strip())
        
        user = users_col.find_one({"user_id": user_id_to_ban})
        if not user:
            bot.send_message(message.chat.id, "❌ User not found in database.")
            return
        
        already_banned = banned_users_col.find_one({"user_id": user_id_to_ban, "status": "active"})
        if already_banned:
            bot.send_message(message.chat.id, "⚠️ User is already banned.")
            return
        
        ban_record = {
            "user_id": user_id_to_ban,
            "banned_by": message.from_user.id,
            "reason": "Admin banned",
            "status": "active",
            "banned_at": datetime.utcnow()
        }
        safe_insert_one(banned_users_col, ban_record, "ban")
        
        bot.send_message(message.chat.id, f"✅ User {user_id_to_ban} has been banned.")
        
        try:
            bot.send_message(
                user_id_to_ban,
                "🚫 **Your Account Has Been Banned**\n\n"
                "You have been banned from using this bot.\n"
                "Contact admin @rchiex if you believe this is a mistake."
            )
        except:
            pass
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid user ID. Please enter numeric ID only.")

def ask_unban_user(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "❌ Unauthorized access")
        return
    
    try:
        user_id_to_unban = int(message.text.strip())
        
        ban_record = banned_users_col.find_one({"user_id": user_id_to_unban, "status": "active"})
        if not ban_record:
            bot.send_message(message.chat.id, "⚠️ User is not banned.")
            return
        
        banned_users_col.update_one(
            {"user_id": user_id_to_unban, "status": "active"},
            {"$set": {"status": "unbanned", "unbanned_at": datetime.utcnow(), "unbanned_by": message.from_user.id}}
        )
        
        bot.send_message(message.chat.id, f"✅ User {user_id_to_unban} has been unbanned.")
        
        try:
            bot.send_message(
                user_id_to_unban,
                "✅ **Your Account Has Been Unbanned**\n\n"
                "Your account access has been restored.\n"
                "You can now use the bot normally."
            )
        except:
            pass
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid user ID. Please enter numeric ID only.")

def show_user_ranking(chat_id):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "❌ Unauthorized access")
        return
    
    try:
        users_ranking = []
        all_wallets = wallets_col.find()
        
        for wallet in all_wallets:
            user_id_rank = wallet.get("user_id")
            balance = float(wallet.get("balance", 0))
            
            if balance > 0:
                user = users_col.find_one({"user_id": user_id_rank}) or {}
                name = user.get("name", "Unknown")
                username_db = user.get("username")
                users_ranking.append({
                    "user_id": user_id_rank,
                    "balance": balance,
                    "name": name,
                    "username": username_db
                })
        
        users_ranking.sort(key=lambda x: x["balance"], reverse=True)
        
        ranking_text = "📊 **User Ranking by Wallet Balance**\n\n"
        if not users_ranking:
            ranking_text = "📊 No users found with balance greater than zero."
        else:
            for index, user_data in enumerate(users_ranking[:20], 1):
                user_link = f"<a href='tg://user?id={user_data['user_id']}'>{user_data['user_id']}</a>"
                username_display = f"@{user_data['username']}" if user_data['username'] else "No Username"
                ranking_text += f"{index}. {user_link} - {username_display}\n"
                ranking_text += f" 💰 Balance: {format_currency(user_data['balance'])}\n\n"
        
        bot.send_message(chat_id, ranking_text, parse_mode="HTML")
    except Exception as e:
        logger.exception("Error in ranking:")
        bot.send_message(chat_id, f"❌ Error generating ranking: {str(e)}")

# ---------------------------------------------------------------------
# BROADCAST FUNCTION - PERFECT FORWARD (PURE TELEBOT)
# ---------------------------------------------------------------------

@bot.message_handler(commands=['resetbroadcast'])
def handle_resetbroadcast_command(msg):
    """Reset stuck IS_BROADCASTING flag"""
    global IS_BROADCASTING
    if not is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "❌ Unauthorized")
        return
    IS_BROADCASTING = False
    bot.send_message(msg.chat.id, "✅ Broadcast status reset. You can now start a new broadcast.")

@bot.message_handler(commands=['sendbroadcast'])
def handle_sendbroadcast_command(msg):
    """Handle /sendbroadcast command - EXACT FORWARD"""
    global IS_BROADCASTING
    
    if not is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "❌ Unauthorized access")
        return
    
    if IS_BROADCASTING:
        bot.send_message(msg.chat.id, "⚠️ Another broadcast is already in progress. Please wait...")
        return
    
    if not msg.reply_to_message:
        bot.send_message(
            msg.chat.id,
            "❌ **Reply to a message first, then use /sendbroadcast**\n\n"
            "📝 **How to use:**\n"
            "1️⃣ Send or forward any message (text/photo/video)\n"
            "2️⃣ Reply to it with `/sendbroadcast`\n\n"
            "📌 **Pin Options:**\n"
            "• `/sendbroadcast` — Send to all users\n"
            "• `/sendbroadcast -pin` — Send + pin silently\n"
            "• `/sendbroadcast -pinloud` — Send + pin with notification\n\n"
            "🔄 If stuck, use `/resetbroadcast`",
            parse_mode="Markdown"
        )
        return

    # Parse options
    cmd_text = msg.text.lower()
    pin_silent = '-pin' in cmd_text and '-pinloud' not in cmd_text
    pin_loud = '-pinloud' in cmd_text
    send_to_users = True  # Always send to all users

    source = msg.reply_to_message

    # Count targets before starting
    target_count = users_col.count_documents({})

    # Send confirmation
    status_msg = bot.send_message(
        msg.chat.id,
        f"📡 **Broadcast Started**\n\n"
        f"👥 Total Users: {target_count}\n"
        f"📌 Pin: {'🔊 Loud' if pin_loud else '🔇 Silent' if pin_silent else '❌ No'}\n\n"
        f"⏳ Processing...",
        parse_mode="Markdown"
    )
    
    IS_BROADCASTING = True
    
    # Start broadcast thread
    threading.Thread(
        target=broadcast_worker,
        args=(
            source,
            pin_silent,
            pin_loud,
            send_to_users,
            msg.chat.id,
            status_msg.message_id,
            msg.from_user.id
        ),
        daemon=True
    ).start()

def broadcast_worker(source_msg, pin_silent, pin_loud, send_to_users, admin_chat_id, status_msg_id, admin_id):
    """Broadcast worker — forwards to ALL users in database"""
    global IS_BROADCASTING

    try:
        # Collect ALL unique user IDs from database (exclude the sending admin)
        chat_ids = set()
        for user in users_col.find({}, {"user_id": 1}):
            uid = user.get("user_id")
            if uid and uid != admin_id:
                chat_ids.add(uid)

        # Also include served_chats if it exists (groups/channels)
        try:
            if 'served_chats' in db.list_collection_names():
                for chat in db['served_chats'].find():
                    cid = chat.get("chat_id")
                    if cid:
                        chat_ids.add(cid)
        except:
            pass

        all_targets = list(chat_ids)
        total = len(all_targets)

        try:
            bot.edit_message_text(
                f"📡 **Broadcast Starting...**\n\n"
                f"👥 Total Targets: {total}\n"
                f"⏳ Sending...",
                admin_chat_id, status_msg_id, parse_mode="Markdown"
            )
        except:
            pass

        sent = 0
        failed = 0
        pinned = 0

        for target_id in all_targets:
            try:
                forwarded_msg = bot.forward_message(
                    target_id,
                    source_msg.chat.id,
                    source_msg.message_id
                )
                sent += 1

                # Pin if requested (works in groups/channels)
                if pin_silent or pin_loud:
                    try:
                        bot.pin_chat_message(
                            target_id,
                            forwarded_msg.message_id,
                            disable_notification=(not pin_loud)
                        )
                        pinned += 1
                    except:
                        pass

                # Update progress every 25 messages
                if sent % 25 == 0:
                    try:
                        bot.edit_message_text(
                            f"📡 **Broadcasting...**\n\n"
                            f"✅ Sent: {sent}/{total}\n"
                            f"❌ Failed: {failed}\n"
                            f"📌 Pinned: {pinned}",
                            admin_chat_id, status_msg_id, parse_mode="Markdown"
                        )
                    except:
                        pass

                time.sleep(0.05)  # Anti-flood: ~20/s

            except Exception as e:
                failed += 1
                logger.error(f"Broadcast failed for {target_id}: {e}")
                time.sleep(0.05)
                continue

        # ----- FINAL REPORT -----
        report = (
            f"🎯 **Broadcast Completed!**\n\n"
            f"✅ Sent: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"📌 Pinned: {pinned}\n"
            f"👥 Total Targets: {total}\n"
            f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}"
        )
        try:
            bot.edit_message_text(report, admin_chat_id, status_msg_id, parse_mode="Markdown")
        except:
            bot.send_message(admin_chat_id, report, parse_mode="Markdown")

    except Exception as e:
        try:
            bot.edit_message_text(
                f"❌ **Broadcast Failed**\n\nError: {str(e)}",
                admin_chat_id, status_msg_id, parse_mode="Markdown"
            )
        except:
            pass
        logger.error(f"Broadcast worker error: {e}")

    finally:
        IS_BROADCASTING = False

# ---------------------------------------------------------------------
# OTHER FUNCTIONS
# ---------------------------------------------------------------------

def ask_refund_user(message):
    try:
        refund_user_id = int(message.text)
        msg = bot.send_message(message.chat.id, "💰 Enter refund amount:")
        bot.register_next_step_handler(msg, process_refund, refund_user_id)
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid user ID. Please enter numeric ID only.")

def process_refund(message, refund_user_id):
    try:
        amount = float(message.text)
        user = users_col.find_one({"user_id": refund_user_id})
        
        if not user:
            bot.send_message(message.chat.id, "⚠️ User not found in database.")
            return
        
        add_balance(refund_user_id, amount)
        new_balance = get_balance(refund_user_id)
        bot.send_message(
            message.chat.id,
            f"✅ Refunded {format_currency(amount)} to user {refund_user_id}\n"
            f"💰 New Balance: {format_currency(new_balance)}"
        )
        
        try:
            bot.send_message(
                refund_user_id,
                f"💸 {format_currency(amount)} refunded to your wallet!\n"
                f"💰 New Balance: {format_currency(new_balance)} ✅"
            )
        except Exception:
            bot.send_message(message.chat.id, "⚠️ Could not DM the user (maybe blocked).")
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid amount entered. Please enter a number.")
    except Exception as e:
        logger.exception("Error in process_refund:")
        bot.send_message(message.chat.id, f"Error processing refund: {e}")

def ask_message_content(msg):
    try:
        target_user_id = int(msg.text)
        user_exists = users_col.find_one({"user_id": target_user_id})
        if not user_exists:
            bot.send_message(msg.chat.id, "❌ User not found in database.")
            return
        
        bot.send_message(msg.chat.id, f"💬 Now send the message (text, photo, video, or document) for user {target_user_id}:")
        bot.register_next_step_handler(msg, process_user_message, target_user_id)
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid user ID. Please enter numeric ID only.")

def process_user_message(msg, target_user_id):
    try:
        text = getattr(msg, "text", None) or getattr(msg, "caption", "") or ""
        is_photo = bool(getattr(msg, "photo", None))
        is_video = getattr(msg, "video", None) is not None
        is_document = getattr(msg, "document", None) is not None
        
        try:
            if is_photo and getattr(msg, "photo", None):
                bot.send_photo(target_user_id, photo=msg.photo[-1].file_id, caption=text or "")
            elif is_video and getattr(msg, "video", None):
                bot.send_video(target_user_id, video=msg.video.file_id, caption=text or "")
            elif is_document and getattr(msg, "document", None):
                bot.send_document(target_user_id, document=msg.document.file_id, caption=text or "")
            else:
                bot.send_message(target_user_id, f"💌 Message from Admin:\n{text}")
            bot.send_message(msg.chat.id, f"✅ Message sent successfully to user {target_user_id}")
        except Exception as e:
            bot.send_message(msg.chat.id, f"❌ Failed to send message to user {target_user_id}. User may have blocked the bot.")
    except Exception as e:
        logger.exception("Error in process_user_message:")
        bot.send_message(msg.chat.id, f"Error sending message: {e}")

# ---------------------------------------------------------------------
# COUNTRY SELECTION FUNCTIONS
# ---------------------------------------------------------------------

COUNTRIES_PER_PAGE = 7

def show_countries(chat_id, page=0, message_id=None):
    if not has_user_joined_channels(chat_id):
        start(bot.send_message(chat_id, "/start"))
        return
    
    countries = get_all_countries()
    if not countries:
        text = "🌍 <b>Select Country</b>\n\n❌ No countries available right now. Please check back later."
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu"))
        sent_msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
        user_last_message[chat_id] = sent_msg.message_id
        return

    total = len(countries)
    total_pages = max(1, (total + COUNTRIES_PER_PAGE - 1) // COUNTRIES_PER_PAGE)
    page = max(0, min(page, total_pages - 1))

    start_idx = page * COUNTRIES_PER_PAGE
    end_idx   = start_idx + COUNTRIES_PER_PAGE
    page_countries = countries[start_idx:end_idx]

    text = (
        f"🌍 <b>Select Country</b>\n"
        f"<i>Page {page + 1} of {total_pages}  •  {total} countries total</i>\n\n"
        f"Choose your country below 👇"
    )
    markup = InlineKeyboardMarkup(row_width=2)

    row = []
    for country in page_countries:
        flag = get_country_flag(country['name'])
        row.append(InlineKeyboardButton(
            f"{flag} {country['name']}",
            callback_data=f"country_raw_{country['name']}"
        ))
        if len(row) == 2:
            markup.add(*row)
            row = []
    if row:
        markup.add(*row)

    # Navigation row
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"countries_pg_{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"countries_pg_{page + 1}"))
    if nav_buttons:
        markup.add(*nav_buttons)

    markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_menu"))

    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
            return
        except:
            pass
    sent_msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
    user_last_message[chat_id] = sent_msg.message_id

def show_country_details(user_id, country_name, chat_id, message_id, callback_id):
    try:
        country = get_country_by_name(country_name)
        if not country:
            bot.answer_callback_query(callback_id, "❌ Country not found", show_alert=True)
            return
        
        accounts_count = get_available_accounts_count(country_name)
        
        # WITH EXPANDABLE BLOCKQUOTE - UI STYLE
        flag = get_country_flag(country_name)
        dial = get_country_code(country_name)
        text = f"""⚡ <b>Telegram Account Info</b>

<blockquote>{flag} Country : {country_name} {dial}
💸 Price : {format_currency(country['price'])}
📦 Available : {accounts_count}

🔍 Reliable | Affordable | Good Quality

⚠️ Use Turbotel and plus messenger only to login.
🚫 Not responsible for freeze / ban.</blockquote>"""
        
        markup = InlineKeyboardMarkup(row_width=1)
        if accounts_count > 0:
            markup.add(InlineKeyboardButton(
                "🛒 Buy Now",
                callback_data=f"buy_now_{country_name}"
            ))
        else:
            markup.add(InlineKeyboardButton("❌ Out of Stock", callback_data="out_of_stock"))
        markup.add(InlineKeyboardButton("⬅️ Back", callback_data="back_to_countries"))
        
        edit_or_resend(
            chat_id,
            message_id,
            text,
            markup=markup,
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Country details error: {e}")
        bot.answer_callback_query(callback_id, "❌ Error loading country details", show_alert=True)

# ---------------------------------------------------------------------
# PROCESS PURCHASE FUNCTION
# ---------------------------------------------------------------------

def process_purchase(user_id, account_or_id, chat_id, message_id, callback_id):
    """
    account_or_id: can be a dict (account document) or a str/ObjectId (account _id).
    Passing the dict directly avoids BSON/re-fetch issues.
    """
    try:
        # Resolve account — accept dict directly to skip fragile re-fetch
        if isinstance(account_or_id, dict):
            account = account_or_id
            account_id = str(account.get('_id', ''))
        else:
            account_id = str(account_or_id)
            account = None
            _oid2 = safe_obj_id(account_id)
            if _oid2:
                try:
                    account = accounts_col.find_one({"_id": _oid2})
                except Exception:
                    pass
            if not account:
                try:
                    account = accounts_col.find_one({"_id": account_id})
                except Exception:
                    pass

        if not account:
            logger.error(f"process_purchase: account not found for id={account_or_id} type={type(account_or_id)}")
            bot.answer_callback_query(callback_id, "❌ Account not available", show_alert=True)
            return

        logger.info(f"process_purchase: account resolved _id={account.get('_id')} used={account.get('used')} country={account.get('country')}")

        if account.get('used', False):
            bot.answer_callback_query(callback_id, "❌ Account already sold out", show_alert=True)
            try:
                bot.delete_message(chat_id, message_id)
            except:
                pass
            show_countries(chat_id)
            return
        
        country = get_country_by_name(account['country'])
        if not country:
            bot.answer_callback_query(callback_id, "❌ Country not found", show_alert=True)
            return
        
        price = country['price']
        balance = get_balance(user_id)
        
        if balance < price:
            needed = price - balance
            bot.answer_callback_query(
                callback_id,
                f"❌ Balance not available for purchase!\n\nRequired: {format_currency(price)}\nYour Balance: {format_currency(balance)}\nShortfall: {format_currency(needed)}\n\nPlease recharge your wallet.",
                show_alert=True
            )
            return
        
        # ── Animated purchase progress bar ──────────────────────────
        try:
            _typing(chat_id)
            _anim_msg = bot.send_message(
                chat_id,
                "🔄 <b>Processing your order...</b>\n\n▱▱▱▱▱▱▱▱▱▱  0%",
                parse_mode="HTML"
            )
            _anim_id = _anim_msg.message_id
            _steps = [
                ("🔄 <b>Verifying account...</b>\n\n▰▰▱▱▱▱▱▱▱▱  20%", 0.5),
                ("🔄 <b>Checking balance...</b>\n\n▰▰▰▰▱▱▱▱▱▱  40%", 0.5),
                ("🔄 <b>Securing session...</b>\n\n▰▰▰▰▰▰▱▱▱▱  60%", 0.5),
                ("🔄 <b>Activating account...</b>\n\n▰▰▰▰▰▰▰▰▱▱  80%", 0.5),
                ("✨ <b>Finalizing order...</b>\n\n▰▰▰▰▰▰▰▰▰▰  100%", 0.4),
            ]
            for _txt, _delay in _steps:
                try:
                    bot.edit_message_text(_txt, chat_id, _anim_id, parse_mode="HTML")
                except:
                    pass
                time.sleep(_delay)
        except:
            _anim_id = message_id  # fallback to original message_id
        # ────────────────────────────────────────────────────────────

        deduct_balance(user_id, price)
        
        try:
            from logs import log_purchase_async
            log_purchase_async(
                user_id=user_id,
                country=account['country'],
                price=price,
                phone=account.get('phone', 'N/A')
            )
        except:
            pass
        
        session_id = f"otp_{user_id}_{int(time.time())}"
        otp_session = {
            "session_id": session_id,
            "user_id": user_id,
            "phone": account['phone'],
            "session_string": account.get('session_string', ''),
            "status": "active",
            "created_at": datetime.utcnow(),
            "account_id": str(account['_id']),
            "has_otp": False,
            "last_otp": None,
            "last_otp_time": None
        }
        safe_insert_one(otp_sessions_col, otp_session, "otp_session")
        
        order = {
            "user_id": user_id,
            "account_id": str(account.get('_id')),
            "country": account['country'],
            "price": price,
            "phone_number": account.get('phone', 'N/A'),
            "session_id": session_id,
            "status": "waiting_otp",
            "created_at": datetime.utcnow(),
            "monitoring_duration": 1800
        }
        _order_res = safe_insert_one(orders_col, order, "order")
        order_id = _order_res.inserted_id if _order_res else None
        
        # Mark account as used — use the _id we already have (no re-fetch)
        accounts_col.update_one(
            {"_id": account["_id"]},
            {"$set": {"used": True, "used_at": datetime.utcnow()}}
        )
        
        def start_simple_monitoring():
            try:
                account_manager.start_simple_monitoring_sync(
                    account.get('session_string', ''),
                    session_id,
                    1800
                )
            except Exception as e:
                logger.error(f"Simple monitoring error: {e}")
        
        thread = threading.Thread(target=start_simple_monitoring, daemon=True)
        thread.start()
        
        account_details = (
            "╔══════════════════════╗\n"
            "  ✅  𝐏𝐔𝐑𝐂𝐇𝐀𝐒𝐄 𝐒𝐔𝐂𝐂𝐄𝐒𝐒𝐅𝐔𝐋  ✅\n"
            "╚══════════════════════╝\n\n"
            f"🌍 **Country:** {account['country']}\n"
            f"💸 **Price Paid:** {format_currency(price)}\n"
            f"📱 **Phone Number:** `{account.get('phone', 'N/A')}`\n"
        )

        if account.get('two_step_password'):
            account_details += f"🔒 **2FA Password:** `{account.get('two_step_password', 'N/A')}`\n"

        account_details += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📲 **How to Login:**\n"
            f"1️⃣ Open **Telegram X** or **Turbotel**\n"
            f"2️⃣ Enter number: `{account.get('phone', 'N/A')}`\n"
            f"3️⃣ Click **Next**\n"
            f"4️⃣ Press **🔢 Get OTP** button below\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ OTP valid for **30 minutes**\n"
            f"💰 Remaining Balance: **{format_currency(get_balance(user_id))}**"
        )

        get_otp_markup = InlineKeyboardMarkup(row_width=1)
        get_otp_markup.add(InlineKeyboardButton("🔢 Get OTP Now", callback_data=f"get_otp_{session_id}"))
        get_otp_markup.add(InlineKeyboardButton("🎉 Join Success Group", url=PURCHASE_SUCCESS_LINK))
        get_otp_markup.add(InlineKeyboardButton("🏠 Back to Menu", callback_data="back_to_menu"))

        # Replace the animated progress bar with purchase details
        _final_sent = None
        try:
            _final_sent = bot.edit_message_text(
                account_details,
                chat_id,
                _anim_id,
                parse_mode="Markdown",
                reply_markup=get_otp_markup
            )
        except:
            _final_sent = edit_or_resend(
                chat_id,
                message_id,
                account_details,
                markup=get_otp_markup,
                parse_mode="Markdown"
            )
        sent_msg = _final_sent
        
        if sent_msg:
            user_last_message[user_id] = sent_msg.message_id
        
        bot.answer_callback_query(callback_id, "✅ Purchase successful! Click Get OTP when needed.", show_alert=True)
    
    except Exception as e:
        logger.error(f"Purchase error: {e}")
        try:
            bot.answer_callback_query(callback_id, "❌ Purchase failed", show_alert=True)
        except:
            pass

# =============================================================
# RESTART COMMAND (VPS + HEROKU SAFE)
# =============================================================

@bot.message_handler(commands=['restart'])
def restart_bot(message):
    user_id = message.from_user.id

    if not is_admin(user_id):
        bot.reply_to(message, "❌ Sirf admin use kar sakta hai!")
        return

    bot.reply_to(message, "♻️ Restarting bot...")

    logger.info(f"Admin {user_id} triggered restart")

    time.sleep(1)

    # Clean restart
    os.execv(sys.executable, ['python'] + sys.argv)

# =============================================================
# ALL SLASH COMMANDS
# =============================================================

# /menu — Re-open main menu
@bot.message_handler(commands=['menu'])
def cmd_menu(msg):
    if is_user_banned(msg.from_user.id): return
    ensure_user_exists(msg.from_user.id, msg.from_user.first_name or "Unknown", msg.from_user.username)
    try: bot.delete_message(msg.chat.id, msg.message_id)
    except: pass
    clean_ui_and_send_menu(msg.chat.id, msg.from_user.id)

# /balance — Wallet balance
@bot.message_handler(commands=['balance'])
def cmd_balance(msg):
    user_id = msg.from_user.id
    if is_user_banned(user_id): return
    _typing(msg.chat.id)
    ensure_user_exists(user_id, msg.from_user.first_name or "Unknown", msg.from_user.username)
    anim = AnimLoader(msg.chat.id, [
        "💰 Loading your wallet...",
        "💰 Loading your wallet..",
        "💳 Fetching balance...",
        "💳 Almost ready...",
    ])
    bal = get_balance(user_id)
    user_data = users_col.find_one({"user_id": user_id}) or {}
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("➕ Recharge Wallet", callback_data="recharge"))
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    anim.finish(
        f"💰 <b>Your Wallet</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💳 Balance: <b>{format_currency(bal)}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Total Referrals: {user_data.get('total_referrals', 0)}\n"
        f"🏆 Commission Earned: {format_currency(user_data.get('total_commission_earned', 0))}\n"
        f"🔑 Your Ref Code: <code>{user_data.get('referral_code', 'REF' + str(user_id))}</code>",
        markup=markup
    )

# /profile — Profile card
@bot.message_handler(commands=['profile'])
def cmd_profile(msg):
    user_id = msg.from_user.id
    if is_user_banned(user_id): return
    _typing(msg.chat.id)
    ensure_user_exists(user_id, msg.from_user.first_name or "Unknown", msg.from_user.username)
    anim = AnimLoader(msg.chat.id, AnimLoader.PROFILE_FRAMES)
    user_data = users_col.find_one({"user_id": user_id}) or {}
    total_orders = orders_col.count_documents({"user_id": user_id})
    bal = get_balance(user_id)
    joined = user_data.get("created_at", datetime.utcnow()).strftime("%d %b %Y") if user_data.get("created_at") else "N/A"
    role = "👑 Super Admin" if is_super_admin(user_id) else ("🛡️ Admin" if is_admin(user_id) else "👤 User")
    total_refs = user_data.get('total_referrals', 0)
    commission = format_currency(user_data.get('total_commission_earned', 0))
    ref_code = user_data.get('referral_code', 'REF' + str(user_id))
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("💰 Check Balance", callback_data="wallet_info"),
        InlineKeyboardButton("🛒 Buy Account", callback_data="buy_account")
    )
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    anim.finish(
        f"🪪 <b>Your Profile Card</b>\n\n"
        f"╔══════════════════════╗\n"
        f"  👤 {msg.from_user.first_name or 'User'}  |  {role}\n"
        f"╚══════════════════════╝\n\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"💰 <b>Balance:</b> {format_currency(bal)}\n"
        f"🛒 <b>Total Orders:</b> {total_orders}\n"
        f"👥 <b>Referrals:</b> {total_refs}  |  💎 Commission: {commission}\n"
        f"📅 <b>Joined:</b> {joined}\n"
        f"🔑 <b>Ref Code:</b> <code>{ref_code}</code>",
        markup=markup
    )

# /price — Live price list with stock
@bot.message_handler(commands=['price'])
def cmd_price(msg):
    user_id = msg.from_user.id
    if is_user_banned(user_id): return
    _typing(msg.chat.id)
    ensure_user_exists(user_id, msg.from_user.first_name or "Unknown", msg.from_user.username)
    anim = AnimLoader(msg.chat.id, AnimLoader.PRICE_FRAMES)
    countries = get_all_countries()
    if not countries:
        anim.finish("❌ No countries available right now.")
        return
    lines = ["📋 <b>Live Price List &amp; Stock</b>\n━━━━━━━━━━━━━━━━━━━━\n"]
    for c in countries:
        stock = accounts_col.count_documents({"country": c['name'], "status": "active", "used": {"$ne": True}})
        status = "🟢" if stock > 0 else "🔴"
        lines.append(f"{status} <b>{c['name']}</b> — {format_currency(c['price'])}  |  📦 Stock: {stock}")
    lines.append("\n━━━━━━━━━━━━━━━━━━━━")
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🛒 Buy Now", callback_data="buy_account"))
    markup.add(InlineKeyboardButton("🔄 Refresh", callback_data="back_to_menu"))
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    anim.finish("\n".join(lines), markup=markup)

# /history — Your last 10 purchases
@bot.message_handler(commands=['history'])
def cmd_history(msg):
    user_id = msg.from_user.id
    if is_user_banned(user_id): return
    _typing(msg.chat.id)
    ensure_user_exists(user_id, msg.from_user.first_name or "Unknown", msg.from_user.username)
    anim = AnimLoader(msg.chat.id, AnimLoader.HISTORY_FRAMES)
    orders = list(orders_col.find({"user_id": user_id}).sort("created_at", -1).limit(10))
    if not orders:
        anim.finish("📭 <b>No Purchase History</b>\n\nYou haven't bought any accounts yet.\n\nUse /price to see available accounts and start buying!")
        return
    lines = ["🛒 <b>Your Last 10 Purchases</b>\n━━━━━━━━━━━━━━━━━━━━\n"]
    medals = ["🥇", "🥈", "🥉"]
    for i, o in enumerate(orders, 1):
        date = o.get("created_at", datetime.utcnow()).strftime("%d %b %H:%M") if o.get("created_at") else "N/A"
        medal = medals[i-1] if i <= 3 else f"{i}."
        lines.append(
            f"{medal} 🌍 <b>{o.get('country','?')}</b>  |  💰 {format_currency(o.get('price',0))}  |  🕐 {date}"
        )
    lines.append("\n━━━━━━━━━━━━━━━━━━━━")
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🛒 Buy Again", callback_data="buy_account"))
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    anim.finish("\n".join(lines), markup=markup)

# /refer — Invite friends and earn
@bot.message_handler(commands=['refer'])
def cmd_refer(msg):
    user_id = msg.from_user.id
    if is_user_banned(user_id): return
    _typing(msg.chat.id)
    ensure_user_exists(user_id, msg.from_user.first_name or "Unknown", msg.from_user.username)
    show_referral_info(user_id, msg.chat.id)

# /myid — Your Telegram ID and role
@bot.message_handler(commands=['myid'])
def cmd_myid(msg):
    _typing(msg.chat.id)
    user_id = msg.from_user.id
    role = "👑 Super Admin" if is_super_admin(user_id) else ("🛡️ Admin" if is_admin(user_id) else "👤 User")
    username = f"@{msg.from_user.username}" if msg.from_user.username else "N/A"
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    bot.send_message(
        msg.chat.id,
        f"🆔 <b>Your Info</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Name: {msg.from_user.first_name or 'N/A'}\n"
        f"🔗 Username: {username}\n"
        f"🆔 User ID: <code>{user_id}</code>\n"
        f"🎭 Role: {role}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>💡 Tip: Apna ID share karo referrals ke liye!</i>",
        parse_mode="HTML", reply_markup=markup
    )

# /support — Contact support
@bot.message_handler(commands=['support'])
def cmd_support(msg):
    _typing(msg.chat.id)
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("💬 Contact Support", url="https://t.me/rchiex"))
    markup.add(InlineKeyboardButton("📢 Updates Channel", url="https://t.me/II_LEGEND_OTP_SELLER_UPDATES_II"))
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    bot.send_message(
        msg.chat.id,
        "🛠️ <b>Support Center</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "❓ Koi problem hai? Hum help karenge!\n\n"
        "💬 <b>Support:</b> @rchiex\n"
        "⏱️ <b>Response Time:</b> Kuch ghanton mein\n\n"
        "📌 <b>Common Issues:</b>\n"
        "• OTP nahi aa raha → /cancel karke dobara try karo\n"
        "• Balance nahi kat raha → Admin se contact karo\n"
        "• Account kaam nahi kar raha → Support ko batao\n"
        "━━━━━━━━━━━━━━━━━━━━",
        parse_mode="HTML", reply_markup=markup
    )

# /safety — How you are protected
@bot.message_handler(commands=['safety'])
def cmd_safety(msg):
    _typing(msg.chat.id)
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    bot.send_message(
        msg.chat.id,
        "🛡️ <b>Your Safety — Humare Vaade</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🔐 Sabhi sessions encrypted hain\n"
        "👤 Aapka data kabhi share nahi hoga\n"
        "✅ Delivery se pehle accounts verify hote hain\n"
        "💰 Balance protected — koi unauthorized deduction nahi\n"
        "🔒 OTP expire ho jaate hain, permanently store nahi hote\n"
        "📋 Full transaction logs maintained hain\n"
        "🚫 Koi bhi scam attempt ban hoga turant\n"
        "🤝 100% genuine accounts guaranteed\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<i>✨ Aapki privacy aur safety humaari top priority hai.</i>",
        parse_mode="HTML", reply_markup=markup
    )

# /cancel — Exit any pending input
@bot.message_handler(commands=['cancel'])
def cmd_cancel(msg):
    _typing(msg.chat.id)
    user_id = msg.from_user.id
    admin_deduct_state.pop(user_id, None)
    admin_add_state.pop(user_id, None)
    admin_remove_state.pop(user_id, None)
    bulk_add_states.pop(user_id, None)
    user_stage.pop(user_id, None)
    gemini_chat_sessions.pop(user_id, None)
    edit_price_state.pop(user_id, None)
    login_states.pop(user_id, None)
    upi_payment_states.pop(user_id, None)
    broadcast_data.pop(user_id, None)
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_menu"))
    bot.send_message(
        msg.chat.id,
        "✅ <b>Sabhi pending actions cancel ho gaye!</b>\n\n"
        "🔄 Fresh start ke liye neeche button dabao.\n\n"
        "<i>Koi bhi active session, login ya input clear ho gaya hai.</i>",
        parse_mode="HTML",
        reply_markup=markup
    )

# /ai — Chat with Legendary AI assistant
@bot.message_handler(commands=['ai'])
def cmd_ai(msg):
    user_id = msg.from_user.id
    if is_user_banned(user_id): return
    ensure_user_exists(user_id, msg.from_user.first_name or "Unknown", msg.from_user.username)
    user_stage[user_id] = "ai_chat"
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🚪 Exit AI Chat", callback_data="exit_ai_chat"))
    sent = bot.send_message(
        msg.chat.id,
        "╔══════════════════════╗\n"
        "  ✨ <b>˹ 𝐋ᴇɢᴇɴᴅᴀʀʏ 𝐀𝐈 𝐀𝐬𝐬𝐢𝐬𝐭𝐚𝐧𝐭 ˺</b> ✨\n"
        "╚══════════════════════╝\n\n"
        "🟢 <b>Online &amp; Ready!</b>\n\n"
        "💬 Mujhse kuch bhi pucho:\n"
        "  • Math, Science, Coding\n"
        "  • Writing, Translation\n"
        "  • Bot support, OTP help\n"
        "  • Ya kuch bhi!\n\n"
        "⚡ <i>Bas message bhejo — main hoon yahan!</i>",
        parse_mode="HTML", reply_markup=markup
    )
    user_last_message[user_id] = sent.message_id

# /endchat — Exit AI chat mode
@bot.message_handler(commands=['endchat'])
def cmd_endchat(msg):
    user_id = msg.from_user.id
    user_stage.pop(user_id, None)
    gemini_chat_sessions.pop(user_id, None)
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_menu"))
    bot.send_message(msg.chat.id, "✅ AI Chat se bahar aa gaye. /start se menu kholo.", reply_markup=markup)

# /ping — Bot status, uptime & stats
@bot.message_handler(commands=['ping'])
def cmd_ping(msg):
    import time as _time
    _typing(msg.chat.id)
    t1 = _time.time()
    sent = bot.send_message(msg.chat.id, "📡 Pinging servers...")
    t2 = _time.time()
    latency = round((t2 - t1) * 1000)
    total_users = users_col.count_documents({})
    total_orders = orders_col.count_documents({})
    total_accounts = accounts_col.count_documents({"status": "active", "used": {"$ne": True}})
    # Latency bar
    if latency < 200:
        bar = "🟢🟢🟢🟢🟢"
        status_icon = "🚀 Excellent"
    elif latency < 500:
        bar = "🟡🟡🟡🟢🟢"
        status_icon = "✅ Good"
    elif latency < 1000:
        bar = "🟠🟠🟡🟡🟢"
        status_icon = "⚡ Average"
    else:
        bar = "🔴🟠🟠🟡🟡"
        status_icon = "⚠️ Slow"
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    bot.edit_message_text(
        f"🏓 <b>Pong!</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>Latency:</b> {latency}ms  {bar}\n"
        f"📶 <b>Status:</b> {status_icon}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Total Users:</b> {total_users}\n"
        f"🛒 <b>Total Orders:</b> {total_orders}\n"
        f"📦 <b>Available Stock:</b> {total_accounts}\n"
        f"🤖 <b>Bot Status:</b> 🟢 Online\n"
        f"━━━━━━━━━━━━━━━━━━━━",
        msg.chat.id, sent.message_id, parse_mode="HTML", reply_markup=markup
    )

# /help — Show all commands
@bot.message_handler(commands=['help'])
def cmd_help(msg):
    _typing(msg.chat.id)
    user_id = msg.from_user.id
    text = (
        "📋 <b>Complete Command List</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👤 <b>USER COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/start — Main menu kholein\n"
        "/menu — Main menu dobara kholein\n"
        "/profile — Apna profile card dekhein\n"
        "/balance — Wallet balance check karein\n"
        "/price — Live price list with stock\n"
        "/history — Last 10 purchases dekhein\n"
        "/refer — Dost bulao, paise kamao\n"
        "/myid — Apna Telegram ID aur role dekhein\n"
        "/support — Support se contact karein\n"
        "/safety — Security protection info\n"
        "/cancel — Koi bhi pending input cancel karein\n"
        "/ai — Legendary AI assistant se baat karein\n"
        "/endchat — AI chat mode se bahar aayein\n"
        "/ping — Bot status, uptime &amp; stats\n"
        "/stock — Live stock sabhi countries ka dekhein\n"
        "/topup — Wallet recharge shortcut\n"
        "/orders — Apne recent 5 orders dekhein\n"
        "/coupon &lt;code&gt; — Coupon redeem karein\n"
        "/leaderboard — Top 10 buyers dekhein\n"
        "/help — Yeh command list dekhein\n"
    )
    if is_admin(user_id):
        text += (
            "\n━━━━━━━━━━━━━━━━━━━━\n"
            "👑 <b>ADMIN COMMANDS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "/sales — Aaj ki aur total sales summary\n"
            "/revenue — Country-wise revenue report\n"
            "/topcountries — Top 10 bestselling countries\n"
            "/serverstats — Server stock dashboard\n"
            "/security — Security &amp; risk dashboard\n"
            "/honeypot_list — Honeypot trap users list\n"
            "/sendbroadcast — Sabhi users ko message bhejein\n"
            "/resetbroadcast — Stuck broadcast reset karein\n"
            "/clearaccounts — DB se saare accounts delete karein ⚠️\n"
            "/restart — Bot restart karein\n"
            "/addadmin &lt;user_id&gt; — Kisi ko admin banao\n"
            "/removeadmin &lt;user_id&gt; — Admin access hatao\n"
            "/userinfo &lt;user_id&gt; — Kisi bhi user ki full detail\n"
            "/addbal &lt;user_id&gt; &lt;amount&gt; — User ko balance do\n"
            "/totalusers — Total users, orders &amp; stock stats\n"
            "/ban &lt;user_id&gt; — User ko turant ban karo\n"
            "/unban &lt;user_id&gt; — User ka ban hatao\n"
            "/setprice &lt;country&gt; &lt;price&gt; — Country ka price badlo\n"
            "/couponlist — Saare coupons ki list\n"
            "/pendingorders — Abhi ke pending orders\n"
        )
    if is_super_admin(user_id):
        text += (
            "\n━━━━━━━━━━━━━━━━━━━━\n"
            "🔐 <b>OWNER ONLY COMMANDS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "/setaikey &lt;key&gt; — Naya Gemini AI key set karein\n"
            "/ohelp — Full owner command guide dekhein\n"
        )
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    bot.send_message(msg.chat.id, text, parse_mode="HTML", reply_markup=markup)

# /sales — Sales summary (admin)
@bot.message_handler(commands=['sales'])
def cmd_sales(msg):
    user_id = msg.from_user.id
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Admin only command.")
        return
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_orders = orders_col.count_documents({"created_at": {"$gte": today}})
    total_orders = orders_col.count_documents({})
    today_pipeline = [{"$match": {"created_at": {"$gte": today}}}, {"$group": {"_id": None, "total": {"$sum": "$price"}}}]
    all_pipeline = [{"$group": {"_id": None, "total": {"$sum": "$price"}}}]
    today_rev = list(orders_col.aggregate(today_pipeline))
    all_rev = list(orders_col.aggregate(all_pipeline))
    today_amount = today_rev[0]["total"] if today_rev else 0
    total_amount = all_rev[0]["total"] if all_rev else 0
    bot.send_message(
        msg.chat.id,
        f"📊 <b>Sales Summary</b>\n\n"
        f"📅 Today's Orders: <b>{today_orders}</b>\n"
        f"💰 Today's Revenue: <b>{format_currency(today_amount)}</b>\n\n"
        f"📦 Total Orders (All Time): <b>{total_orders}</b>\n"
        f"🏦 Total Revenue (All Time): <b>{format_currency(total_amount)}</b>",
        parse_mode="HTML"
    )

# /revenue — Revenue report (admin)
@bot.message_handler(commands=['revenue'])
def cmd_revenue(msg):
    user_id = msg.from_user.id
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Admin only command.")
        return
    pipeline = [
        {"$group": {"_id": "$country", "count": {"$sum": 1}, "revenue": {"$sum": "$price"}}},
        {"$sort": {"revenue": -1}}
    ]
    results = list(orders_col.aggregate(pipeline))
    if not results:
        bot.send_message(msg.chat.id, "📭 No revenue data yet.")
        return
    lines = ["💰 <b>Revenue Report by Country</b>\n"]
    for r in results[:15]:
        lines.append(f"🌍 <b>{r['_id']}</b>: {r['count']} sales = {format_currency(r['revenue'])}")
    total = sum(r['revenue'] for r in results)
    lines.append(f"\n🏦 <b>Total Revenue: {format_currency(total)}</b>")
    bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="HTML")

# /topcountries — Top selling countries (admin)
@bot.message_handler(commands=['topcountries'])
def cmd_topcountries(msg):
    user_id = msg.from_user.id
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Admin only command.")
        return
    pipeline = [
        {"$group": {"_id": "$country", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = list(orders_col.aggregate(pipeline))
    if not results:
        bot.send_message(msg.chat.id, "📭 No sales data yet.")
        return
    lines = ["🏆 <b>Top Selling Countries</b>\n"]
    medals = ["🥇", "🥈", "🥉"] + ["🔹"] * 10
    for i, r in enumerate(results):
        stock = accounts_col.count_documents({"country": r["_id"], "status": "active", "used": {"$ne": True}})
        lines.append(f"{medals[i]} <b>{r['_id']}</b> — {r['count']} sold | Stock: {stock}")
    bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="HTML")

# /serverstats — Server stock dashboard (admin)
@bot.message_handler(commands=['serverstats'])
def cmd_serverstats(msg):
    user_id = msg.from_user.id
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Admin only command.")
        return
    s1 = accounts_col.count_documents({"status": "active", "used": {"$ne": True}, "server": {"$ne": 2}})
    s2 = accounts_col.count_documents({"status": "active", "used": {"$ne": True}, "server": 2})
    total = s1 + s2
    used = accounts_col.count_documents({"used": True})
    countries = get_all_countries()
    lines = [
        "📦 <b>Stock Dashboard</b>\n",
        f"📦 Total Available: <b>{total}</b>",
        f"✅ Total Sold: <b>{used}</b>\n",
        "<b>Per Country:</b>"
    ]
    for c in countries:
        cs1 = accounts_col.count_documents({"country": c["name"], "status": "active", "used": {"$ne": True}, "server": {"$ne": 2}})
        cs2 = accounts_col.count_documents({"country": c["name"], "status": "active", "used": {"$ne": True}, "server": 2})
        ctotal = cs1 + cs2
        lines.append(f"🌍 {c['name']}: <b>{ctotal}</b> available")
    bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="HTML")

# ── Rate limiting store (honeypot layer) ──────────────────────────────
_user_msg_times = {}   # user_id → [timestamps]
_RATE_LIMIT = 15       # max messages per 60 seconds
_RATE_WINDOW = 60      # seconds

def _is_rate_limited(user_id):
    """Return True if user is sending too many messages (honeypot layer)."""
    now = time.time()
    times = _user_msg_times.get(user_id, [])
    times = [t for t in times if now - t < _RATE_WINDOW]
    times.append(now)
    _user_msg_times[user_id] = times
    if len(times) > _RATE_LIMIT:
        return True
    return False

# /clearaccounts — Remove ALL accounts from DB (super admin only)
@bot.message_handler(commands=['clearaccounts'])
def cmd_clear_accounts(msg):
    user_id = msg.from_user.id
    if not is_super_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Owner only command.")
        return
    try:
        result = accounts_col.delete_many({})
        deleted = result.deleted_count
        bot.send_message(
            msg.chat.id,
            f"🗑️ <b>All Accounts Cleared</b>\n\n"
            f"✅ Deleted: <b>{deleted}</b> accounts\n"
            f"📦 Database is now empty.\n\n"
            f"You can now re-add accounts via bulk upload.",
            parse_mode="HTML"
        )
        logger.info(f"Admin {user_id} cleared {deleted} accounts from DB")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error clearing accounts: {e}")

# /security — Security dashboard (admin)
@bot.message_handler(commands=['security'])
def cmd_security(msg):
    user_id = msg.from_user.id
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Admin only command.")
        return
    banned_db = banned_users_col.count_documents({"status": "active"})
    total_users = users_col.count_documents({})
    admins = len(get_all_admins())
    total_accounts = accounts_col.count_documents({})
    used_accounts = accounts_col.count_documents({"used": True})
    rate_flagged = sum(1 for t in _user_msg_times.values() if len(t) >= _RATE_LIMIT)
    bot.send_message(
        msg.chat.id,
        f"🛡️ <b>Security Dashboard</b>\n\n"
        f"👥 Total Users: <b>{total_users}</b>\n"
        f"🚫 Banned Users: <b>{banned_db}</b>\n"
        f"👑 Active Admins: <b>{admins}</b>\n"
        f"📦 Accounts in DB: <b>{total_accounts}</b> ({used_accounts} used)\n"
        f"⚡ Rate-Flagged Now: <b>{rate_flagged}</b>\n"
        f"🔐 Webhook: Secured (mTLS)\n"
        f"🍯 Honeypot: <b>Active</b> ({_RATE_LIMIT} msg/{_RATE_WINDOW}s limit)\n"
        f"✅ Bot Status: Online & Protected",
        parse_mode="HTML"
    )

# /honeypot_list — Honeypot / suspicious users (admin)
@bot.message_handler(commands=['honeypot_list'])
def cmd_honeypot_list(msg):
    user_id = msg.from_user.id
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Admin only command.")
        return
    now = time.time()
    lines = ["🕵️ <b>Honeypot Monitor — Active Flagged Users</b>\n"]
    flagged = []
    for uid, times in _user_msg_times.items():
        recent = [t for t in times if now - t < _RATE_WINDOW]
        if len(recent) >= _RATE_LIMIT:
            bal = users_col.find_one({"user_id": uid}, {"balance": 1}) or {}
            orders = orders_col.count_documents({"user_id": uid})
            flagged.append((uid, len(recent), bal.get("balance", 0), orders))
    if not flagged:
        lines.append("✅ No suspicious activity detected right now.")
    else:
        for uid, count, bal, orders in sorted(flagged, key=lambda x: -x[1])[:20]:
            lines.append(f"🚨 <code>{uid}</code> | {count} msgs/min | Bal: {format_currency(bal)} | Orders: {orders}")
    # Also show recent new users
    suspicious = list(users_col.find({}).sort("created_at", -1).limit(10))
    if suspicious:
        lines.append("\n📋 <b>Recent 10 Users:</b>")
        for u in suspicious:
            uid2 = u.get("user_id", "?")
            bal2 = u.get("balance", 0)
            orders2 = orders_col.count_documents({"user_id": uid2})
            flag2 = "⚠️" if (bal2 <= 0 and orders2 == 0) else "✅"
            lines.append(f"{flag2} <code>{uid2}</code> | Bal: {format_currency(bal2)} | Orders: {orders2}")
    bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="HTML")

# /ohelp — Full owner command guide (super admin)
@bot.message_handler(commands=['ohelp'])
def cmd_ohelp(msg):
    user_id = msg.from_user.id
    if not is_super_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Owner only command.")
        return
    bot.send_message(
        msg.chat.id,
        "📖 <b>Full Owner Command Guide</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👤 <b>USER COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/start — Main menu\n"
        "/menu — Main menu dobara\n"
        "/profile — Profile card\n"
        "/balance — Wallet balance\n"
        "/price — Live price list\n"
        "/history — Last 10 purchases\n"
        "/refer — Referral link\n"
        "/myid — Telegram ID + role\n"
        "/support — Support contact\n"
        "/safety — Security info\n"
        "/cancel — Koi bhi input cancel\n"
        "/ai — AI assistant chat\n"
        "/endchat — AI chat exit\n"
        "/ping — Bot status + uptime\n"
        "/stock — Live stock sabhi countries\n"
        "/topup — Wallet recharge shortcut\n"
        "/orders — Recent 5 orders dekhein\n"
        "/coupon <code> — Coupon redeem karein\n"
        "/leaderboard — Top 10 buyers\n"
        "/help — Command list\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👑 <b>ADMIN COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/sales — Sales summary (aaj + total)\n"
        "/revenue — Country-wise revenue\n"
        "/topcountries — Top 10 countries by sales\n"
        "/serverstats — Server stock dashboard\n"
        "/security — Security + risk dashboard\n"
        "/honeypot_list — Honeypot trap users\n"
        "/sendbroadcast — Sabhi users ko broadcast\n"
        "/resetbroadcast — Stuck broadcast reset\n"
        "/clearaccounts — Saare accounts DB se delete ⚠️\n"
        "/restart — Bot restart\n"
        "/addadmin &lt;user_id&gt; — Admin banao\n"
        "/removeadmin &lt;user_id&gt; — Admin hatao\n"
        "/userinfo &lt;user_id&gt; — Kisi bhi user ki full detail\n"
        "/addbal &lt;user_id&gt; &lt;amount&gt; — User ko balance do\n"
        "/totalusers — Total users, orders &amp; stock stats\n"
        "/ban &lt;user_id&gt; — User ko turant ban karo\n"
        "/unban &lt;user_id&gt; — User ka ban hatao\n"
        "/setprice &lt;country&gt; &lt;price&gt; — Country ka price badlo\n"
        "/couponlist — Saare coupons ki list\n"
        "/pendingorders — Abhi ke pending orders\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🔐 <b>OWNER ONLY COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/setaikey &lt;api_key&gt; — Naya Gemini AI key set karo (MongoDB mein save)\n"
        "/ohelp — Yeh guide\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🛠️ <b>ADMIN PANEL (Button) Features:</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "• ➕ Account add/manage\n"
        "• 📢 Broadcast message\n"
        "• 💸 Balance deduct karo\n"
        "• ↩️ Refund karo\n"
        "• 🚫 Ban / ✅ Unban users\n"
        "• 💬 Kisi bhi user ko message\n"
        "• 🌍 Country manage karo\n"
        "• 🎟 Coupon management\n"
        "• 💳 Pending recharge approve/reject\n"
        "• 👥 Admin add/remove (Owner)\n"
        "• 🔐 Admin permissions toggle (Owner)\n\n"
        "<i>👑 Tum Super Admin ho — sabhi commands available hain.</i>",
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------
# NEW USEFUL COMMANDS
# ---------------------------------------------------------------------

# /stock — Live stock count (public)
@bot.message_handler(commands=['stock'])
def cmd_stock(msg):
    _typing(msg.chat.id)
    try:
        countries = list(countries_col.find({"status": "active"}).sort("name", 1))
        if not countries:
            bot.send_message(msg.chat.id, "❌ Koi active country nahi hai abhi.", parse_mode="HTML")
            return
        lines = []
        for c in countries:
            name = c.get("name", "Unknown")
            price = c.get("price", 0)
            cnt = accounts_col.count_documents({
                "$or": [
                    {"country": name, "status": "active", "used": False},
                    {"country": name, "used": {"$exists": False}}
                ]
            })
            status = "✅" if cnt > 0 else "❌"
            lines.append(f"{status} <b>{name}</b> — ₹{price} | Stock: <code>{cnt}</code>")
        text = (
            "╔══════════════════════╗\n"
            "  📦 <b>˹ Live Stock Status ˺</b>\n"
            "╚══════════════════════╝\n\n"
            + "\n".join(lines) +
            "\n\n<i>✅ = Available  ❌ = Out of Stock</i>"
        )
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🛒 Buy Now", callback_data="buy_account"),
                   InlineKeyboardButton("⬅️ Menu", callback_data="back_to_menu"))
        bot.send_message(msg.chat.id, text, parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        logger.error(f"/stock error: {e}")
        bot.send_message(msg.chat.id, "❌ Stock fetch karne mein error aaya.")


# /topup — Quick recharge shortcut
@bot.message_handler(commands=['topup'])
def cmd_topup(msg):
    user_id = msg.from_user.id
    ensure_user_exists(user_id, msg.from_user.first_name,
                       f"@{msg.from_user.username}" if msg.from_user.username else None)
    bal = get_balance(user_id)
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("💳 Add Funds", callback_data="add_funds"))
    markup.add(InlineKeyboardButton("⬅️ Menu", callback_data="back_to_menu"))
    bot.send_message(
        msg.chat.id,
        f"💰 <b>Wallet Recharge</b>\n\n"
        f"📊 Current Balance: <b>₹{bal:.2f}</b>\n\n"
        f"Neeche button dabaao aur amount enter karo:",
        parse_mode="HTML",
        reply_markup=markup
    )


# /orders — User's recent orders
@bot.message_handler(commands=['orders'])
def cmd_orders(msg):
    _typing(msg.chat.id)
    user_id = msg.from_user.id
    try:
        orders = list(orders_col.find({"user_id": user_id}).sort("created_at", -1).limit(5))
        if not orders:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🛒 Buy Now", callback_data="buy_account"))
            bot.send_message(msg.chat.id, "📭 <b>Koi order nahi mila abhi tak.</b>\n\nPehla account kharido!", parse_mode="HTML", reply_markup=markup)
            return
        lines = []
        for o in orders:
            status = o.get("status", "unknown")
            status_icon = {"waiting_otp": "⏳", "completed": "✅", "expired": "❌", "failed": "🔴"}.get(status, "🔵")
            country = o.get("country", "N/A")
            phone = o.get("phone_number", "N/A")
            price = o.get("price", 0)
            created = o.get("created_at")
            date_str = created.strftime("%d %b %H:%M") if created else "N/A"
            lines.append(
                f"{status_icon} <b>{country}</b> | {phone}\n"
                f"   ₹{price} — {date_str} — <i>{status}</i>"
            )
        text = (
            "📦 <b>Your Recent Orders</b> (last 5)\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            + "\n\n".join(lines)
        )
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("⬅️ Menu", callback_data="back_to_menu"))
        bot.send_message(msg.chat.id, text, parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        logger.error(f"/orders error: {e}")
        bot.send_message(msg.chat.id, "❌ Orders fetch karne mein error aaya.")


# /userinfo <user_id> — Admin: detailed user info
@bot.message_handler(commands=['userinfo'])
def cmd_userinfo(msg):
    user_id = msg.from_user.id
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Sirf admin kar sakta hai.")
        return
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /userinfo <user_id>")
        return
    try:
        target_id = int(parts[1].strip())
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid user ID.")
        return
    _typing(msg.chat.id)
    try:
        user = users_col.find_one({"user_id": target_id})
        if not user:
            bot.send_message(msg.chat.id, f"❌ User <code>{target_id}</code> nahi mila DB mein.", parse_mode="HTML")
            return
        bal = get_balance(target_id)
        total_orders = orders_col.count_documents({"user_id": target_id})
        completed = orders_col.count_documents({"user_id": target_id, "status": "completed"})
        referrals = user.get("total_referrals", 0)
        commission = user.get("total_commission_earned", 0.0)
        name = user.get("name", "Unknown")
        username = user.get("username", "No username")
        joined = user.get("created_at")
        joined_str = joined.strftime("%d %b %Y") if joined else "N/A"
        banned = banned_users_col.find_one({"user_id": target_id, "status": "active"})
        is_banned = "🚫 BANNED" if banned else "✅ Active"
        role = "👑 Super Admin" if is_super_admin(target_id) else ("🔰 Admin" if is_admin(target_id) else "👤 User")
        bot.send_message(
            msg.chat.id,
            f"👤 <b>User Info</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: <code>{target_id}</code>\n"
            f"📛 Name: {name}\n"
            f"🔗 Username: {username}\n"
            f"🎭 Role: {role}\n"
            f"📅 Joined: {joined_str}\n"
            f"💰 Balance: ₹{bal:.2f}\n"
            f"📦 Total Orders: {total_orders}\n"
            f"✅ Completed: {completed}\n"
            f"👥 Referrals: {referrals}\n"
            f"💸 Commission: ₹{commission:.2f}\n"
            f"🔒 Status: {is_banned}",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"/userinfo error: {e}")
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# /addbal <user_id> <amount> — Admin: add balance
@bot.message_handler(commands=['addbal'])
def cmd_addbal(msg):
    user_id = msg.from_user.id
    if not is_admin(user_id):
        bot.send_message(msg.chat.id, "❌ Sirf admin kar sakta hai.")
        return
    parts = msg.text.strip().split()
    if len(parts) < 3:
        bot.send_message(msg.chat.id, "Usage: /addbal <user_id> <amount>\nExample: /addbal 123456789 50")
        return
    try:
        target_id = int(parts[1])
        amount = float(parts[2])
        if amount <= 0:
            raise ValueError("Amount positive hona chahiye")
    except ValueError as e:
        bot.send_message(msg.chat.id, f"❌ Invalid input: {e}")
        return
    _typing(msg.chat.id)
    try:
        old_bal = get_balance(target_id)
        add_balance(target_id, amount)
        new_bal = get_balance(target_id)
        bot.send_message(
            msg.chat.id,
            f"✅ <b>Balance Added!</b>\n\n"
            f"👤 User: <code>{target_id}</code>\n"
            f"💰 Added: ₹{amount:.2f}\n"
            f"📊 Old Balance: ₹{old_bal:.2f}\n"
            f"📊 New Balance: ₹{new_bal:.2f}",
            parse_mode="HTML"
        )
        try:
            bot.send_message(
                target_id,
                f"🎉 <b>Aapke wallet mein ₹{amount:.2f} add ho gaye!</b>\n\n"
                f"💰 New Balance: ₹{new_bal:.2f}\n\n"
                f"<i>Admin ke dwara add kiya gaya.</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass
    except Exception as e:
        logger.error(f"/addbal error: {e}")
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# /totalusers — Admin: total registered users
@bot.message_handler(commands=['totalusers'])
def cmd_totalusers(msg):
    if not is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "❌ Sirf admin dekh sakta hai.")
        return
    _typing(msg.chat.id)
    try:
        total = users_col.count_documents({})
        active_today = users_col.count_documents({
            "created_at": {"$gte": datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)}
        })
        total_orders = orders_col.count_documents({})
        completed_orders = orders_col.count_documents({"status": "completed"})
        total_accounts = accounts_col.count_documents({})
        available = accounts_col.count_documents({
            "$or": [
                {"status": "active", "used": False},
                {"used": {"$exists": False}}
            ]
        })
        bot.send_message(
            msg.chat.id,
            f"📊 <b>Bot Statistics</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Total Users: <b>{total}</b>\n"
            f"🆕 New Today: <b>{active_today}</b>\n\n"
            f"📦 Total Orders: <b>{total_orders}</b>\n"
            f"✅ Completed Orders: <b>{completed_orders}</b>\n\n"
            f"📱 Total Accounts in DB: <b>{total_accounts}</b>\n"
            f"🟢 Available Stock: <b>{available}</b>",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"/totalusers error: {e}")
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# /coupon <code> — Quick coupon redeem via command
@bot.message_handler(commands=['coupon'])
def cmd_coupon(msg):
    user_id = msg.from_user.id
    ensure_user_exists(user_id, msg.from_user.first_name,
                       f"@{msg.from_user.username}" if msg.from_user.username else None)
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.send_message(msg.chat.id,
            "🎟 <b>Coupon Redeem</b>\n\nUsage: /coupon &lt;CODE&gt;\nExample: /coupon SAVE50",
            parse_mode="HTML")
        return
    code = parts[1].strip().upper()
    _typing(msg.chat.id)
    success, result_msg = claim_coupon(code, user_id)
    if success:
        bal = get_balance(user_id)
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🛒 Buy Now", callback_data="buy_account"))
        bot.send_message(msg.chat.id,
            f"🎉 <b>Coupon Redeemed!</b>\n\n"
            f"🎟 Code: <code>{code}</code>\n"
            f"✅ {result_msg}\n"
            f"💰 New Balance: ₹{bal:.2f}",
            parse_mode="HTML", reply_markup=markup)
    else:
        bot.send_message(msg.chat.id,
            f"❌ <b>Coupon Failed</b>\n\n🎟 Code: <code>{code}</code>\n⚠️ {result_msg}",
            parse_mode="HTML")


# /ban <user_id> — Admin quick ban
@bot.message_handler(commands=['ban'])
def cmd_ban(msg):
    if not is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "❌ Sirf admin kar sakta hai.")
        return
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /ban <user_id>")
        return
    try:
        target_id = int(parts[1].strip())
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid user ID.")
        return
    try:
        if is_user_banned(target_id):
            bot.send_message(msg.chat.id, f"ℹ️ User <code>{target_id}</code> pehle se banned hai.", parse_mode="HTML")
            return
        safe_insert_one(banned_users_col, {
            "user_id": target_id,
            "banned_by": msg.from_user.id,
            "reason": "Admin banned via /ban",
            "status": "active",
            "banned_at": datetime.utcnow()
        }, "ban")
        bot.send_message(msg.chat.id, f"🚫 <b>User Banned!</b>\n👤 ID: <code>{target_id}</code>", parse_mode="HTML")
        try:
            bot.send_message(target_id, "🚫 <b>Aapko is bot se ban kar diya gaya hai.</b>", parse_mode="HTML")
        except Exception:
            pass
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# /unban <user_id> — Admin quick unban
@bot.message_handler(commands=['unban'])
def cmd_unban(msg):
    if not is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "❌ Sirf admin kar sakta hai.")
        return
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(msg.chat.id, "Usage: /unban <user_id>")
        return
    try:
        target_id = int(parts[1].strip())
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid user ID.")
        return
    try:
        result = banned_users_col.update_one(
            {"user_id": target_id, "status": "active"},
            {"$set": {"status": "unbanned", "unbanned_by": msg.from_user.id, "unbanned_at": datetime.utcnow()}}
        )
        if result.modified_count == 0:
            bot.send_message(msg.chat.id, f"ℹ️ User <code>{target_id}</code> banned nahi hai.", parse_mode="HTML")
        else:
            bot.send_message(msg.chat.id, f"✅ <b>User Unbanned!</b>\n👤 ID: <code>{target_id}</code>", parse_mode="HTML")
            try:
                bot.send_message(target_id, "✅ <b>Aapka ban hat gaya hai. Ab bot use kar sakte ho!</b>", parse_mode="HTML")
            except Exception:
                pass
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# /setprice <country> <price> — Admin quick price change
@bot.message_handler(commands=['setprice'])
def cmd_setprice(msg):
    if not is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "❌ Sirf admin kar sakta hai.")
        return
    parts = msg.text.strip().split()
    if len(parts) < 3:
        bot.send_message(msg.chat.id,
            "Usage: /setprice &lt;Country Name&gt; &lt;price&gt;\nExample: /setprice India 30",
            parse_mode="HTML")
        return
    try:
        price = float(parts[-1])
        country_name = " ".join(parts[1:-1]).strip()
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid price. Number daalo.")
        return
    _typing(msg.chat.id)
    try:
        result = countries_col.update_one(
            {"name": {"$regex": f"^{country_name}$", "$options": "i"}},
            {"$set": {"price": price}}
        )
        if result.matched_count == 0:
            bot.send_message(msg.chat.id, f"❌ Country <b>{country_name}</b> nahi mili.", parse_mode="HTML")
        else:
            bot.send_message(msg.chat.id,
                f"✅ <b>Price Updated!</b>\n\n🌍 Country: <b>{country_name}</b>\n💰 New Price: ₹{price:.2f}",
                parse_mode="HTML")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# /couponlist — Admin: list all coupons
@bot.message_handler(commands=['couponlist'])
def cmd_couponlist(msg):
    if not is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "❌ Sirf admin dekh sakta hai.")
        return
    _typing(msg.chat.id)
    try:
        coupons = list(coupons_col.find({}).sort("created_at", -1).limit(20))
        if not coupons:
            bot.send_message(msg.chat.id, "📭 Koi coupon nahi bana abhi tak.")
            return
        lines = []
        for c in coupons:
            status = c.get("status", "active")
            icon = "✅" if status == "active" else "❌"
            code = c.get("coupon_code", "N/A")
            amount = c.get("amount", 0)
            claimed = c.get("total_claimed_count", 0)
            max_u = c.get("max_users", 0)
            lines.append(f"{icon} <code>{code}</code> — ₹{amount} | {claimed}/{max_u} used | {status}")
        bot.send_message(msg.chat.id,
            "🎟 <b>All Coupons</b> (last 20)\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n" + "\n".join(lines),
            parse_mode="HTML")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# /pendingorders — Admin: pending/active orders
@bot.message_handler(commands=['pendingorders'])
def cmd_pendingorders(msg):
    if not is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "❌ Sirf admin dekh sakta hai.")
        return
    _typing(msg.chat.id)
    try:
        pending = list(orders_col.find({"status": "waiting_otp"}).sort("created_at", -1).limit(15))
        if not pending:
            bot.send_message(msg.chat.id, "✅ Koi pending order nahi hai abhi.")
            return
        lines = []
        for o in pending:
            uid = o.get("user_id", "N/A")
            country = o.get("country", "N/A")
            phone = o.get("phone_number", "N/A")
            price = o.get("price", 0)
            created = o.get("created_at")
            age = ""
            if created:
                diff = datetime.utcnow() - created
                mins = int(diff.total_seconds() // 60)
                age = f"{mins}m ago"
            lines.append(f"⏳ <code>{uid}</code> | {country} | {phone} | ₹{price} | {age}")
        bot.send_message(msg.chat.id,
            f"📋 <b>Pending Orders</b> ({len(pending)})\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n" + "\n".join(lines),
            parse_mode="HTML")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# /leaderboard — Top 10 buyers
@bot.message_handler(commands=['leaderboard'])
def cmd_leaderboard(msg):
    _typing(msg.chat.id)
    try:
        pipeline = [
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        top = list(orders_col.aggregate(pipeline))
        if not top:
            bot.send_message(msg.chat.id, "📭 Abhi koi purchases nahi hue.")
            return
        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, entry in enumerate(top):
            uid = entry["_id"]
            count = entry["count"]
            medal = medals[i] if i < 3 else f"{i+1}."
            user = users_col.find_one({"user_id": uid})
            name = user.get("name", "User") if user else "User"
            lines.append(f"{medal} <b>{name}</b> — {count} purchases")
        bot.send_message(msg.chat.id,
            "🏆 <b>˹ Top Buyers Leaderboard ˺</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n" + "\n".join(lines),
            parse_mode="HTML")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")


# ---------------------------------------------------------------------
# MESSAGE HANDLER FOR ADMIN DEDUCT
# ---------------------------------------------------------------------

@bot.message_handler(func=lambda m: True, content_types=['text','photo','video','document'])
def chat_handler(msg):
    user_id = msg.from_user.id

    # Honeypot: rate limiting — silently drop flood messages
    if not is_admin(user_id) and _is_rate_limited(user_id):
        logger.warning(f"🍯 Honeypot: rate-limited user {user_id}")
        return

    # Check if user is in admin add flow
    if user_id in admin_add_state:
        handle_add_admin_userid(msg)
        return
    
    # Check if user is in admin remove flow
    if user_id in admin_remove_state:
        handle_remove_admin_userid(msg)
        return
    
    if is_admin(user_id) and user_id in admin_deduct_state:
        pass
    
    if is_user_banned(user_id):
        return
    
    ensure_user_exists(
        user_id,
        msg.from_user.first_name or "Unknown",
        msg.from_user.username
    )
    
    if (
        msg.text and msg.text.startswith('/') and
        not (is_admin(user_id) and user_id in admin_deduct_state)
    ):
        return
    
    if is_admin(user_id) and user_id in admin_deduct_state:
        state = admin_deduct_state[user_id]
        
        if state["step"] == "ask_user_id":
            try:
                target_user_id = int(msg.text.strip())
                user_exists = users_col.find_one({"user_id": target_user_id})
                if not user_exists:
                    bot.send_message(user_id, "❌ User not found. Enter valid User ID:")
                    return
                
                current_balance = get_balance(target_user_id)
                admin_deduct_state[user_id] = {
                    "step": "ask_amount",
                    "target_user_id": target_user_id,
                    "current_balance": current_balance
                }
                bot.send_message(
                    user_id,
                    f"👤 User ID: {target_user_id}\n"
                    f"💰 Current Balance: {format_currency(current_balance)}\n\n"
                    f"💸 Enter amount to deduct:"
                )
                return
            except ValueError:
                bot.send_message(user_id, "❌ Invalid User ID. Enter numeric ID:")
                return
        
        elif state["step"] == "ask_amount":
            try:
                amount = float(msg.text.strip())
                current_balance = state["current_balance"]
                if amount <= 0:
                    bot.send_message(user_id, "❌ Amount must be greater than 0:")
                    return
                if amount > current_balance:
                    bot.send_message(
                        user_id,
                        f"❌ Amount exceeds balance ({format_currency(current_balance)}):"
                    )
                    return
                
                admin_deduct_state[user_id] = {
                    "step": "ask_reason",
                    "target_user_id": state["target_user_id"],
                    "amount": amount,
                    "current_balance": current_balance
                }
                bot.send_message(user_id, "📝 Enter reason for deduction:")
                return
            except ValueError:
                bot.send_message(user_id, "❌ Invalid amount. Enter number:")
                return
        
        elif state["step"] == "ask_reason":
            reason = msg.text.strip()
            if not reason:
                bot.send_message(user_id, "❌ Reason cannot be empty:")
                return
            
            target_user_id = state["target_user_id"]
            amount = state["amount"]
            old_balance = state["current_balance"]
            
            deduct_balance(target_user_id, amount)
            new_balance = get_balance(target_user_id)
            
            transaction_id = f"DEDUCT{target_user_id}{int(time.time())}"
            if 'deductions' not in db.list_collection_names():
                db.create_collection('deductions')
            safe_insert_one(db['deductions'], {
                "transaction_id": transaction_id,
                "user_id": target_user_id,
                "amount": amount,
                "reason": reason,
                "admin_id": user_id,
                "old_balance": old_balance,
                "new_balance": new_balance,
                "timestamp": datetime.utcnow()
            }, "deduction")
            
            bot.send_message(
                user_id,
                f"✅ Balance Deducted Successfully\n\n"
                f"👤 User: {target_user_id}\n"
                f"💰 Amount: {format_currency(amount)}\n"
                f"📝 Reason: {reason}\n"
                f"📉 Old Balance: {format_currency(old_balance)}\n"
                f"📈 New Balance: {format_currency(new_balance)}\n"
                f"🆔 Txn ID: {transaction_id}"
            )
            
            try:
                bot.send_message(
                    target_user_id,
                    f"⚠️ Balance Deducted by Admin\n\n"
                    f"💰 Amount: {format_currency(amount)}\n"
                    f"📝 Reason: {reason}\n"
                    f"📈 New Balance: {format_currency(new_balance)}\n"
                    f"🆔 Txn ID: {transaction_id}"
                )
            except:
                bot.send_message(ADMIN_ID, "⚠️ User notification failed (maybe blocked)")
            
            del admin_deduct_state[user_id]
            return
    
    # ── AI Chat mode ──────────────────────────────────────────────────
    if user_stage.get(user_id) == "ai_chat" and msg.text:
        handle_gemini_chat(msg)
        return

    if msg.chat.type == "private":
        bot.send_message(
            user_id,
            "⚠️ Please use /start or buttons from the menu."
        )

# ---------------------------------------------------------------------
# LEGENDARY AI CHATBOT HANDLER
# ---------------------------------------------------------------------

LEGENDARY_AI_SYSTEM = (
    "Your name is 'Legendary AI Assistant' — a powerful, stylish AI built into the "
    "Legendary OTP Seller Telegram bot. "
    "You can help with ANYTHING: math, science, coding, writing, translation, history, "
    "geography, creative tasks, general knowledge, reasoning, and more. "
    "You also know about this bot — help users with buying Telegram accounts, "
    "recharging wallet, getting OTP, referral system, and support. "
    "When greeting (e.g. 'Hello', 'Hi', 'Namaste'), always introduce yourself as "
    "'Legendary AI Assistant' in a stylish, friendly way. "
    "NEVER reveal bot tokens, API keys, database URLs, admin IDs, passwords, or any private config. "
    "Always reply in the same language the user writes in (Hindi, English, Hinglish, Bengali, etc.). "
    "Be accurate, helpful, friendly and concise. "
    "Do NOT use markdown like **, ##, __, ~~, ``` — write plain text only like a normal chat message."
)

def handle_gemini_chat(msg):
    user_id = msg.from_user.id
    text = msg.text.strip() if msg.text else ""

    if not gemini_model:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🚪 Exit AI Chat", callback_data="exit_ai_chat"))
        bot.send_message(msg.chat.id,
            "⚠️ Legendary AI abhi setup nahi hai. Admin se contact karo.",
            reply_markup=markup)
        return

    # ── Privacy Protection + Warn System ─────────────────────────────
    if is_privacy_question(text):
        warn_count = add_privacy_warn(user_id)
        remaining = 3 - warn_count
        user_name = msg.from_user.first_name or "User"
        username = f"@{msg.from_user.username}" if msg.from_user.username else f"ID: {user_id}"
        try:
            bot.send_message(
                ADMIN_ID,
                f"🚨 <b>Privacy Alert!</b>\n\n"
                f"👤 User: {user_name} ({username})\n"
                f"🆔 ID: <code>{user_id}</code>\n"
                f"⚠️ Warn Count: {warn_count}/3\n\n"
                f"💬 Message:\n<code>{text[:500]}</code>",
                parse_mode="HTML"
            )
        except Exception:
            pass
        if warn_count >= 3:
            try:
                banned_users_col.update_one(
                    {"user_id": user_id},
                    {"$set": {"user_id": user_id, "banned_at": datetime.utcnow(), "reason": "Privacy violation (3 warns)"}},
                    upsert=True
                )
            except Exception:
                pass
            user_stage.pop(user_id, None)
            gemini_chat_sessions.pop(user_id, None)
            bot.send_message(msg.chat.id,
                "🚫 <b>Aapko ban kar diya gaya hai.</b>\n\n"
                "3 baar private bot information maangne ki koshish ki — yeh allowed nahi hai.",
                parse_mode="HTML")
            try:
                bot.send_message(ADMIN_ID,
                    f"🔨 <b>Auto-Banned!</b>\n👤 {user_name} ({username})\n🆔 <code>{user_id}</code>\nReason: 3 privacy warns",
                    parse_mode="HTML")
            except Exception:
                pass
            return
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🚪 Exit AI Chat", callback_data="exit_ai_chat"))
        bot.send_message(msg.chat.id,
            f"⚠️ <b>Warning {warn_count}/3</b>\n\n"
            f"Aap bot ki private information access karne ki koshish kar rahe ho.\n"
            f"Yeh allowed nahi hai.\n\n"
            f"{'🚫 Agli galti pe ban ho jaoge!' if remaining == 1 else f'Aur {remaining} warn baad ban ho jaoge.'}",
            parse_mode="HTML", reply_markup=markup)
        return

    try:
        bot.send_chat_action(msg.chat.id, "typing")
    except:
        pass

    last_error = None
    models_to_try = GEMINI_FALLBACK_MODELS if gemini_model else [GEMINI_MODEL_NAME]

    for model_name in models_to_try:
        for attempt in range(2):
            try:
                ai_client = get_genai_client()
                if not ai_client:
                    raise Exception("AI client banane mein problem aayi")

                history = list(gemini_chat_sessions.get(user_id, []))
                history.append({"role": "user", "parts": [{"text": text}]})

                response = ai_client.models.generate_content(
                    model=model_name,
                    contents=history,
                    config=_genai_types.GenerateContentConfig(
                        system_instruction=LEGENDARY_AI_SYSTEM,
                        temperature=0.8,
                        max_output_tokens=2048,
                    )
                )

                raw_reply = response.text.strip() if response.text else "Koi response nahi mila."
                reply = clean_ai_response(raw_reply)

                history.append({"role": "model", "parts": [{"text": reply}]})
                if len(history) > 50:
                    history = history[-50:]
                gemini_chat_sessions[user_id] = history

                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🚪 Exit AI Chat", callback_data="exit_ai_chat"))
                bot.send_message(
                    msg.chat.id,
                    f"✨ <b>˹ 𝐋ᴇɢᴇɴᴅᴀʀʏ 𝐀𝐈 ˺</b>\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"{reply}",
                    parse_mode="HTML",
                    reply_markup=markup
                )
                return

            except Exception as e:
                last_error = e
                err_str = str(e).lower()
                logger.error(f"Legendary AI model={model_name} attempt {attempt+1} failed: {e}")
                gemini_chat_sessions.pop(user_id, None)
                if "quota" in err_str or "resource_exhausted" in err_str:
                    break  # try next model
                if "permission_denied" in err_str or "api_key" in err_str or "invalid" in err_str:
                    break  # key issue, try next model
                time.sleep(0.5)

    logger.error(f"Legendary AI all models failed: {last_error}")
    err_msg = str(last_error).lower() if last_error else ""
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🚪 Exit AI Chat", callback_data="exit_ai_chat"))
    if "api_key" in err_msg or "permission_denied" in err_msg or "invalid" in err_msg:
        reply_text = "⚠️ AI key issue hai. Admin se contact karo."
    elif "quota" in err_msg or "resource_exhausted" in err_msg:
        reply_text = "⏳ AI ka limit reach ho gaya. Thodi der baad try karo."
    else:
        reply_text = "⚠️ Legendary AI abhi respond nahi kar pa raha. Dobara try karo."
    bot.send_message(msg.chat.id, reply_text, reply_markup=markup)

# ---------------------------------------------------------------------
# MANAGE ADMINS PANEL FUNCTION
# ---------------------------------------------------------------------

def show_manage_admins_panel(chat_id, message_id=None):
    if not is_super_admin(chat_id):
        bot.send_message(chat_id, "❌ Only the owner can manage admins!")
        return

    admins = get_all_admins()
    total = len(admins)
    max_admins = 6

    text = (
        "👥 <b>Manage Admins</b>\n\n"
        f"📊 Total Admins: <b>{total}/{max_admins}</b>\n\n"
        "<b>Current Admin List:</b>\n"
    )
    for adm in admins:
        crown = "👑" if adm.get("is_super_admin") else "👤"
        name = adm.get("name", "Unknown")
        uid = adm["user_id"]
        text += f"{crown} <code>{uid}</code> — {name}\n"

    markup = InlineKeyboardMarkup(row_width=2)
    if is_super_admin(chat_id):
        markup.add(
            InlineKeyboardButton("➕ Add Admin", callback_data="admin_add_new"),
            InlineKeyboardButton("🗑 Remove Admin", callback_data="admin_remove_existing")
        )
    markup.add(InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_panel"))

    try:
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
        else:
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

# ---------------------------------------------------------------------
# ADMIN PERMISSIONS PANEL FUNCTIONS
# ---------------------------------------------------------------------

ADMIN_PERMISSIONS = [
    ("add_accounts", "➕ Add Accounts"),
    ("approve_recharge", "💳 Approve Recharge"),
    ("manage_countries", "🌍 Manage Countries"),
    ("ban_users", "🚫 Ban/Unban Users"),
    ("broadcast", "📢 Broadcast"),
    ("deduct_balance", "💸 Deduct Balance"),
    ("refund", "↩️ Refund"),
    ("message_user", "💬 Message User"),
]

def show_admin_permissions_panel(chat_id, message_id=None):
    if not is_super_admin(chat_id):
        bot.send_message(chat_id, "❌ Only owner can access this!")
        return

    admins = get_all_admins()
    non_super = [a for a in admins if not a.get("is_super_admin", False)]

    text = "🔐 <b>Admin Permissions</b>\n\nClick an admin to manage their permissions:\n\n"
    markup = InlineKeyboardMarkup(row_width=1)

    for adm in non_super:
        uid = adm["user_id"]
        name = adm.get("name", "Unknown")
        perms = adm.get("permissions", {})
        # Count enabled permissions (default all True if not set)
        enabled = sum(1 for pk, _ in ADMIN_PERMISSIONS if perms.get(pk, True))
        total_p = len(ADMIN_PERMISSIONS)
        markup.add(InlineKeyboardButton(
            f"👤 {name} ({uid}) — {enabled}/{total_p} perms",
            callback_data=f"view_perms_{uid}"
        ))

    if not non_super:
        text += "No sub-admins added yet."

    markup.add(InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="admin_panel"))

    try:
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
        else:
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)


def show_single_admin_perms(chat_id, message_id, target_uid):
    if not is_super_admin(chat_id):
        return

    admin_doc = admins_col.find_one({"user_id": target_uid})
    if not admin_doc:
        bot.send_message(chat_id, "❌ Admin not found.")
        return

    name = admin_doc.get("name", "Unknown")
    perms = admin_doc.get("permissions", {})

    text = f"🔐 <b>Permissions for</b> <code>{target_uid}</code> ({name})\n\n"
    text += "Toggle permissions on/off:\n"

    markup = InlineKeyboardMarkup(row_width=2)
    perm_buttons = []
    for perm_key, perm_label in ADMIN_PERMISSIONS:
        enabled = perms.get(perm_key, True)
        icon = "✅" if enabled else "❌"
        perm_buttons.append(InlineKeyboardButton(
            f"{icon} {perm_label}",
            callback_data=f"toggle_perm_{target_uid}_{perm_key}"
        ))
    markup.add(*perm_buttons)
    markup.add(InlineKeyboardButton("⬅️ Back", callback_data="admin_permissions"))

    try:
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
        else:
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)


# /setaikey command — owner only, updates Gemini API key in MongoDB
@bot.message_handler(commands=['setaikey'])
def cmd_setaikey(msg):
    if msg.from_user.id != ADMIN_ID:
        bot.send_message(msg.chat.id, "❌ Only the owner can use this command.")
        return
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.send_message(msg.chat.id, "Usage: /setaikey <your_new_gemini_api_key>")
        return
    new_key = parts[1].strip()
    # Test the key before saving
    test_ok = False
    try:
        test_client = _genai.Client(api_key=new_key)
        test_client.models.get(model=GEMINI_MODEL_NAME)
        test_ok = True
    except Exception as e:
        err = str(e)
        if "not found" in err.lower() or "404" in err:
            test_ok = True  # key works, model name issue
        else:
            bot.send_message(msg.chat.id, f"❌ Key test failed: {err[:200]}\n\nKey NOT saved.")
            return
    db['bot_config'].update_one(
        {"key": "gemini_api_key"},
        {"$set": {"key": "gemini_api_key", "value": new_key}},
        upsert=True
    )
    bot.send_message(msg.chat.id, "✅ Gemini API key updated! AI ab naye key se kaam karega.")


# /removewarn — Owner only: remove all privacy warns from a user
@bot.message_handler(commands=['removewarn'])
def cmd_removewarn(msg):
    if msg.from_user.id != ADMIN_ID:
        bot.send_message(msg.chat.id, "❌ Sirf owner hi warns hata sakta hai.")
        return
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.send_message(msg.chat.id, "Usage: /removewarn <user_id>")
        return
    try:
        target_id = int(parts[1].strip())
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid user ID. Sirf number daalo.")
        return
    warn_before = get_privacy_warn_count(target_id)
    if warn_before == 0:
        bot.send_message(msg.chat.id, f"ℹ️ User <code>{target_id}</code> ka koi warn nahi hai.", parse_mode="HTML")
        return
    remove_privacy_warn(target_id)
    bot.send_message(
        msg.chat.id,
        f"✅ <b>Warns removed!</b>\n\n"
        f"👤 User ID: <code>{target_id}</code>\n"
        f"🗑 Removed: {warn_before} warn(s)\n\n"
        f"User ab clean slate pe hai.",
        parse_mode="HTML"
    )
    # Notify user their warns were cleared
    try:
        bot.send_message(
            target_id,
            "✅ <b>Aapki saari warnings hataa di gayi hain.</b>\n\n"
            "Owner ne aapko clean slate diya hai. Agli baar rules follow karo.",
            parse_mode="HTML"
        )
    except Exception:
        pass


# /warnlist — Owner only: see all warned users
@bot.message_handler(commands=['warnlist'])
def cmd_warnlist(msg):
    if msg.from_user.id != ADMIN_ID:
        bot.send_message(msg.chat.id, "❌ Sirf owner dekh sakta hai.")
        return
    try:
        warned = list(privacy_warns_col.find({"warns": {"$gt": 0}}).sort("warns", -1).limit(20))
    except Exception:
        bot.send_message(msg.chat.id, "❌ Database error.")
        return
    if not warned:
        bot.send_message(msg.chat.id, "✅ Koi bhi warn nahi hai abhi.")
        return
    lines = ["⚠️ <b>Privacy Warn List:</b>\n"]
    for w in warned:
        lines.append(f"👤 ID: <code>{w['user_id']}</code> — {w.get('warns', 0)}/3 warns")
    lines.append("\n<i>Use /removewarn &lt;user_id&gt; to clear warns.</i>")
    bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="HTML")


# ---------------------------------------------------------------------
# FLASK WEBHOOK SERVER — exclusive control, no polling conflicts
# ---------------------------------------------------------------------
from flask import Flask, request as flask_request, abort

flask_app = Flask(__name__)

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_PORT = int(os.getenv("PORT", 8080))
REPLIT_DOMAIN = os.getenv("REPLIT_DEV_DOMAIN", "")

@flask_app.route(WEBHOOK_PATH, methods=["POST"])
def telegram_webhook():
    try:
        if flask_request.headers.get("content-type") == "application/json":
            json_str = flask_request.get_data(as_text=True)
            try:
                update = telebot.types.Update.de_json(json_str)
                bot.process_new_updates([update])
            except Exception as _ue:
                logger.error(f"Update processing error: {_ue}")
            return "OK", 200
        abort(403)
    except Exception as _we:
        logger.error(f"Webhook handler error: {_we}")
        return "OK", 200  # Always return 200 to Telegram — never let it retry forever

@flask_app.route("/", methods=["GET"])
def health():
    return "˹ 𝐋ᴇɢᴇɴᴅᴀʀʏ ꭙ 𝐎ᴛᴘ 𝐒ᴇʟʟᴇʀ [ 𝐁ᴏᴛ ] ❤️‍🔥 is running via webhook ✅", 200

@flask_app.errorhandler(Exception)
def handle_flask_exception(e):
    logger.error(f"Flask unhandled exception: {e}")
    return "Internal error — bot still running", 200

@flask_app.errorhandler(500)
def handle_500(e):
    logger.error(f"Flask 500 error: {e}")
    return "OK", 200

# ---------------------------------------------------------------------
# HEARTBEAT SYSTEM — keeps bot alive 24x7 on Railway
# ---------------------------------------------------------------------

def heartbeat_worker():
    """Pings own health endpoint every 4 minutes to prevent Railway from sleeping."""
    import urllib.request
    time.sleep(30)  # wait for server to start
    railway_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    replit_url = os.getenv("REPLIT_DEV_DOMAIN", "")
    base_url = None
    if railway_url:
        base_url = f"https://{railway_url}"
    elif replit_url:
        base_url = f"https://{replit_url}"

    if not base_url:
        logger.warning("⚠️ Heartbeat: No public domain found, skipping.")
        return

    while True:
        try:
            urllib.request.urlopen(f"{base_url}/", timeout=10)
            logger.info("💓 Heartbeat OK")
        except Exception as e:
            logger.warning(f"💔 Heartbeat failed: {e}")
        time.sleep(240)  # every 4 minutes

# ---------------------------------------------------------------------
# RUN BOT
# ---------------------------------------------------------------------

import sys as _sys
import threading as _thr

def _thread_excepthook(args):
    """Log uncaught thread exceptions — prevents silent Railway crashes."""
    logger.error(f"Uncaught thread exception: {args.exc_type.__name__}: {args.exc_value}")

try:
    _thr.excepthook = _thread_excepthook
except AttributeError:
    pass  # Python < 3.8 fallback

def _global_excepthook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        _sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    logger.error(f"Uncaught global exception: {exc_type.__name__}: {exc_value}")

_sys.excepthook = _global_excepthook

if __name__ == "__main__":
    logger.info(f"🤖 ˹ 𝐋ᴇɢᴇɴᴅᴀʀʏ ꭙ 𝐎ᴛᴘ 𝐒ᴇʟʟᴇʀ [ 𝐁ᴏᴛ ] ❤️‍🔥 Starting (Webhook Mode)...")
    logger.info(f"Admin ID: {ADMIN_ID}")
    logger.info(f"Bot Token: {BOT_TOKEN[:10]}...")
    logger.info(f"Must Join Channel 1: {MUST_JOIN_CHANNEL_1}")
    logger.info(f"Must Join Channel 2: {MUST_JOIN_CHANNEL_2}")
    logger.info(f"Log Channel ID: {LOG_CHANNEL_ID}")
    logger.info(f"UPI ID: {UPI_ID}")

    IS_BROADCASTING = False

    try:
        coupons_col.create_index([("coupon_code", 1)], unique=True)
        coupons_col.create_index([("status", 1)])
        coupons_col.create_index([("created_at", -1)])
        logger.info("✅ Coupon indexes created")
    except Exception as e:
        logger.error(f"❌ Failed to create coupon indexes: {e}")

    try:
        admins_col.create_index([("user_id", 1)], unique=True)
        logger.info("✅ Admin indexes created")
    except Exception as e:
        logger.error(f"❌ Failed to create admin indexes: {e}")

    # Start heartbeat thread — keeps bot alive 24x7
    hb_thread = threading.Thread(target=heartbeat_worker, daemon=True)
    hb_thread.start()
    logger.info("💓 Heartbeat system started")

    # Set webhook — supports both Railway and Replit
    RAILWAY_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    public_domain = RAILWAY_DOMAIN or REPLIT_DOMAIN

    if public_domain:
        WEBHOOK_URL = f"https://{public_domain}{WEBHOOK_PATH}"
        try:
            bot.remove_webhook()
            time.sleep(1)
            bot.set_webhook(url=WEBHOOK_URL, drop_pending_updates=True)
            logger.info(f"✅ Webhook set: {WEBHOOK_URL}")
        except Exception as e:
            logger.error(f"❌ Failed to set webhook: {e}")
    else:
        logger.warning("⚠️ No public domain found, falling back to polling")
        def polling_thread():
            while True:
                try:
                    bot.infinity_polling(timeout=60, long_polling_timeout=60, skip_pending=True)
                except Exception as e:
                    logger.error(f"Polling error: {e}")
                    time.sleep(15)
        pt = threading.Thread(target=polling_thread, daemon=True)
        pt.start()

    logger.info(f"🚀 Starting Flask server on port {WEBHOOK_PORT}...")
    flask_app.run(host="0.0.0.0", port=WEBHOOK_PORT, debug=False)

