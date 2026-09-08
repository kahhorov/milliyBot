import logging
import asyncio
import os
import json
import hmac
import aiosqlite
from dotenv import load_dotenv

load_dotenv()
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List, Optional, Dict

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup,
    InlineKeyboardButton, ReplyKeyboardRemove, Update
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters.callback_data import CallbackData
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

# --- Firebase Admin ---
import firebase_admin
from firebase_admin import credentials, firestore as fb_firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================================
# --- SOZLAMALAR ---
# =====================================================================
BOT_TOKEN     = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
MAIN_ADMIN_ID = int(os.getenv("MAIN_ADMIN_ID", "0"))
FRONTEND_URL  = os.getenv("FRONTEND_URL", "")
# WEBHOOK_URL berilmasa — Render avtomatik beradigan RENDER_EXTERNAL_URL ishlatiladi.
# Shu tufayli Render'da qo'shimcha sozlamasiz webhook rejimi o'zi yoqiladi.
WEBHOOK_URL   = os.getenv("WEBHOOK_URL", "") or os.getenv("RENDER_EXTERNAL_URL", "")
WEBHOOK_PATH  = f"/webhook/{BOT_TOKEN}"
DB_PATH       = "database.db"
PORT          = int(os.getenv("PORT", "8000"))

# CRM → /send-notifications endpoint uchun maxfiy kalit.
# CRM bu kalitni "X-API-Key" header'da yuborishi shart.
API_SECRET = os.getenv("API_SECRET", "")

# Telegram global limiti ~30 xabar/sekund. Xavfsiz chegara: har partiyadan keyin
# 1 soniya kutamiz. Bu 300+ o'quvchiga ommaviy yuborishda 429 xatolikning oldini oladi.
NOTIFY_BATCH_SIZE  = 20    # nechta xabardan keyin pauza
NOTIFY_BATCH_PAUSE = 1.0   # pauza (soniya)

# serviceAccountKey.json fayli bot papkasida bo'lishi kerak.
# Render/hosting uchun: butun JSON kontentini FIREBASE_KEY_JSON env-var ga
# joylashtirish mumkin (fayl kerak bo'lmaydi). Bo'lmasa — fayl yo'lidan o'qiladi.
SERVICE_ACCOUNT_KEY  = os.getenv("FIREBASE_KEY_PATH", "serviceAccountKey.json")
FIREBASE_KEY_JSON    = os.getenv("FIREBASE_KEY_JSON", "")
# =====================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher()

# --- RAM cache ---
users: Dict[int, dict]    = {}
admins: set               = {MAIN_ADMIN_ID}
channels: Dict[str, dict] = {}
support_chats: Dict[int, int] = {}

# --- Firebase Firestore client ---
fs_db = None  # firebase_admin firestore client


def _build_firebase_credential():
    """
    Firebase credential ni ikki manbadan yaratadi:
      1) FIREBASE_KEY_JSON env-var (butun JSON kontent) — hosting uchun qulay.
      2) SERVICE_ACCOUNT_KEY fayl yo'li — lokal ishlash uchun.
    """
    if FIREBASE_KEY_JSON.strip():
        info = json.loads(FIREBASE_KEY_JSON)
        logger.info("Firebase credential: FIREBASE_KEY_JSON env-var dan olindi")
        return credentials.Certificate(info)
    if os.path.exists(SERVICE_ACCOUNT_KEY):
        logger.info(f"Firebase credential: {SERVICE_ACCOUNT_KEY} faylidan olindi")
        return credentials.Certificate(SERVICE_ACCOUNT_KEY)
    raise FileNotFoundError(
        "Firebase kaliti topilmadi: FIREBASE_KEY_JSON env-var ni yoki "
        f"'{SERVICE_ACCOUNT_KEY}' faylini sozlang."
    )


def init_firebase():
    global fs_db
    try:
        if not firebase_admin._apps:
            cred = _build_firebase_credential()
            firebase_admin.initialize_app(cred)
        fs_db = fb_firestore.client()
        logger.info("✅ Firebase Admin tayyor")
    except Exception as e:
        logger.warning(f"⚠️ Firebase Admin ishga tushmadi: {e}")
        fs_db = None


# =====================================================================
# --- Firebase HELPERS — asyncio.to_thread() bilan sinxron clientni wraplash
# =====================================================================
async def find_student_by_code(code: int) -> Optional[dict]:
    """
    studentCode bo'yicha barcha guruhlarda qidirish.
    1-usul: collectionGroup query (tez, index kerak)
    2-usul (fallback): har bir guruh bo'yicha alohida qidirish (index kerak emas)
    studentCode int yoki string bo'lishi mumkin — ikkalasini tekshiradi.
    """
    if not fs_db:
        logger.warning("Firebase ulanmagan")
        return None

    def _find_in_all_groups() -> Optional[dict]:
        """Fallback: barcha guruhlarni aylanib, studentni qidirish"""
        try:
            groups = list(fs_db.collection("groups").stream())
            logger.info(f"Fallback: {len(groups)} ta guruh tekshirilmoqda, code={code}")
            for group_doc in groups:
                # int va string sifatida ikkalasini tekshirish
                for val in [int(code), str(code)]:
                    try:
                        docs = list(
                            group_doc.reference.collection("students")
                                     .where(filter=FieldFilter("studentCode", "==", val))
                                     .limit(1)
                                     .stream()
                        )
                        if docs:
                            d = docs[0]
                            data = d.to_dict()
                            data["_id"]   = d.id
                            data["_path"] = d.reference.path
                            logger.info(f"Student topildi (fallback): path={data['_path']}")
                            return data
                    except Exception:
                        pass
            return None
        except Exception as e:
            logger.error(f"_find_in_all_groups xatolik: {e}")
            return None

    def _sync() -> Optional[dict]:
        # 1-usul: collectionGroup query (indeks bo'lsa — 1 read, arzon).
        #         Indeks bo'lmasa xatolik beradi → 2-usul (scan) ishlaydi.
        index_ok = True
        for val in (int(code), str(code)):
            try:
                docs = list(
                    fs_db.collection_group("students")
                         .where(filter=FieldFilter("studentCode", "==", val))
                         .limit(1)
                         .stream()
                )
                if docs:
                    d = docs[0]
                    data = d.to_dict()
                    data["_id"]   = d.id
                    data["_path"] = d.reference.path
                    logger.info(f"Student topildi (collectionGroup): {data['_path']}")
                    return data
            except Exception as e:
                index_ok = False
                logger.info(f"collectionGroup(studentCode) indekssiz → scan: {e}")
                break

        if index_ok:
            # Indeks ishladi, lekin topilmadi — behuda scan qilmaymiz (reads tejaladi)
            logger.warning(f"Student topilmadi (collectionGroup): studentCode={code}")
            return None

        # 2-usul (fallback): barcha guruhlarni aylanib chiqish (indeks kerak emas)
        result = _find_in_all_groups()
        if result:
            logger.info(f"Student topildi (scan): {result.get('studentName')} | code={code}")
        else:
            logger.warning(f"Student topilmadi: studentCode={code}")
        return result

    try:
        return await asyncio.to_thread(_sync)
    except Exception as e:
        logger.error(f"find_student_by_code kutilmagan xatolik: {e}")
        return None


async def verify_and_link_student(student_doc: dict, telegram_id: int) -> bool:
    """Student Firestore dokumentiga telegramId yozish (thread-safe)"""
    if not fs_db:
        return False

    path_parts = student_doc.get("_path", "").split("/")
    if len(path_parts) < 4:
        logger.error(f"Noto'g'ri _path: {student_doc.get('_path')}")
        return False

    group_id   = path_parts[1]
    student_id = path_parts[3]

    def _sync():
        ref = (
            fs_db.collection("groups")
                 .document(group_id)
                 .collection("students")
                 .document(student_id)
        )
        ref.update({"telegramId": str(telegram_id)})
        return True

    try:
        return await asyncio.to_thread(_sync)
    except Exception as e:
        logger.error(f"verify_and_link_student xatolik: {e}")
        return False


def _doc_to_student(d) -> dict:
    data = d.to_dict()
    data["_id"]   = d.id
    data["_path"] = d.reference.path
    return data


async def _cache_link_from_doc(telegram_id: int, data: dict):
    """Topilgan student hujjatidan lokal keshni yangilash."""
    parts = (data.get("_path") or "").split("/")
    if len(parts) < 4:
        return
    first = data.get("studentName") or data.get("firstName") or ""
    last  = data.get("lastName")  or data.get("surname")    or ""
    name  = f"{last} {first}".strip()
    await db_save_student_link(
        telegram_id, parts[1], parts[3],
        str(data.get("studentCode") or ""), name, data.get("phoneNumber") or "",
    )


async def get_linked_student(telegram_id: int) -> Optional[dict]:
    """
    telegram_id bo'yicha bog'langan studentni topish.
    Resurs tejash tartibi:
      1) Lokal kesh (student_links) → to'g'ridan-to'g'ri path bo'yicha 1 ta read
      2) collectionGroup query (indeks bo'lsa — arzon)
      3) Barcha guruhlarni scan qilish (indeks kerak emas, oxirgi chora)
    """
    if not fs_db:
        return None

    tg_str = str(telegram_id)

    # 1) Lokal kesh — bitta hujjat o'qish (guruhlarni skanlamaymiz)
    cached = await db_get_student_link(telegram_id)
    if cached and cached.get("group_id") and cached.get("student_id"):
        def _by_path():
            ref = (
                fs_db.collection("groups")
                     .document(cached["group_id"])
                     .collection("students")
                     .document(cached["student_id"])
            )
            snap = ref.get()
            if not snap.exists:
                return None
            data = _doc_to_student(snap)
            # telegramId hali ham shu foydalanuvchiniki ekanini tekshiramiz
            if str(data.get("telegramId") or "") != tg_str:
                return "stale"
            return data
        try:
            res = await asyncio.to_thread(_by_path)
            if res == "stale":
                await db_delete_student_link(telegram_id)  # kesh eskirgan
            elif res:
                return res
        except Exception as e:
            logger.warning(f"kesh path o'qishda xatolik: {e}")

    # 2) collectionGroup (indeks) → 3) scan
    def _sync():
        try:
            docs = list(
                fs_db.collection_group("students")
                     .where(filter=FieldFilter("telegramId", "==", tg_str))
                     .limit(1)
                     .stream()
            )
            return _doc_to_student(docs[0]) if docs else None  # indeks ishladi
        except Exception as e:
            logger.info(f"collectionGroup(telegramId) indekssiz → scan: {e}")

        try:
            for group_doc in fs_db.collection("groups").stream():
                docs = list(
                    group_doc.reference.collection("students")
                             .where(filter=FieldFilter("telegramId", "==", tg_str))
                             .limit(1)
                             .stream()
                )
                if docs:
                    return _doc_to_student(docs[0])
        except Exception as e:
            logger.error(f"get_linked_student scan xatolik: {e}")
        return None

    try:
        data = await asyncio.to_thread(_sync)
    except Exception as e:
        logger.error(f"get_linked_student xatolik: {e}")
        return None

    if data:
        await _cache_link_from_doc(telegram_id, data)  # keyingi safar arzon bo'ladi
    return data


# =====================================================================
# --- DATABASE ---
# =====================================================================
async def init_db():
    # SQLite faqat student_links keshi uchun (rebuild bo'ladigan, vaqtinchalik ma'lumot).
    # Doimiy ma'lumot (users/admins/channels) Firestore da saqlanadi → server
    # o'chib-yonsa ham yo'qolmaydi (render.com bepul tarifida disk vaqtinchalik).
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS student_links (
                telegram_id  INTEGER PRIMARY KEY,
                group_id     TEXT,
                student_id   TEXT,
                student_code TEXT,
                student_name TEXT,
                phone        TEXT,
                linked_at    TEXT
            )
        """)
        await db.commit()
    logger.info("✅ Database tayyor")


# --- student_links keshi ---
async def db_save_student_link(telegram_id: int, group_id: str, student_id: str,
                               student_code: str, student_name: str, phone: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO student_links "
            "(telegram_id, group_id, student_id, student_code, student_name, phone, linked_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (telegram_id, group_id, student_id, student_code, student_name, phone, get_now()),
        )
        await db.commit()


async def db_get_student_link(telegram_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT group_id, student_id, student_code, student_name, phone "
            "FROM student_links WHERE telegram_id=?", (telegram_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return {
        "group_id": row[0], "student_id": row[1], "student_code": row[2],
        "student_name": row[3], "phone": row[4],
    }


async def db_delete_student_link(telegram_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM student_links WHERE telegram_id=?", (telegram_id,))
        await db.commit()


# =====================================================================
# --- DOIMIY BOT MA'LUMOTI — FIRESTORE (server restart-safe) ---
#
# render.com bepul tarifida disk vaqtinchalik (ephemeral) → SQLite o'chadi.
# Shuning uchun users/admins/channels Firestore da saqlanadi.
# 3 ta hujjatda saqlanadi (foydalanuvchi soniga bog'liq emas):
#   botState/admins   → { ids: [int, ...] }
#   botState/channels → { items: { chId: {name, link} } }
#   botState/users    → { items: { "tgId": {full_name, phone, is_blocked, registered_at} } }
# Startupda faqat 3 ta read (300 o'quvchi bo'lsa ham). Yozishlar kam va arzon.
# =====================================================================
BOT_STATE_COLLECTION = "botState"


def _fs_load_state_sync() -> dict:
    state = {"users": {}, "admins": set(), "channels": {}}
    if not fs_db:
        return state
    try:
        adm = fs_db.collection(BOT_STATE_COLLECTION).document("admins").get()
        if adm.exists:
            for i in (adm.to_dict() or {}).get("ids", []) or []:
                try:
                    state["admins"].add(int(i))
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"admins load: {e}")
    try:
        ch = fs_db.collection(BOT_STATE_COLLECTION).document("channels").get()
        if ch.exists:
            for cid, cd in ((ch.to_dict() or {}).get("items", {}) or {}).items():
                cd = cd or {}
                state["channels"][cid] = {"name": cd.get("name", ""), "link": cd.get("link", "")}
    except Exception as e:
        logger.warning(f"channels load: {e}")
    try:
        ud = fs_db.collection(BOT_STATE_COLLECTION).document("users").get()
        if ud.exists:
            for uid_str, d in ((ud.to_dict() or {}).get("items", {}) or {}).items():
                try:
                    uid = int(uid_str)
                except Exception:
                    continue
                d = d or {}
                state["users"][uid] = {
                    "full_name":     d.get("full_name", ""),
                    "phone":         d.get("phone", ""),
                    "is_blocked":    bool(d.get("is_blocked", False)),
                    "registered_at": d.get("registered_at", ""),
                }
    except Exception as e:
        logger.warning(f"users load: {e}")
    return state


def _fs_save_admins_sync(ids):
    if not fs_db:
        return
    fs_db.collection(BOT_STATE_COLLECTION).document("admins").set(
        {"ids": sorted(int(i) for i in ids)}
    )


def _fs_save_channels_sync(channels_map: dict):
    if not fs_db:
        return
    fs_db.collection(BOT_STATE_COLLECTION).document("channels").set(
        {"items": {str(k): v for k, v in channels_map.items()}}
    )


def _fs_save_user_sync(uid: int, d: dict):
    if not fs_db:
        return
    fs_db.collection(BOT_STATE_COLLECTION).document("users").set(
        {"items": {str(uid): {
            "full_name":     d.get("full_name", ""),
            "phone":         d.get("phone", ""),
            "is_blocked":    bool(d.get("is_blocked", False)),
            "registered_at": d.get("registered_at", ""),
        }}},
        merge=True,  # boshqa foydalanuvchilarni o'chirmaydi
    )


def _fs_set_blocked_sync(uid: int, blocked: bool):
    if not fs_db:
        return
    fs_db.collection(BOT_STATE_COLLECTION).document("users").set(
        {"items": {str(uid): {"is_blocked": bool(blocked)}}},
        merge=True,
    )


async def _read_legacy_sqlite() -> dict:
    """Eski SQLite jadvallaridan (users/admins/channels) ma'lumot o'qish — migratsiya uchun."""
    legacy = {"users": {}, "admins": set(), "channels": {}}
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async def _has(name):
                async with db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
                ) as cur:
                    return await cur.fetchone() is not None
            if await _has("users"):
                async with db.execute(
                    "SELECT user_id, full_name, phone, is_blocked, registered_at FROM users"
                ) as cur:
                    async for r in cur:
                        legacy["users"][r[0]] = {
                            "full_name": r[1] or "", "phone": r[2] or "",
                            "is_blocked": bool(r[3]), "registered_at": r[4] or "",
                        }
            if await _has("admins"):
                async with db.execute("SELECT admin_id FROM admins") as cur:
                    async for r in cur:
                        legacy["admins"].add(r[0])
            if await _has("channels"):
                async with db.execute("SELECT channel_id, name, link FROM channels") as cur:
                    async for r in cur:
                        legacy["channels"][r[0]] = {"name": r[1] or "", "link": r[2] or ""}
    except Exception as e:
        logger.warning(f"legacy SQLite o'qishda xatolik: {e}")
    return legacy


def _fs_save_all_users_sync(users_map: dict):
    if not fs_db:
        return
    items = {
        str(uid): {
            "full_name": d.get("full_name", ""), "phone": d.get("phone", ""),
            "is_blocked": bool(d.get("is_blocked", False)),
            "registered_at": d.get("registered_at", ""),
        }
        for uid, d in users_map.items()
    }
    fs_db.collection(BOT_STATE_COLLECTION).document("users").set({"items": items}, merge=True)


async def load_from_db():
    """Doimiy ma'lumotni Firestore dan RAM ga yuklash (startupda 3 ta read)."""
    state = await asyncio.to_thread(_fs_load_state_sync)
    users.update(state["users"])
    admins.update(state["admins"])
    admins.add(MAIN_ADMIN_ID)  # asosiy admin har doim mavjud (env dan)
    channels.update(state["channels"])

    # ── Bir martalik migratsiya: eski SQLite ma'lumotini Firestore ga ko'chirish ──
    # (yangi versiyaga o'tishda mavjud foydalanuvchi/admin/kanallar yo'qolmasligi uchun)
    if fs_db:
        legacy = await _read_legacy_sqlite()
        new_users = {uid: d for uid, d in legacy["users"].items() if uid not in users}
        new_admins = legacy["admins"] - admins
        new_channels = {c: d for c, d in legacy["channels"].items() if c not in channels}
        if new_users:
            users.update(new_users)
            try:
                await asyncio.to_thread(_fs_save_all_users_sync, dict(users))
            except Exception as e:
                logger.warning(f"migratsiya (users): {e}")
        if new_admins:
            admins.update(new_admins)
            try:
                await asyncio.to_thread(_fs_save_admins_sync, set(admins))
            except Exception as e:
                logger.warning(f"migratsiya (admins): {e}")
        if new_channels:
            channels.update(new_channels)
            try:
                await asyncio.to_thread(_fs_save_channels_sync, dict(channels))
            except Exception as e:
                logger.warning(f"migratsiya (channels): {e}")
        if new_users or new_admins or new_channels:
            logger.info(
                f"🔄 Migratsiya: +{len(new_users)} user, +{len(new_admins)} admin, "
                f"+{len(new_channels)} kanal SQLite dan Firestore ga ko'chirildi"
            )

    logger.info(
        f"✅ Yuklandi: {len(users)} user | {len(admins)} admin | {len(channels)} kanal"
    )


async def db_save_user(user_id: int):
    d = users.get(user_id)
    if not d:
        return
    try:
        await asyncio.to_thread(_fs_save_user_sync, user_id, d)
    except Exception as e:
        logger.warning(f"db_save_user xatolik: {e}")


async def db_update_blocked(user_id: int, is_blocked: bool):
    try:
        await asyncio.to_thread(_fs_set_blocked_sync, user_id, is_blocked)
    except Exception as e:
        logger.warning(f"db_update_blocked xatolik: {e}")


async def db_add_admin(admin_id: int):
    admins.add(admin_id)
    try:
        await asyncio.to_thread(_fs_save_admins_sync, set(admins))
    except Exception as e:
        logger.warning(f"db_add_admin xatolik: {e}")


async def db_remove_admin(admin_id: int):
    admins.discard(admin_id)
    try:
        await asyncio.to_thread(_fs_save_admins_sync, set(admins))
    except Exception as e:
        logger.warning(f"db_remove_admin xatolik: {e}")


async def db_add_channel(ch_id: str, name: str, link: str):
    channels[ch_id] = {"name": name, "link": link}
    try:
        await asyncio.to_thread(_fs_save_channels_sync, dict(channels))
    except Exception as e:
        logger.warning(f"db_add_channel xatolik: {e}")


async def db_remove_channel(ch_id: str):
    channels.pop(ch_id, None)
    try:
        await asyncio.to_thread(_fs_save_channels_sync, dict(channels))
    except Exception as e:
        logger.warning(f"db_remove_channel xatolik: {e}")


# =====================================================================
# --- FSM STATES ---
# =====================================================================
class StudentLinkState(StatesGroup):
    waiting_for_code  = State()  # ID raqam kiritilishi kutilmoqda
    waiting_for_token = State()  # Token kiritilishi kutilmoqda

class ChannelState(StatesGroup):
    waiting_for_id        = State()
    waiting_for_name      = State()

class AdminState(StatesGroup):
    waiting_for_id = State()

class UserState(StatesGroup):
    waiting_for_block_id   = State()
    waiting_for_unblock_id = State()

class BroadcastState(StatesGroup):
    waiting_for_message = State()

class SupportState(StatesGroup):
    in_chat_user  = State()
    in_chat_admin = State()

class SendToUserState(StatesGroup):
    waiting_for_message = State()


# =====================================================================
# --- CALLBACK DATA ---
# =====================================================================
class MenuCB(CallbackData, prefix="menu"):
    action: str

class ChannelCB(CallbackData, prefix="ch"):
    action: str
    channel_id: str

class AdminCB(CallbackData, prefix="adm"):
    action: str
    admin_id: int

class ConfirmCB(CallbackData, prefix="conf"):
    action: str
    target_type: str
    target_id: str

class UserListCB(CallbackData, prefix="ul"):
    page: int
    user_id: int = 0

class SendToUserCB(CallbackData, prefix="stu"):
    user_id: int

class ReplyToAdminCB(CallbackData, prefix="rta"):
    admin_id: int
    user_id: int


# =====================================================================
# --- HELPERS ---
# =====================================================================
def get_now() -> str:
    return datetime.now().strftime("%d.%m.%Y | %H:%M:%S")


async def check_subscription(user_id: int) -> bool:
    if not channels:
        return True
    for ch_id in channels.keys():
        try:
            member = await bot.get_chat_member(chat_id=ch_id, user_id=user_id)
            if member.status in ['left', 'kicked', 'restricted']:
                return False
        except Exception:
            return False
    return True


async def get_sub_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for ch_id, ch_data in channels.items():
        builder.row(InlineKeyboardButton(text=f"📢 {ch_data['name']}", url=ch_data['link']))
    builder.row(InlineKeyboardButton(text="🔄 Tekshirish", callback_data="check_sub"))
    return builder.as_markup()


# =====================================================================
# --- KEYBOARDLAR ---
# =====================================================================
def get_admin_menu():
    builder = ReplyKeyboardBuilder()
    builder.button(text="📊 Statistika")
    builder.button(text="📢 Kanallar")
    builder.button(text="👮‍♂️ Adminlar")
    builder.button(text="📋 Foydalanuvchilar")
    builder.button(text="✉️ Xabar yuborish")
    builder.button(text="🚫 Bloklash")
    builder.button(text="✅ Blokdan ochish")
    builder.button(text="🔙 Chiqish")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_back_admin_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔙 Admin Menyuga Qaytish")]],
        resize_keyboard=True
    )

def get_user_menu():
    builder = ReplyKeyboardBuilder()
    builder.button(text="👤 Mening ma'lumotlarim")
    builder.button(text="🆘 Yordam (Admin bilan chat)")
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

def get_link_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔗 Hisobimni bog'lash", callback_data="start_link"))
    return builder.as_markup()

contact_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📱 Telefon raqamni yuborish", request_contact=True)]],
    resize_keyboard=True, one_time_keyboard=True
)


# =====================================================================
# --- MIDDLEWARE ---
# =====================================================================
async def is_user_allowed(message: types.Message) -> bool:
    user_id = message.from_user.id
    if user_id in users and users[user_id].get("is_blocked"):
        await message.answer("🚫 Siz botdan foydalanishdan cheklangansiz.")
        return False
    if not await check_subscription(user_id):
        sub_kb = await get_sub_keyboard()
        await message.answer(
            "🛑 <b>Botdan foydalanish uchun quyidagi kanallarga obuna bo'lishingiz shart!</b>",
            reply_markup=sub_kb
        )
        return False
    return True


# =====================================================================
# --- /cancel KOMANDASI ---
# =====================================================================
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current = await state.get_state()
    await state.clear()
    if current:
        await message.answer("❌ Bekor qilindi.", reply_markup=ReplyKeyboardRemove())
    else:
        await message.answer("Hech narsa bajarilmayotgan edi.", reply_markup=ReplyKeyboardRemove())


# =====================================================================
# --- /start KOMANDASI ---
# =====================================================================
@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id

    if user_id in users and users[user_id].get("is_blocked"):
        await message.answer("🚫 Siz botdan foydalanishdan cheklangansiz.")
        return

    # Firebase da bu telegram_id bog'langan student bormi?
    linked = await get_linked_student(user_id)
    if linked:
        first = linked.get("studentName") or linked.get("firstName") or ""
        last  = linked.get("lastName") or linked.get("surname") or ""
        full  = f"{last} {first}".strip() or "O'quvchi"
        await message.answer(
            f"👋 Xush kelibsiz, <b>{full}</b>!\n\n"
            f"🔗 Hisobingiz muvaffaqiyatli bog'langan.\n"
            f"📱 Eslatmalar ushbu bot orqali keladi.",
            reply_markup=get_user_menu()
        )
        return

    # Bog'lanmagan — ID so'rash
    await message.answer(
        "👋 <b>Assalomu alaykum!</b>\n\n"
        "Bu Milliy o'quv markazi xabarnoma boti.\n\n"
        "📌 Eslatmalar va to'lov ma'lumotlarini olish uchun\n"
        "hisobingizni bog'lashingiz kerak.\n\n"
        "🔢 Iltimos, <b>o'quvchi ID raqamingizni</b> kiriting:\n"
        "<i>(ID raqamni o'qituvchingizdan oling)</i>",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(StudentLinkState.waiting_for_code)


# ── ID raqam kiritildi ──
@dp.message(StateFilter(StudentLinkState.waiting_for_code))
async def process_student_code(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()

    # /cancel yoki /start tugmachasi
    if text in ("/cancel", "/start"):
        await state.clear()
        await message.answer("❌ Bekor qilindi. Qaytadan /start bosing.", reply_markup=ReplyKeyboardRemove())
        return

    if not text.isdigit():
        await message.answer(
            "❌ <b>ID faqat raqamlardan iborat bo'lishi kerak.</b>\n"
            "Masalan: <code>1001</code>\n\n"
            "Qayta kiriting yoki /cancel yozing:"
        )
        return

    code = int(text)
    if code < 1000:
        await message.answer("❌ ID raqam 1000 dan katta bo'lishi kerak. Qayta kiriting:")
        return

    # Firebase dan qidiramiz — "Qidirilmoqda..." xabari
    wait_msg = await message.answer("⏳ Tekshirilmoqda...")

    student = await find_student_by_code(code)

    try:
        await wait_msg.delete()
    except Exception:
        pass

    if not student:
        await message.answer(
            "❌ <b>Bu ID raqam topilmadi.</b>\n\n"
            "O'qituvchingizdan to'g'ri ID raqamni oling va qayta kiriting.\n"
            "<i>Bekor qilish: /cancel</i>"
        )
        return

    # Allaqachon boshqa Telegram bilan bog'langanmi?
    existing_tg = (student.get("telegramId") or "").strip()
    if existing_tg and existing_tg != str(message.from_user.id):
        await message.answer(
            "⚠️ <b>Bu ID allaqachon boshqa hisob bilan bog'langan.</b>\n\n"
            "Agar xato bo'lsa, o'qituvchingizga murojaat qiling."
        )
        return

    first = student.get("studentName") or student.get("firstName") or ""
    last  = student.get("lastName")  or student.get("surname")    or ""
    full  = f"{last} {first}".strip() or "O'quvchi"

    await state.update_data(student_code=code, student_doc=student, student_name=full)
    await message.answer(
        f"✅ <b>ID topildi!</b>\n"
        f"👤 <b>{full}</b>\n\n"
        "🔐 Endi <b>maxfiy tokeningizni</b> kiriting.\n"
        "<i>Token — o'qituvchingiz bergan uzun kod (UUID)</i>\n\n"
        "<i>Bekor qilish: /cancel</i>"
    )
    await state.set_state(StudentLinkState.waiting_for_token)


# ── Token kiritildi ──
@dp.message(StateFilter(StudentLinkState.waiting_for_token))
async def process_student_token(message: types.Message, state: FSMContext):
    entered = (message.text or "").strip()

    # /cancel yoki /start
    if entered in ("/cancel", "/start"):
        await state.clear()
        await message.answer("❌ Bekor qilindi. Qaytadan /start bosing.", reply_markup=ReplyKeyboardRemove())
        return

    fsm_data     = await state.get_data()
    student_doc  = fsm_data.get("student_doc", {})
    student_name = fsm_data.get("student_name", "O'quvchi")
    correct      = (student_doc.get("token") or "").strip()

    # Token majburiy — bo'sh bo'lmasin
    if not entered:
        await message.answer("❌ Token kiritilmadi. Iltimos, tokenni yuboring:")
        return

    # Case-insensitive solishtirish
    if not correct or entered.lower() != correct.lower():
        await message.answer(
            "❌ <b>Token noto'g'ri.</b>\n\n"
            "O'qituvchingizdan to'g'ri tokenni oling va qayta kiriting.\n"
            "<i>Bekor qilish: /cancel</i>"
        )
        return

    # ── Token to'g'ri → Firebase ga telegramId yozish ──
    wait_msg = await message.answer("⏳ Bog'lanmoqda...")
    success  = await verify_and_link_student(student_doc, message.from_user.id)
    await state.clear()

    try:
        await wait_msg.delete()
    except Exception:
        pass

    if success:
        # Lokal RAM cache ga ham qo'shish (eslatmalar uchun)
        users[message.from_user.id] = {
            "full_name":     student_name,
            "phone":         student_doc.get("phoneNumber", ""),
            "is_blocked":    False,
            "registered_at": get_now(),
        }
        await db_save_user(message.from_user.id)
        # student_links keshiga yozish — keyingi get_linked_student arzon bo'ladi
        await _cache_link_from_doc(message.from_user.id, student_doc)

        code = student_doc.get("studentCode", "")
        await message.answer(
            f"🎉 <b>Muvaffaqiyatli bog'landi!</b>\n\n"
            f"👤 <b>{student_name}</b>\n"
            f"🔢 ID: <code>{code}</code>\n\n"
            f"✅ Endi to'lov eslatmalari va xabarnomalari\n"
            f"shu botga kelib turadi!\n\n"
            f"📚 Darslaringizda omad!",
            reply_markup=get_user_menu()
        )
        logger.info(f"Student bog'landi: {student_name} (code={code}, tg={message.from_user.id})")
    else:
        await message.answer(
            "❌ <b>Xatolik yuz berdi.</b>\n\n"
            "Internet yoki server muammosi bo'lishi mumkin.\n"
            "Iltimos, /start bosib qayta urinib ko'ring."
        )


# ── Inline "Bog'lash" tugmasi ──
@dp.callback_query(F.data == "start_link")
async def start_link_callback(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer(
        "🔢 O'quvchi ID raqamingizni kiriting:",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(StudentLinkState.waiting_for_code)
    await call.answer()


# =====================================================================
# --- FOYDALANUVCHI MENYUSI ---
# =====================================================================
@dp.message(F.text == "👤 Mening ma'lumotlarim")
async def my_info(message: types.Message):
    user_id = message.from_user.id
    linked = await get_linked_student(user_id)
    if not linked:
        await message.answer(
            "❌ Hisobingiz bog'lanmagan.\n/start bosib bog'lashingiz mumkin."
        )
        return
    first = linked.get("studentName") or linked.get("firstName") or ""
    last  = linked.get("lastName") or linked.get("surname") or ""
    full  = f"{last} {first}".strip()
    phone = linked.get("phoneNumber") or "—"
    code  = linked.get("studentCode") or "—"
    await message.answer(
        "👤 <b>Sizning ma'lumotlaringiz</b>\n\n"
        f"📛 <b>Ism:</b> {full}\n"
        f"📞 <b>Telefon:</b> {phone}\n"
        f"🔢 <b>ID:</b> <code>{code}</code>\n"
        f"🔗 <b>Telegram:</b> Bog'langan ✅"
    )


@dp.message(F.contact)
async def handle_contact(message: types.Message):
    user_id = message.from_user.id
    contact = message.contact
    users[user_id] = {
        "full_name":     message.from_user.full_name,
        "phone":         contact.phone_number,
        "is_blocked":    False,
        "registered_at": get_now()
    }
    await db_save_user(user_id)
    await message.answer("✅ Raqamingiz qabul qilindi!", reply_markup=ReplyKeyboardRemove())
    if not await is_user_allowed(message): return
    await message.answer("🌟 <b>Asosiy Menyu</b>", reply_markup=get_user_menu())


@dp.callback_query(F.data == "check_sub")
async def check_sub_callback(call: types.CallbackQuery):
    if await check_subscription(call.from_user.id):
        await call.message.delete()
        await call.message.answer("✅ <b>Siz barcha kanallarga a'zosiz.</b>", reply_markup=get_user_menu())
    else:
        await call.answer("❌ Barcha kanallarga obuna bo'lmagansiz!", show_alert=True)
        sub_kb = await get_sub_keyboard()
        await bot.send_message(call.from_user.id, "🛑 Iltimos, barcha kanallarga obuna bo'ling.", reply_markup=sub_kb)


# =====================================================================
# --- YORDAM (SUPPORT CHAT) ---
# =====================================================================
@dp.message(F.text == "🆘 Yordam (Admin bilan chat)")
async def support_start(message: types.Message, state: FSMContext):
    if not await is_user_allowed(message): return
    await state.set_state(SupportState.in_chat_user)
    await message.answer(
        "🎧 <b>Qo'llab-quvvatlash</b>\n\nXabaringizni yozing. /stop — yakunlash.",
        reply_markup=ReplyKeyboardRemove()
    )


@dp.message(StateFilter(SupportState.in_chat_user))
async def support_user_msg(message: types.Message, state: FSMContext):
    if message.text == "/stop":
        await state.clear()
        await message.answer("Chat yakunlandi.", reply_markup=get_user_menu())
        return
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Javob berish", callback_data=f"reply_{message.from_user.id}")]
    ])
    for admin_id in admins:
        try:
            await bot.send_message(
                admin_id,
                f"📩 <b>Yangi Murojaat!</b>\n"
                f"👤 {message.from_user.full_name} (<code>{message.from_user.id}</code>)\n\n"
                f"💬 {message.text}",
                reply_markup=markup
            )
        except: pass
    await message.answer("✅ Xabaringiz adminlarga yuborildi.")


@dp.callback_query(F.data.startswith("reply_"))
async def admin_reply_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id not in admins: return
    user_id = int(call.data.split("_")[1])
    support_chats[call.from_user.id] = user_id
    await state.set_state(SupportState.in_chat_admin)
    await call.message.answer(f"✍️ Foydalanuvchi (<code>{user_id}</code>) uchun javob yozing:\n/stop — yakunlash.")
    await call.answer()


@dp.message(StateFilter(SupportState.in_chat_admin))
async def admin_reply_msg(message: types.Message, state: FSMContext):
    if message.text == "/stop":
        await state.clear()
        support_chats.pop(message.from_user.id, None)
        await message.answer("Muloqot yakunlandi.", reply_markup=get_admin_menu())
        return
    user_id = support_chats.get(message.from_user.id)
    if user_id:
        try:
            await bot.send_message(user_id, f"👨‍💻 <b>Admindan Javob:</b>\n\n{message.text}")
            await message.answer("✅ Javob yuborildi.")
        except:
            await message.answer("❌ Foydalanuvchiga xabar yuborib bo'lmadi.")


# =====================================================================
# --- ADMIN PANEL ---
# =====================================================================
@dp.message(Command("admin"))
async def cmd_admin(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await state.clear()
    await message.answer("👑 <b>Admin Panel</b>", reply_markup=get_admin_menu())

@dp.message(F.text == "🔙 Chiqish")
async def admin_exit(message: types.Message):
    await message.answer("Chiqildi.", reply_markup=get_user_menu())

@dp.message(F.text == "🔙 Admin Menyuga Qaytish")
async def back_to_admin(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await state.clear()
    await message.answer("👑 <b>Admin Panel</b>", reply_markup=get_admin_menu())


@dp.message(F.text == "📊 Statistika")
async def admin_stats(message: types.Message):
    if message.from_user.id not in admins: return
    total   = len(users)
    blocked = sum(1 for u in users.values() if u.get("is_blocked"))
    text = (
        "📈 <b>STATISTIKA</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🗓 <b>Sana:</b> <i>{get_now()}</i>\n\n"
        f"👥 <b>Umumiy foydalanuvchilar:</b> {total}\n"
        f"✅ <b>Faol:</b> {total - blocked}\n"
        f"🚫 <b>Bloklangan:</b> {blocked}\n"
        f"📢 <b>Kanallar:</b> {len(channels)}\n"
        f"👮‍♂️ <b>Adminlar:</b> {len(admins)}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    await message.answer(text, reply_markup=get_admin_menu())


# =====================================================================
# --- KANALLAR ---
# =====================================================================
@dp.message(F.text == "📢 Kanallar")
async def channels_menu(message: types.Message):
    if message.from_user.id not in admins: return
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Kanal qo'shish",   callback_data="add_channel"))
    builder.row(InlineKeyboardButton(text="📋 Kanallar ro'yxati", callback_data="list_channels"))
    await message.answer("📢 <b>Kanallar</b>", reply_markup=builder.as_markup())


@dp.callback_query(F.data == "list_channels")
async def list_channels(call: types.CallbackQuery):
    if not channels:
        return await call.answer("Kanallar yo'q!", show_alert=True)
    builder = InlineKeyboardBuilder()
    for ch_id, ch_data in channels.items():
        builder.button(text=ch_data["name"], callback_data=ChannelCB(action="view", channel_id=ch_id).pack())
    builder.adjust(2)
    await call.message.edit_text("📋 <b>Kanallar:</b>", reply_markup=builder.as_markup())


@dp.callback_query(ChannelCB.filter(F.action == "view"))
async def view_channel(call: types.CallbackQuery, callback_data: ChannelCB):
    ch_id = callback_data.channel_id
    if ch_id not in channels: return await call.answer("Kanal topilmadi", show_alert=True)
    data  = channels[ch_id]
    builder = InlineKeyboardBuilder()
    builder.button(text="🗑 O'chirish", callback_data=ChannelCB(action="delete", channel_id=ch_id).pack())
    builder.button(text="🔙 Orqaga",   callback_data="list_channels")
    builder.adjust(2, 1)
    await call.message.edit_text(
        f"📢 <b>Kanal</b>\n🆔 <code>{ch_id}</code>\n📝 {data['name']}\n🔗 {data['link']}",
        reply_markup=builder.as_markup()
    )


@dp.callback_query(F.data == "add_channel")
async def start_add_channel(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("✍️ Kanal ID yoki Username kiritng:", reply_markup=get_back_admin_kb())
    await state.set_state(ChannelState.waiting_for_id)
    await call.answer()


@dp.message(StateFilter(ChannelState.waiting_for_id))
async def process_channel_id(message: types.Message, state: FSMContext):
    ch_id = message.text
    try:
        member = await bot.get_chat_member(chat_id=ch_id, user_id=bot.id)
        if member.status not in ['administrator', 'creator']:
            return await message.answer("❌ Bot bu kanalda admin emas.")
        chat_info   = await bot.get_chat(ch_id)
        invite_link = chat_info.invite_link or (f"https://t.me/{chat_info.username}" if chat_info.username else "Havola yo'q")
        await state.update_data(ch_id=str(chat_info.id), link=invite_link)
        await message.answer("✅ Tasdiqlandi. Kanal nomini yozing:")
        await state.set_state(ChannelState.waiting_for_name)
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}")


@dp.message(StateFilter(ChannelState.waiting_for_name))
async def process_channel_name(message: types.Message, state: FSMContext):
    data  = await state.get_data()
    channels[data['ch_id']] = {"name": message.text, "link": data['link']}
    await db_add_channel(data['ch_id'], message.text, data['link'])
    await state.clear()
    await message.answer("🎉 Kanal qo'shildi!", reply_markup=get_admin_menu())


@dp.callback_query(ChannelCB.filter(F.action == "delete"))
async def delete_channel_ask(call: types.CallbackQuery, callback_data: ChannelCB):
    ch_id = callback_data.channel_id
    name  = channels[ch_id]["name"]
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Ha",   callback_data=ConfirmCB(action="yes", target_type="ch_del", target_id=ch_id).pack())
    builder.button(text="❌ Yo'q", callback_data=ConfirmCB(action="no",  target_type="ch_del", target_id=ch_id).pack())
    await call.message.edit_text(f"⚠️ <b>{name}</b> o'chirilsinmi?", reply_markup=builder.as_markup())


@dp.callback_query(ConfirmCB.filter(F.target_type == "ch_del"))
async def delete_channel_confirm(call: types.CallbackQuery, callback_data: ConfirmCB):
    if callback_data.action == "yes":
        channels.pop(callback_data.target_id, None)
        await db_remove_channel(callback_data.target_id)
        await call.message.edit_text("✅ Kanal o'chirildi.")
    else:
        await call.message.edit_text("❌ Bekor qilindi.")


# =====================================================================
# --- FOYDALANUVCHILAR ---
# =====================================================================
@dp.message(F.text == "📋 Foydalanuvchilar")
async def users_list(message: types.Message):
    if message.from_user.id not in admins: return
    await show_users_page(message, page=1)


async def show_users_page(message: types.Message, page: int, edit: bool = False):
    all_users   = list(users.keys())
    total_pages = max(1, (len(all_users) + 9) // 10)
    page        = max(1, min(page, total_pages))
    page_users  = all_users[(page-1)*10 : page*10]

    builder = InlineKeyboardBuilder()
    for uid in page_users:
        name = users.get(uid, {}).get("full_name", "Noma'lum")[:15]
        builder.button(text=f"{uid}: {name}", callback_data=UserListCB(page=page, user_id=uid).pack())
    builder.adjust(2)

    nav = []
    if page > 1:       nav.append(InlineKeyboardButton(text="⬅️",  callback_data=UserListCB(page=page-1, user_id=0).pack()))
    if page < total_pages: nav.append(InlineKeyboardButton(text="➡️",  callback_data=UserListCB(page=page+1, user_id=0).pack()))
    if nav: builder.row(*nav)

    text = f"📋 <b>Foydalanuvchilar</b> ({page}/{total_pages})"
    if edit: await message.edit_text(text, reply_markup=builder.as_markup())
    else:    await message.answer(text, reply_markup=builder.as_markup())


@dp.callback_query(UserListCB.filter())
async def users_pagination(call: types.CallbackQuery, callback_data: UserListCB):
    if callback_data.user_id == 0:
        await show_users_page(call.message, callback_data.page, edit=True)
        await call.answer()
    else:
        uid       = callback_data.user_id
        user_data = users.get(uid)
        if not user_data:
            await call.answer("Topilmadi!", show_alert=True)
            return
        builder = InlineKeyboardBuilder()
        builder.button(
            text="🚫 Bloklash" if not user_data.get("is_blocked") else "✅ Blokdan ochish",
            callback_data=ConfirmCB(action="toggle_block", target_type="user", target_id=str(uid)).pack()
        )
        builder.button(text="✉️ Xabar", callback_data=SendToUserCB(user_id=uid).pack())
        builder.button(text="🔙 Orqaga", callback_data=UserListCB(page=callback_data.page, user_id=0).pack())
        builder.adjust(2, 1)
        blk_text = "Ha" if user_data.get("is_blocked") else "Yo'q"
        await call.message.edit_text(
            f"👤 <b>ID:</b> <code>{uid}</code>\n"
            f"📛 {user_data.get('full_name', '?')}\n"
            f"📞 {user_data.get('phone', '?')}\n"
            f"🚫 Bloklangan: {blk_text}",
            reply_markup=builder.as_markup()
        )
        await call.answer()


@dp.callback_query(ConfirmCB.filter(F.target_type == "user"))
async def toggle_user_block(call: types.CallbackQuery, callback_data: ConfirmCB):
    uid = int(callback_data.target_id)
    if uid not in users:
        await call.answer("Topilmadi!", show_alert=True)
        return
    if callback_data.action == "toggle_block":
        new_status = not users[uid]["is_blocked"]
        users[uid]["is_blocked"] = new_status
        await db_update_blocked(uid, new_status)
        await call.answer(f"{'Bloklandi' if new_status else 'Blokdan ochildi'}.")
        await show_users_page(call.message, 1, edit=True)


# =====================================================================
# --- XABAR YUBORISH ---
# =====================================================================
@dp.callback_query(SendToUserCB.filter())
async def send_to_user_start(call: types.CallbackQuery, callback_data: SendToUserCB, state: FSMContext):
    user_id = callback_data.user_id
    if user_id not in users:
        await call.answer("Topilmadi!", show_alert=True)
        return
    await state.update_data(target_user_id=user_id)
    await state.set_state(SendToUserState.waiting_for_message)
    await call.message.answer(f"✉️ Foydalanuvchi <code>{user_id}</code> ga xabar yozing.\n/stop — bekor qilish.")
    await call.answer()


@dp.message(StateFilter(SendToUserState.waiting_for_message))
async def send_to_user_message(message: types.Message, state: FSMContext):
    if message.text == "/stop":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=get_admin_menu())
        return
    data    = await state.get_data()
    user_id = data.get("target_user_id")
    if not user_id:
        await state.clear()
        return
    try:
        markup = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✍️ Javob yozish",
                callback_data=ReplyToAdminCB(admin_id=message.from_user.id, user_id=user_id).pack()
            )
        ]])
        await message.copy_to(chat_id=user_id, reply_markup=markup)
        await message.answer("✅ Yuborildi.", reply_markup=get_admin_menu())
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}")


@dp.callback_query(ReplyToAdminCB.filter())
async def reply_to_admin_start(call: types.CallbackQuery, callback_data: ReplyToAdminCB, state: FSMContext):
    if call.from_user.id != callback_data.user_id:
        await call.answer("Bu xabar sizga tegishli emas!", show_alert=True)
        return
    support_chats[callback_data.admin_id] = callback_data.user_id
    await state.set_state(SupportState.in_chat_user)
    await call.message.answer("💬 Adminga javob yozing.\n/stop — yakunlash.", reply_markup=ReplyKeyboardRemove())
    await call.answer()


# =====================================================================
# --- ADMINLAR ---
# =====================================================================
@dp.message(F.text == "👮‍♂️ Adminlar")
async def admins_menu(message: types.Message):
    if message.from_user.id not in admins: return
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Admin qo'shish",    callback_data="add_admin"))
    builder.row(InlineKeyboardButton(text="📋 Adminlar ro'yxati", callback_data="list_admins"))
    await message.answer("👮‍♂️ <b>Adminlar</b>", reply_markup=builder.as_markup())


@dp.callback_query(F.data == "list_admins")
async def list_admins(call: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    for adm in admins:
        builder.button(text=str(adm), callback_data=AdminCB(action="view", admin_id=adm).pack())
    builder.adjust(2)
    await call.message.edit_text("📋 <b>Adminlar:</b>", reply_markup=builder.as_markup())


@dp.callback_query(AdminCB.filter(F.action == "view"))
async def view_admin(call: types.CallbackQuery, callback_data: AdminCB):
    adm_id    = callback_data.admin_id
    user_data = users.get(adm_id, {})
    builder = InlineKeyboardBuilder()
    if adm_id != MAIN_ADMIN_ID:
        builder.button(text="🗑 Adminlikdan olish", callback_data=AdminCB(action="remove", admin_id=adm_id).pack())
    builder.button(text="🔙 Orqaga", callback_data="list_admins")
    builder.adjust(1)
    await call.message.edit_text(
        f"👮‍♂️ <b>Admin:</b> <code>{adm_id}</code>\n📛 {user_data.get('full_name','?')}",
        reply_markup=builder.as_markup()
    )


@dp.callback_query(AdminCB.filter(F.action == "remove"))
async def remove_admin(call: types.CallbackQuery, callback_data: AdminCB):
    adm_id = callback_data.admin_id
    if adm_id in admins and adm_id != MAIN_ADMIN_ID:
        admins.remove(adm_id)
        await db_remove_admin(adm_id)
        await call.message.edit_text("✅ Adminlikdan olindi.")
    else:
        await call.answer("Bu amalni bajara olmaysiz!", show_alert=True)


@dp.callback_query(F.data == "add_admin")
async def add_admin_ask(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("✍️ Yangi admin Telegram ID raqamini kiriting:", reply_markup=get_back_admin_kb())
    await state.set_state(AdminState.waiting_for_id)
    await call.answer()


@dp.message(StateFilter(AdminState.waiting_for_id))
async def process_new_admin(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("❌ ID faqat raqamlardan iborat bo'lishi kerak.")
    new_adm = int(message.text)
    admins.add(new_adm)
    await db_add_admin(new_adm)
    await state.clear()
    await message.answer(f"✅ <code>{new_adm}</code> admin qo'shildi!", reply_markup=get_admin_menu())
    try:
        await bot.send_message(new_adm, "🎉 Siz adminlikka tayinlandingiz! /admin")
    except: pass


# =====================================================================
# --- BLOKLASH ---
# =====================================================================
@dp.message(F.text == "🚫 Bloklash")
async def block_user_ask(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await message.answer("🚫 Bloklash uchun Telegram ID:", reply_markup=get_back_admin_kb())
    await state.set_state(UserState.waiting_for_block_id)


@dp.message(StateFilter(UserState.waiting_for_block_id))
async def block_user(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        if uid in users:
            users[uid]["is_blocked"] = True
            await db_update_blocked(uid, True)
            await message.answer(f"✅ {uid} bloklandi.", reply_markup=get_admin_menu())
        else:
            await message.answer("❌ ID topilmadi.")
    except ValueError:
        await message.answer("❌ ID raqam bo'lishi kerak.")
    await state.clear()


@dp.message(F.text == "✅ Blokdan ochish")
async def unblock_user_ask(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await message.answer("✅ Blokdan ochish uchun Telegram ID:", reply_markup=get_back_admin_kb())
    await state.set_state(UserState.waiting_for_unblock_id)


@dp.message(StateFilter(UserState.waiting_for_unblock_id))
async def unblock_user(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        if uid in users:
            users[uid]["is_blocked"] = False
            await db_update_blocked(uid, False)
            await message.answer(f"✅ {uid} blokdan ochildi.", reply_markup=get_admin_menu())
        else:
            await message.answer("❌ ID topilmadi.")
    except ValueError:
        await message.answer("❌ ID raqam bo'lishi kerak.")
    await state.clear()


# =====================================================================
# --- BROADCAST ---
# =====================================================================
@dp.message(F.text == "✉️ Xabar yuborish")
async def broadcast_ask(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await message.answer("✉️ Barcha foydalanuvchilarga xabar kiriting:", reply_markup=get_back_admin_kb())
    await state.set_state(BroadcastState.waiting_for_message)


@dp.message(StateFilter(BroadcastState.waiting_for_message))
async def broadcast_send(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("⏳ Yuborilmoqda...", reply_markup=get_admin_menu())
    success, failed = 0, 0
    for u_id in list(users.keys()):
        if users[u_id].get("is_blocked"): continue
        try:
            await message.copy_to(u_id, reply_markup=None)
            success += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    await message.answer(f"📊 Yuborildi: {success} ✅ | Xatolik: {failed} ❌")


# =====================================================================
# --- FASTAPI ---
# =====================================================================
class NotificationItem(BaseModel):
    studentId:   str
    studentName: str
    message:     str
    telegramId:  Optional[str] = None
    phoneNumber: Optional[str] = None

class NotificationRequest(BaseModel):
    notifications: List[NotificationItem]
    date: str

class NotificationResponse(BaseModel):
    success:        bool
    deliveredCount: int
    failedCount:    int
    errors:         List[dict] = []


USE_POLLING  = not WEBHOOK_URL
_polling_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _polling_task
    init_firebase()
    await init_db()
    await load_from_db()
    # bot.id ni oldindan yuklash (aiogram v3 da kerak)
    await bot.get_me()
    if USE_POLLING:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Bot tayyor! Polling rejimi")
        _polling_task = asyncio.create_task(dp.start_polling(bot))
    else:
        webhook_full_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        await bot.set_webhook(url=webhook_full_url, drop_pending_updates=True)
        logger.info(f"✅ Bot tayyor! Webhook: {webhook_full_url}")
    yield
    # Polling to'xtatish — avval dispatcher, keyin task
    if USE_POLLING:
        await dp.stop_polling()
        if _polling_task and not _polling_task.done():
            _polling_task.cancel()
            try:
                await _polling_task
            except (asyncio.CancelledError, Exception):
                pass
    else:
        await bot.delete_webhook()
    await bot.session.close()
    logger.info("Bot to'xtatildi.")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        *([FRONTEND_URL] if FRONTEND_URL else []),
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        update_data = await request.json()
        update = Update.model_validate(update_data, context={"bot": bot})
        await dp.feed_update(bot, update)
        return JSONResponse(content={"ok": True})
    except Exception as e:
        logger.error(f"Webhook xatolik: {e}")
        return JSONResponse(content={"ok": False}, status_code=200)


@app.get("/")
async def root():
    return {"status": "ok", "mode": "Polling" if USE_POLLING else "Webhook", "total_users": len(users)}


def _verify_api_key(x_api_key: Optional[str]):
    """
    /send-notifications uchun autentifikatsiya.
    API_SECRET sozlangan bo'lsa — X-API-Key header majburiy va mos kelishi kerak.
    Taqqoslash timing-attack ga qarshi hmac.compare_digest bilan qilinadi.
    API_SECRET bo'sh bo'lsa — himoya o'chirilgan (faqat lokal test uchun).
    """
    if not API_SECRET:
        logger.warning("⚠️ API_SECRET sozlanmagan — /send-notifications himoyasiz!")
        return
    if not x_api_key or not hmac.compare_digest(x_api_key, API_SECRET):
        raise HTTPException(status_code=401, detail="Unauthorized: noto'g'ri yoki yo'q X-API-Key")


@app.post("/send-notifications", response_model=NotificationResponse)
async def send_notifications_api(
    data: NotificationRequest,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
):
    """
    CRM dan Telegram xabar yuborish.
    Ustuvorlik: telegramId → to'g'ridan-to'g'ri yuborish.
    Fallback: phoneNumber → users dict qidirish.
    """
    _verify_api_key(x_api_key)

    success_count, failed_count, errors = 0, 0, []
    sent_in_batch = 0  # Telegram rate-limit uchun partiya hisoblagichi

    for item in data.notifications:
        target_chat_id: Optional[int] = None

        # 1) telegramId — bot orqali bog'langan student ID si
        tg = (item.telegramId or "").strip()
        if tg and tg.lstrip("-").isdigit() and int(tg) != 0:
            target_chat_id = int(tg)

        # 2) Fallback: telefon raqam → users dict (eski usul)
        if not target_chat_id and item.phoneNumber:
            clean = item.phoneNumber.replace("+", "").replace(" ", "").replace("-", "")
            for uid, udata in users.items():
                uphone = (udata.get("phone") or "").replace("+", "").replace(" ", "").replace("-", "")
                if uphone and uphone == clean:
                    target_chat_id = uid
                    break

        if not target_chat_id:
            failed_count += 1
            errors.append({"student": item.studentName, "error": "Telegram bog'lanmagan"})
            continue

        # Bloklangan foydalanuvchi — faqat users dict da bo'lsa tekshirish
        if users.get(target_chat_id, {}).get("is_blocked"):
            failed_count += 1
            errors.append({"student": item.studentName, "error": "Bloklangan"})
            continue

        try:
            # Telegram 429 bo'lsa — ko'rsatilgan vaqt kutib, bir marta qayta urinamiz.
            try:
                await bot.send_message(
                    chat_id=target_chat_id,
                    text=item.message,
                    parse_mode=ParseMode.HTML,
                )
            except TelegramRetryAfter as ra:
                logger.warning(f"Rate-limit: {ra.retry_after}s kutilmoqda...")
                await asyncio.sleep(ra.retry_after + 1)
                await bot.send_message(
                    chat_id=target_chat_id,
                    text=item.message,
                    parse_mode=ParseMode.HTML,
                )
            success_count += 1
            logger.info(f"Xabar yuborildi: {item.studentName} → {target_chat_id}")

            # ── Rate-limit throttle: har partiyadan keyin qisqa pauza ──
            sent_in_batch += 1
            if sent_in_batch >= NOTIFY_BATCH_SIZE:
                await asyncio.sleep(NOTIFY_BATCH_PAUSE)
                sent_in_batch = 0
        except TelegramForbiddenError:
            # Foydalanuvchi botni bloklagan
            failed_count += 1
            errors.append({"student": item.studentName, "error": "Bot bloklangan (ForbiddenError)"})
        except Exception as e:
            failed_count += 1
            errors.append({"student": item.studentName, "error": str(e)})

    return {
        "success":        success_count > 0,
        "deliveredCount": success_count,
        "failedCount":    failed_count,
        "errors":         errors,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
