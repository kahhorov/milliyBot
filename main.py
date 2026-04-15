import logging
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List, Optional, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, 
    InlineKeyboardButton, ReplyKeyboardRemove
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters.callback_data import CallbackData
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- SOZLAMALAR ---
BOT_TOKEN = "8634701175:AAHUDA6pAiuSHViQttID5Z6kwtufb5CuRAo"
FRONTEND_URL = "http://localhost:5173"
MAIN_ADMIN_ID = 7114973309  # O'ZINGIZNING TELEGRAM ID RAQAMINGIZNI SHU YERGA YOZING!

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- XOTIRA (IN-MEMORY DATABASES) ---
users: Dict[int, dict] = {}       # {user_id: {"full_name": str, "phone": str, "is_blocked": bool, "registered_at": str}}
admins: set = {MAIN_ADMIN_ID}     # Barcha adminlar ro'yxati (ID lar)
channels: Dict[str, dict] = {}    # {"-100123...": {"name": "Kanal Nomi", "link": "t.me/kanal"}}
support_chats: Dict[int, int] = {} # {admin_id: user_id} - Kim kim bilan gaplashayotgani
# --- FSM STATES ---
class ChannelState(StatesGroup):
    waiting_for_id = State()
    waiting_for_name = State()
    waiting_for_edit_name = State()

class AdminState(StatesGroup):
    waiting_for_id = State()

class UserState(StatesGroup):
    waiting_for_block_id = State()
    waiting_for_unblock_id = State()

class BroadcastState(StatesGroup):
    waiting_for_message = State()

class SupportState(StatesGroup):
    in_chat_user = State()   # Foydalanuvchi admin bilan chatda
    in_chat_admin = State()  # Admin foydalanuvchi bilan chatda

class SendToUserState(StatesGroup):
    waiting_for_message = State()  # Admin foydalanuvchiga xabar yuborayotganida

# --- CALLBACK DATA CLASSES ---
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
    user_id: int = 0  # 0 bo'lsa sahifa, aks holda foydalanuvchi

class SendToUserCB(CallbackData, prefix="stu"):
    user_id: int

class ReplyToAdminCB(CallbackData, prefix="rta"):
    admin_id: int
    user_id: int

# --- YORDAMCHI FUNKSIYALAR ---
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

# --- KEYBOARDLAR ---
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
    builder.adjust(2)  # 2 ustun
    return builder.as_markup(resize_keyboard=True)

def get_back_admin_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Admin Menyuga Qaytish")]], resize_keyboard=True)

def get_user_menu():
    builder = ReplyKeyboardBuilder()
    builder.button(text="🆘 Yordam (Admin bilan chat)")
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

contact_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📱 Telefon raqamni yuborish", request_contact=True)]],
    resize_keyboard=True, one_time_keyboard=True
)

# --- MIDDLEWARE / CHECKERS ---
async def is_user_allowed(message: types.Message) -> bool:
    user_id = message.from_user.id
    if user_id in users and users[user_id].get("is_blocked"):
        await message.answer("🚫 Siz botdan foydalanishdan cheklangansiz.")
        return False
    
    if not await check_subscription(user_id):
        sub_kb = await get_sub_keyboard()
        await message.answer(
            "🛑 <b>DIQQAT! Botdan foydalanish uchun quyidagi kanallarga obuna bo'lishingiz shart!</b>\n\n"
            "<i>Iltimos, barcha kanallarga a'zo bo'lgach, «Tekshirish» tugmasini bosing.</i>",
            reply_markup=sub_kb
        )
        return False
    return True

# --- FOYDALANUVCHI QISMI ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    if user_id in users and users[user_id].get("is_blocked"):
        await message.answer("🚫 Siz botdan foydalanishdan cheklangansiz.")
        return

    if user_id not in users or not users[user_id].get("phone"):
        await message.answer(
            f"Assalomu alaykum, <b>{message.from_user.full_name}</b>! 💎\n\n"
            "Tizimdan foydalanish uchun, iltimos telefon raqamingizni tasdiqlang.",
            reply_markup=contact_keyboard
        )
    else:
        if not await is_user_allowed(message): return
        await message.answer(
            f"🌟 <b>Xush kelibsiz!</b>\n\nSizning ID raqamingiz: <code>{user_id}</code>",
            reply_markup=get_user_menu()
        )

@dp.message(F.contact)
async def handle_contact(message: types.Message):
    user_id = message.from_user.id
    contact = message.contact
    
    users[user_id] = {
        "full_name": message.from_user.full_name,
        "phone": contact.phone_number,
        "is_blocked": False,
        "registered_at": get_now()
    }
    
    await message.answer("✅ Raqamingiz muvaffaqiyatli qabul qilindi!", reply_markup=ReplyKeyboardRemove())
    
    if not await is_user_allowed(message): return
    await message.answer(
        f"🌟 <b>Asosiy Menyu</b>\n\nSizning Telegram ID: <code>{user_id}</code>",
        reply_markup=get_user_menu()
    )

@dp.callback_query(F.data == "check_sub")
async def check_sub_callback(call: types.CallbackQuery):
    if await check_subscription(call.from_user.id):
        await call.message.delete()
        await call.message.answer("✅ <b>Ajoyib! Siz barcha kanallarga a'zosiz.</b>", reply_markup=get_user_menu())
    else:
        await call.answer("❌ Siz barcha kanallarga obuna bo'lmagansiz!", show_alert=True)
        await call.message.delete()
        sub_kb = await get_sub_keyboard()
        await bot.send_message(
            call.from_user.id,
            "🛑 <b>DIQQAT! Botdan foydalanish uchun quyidagi kanallarga obuna bo'lishingiz shart!</b>\n\n"
            "<i>Iltimos, barcha kanallarga a'zo bo'lgach, «Tekshirish» tugmasini bosing.</i>",
            reply_markup=sub_kb
        )

# --- YORDAM (CHAT) QISMI ---
@dp.message(F.text == "🆘 Yordam (Admin bilan chat)")
async def support_start(message: types.Message, state: FSMContext):
    if not await is_user_allowed(message): return
    await state.set_state(SupportState.in_chat_user)
    await message.answer(
        "🎧 <b>Qo'llab-quvvatlash bo'limi</b>\n\n"
        "Xabaringizni yozing, adminlarimiz sizga tez orada javob berishadi.\n"
        "<i>Chatni yakunlash uchun /stop yozing.</i>",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(StateFilter(SupportState.in_chat_user))
async def support_user_msg(message: types.Message, state: FSMContext):
    if message.text == "/stop":
        await state.clear()
        await message.answer("Chat yakunlandi.", reply_markup=get_user_menu())
        return
    
    # Xabarni adminlarga yuborish
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Javob berish", callback_data=f"reply_{message.from_user.id}")]
    ])
    for admin_id in admins:
        try:
            await bot.send_message(
                admin_id,
                f"📩 <b>Yangi Murojaat!</b>\n👤 <b>Foydalanuvchi:</b> {message.from_user.full_name} (<code>{message.from_user.id}</code>)\n\n"
                f"💬 <b>Xabar:</b> {message.text}",
                reply_markup=markup
            )
        except: pass
    await message.answer("✅ Xabaringiz adminlarga yuborildi. Javob kutmoqdamiz...")

@dp.callback_query(F.data.startswith("reply_"))
async def admin_reply_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id not in admins: return
    user_id = int(call.data.split("_")[1])
    
    support_chats[call.from_user.id] = user_id
    await state.set_state(SupportState.in_chat_admin)
    await call.message.answer(
        f"✍️ Foydalanuvchi (<code>{user_id}</code>) uchun javobingizni yozing:\n"
        f"<i>Chatni yakunlash uchun /stop yozing.</i>"
    )
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
            await message.answer("✅ Javob yuborildi. Yana yozaverishingiz mumkin.")
        except:
            await message.answer("❌ Foydalanuvchiga xabar yuborib bo'lmadi.")

# --- ADMIN PANEL ---
@dp.message(Command("admin"))
async def cmd_admin(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins:
        return
    await state.clear()
    await message.answer("👑 <b>Admin Panelga Xush Kelibsiz!</b>\n\nQuyidagi menyudan kerakli bo'limni tanlang:", reply_markup=get_admin_menu())

@dp.message(F.text == "🔙 Chiqish")
async def admin_exit(message: types.Message):
    await message.answer("Admin paneldan chiqildi.", reply_markup=get_user_menu())

@dp.message(F.text == "🔙 Admin Menyuga Qaytish")
async def back_to_admin(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await state.clear()
    await message.answer("👑 <b>Admin Panel</b>", reply_markup=get_admin_menu())

# --- STATISTIKA ---
@dp.message(F.text == "📊 Statistika")
async def admin_stats(message: types.Message):
    if message.from_user.id not in admins: return
    total = len(users)
    blocked = sum(1 for u in users.values() if u.get("is_blocked"))
    active = total - blocked
    
    text = (
        "📈 <b>PREMIUM STATISTIKA</b> 📈\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🗓 <b>Sana:</b> <i>{get_now()}</i>\n\n"
        f"👥 <b>Umumiy foydalanuvchilar:</b> {total} ta\n"
        f"✅ <b>Faol foydalanuvchilar:</b> {active} ta\n"
        f"🚫 <b>Bloklanganlar:</b> {blocked} ta\n"
        f"📢 <b>Majburiy kanallar:</b> {len(channels)} ta\n"
        f"👮‍♂️ <b>Adminlar soni:</b> {len(admins)} ta\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    await message.answer(text, reply_markup=get_admin_menu())

# --- KANALLAR CRUD ---
@dp.message(F.text == "📢 Kanallar")
async def channels_menu(message: types.Message):
    if message.from_user.id not in admins: return
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Kanal qo'shish", callback_data="add_channel"))
    builder.row(InlineKeyboardButton(text="📋 Kanallar ro'yxati", callback_data="list_channels"))
    await message.answer("📢 <b>Kanallar Boshqaruvi</b>\nTanlang:", reply_markup=builder.as_markup())

@dp.callback_query(F.data == "list_channels")
async def list_channels(call: types.CallbackQuery):
    if not channels:
        return await call.answer("Bazada kanallar yo'q!", show_alert=True)
    
    builder = InlineKeyboardBuilder()
    for ch_id, ch_data in channels.items():
        builder.button(text=ch_data["name"], callback_data=ChannelCB(action="view", channel_id=ch_id).pack())
    builder.adjust(2) # 2 ustun
    await call.message.edit_text("📋 <b>Kanallar Ro'yxati:</b>", reply_markup=builder.as_markup())

@dp.callback_query(ChannelCB.filter(F.action == "view"))
async def view_channel(call: types.CallbackQuery, callback_data: ChannelCB):
    ch_id = callback_data.channel_id
    if ch_id not in channels: return await call.answer("Kanal topilmadi", show_alert=True)
    
    data = channels[ch_id]
    text = (
        "📢 <b>Kanal Ma'lumotlari</b> 💎\n\n"
        f"🆔 <b>ID:</b> <code>{ch_id}</code>\n"
        f"📝 <b>Nomi:</b> {data['name']}\n"
        f"🔗 <b>Havola:</b> {data['link']}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Tahrirlash", callback_data=ChannelCB(action="edit", channel_id=ch_id).pack())
    builder.button(text="🗑 O'chirish", callback_data=ChannelCB(action="delete", channel_id=ch_id).pack())
    builder.button(text="🔙 Orqaga", callback_data="list_channels")
    builder.adjust(2, 1)
    
    await call.message.edit_text(text, reply_markup=builder.as_markup())

# Kanal Qo'shish
@dp.callback_query(F.data == "add_channel")
async def start_add_channel(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("✍️ <b>Kanal ID yoki Username kiritng:</b>\n<i>Masalan: -100123456789 yoki @kanal_useri</i>", reply_markup=get_back_admin_kb())
    await state.set_state(ChannelState.waiting_for_id)
    await call.answer()

@dp.message(StateFilter(ChannelState.waiting_for_id))
async def process_channel_id(message: types.Message, state: FSMContext):
    ch_id = message.text
    if ch_id in channels:
        return await message.answer("⚠️ <b>Bu kanal allaqachon bazada mavjud!</b> Boshqa ID kiriting:")
    
    try:
        member = await bot.get_chat_member(chat_id=ch_id, user_id=bot.id)
        if member.status not in ['administrator', 'creator']:
            return await message.answer("❌ <b>Xatolik!</b> Bot bu kanalda admin emas. Iltimos, oldin botni admin qiling va qayta urinib ko'ring.")
        
        chat_info = await bot.get_chat(ch_id)
        invite_link = chat_info.invite_link or (f"https://t.me/{chat_info.username}" if chat_info.username else "Havola yo'q")
        
        await state.update_data(ch_id=str(chat_info.id), link=invite_link)
        await message.answer("✅ Kanal tasdiqlandi. Endi tugmalarda chiqadigan <b>Nomi</b>ni yozing:")
        await state.set_state(ChannelState.waiting_for_name)
    except Exception as e:
        await message.answer(f"❌ <b>Xatolik:</b> Kanal topilmadi yoki bot u yerda umuman yo'q. ID to'g'riligini tekshiring.\n({e})")

@dp.message(StateFilter(ChannelState.waiting_for_name))
async def process_channel_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    ch_id = data['ch_id']
    channels[ch_id] = {"name": message.text, "link": data['link']}
    await state.clear()
    await message.answer("🎉 <b>Kanal muvaffaqiyatli qo'shildi!</b>", reply_markup=get_admin_menu())

# Kanal O'chirish
@dp.callback_query(ChannelCB.filter(F.action == "delete"))
async def delete_channel_ask(call: types.CallbackQuery, callback_data: ChannelCB):
    ch_id = callback_data.channel_id
    name = channels[ch_id]["name"]
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Ha", callback_data=ConfirmCB(action="yes", target_type="ch_del", target_id=ch_id).pack())
    builder.button(text="❌ Yo'q", callback_data=ConfirmCB(action="no", target_type="ch_del", target_id=ch_id).pack())
    await call.message.edit_text(f"⚠️ Rostdan ham <b>{name}</b> kanalini o'chirmoqchimisiz?", reply_markup=builder.as_markup())

@dp.callback_query(ConfirmCB.filter(F.target_type == "ch_del"))
async def delete_channel_confirm(call: types.CallbackQuery, callback_data: ConfirmCB):
    if callback_data.action == "yes":
        channels.pop(callback_data.target_id, None)
        await call.message.edit_text("✅ Kanal o'chirildi.")
    else:
        await call.message.edit_text("❌ Bekor qilindi.")
        await list_channels(call)

# --- FOYDALANUVCHILAR RO'YXATI (PAGINATION) ---
@dp.message(F.text == "📋 Foydalanuvchilar")
async def users_list(message: types.Message):
    if message.from_user.id not in admins: return
    await show_users_page(message, page=1)

async def show_users_page(message: types.Message, page: int, edit: bool = False):
    all_users = list(users.keys())
    total_pages = (len(all_users) + 9) // 10
    if page < 1: page = 1
    if page > total_pages: page = total_pages
    
    start = (page - 1) * 10
    end = start + 10
    page_users = all_users[start:end]
    
    builder = InlineKeyboardBuilder()
    for uid in page_users:
        user_data = users.get(uid, {})
        name = user_data.get("full_name", "Noma'lum")[:15]
        builder.button(text=f"{uid}: {name}", callback_data=UserListCB(page=page, user_id=uid).pack())
    builder.adjust(2)
    
    # Pagination tugmalari
    nav_btns = []
    if page > 1:
        nav_btns.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=UserListCB(page=page-1, user_id=0).pack()))
    if page < total_pages:
        nav_btns.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=UserListCB(page=page+1, user_id=0).pack()))
    if nav_btns:
        builder.row(*nav_btns)
    
    text = f"📋 <b>Foydalanuvchilar</b> (Sahifa {page}/{total_pages})"
    
    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())

@dp.callback_query(UserListCB.filter())
async def users_pagination(call: types.CallbackQuery, callback_data: UserListCB):
    if callback_data.user_id == 0:
        # Sahifa o'zgartirish
        await show_users_page(call.message, callback_data.page, edit=True)
        await call.answer()
    else:
        # Foydalanuvchi detalini ko'rsatish
        uid = callback_data.user_id
        user_data = users.get(uid)
        if not user_data:
            await call.answer("Foydalanuvchi topilmadi!", show_alert=True)
            return
        
        text = (
            "👤 <b>Foydalanuvchi Ma'lumotlari</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>ID:</b> <code>{uid}</code>\n"
            f"📛 <b>Ism:</b> {user_data.get('full_name', 'Nomaʼlum')}\n"
            f"📞 <b>Telefon:</b> {user_data.get('phone', 'Nomaʼlum')}\n"
            f"📅 <b>Ro'yxatdan o'tgan:</b> {user_data.get('registered_at', 'Nomaʼlum')}\n"
            f"🚫 <b>Bloklangan:</b> {'Ha' if user_data.get('is_blocked') else 'Yoʻq'}\n"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🚫 Bloklash" if not user_data.get("is_blocked") else "✅ Blokdan ochish", 
                       callback_data=ConfirmCB(action="toggle_block", target_type="user", target_id=str(uid)).pack())
        builder.button(text="✉️ Xabar yuborish", callback_data=SendToUserCB(user_id=uid).pack())
        builder.button(text="🔙 Orqaga", callback_data=UserListCB(page=callback_data.page, user_id=0).pack())
        builder.adjust(2, 1)
        
        await call.message.edit_text(text, reply_markup=builder.as_markup())
        await call.answer()

# Bloklash/ochish tugmasi (foydalanuvchi detalida)
@dp.callback_query(ConfirmCB.filter(F.target_type == "user"))
async def toggle_user_block(call: types.CallbackQuery, callback_data: ConfirmCB):
    uid = int(callback_data.target_id)
    if uid not in users:
        await call.answer("Foydalanuvchi topilmadi!", show_alert=True)
        return
    
    if callback_data.action == "toggle_block":
        users[uid]["is_blocked"] = not users[uid]["is_blocked"]
        status = "bloklandi" if users[uid]["is_blocked"] else "blokdan ochildi"
        await call.answer(f"✅ Foydalanuvchi {status}.")
        # Qayta yuklash
        await show_users_page(call.message, 1, edit=True)

# --- XABAR YUBORISH (FAYDALANUVCHIGA) ---
@dp.callback_query(SendToUserCB.filter())
async def send_to_user_start(call: types.CallbackQuery, callback_data: SendToUserCB, state: FSMContext):
    user_id = callback_data.user_id
    if user_id not in users:
        await call.answer("Foydalanuvchi topilmadi!", show_alert=True)
        return
    
    await state.update_data(target_user_id=user_id)
    await state.set_state(SendToUserState.waiting_for_message)
    await call.message.answer(
        f"✉️ <b>Foydalanuvchi (ID: {user_id})</b> ga yubormoqchi bo'lgan xabaringizni kiriting.\n"
        "<i>Matn, rasm, video, audio, hujjat yoki ovozli xabar bo'lishi mumkin.</i>\n"
        "Bekor qilish uchun /stop yozing.",
        reply_markup=get_back_admin_kb()
    )
    await call.answer()

@dp.message(StateFilter(SendToUserState.waiting_for_message))
async def send_to_user_message(message: types.Message, state: FSMContext):
    if message.text == "/stop":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=get_admin_menu())
        return
    
    data = await state.get_data()
    user_id = data.get("target_user_id")
    if not user_id:
        await state.clear()
        return
    
    try:
        # Xabarni foydalanuvchiga yuborish, javob tugmasi bilan
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Javob yozish", callback_data=ReplyToAdminCB(admin_id=message.from_user.id, user_id=user_id).pack())]
        ])
        await message.copy_to(chat_id=user_id, reply_markup=markup)
        await message.answer("✅ Xabar yuborildi.", reply_markup=get_admin_menu())
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}")

# Foydalanuvchi admin xabariga javob beradi
@dp.callback_query(ReplyToAdminCB.filter())
async def reply_to_admin_start(call: types.CallbackQuery, callback_data: ReplyToAdminCB, state: FSMContext):
    admin_id = callback_data.admin_id
    user_id = callback_data.user_id
    
    if call.from_user.id != user_id:
        await call.answer("Bu xabar sizga tegishli emas!", show_alert=True)
        return
    
    # Foydalanuvchini chat holatiga o'tkazamiz
    support_chats[admin_id] = user_id  # Admin bu foydalanuvchi bilan chatda
    await state.set_state(SupportState.in_chat_user)
    await call.message.answer(
        "💬 Endi adminga javobingizni yozishingiz mumkin.\n"
        "<i>Chatni yakunlash uchun /stop yozing.</i>",
        reply_markup=ReplyKeyboardRemove()
    )
    await call.answer()

# --- ADMINLAR CRUD (takomillashtirilgan) ---
@dp.message(F.text == "👮‍♂️ Adminlar")
async def admins_menu(message: types.Message):
    if message.from_user.id not in admins: return
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Admin qo'shish", callback_data="add_admin"))
    builder.row(InlineKeyboardButton(text="📋 Adminlar ro'yxati", callback_data="list_admins"))
    await message.answer("👮‍♂️ <b>Adminlar Boshqaruvi</b>", reply_markup=builder.as_markup())

@dp.callback_query(F.data == "list_admins")
async def list_admins(call: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    for adm in admins:
        builder.button(text=str(adm), callback_data=AdminCB(action="view", admin_id=adm).pack())
    builder.adjust(2)
    await call.message.edit_text("📋 <b>Adminlar Ro'yxati:</b>", reply_markup=builder.as_markup())

@dp.callback_query(AdminCB.filter(F.action == "view"))
async def view_admin(call: types.CallbackQuery, callback_data: AdminCB):
    adm_id = callback_data.admin_id
    user_data = users.get(adm_id, {})
    text = (
        f"👮‍♂️ <b>Admin Ma'lumoti</b>\n\n"
        f"🆔 <b>ID:</b> <code>{adm_id}</code>\n"
        f"📛 <b>Ism:</b> {user_data.get('full_name', 'Nomaʼlum')}\n"
        f"📞 <b>Telefon:</b> {user_data.get('phone', 'Nomaʼlum')}\n"
        f"📅 <b>Ro'yxatdan o'tgan:</b> {user_data.get('registered_at', 'Nomaʼlum')}\n"
    )
    if adm_id == MAIN_ADMIN_ID:
        text += "👑 <i>Asosiy Admin</i>\n"
    
    builder = InlineKeyboardBuilder()
    if adm_id != MAIN_ADMIN_ID:
        builder.button(text="🗑 Adminlikdan olish", callback_data=AdminCB(action="remove", admin_id=adm_id).pack())
    builder.button(text="🔙 Orqaga", callback_data="list_admins")
    builder.adjust(1)
    await call.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(AdminCB.filter(F.action == "remove"))
async def remove_admin(call: types.CallbackQuery, callback_data: AdminCB):
    adm_id = callback_data.admin_id
    if adm_id in admins and adm_id != MAIN_ADMIN_ID:
        admins.remove(adm_id)
        try:
            await bot.send_message(adm_id, "⚠️ <b>Siz adminlik huquqidan olib tashlandingiz.</b>")
        except: pass
        await call.message.edit_text("✅ Adminlikdan olindi.")
    else:
        await call.answer("Bu amalni bajara olmaysiz!", show_alert=True)

@dp.callback_query(F.data == "add_admin")
async def add_admin_ask(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("✍️ Yangi adminning <b>Telegram ID</b> raqamini kiriting:", reply_markup=get_back_admin_kb())
    await state.set_state(AdminState.waiting_for_id)
    await call.answer()

@dp.message(StateFilter(AdminState.waiting_for_id))
async def process_new_admin(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("❌ ID faqat raqamlardan iborat bo'lishi kerak.")
    new_adm = int(message.text)
    admins.add(new_adm)
    await state.clear()
    await message.answer(f"✅ ID <code>{new_adm}</code> adminlar safiga qo'shildi!", reply_markup=get_admin_menu())
    try:
        await bot.send_message(new_adm, "🎉 <b>Tabriklaymiz! Siz ushbu botga yordamchi admin etib tayinlandingiz!</b>\n/admin buyrug'ini bosing.")
    except: pass

# --- BLOKLASH / OCHISH (admin panel umumiy) ---
@dp.message(F.text == "🚫 Bloklash")
async def block_user_ask(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await message.answer("🚫 Bloklanadigan foydalanuvchining <b>Telegram ID</b> raqamini kiriting:", reply_markup=get_back_admin_kb())
    await state.set_state(UserState.waiting_for_block_id)

@dp.message(StateFilter(UserState.waiting_for_block_id))
async def block_user(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        if uid in users:
            users[uid]["is_blocked"] = True
            await message.answer(f"✅ Foydalanuvchi {uid} bloklandi.", reply_markup=get_admin_menu())
        else:
            await message.answer("❌ Bu ID bazada topilmadi.")
    except ValueError:
        await message.answer("❌ ID raqam bo'lishi kerak.")
    await state.clear()

@dp.message(F.text == "✅ Blokdan ochish")
async def unblock_user_ask(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await message.answer("✅ Blokdan olinadigan foydalanuvchining <b>Telegram ID</b> raqamini kiriting:", reply_markup=get_back_admin_kb())
    await state.set_state(UserState.waiting_for_unblock_id)

@dp.message(StateFilter(UserState.waiting_for_unblock_id))
async def unblock_user(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        if uid in users:
            users[uid]["is_blocked"] = False
            await message.answer(f"✅ Foydalanuvchi {uid} blokdan chiqarildi.", reply_markup=get_admin_menu())
        else:
            await message.answer("❌ Bu ID bazada topilmadi.")
    except ValueError:
        await message.answer("❌ ID raqam bo'lishi kerak.")
    await state.clear()

# --- XABAR YUBORISH (BROADCAST) ---
@dp.message(F.text == "✉️ Xabar yuborish")
async def broadcast_ask(message: types.Message, state: FSMContext):
    if message.from_user.id not in admins: return
    await message.answer("✉️ <b>Barcha foydalanuvchilarga yuboriladigan xabarni kiriting:</b>\n<i>(Matn, rasm yoki video yuborishingiz mumkin)</i>", reply_markup=get_back_admin_kb())
    await state.set_state(BroadcastState.waiting_for_message)

@dp.message(StateFilter(BroadcastState.waiting_for_message))
async def broadcast_send(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("⏳ Xabar yuborilmoqda, kutib turing...", reply_markup=get_admin_menu())
    success, failed = 0, 0
    for u_id in list(users.keys()):
        if users[u_id].get("is_blocked"): continue
        try:
            await message.copy_to(u_id, reply_markup=None)
            success += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    
    await message.answer(f"📊 <b>Xabar yuborish yakunlandi!</b>\n\n✅ Yuborildi: {success}\n❌ Xatolik: {failed}")

# --- FASTAPI SERVER QISMI ---
class NotificationItem(BaseModel):
    studentId: str
    studentName: str
    message: str
    telegramId: Optional[str] = None
    phoneNumber: Optional[str] = None

class NotificationRequest(BaseModel):
    notifications: List[NotificationItem]
    date: str

class NotificationResponse(BaseModel):
    success: bool
    deliveredCount: int
    failedCount: int
    errors: List[dict] = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Bot ishga tushmoqda...")
    asyncio.create_task(dp.start_polling(bot))
    yield
    logger.info("Bot to'xtatilmoqda...")
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:5173", "http://127.0.0.1:5173"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "ok", "message": "Bot server ishlamoqda (In-Memory xotira)"}

@app.post("/send-notifications", response_model=NotificationResponse)
async def send_notifications_api(data: NotificationRequest):
    success_count = 0
    failed_count = 0
    errors = []

    for item in data.notifications:
        target_chat_id = None
        
        if item.telegramId and item.telegramId.isdigit():
            tid = int(item.telegramId)
            if tid in users: target_chat_id = tid
        elif item.phoneNumber:
            clean_phone = item.phoneNumber.replace("+", "").replace(" ", "")
            for uid, udata in users.items():
                if udata.get("phone", "").replace("+", "").replace(" ", "") == clean_phone:
                    target_chat_id = uid
                    break
        
        if target_chat_id:
            try:
                await bot.send_message(chat_id=target_chat_id, text=item.message, parse_mode=ParseMode.HTML)
                success_count += 1
            except Exception as e:
                failed_count += 1
                errors.append({"student": item.studentName, "error": str(e)})
        else:
            failed_count += 1
            errors.append({"student": item.studentName, "error": "Botda ro'yxatdan o'tmagan"})

    return {
        "success": success_count > 0,
        "deliveredCount": success_count,
        "failedCount": failed_count,
        "errors": errors
    }

if __name__ == "__main__":
    import uvicorn
    logger.info("Server ishga tushdi.")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")