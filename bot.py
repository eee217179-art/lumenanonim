import asyncio
import logging
import os
import random
import re
import string
import time
from collections import defaultdict, deque
from datetime import datetime

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv("BOT_TOKEN", "")
ADMINS = {
    int(x)
    for x in os.getenv("ADMIN_IDS", os.getenv("ADMIN_ID", "")).replace(" ", "").split(",")
    if x.isdigit()
}
DB_PATH = os.getenv("DB_PATH", "lumenanonim.db")

MAX_WARNS = 3            # предупреждений до автобана
AUTO_BAN_HOURS = 24      # срок автобана
FLOOD_MSGS = 7           # сообщений ...
FLOOD_SECONDS = 5        # ... за столько секунд = флуд
FLOOD_MUTE = 60          # мут за флуд, сек

BTN_SEARCH = "🎲 Найти собеседника"
BTN_LINK = "🔗 Моя ссылка"
BTN_HELP = "ℹ️ Помощь"
BTN_NEXT = "⏭ Следующий"
BTN_STOP = "⛔ Стоп"
BTN_REPORT = "🚨 Пожаловаться"

router = Router()
db: aiosqlite.Connection = None  # type: ignore
bot: Bot = None  # type: ignore

# ---------- состояние в памяти ----------
queue: list[int] = []
pairs: dict[int, int] = {}
chat_logs: dict[int, deque] = {}
last_chat: dict[int, tuple[int, list]] = {}
states: dict[int, tuple[str, int]] = {}  # uid -> ("send"|"reply", target)
qlock = asyncio.Lock()
flood: dict[int, deque] = defaultdict(lambda: deque(maxlen=FLOOD_MSGS + 1))
muted_until: dict[int, float] = {}
ban_notice_at: dict[int, float] = {}
STOPWORDS: set[str] = set()

LINK_RE = re.compile(r"(https?://|www\.|t\.me/|telegram\.me/|@[A-Za-z0-9_]{4,})", re.I)


# ---------- БД ----------
async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            warns INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS anon_msgs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_id INTEGER NOT NULL,
            to_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS blocks (
            owner INTEGER NOT NULL,
            blocked INTEGER NOT NULL,
            PRIMARY KEY (owner, blocked)
        );
        CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY,
            until INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            by_id INTEGER,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reporter INTEGER NOT NULL,
            target INTEGER NOT NULL,
            kind TEXT NOT NULL,
            content TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stopwords (word TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    await db.commit()
    await load_stopwords()


async def load_stopwords():
    STOPWORDS.clear()
    async with db.execute("SELECT word FROM stopwords") as c:
        async for (w,) in c:
            STOPWORDS.add(w)


async def get_setting(key: str, default: str) -> str:
    async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as c:
        row = await c.fetchone()
    return row[0] if row else default


async def set_setting(key: str, value: str):
    await db.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?,?)", (key, value))
    await db.commit()


async def ensure_user(uid: int) -> str:
    async with db.execute("SELECT code FROM users WHERE id=?", (uid,)) as c:
        row = await c.fetchone()
    if row:
        return row[0]
    alphabet = string.ascii_lowercase.replace("l", "").replace("o", "") + "23456789"
    while True:
        code = "".join(random.choices(alphabet, k=8))
        try:
            await db.execute(
                "INSERT INTO users(id, code, created_at) VALUES(?,?,?)", (uid, code, int(time.time()))
            )
            await db.commit()
            return code
        except aiosqlite.IntegrityError:
            async with db.execute("SELECT code FROM users WHERE id=?", (uid,)) as c:
                row = await c.fetchone()
            if row:
                return row[0]


async def user_by_code(code: str):
    async with db.execute("SELECT id FROM users WHERE code=?", (code,)) as c:
        row = await c.fetchone()
    return row[0] if row else None


async def is_blocked(owner: int, who: int) -> bool:
    async with db.execute("SELECT 1 FROM blocks WHERE owner=? AND blocked=?", (owner, who)) as c:
        return await c.fetchone() is not None


async def get_ban(uid: int):
    async with db.execute("SELECT until, reason FROM bans WHERE user_id=?", (uid,)) as c:
        row = await c.fetchone()
    if not row:
        return None
    until, reason = row
    if until and until < time.time():
        await db.execute("DELETE FROM bans WHERE user_id=?", (uid,))
        await db.commit()
        return None
    return until, reason


# ---------- клавиатуры / отправка ----------
def kb(rows: list[list[str]]) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t) for t in row] for row in rows], resize_keyboard=True
    )


def menu_kb():
    return kb([[BTN_SEARCH], [BTN_LINK, BTN_HELP]])


def chat_kb():
    return kb([[BTN_NEXT, BTN_STOP], [BTN_REPORT]])


def search_kb():
    return kb([[BTN_STOP]])


async def send(chat_id: int, text: str, **kw) -> bool:
    try:
        await bot.send_message(chat_id, text, **kw)
        return True
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        logging.warning("send to %s failed: %s", chat_id, e)
        return False


def fmt_until(until: int) -> str:
    return "навсегда" if not until else "до " + datetime.fromtimestamp(until).strftime("%d.%m.%Y %H:%M")


# ---------- модерация ----------
async def ban_user(uid: int, hours: int, reason: str, by: int):
    until = int(time.time() + hours * 3600) if hours else 0
    await db.execute(
        "INSERT OR REPLACE INTO bans(user_id, until, reason, by_id, created_at) VALUES(?,?,?,?,?)",
        (uid, until, reason, by, int(time.time())),
    )
    await db.commit()
    await leave_roulette(uid, silent=True)
    states.pop(uid, None)
    await send(
        uid,
        f"🔨 Вы заблокированы {fmt_until(until)}.\nПричина: {reason or 'нарушение правил'}",
        reply_markup=ReplyKeyboardRemove(),
    )


async def unban_user(uid: int):
    await db.execute("DELETE FROM bans WHERE user_id=?", (uid,))
    await db.execute("UPDATE users SET warns=0 WHERE id=?", (uid,))
    await db.commit()
    await send(uid, "✅ Вы разблокированы. Пожалуйста, соблюдайте правила.", reply_markup=menu_kb())


async def add_warn(uid: int, reason: str, by: int = 0) -> int:
    await db.execute("UPDATE users SET warns=warns+1 WHERE id=?", (uid,))
    await db.commit()
    async with db.execute("SELECT warns FROM users WHERE id=?", (uid,)) as c:
        row = await c.fetchone()
    warns = row[0] if row else 1
    if warns >= MAX_WARNS:
        await db.execute("UPDATE users SET warns=0 WHERE id=?", (uid,))
        await db.commit()
        await ban_user(uid, AUTO_BAN_HOURS, f"{MAX_WARNS} предупреждения ({reason})", by)
    else:
        await send(uid, f"⚠️ Предупреждение {warns}/{MAX_WARNS}: {reason}")
    return warns


async def check_content(m: Message) -> str | None:
    """Возвращает причину, если сообщение нарушает правила."""
    text = (m.text or m.caption or "").lower().replace("ё", "е")
    if not text:
        return None
    for w in STOPWORDS:
        if w in text:
            return "запрещённые слова"
    if await get_setting("block_links", "1") == "1" and LINK_RE.search(text):
        return "ссылки и реклама запрещены"
    return None


async def violation(m: Message, reason: str):
    uid = m.from_user.id
    warns = await add_warn(uid, reason)
    if warns < MAX_WARNS:
        await m.answer("🚫 Сообщение не доставлено.")


async def create_report(reporter: int, target: int, kind: str, content: str, evidence: Message | None):
    cur = await db.execute(
        "INSERT INTO reports(reporter, target, kind, content, created_at) VALUES(?,?,?,?,?)",
        (reporter, target, kind, content, int(time.time())),
    )
    await db.commit()
    rid = cur.lastrowid
    text = (
        f"🚨 Жалоба #{rid}\nТип: {kind}\nОбвиняемый ID: {target}\nЖалобщик ID: {reporter}\n\n{content}"
    )[:3900]
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⚠️ Варн", callback_data=f"mod:warn:{rid}"),
                InlineKeyboardButton(text="⛔ Бан 24ч", callback_data=f"mod:ban24:{rid}"),
            ],
            [
                InlineKeyboardButton(text="🔨 Бан навсегда", callback_data=f"mod:banperm:{rid}"),
                InlineKeyboardButton(text="✖️ Отклонить", callback_data=f"mod:dismiss:{rid}"),
            ],
        ]
    )
    for admin in ADMINS:
        if evidence is not None:
            try:
                await evidence.copy_to(admin)
            except Exception as e:  # noqa: BLE001
                logging.warning("evidence copy failed: %s", e)
        await send(admin, text, reply_markup=markup)
    return rid


# ---------- рулетка ----------
async def start_search(uid: int):
    states.pop(uid, None)
    partner = None
    async with qlock:
        if uid in pairs:
            await send(uid, "Ты уже в диалоге. Нажми «Следующий» или «Стоп».", reply_markup=chat_kb())
            return
        if uid in queue:
            await send(uid, "🔎 Ты уже в поиске...", reply_markup=search_kb())
            return
        for cand in list(queue):
            if cand != uid and not await is_blocked(cand, uid) and not await is_blocked(uid, cand):
                queue.remove(cand)
                partner = cand
                break
        if partner is None:
            queue.append(uid)
        else:
            pairs[uid] = partner
            pairs[partner] = uid
            log: deque = deque(maxlen=15)
            chat_logs[uid] = chat_logs[partner] = log
    if partner is None:
        await send(uid, "🔎 Ищу собеседника...", reply_markup=search_kb())
        return
    txt = "✅ Собеседник найден! Пиши — он увидит сообщение анонимно.\n\n/next — следующий, /stop — выйти, /report — пожаловаться."
    await send(uid, txt, reply_markup=chat_kb())
    await send(partner, txt, reply_markup=chat_kb())


async def leave_roulette(uid: int, silent: bool = False) -> bool:
    was = False
    partner = None
    async with qlock:
        if uid in queue:
            queue.remove(uid)
            was = True
        if uid in pairs:
            partner = pairs.pop(uid)
            pairs.pop(partner, None)
            log = list(chat_logs.pop(uid, []))
            chat_logs.pop(partner, None)
            last_chat[uid] = (partner, log)
            last_chat[partner] = (uid, log)
            was = True
    if partner is not None:
        await send(
            partner,
            "💔 Собеседник покинул чат.\nНажми «Найти собеседника», чтобы начать новый.",
            reply_markup=menu_kb(),
        )
        if not silent:
            await send(uid, "Диалог завершён.", reply_markup=menu_kb())
    elif was and not silent:
        await send(uid, "Поиск остановлен.", reply_markup=menu_kb())
    return was


async def relay(m: Message) -> bool:
    uid = m.from_user.id
    partner = pairs.get(uid)
    if partner is None:
        return False
    reason = await check_content(m)
    if reason:
        await violation(m, reason)
        return True
    log = chat_logs.get(uid)
    if log is not None:
        log.append((uid, (m.text or m.caption or f"[{m.content_type}]")[:300]))
    try:
        await m.copy_to(partner)
    except (TelegramForbiddenError, TelegramBadRequest):
        await leave_roulette(uid)
    return True


async def report_roulette(m: Message):
    uid = m.from_user.id
    partner = pairs.get(uid)
    entries = []
    if partner is not None:
        entries = list(chat_logs.get(uid, []))
    elif uid in last_chat:
        partner, entries = last_chat[uid]
    if partner is None:
        await m.answer("Нет собеседника, на которого можно пожаловаться.", reply_markup=menu_kb())
        return
    lines = [f"{'Жалобщик' if s == uid else 'Обвиняемый'}: {t}" for s, t in entries]
    content = "Последние сообщения в рулетке:\n" + ("\n".join(lines) if lines else "(пусто)")
    await create_report(uid, partner, "рулетка", content, None)
    last_chat.pop(uid, None)
    await leave_roulette(uid, silent=True)
    await m.answer("✅ Жалоба отправлена модераторам, диалог завершён. Спасибо!", reply_markup=menu_kb())


# ---------- анонимные сообщения ----------
async def deliver_anon(m: Message, to: int, header: str):
    uid = m.from_user.id
    if await is_blocked(to, uid):
        await m.answer("🚫 Этот пользователь не принимает от тебя сообщения.", reply_markup=menu_kb())
        return
    if await get_ban(to):
        await m.answer("⚠️ Не удалось доставить сообщение.", reply_markup=menu_kb())
        return
    reason = await check_content(m)
    if reason:
        await violation(m, reason)
        return
    cur = await db.execute(
        "INSERT INTO anon_msgs(from_id, to_id, created_at) VALUES(?,?,?)", (uid, to, int(time.time()))
    )
    await db.commit()
    rid = cur.lastrowid
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💬 Ответить", callback_data=f"r:{rid}"),
                InlineKeyboardButton(text="🚫 Блок", callback_data=f"b:{rid}"),
                InlineKeyboardButton(text="⚠️ Жалоба", callback_data=f"p:{rid}"),
            ]
        ]
    )
    if not await send(to, header):
        await m.answer("⚠️ Не удалось доставить: пользователь недоступен.", reply_markup=menu_kb())
        return
    try:
        await m.copy_to(to, reply_markup=markup)
    except (TelegramForbiddenError, TelegramBadRequest):
        await m.answer("⚠️ Не удалось доставить: пользователь недоступен.", reply_markup=menu_kb())
        return
    await m.answer("✅ Отправлено анонимно.", reply_markup=menu_kb())


async def load_anon(rid: int):
    async with db.execute("SELECT from_id, to_id FROM anon_msgs WHERE id=?", (rid,)) as c:
        return await c.fetchone()


@router.callback_query(F.data.regexp(r"^[rbp]:\d+$"))
async def anon_buttons(cb: CallbackQuery):
    action, rid = cb.data.split(":")
    row = await load_anon(int(rid))
    uid = cb.from_user.id
    if not row or row[1] != uid:
        await cb.answer("Сообщение не найдено", show_alert=True)
        return
    from_id = row[0]
    if action == "r":
        await leave_roulette(uid, silent=True)
        states[uid] = ("reply", from_id)
        await cb.answer()
        await send(uid, "✍️ Напиши ответ — я отправлю его анонимно. (/cancel — отмена)")
    elif action == "b":
        await db.execute("INSERT OR IGNORE INTO blocks(owner, blocked) VALUES(?,?)", (uid, from_id))
        await db.commit()
        await cb.answer("Отправитель заблокирован", show_alert=True)
    elif action == "p":
        content = f"Анонимное сообщение #{rid} (текст/медиа выше)."
        await create_report(uid, from_id, "анонимное сообщение", content, cb.message)
        await db.execute("INSERT OR IGNORE INTO blocks(owner, blocked) VALUES(?,?)", (uid, from_id))
        await db.commit()
        await cb.answer("Жалоба отправлена, отправитель заблокирован.", show_alert=True)


# ---------- кнопки модераторов ----------
@router.callback_query(F.data.startswith("mod:"))
async def mod_buttons(cb: CallbackQuery):
    if cb.from_user.id not in ADMINS:
        await cb.answer("Нет доступа", show_alert=True)
        return
    _, action, rid = cb.data.split(":")
    async with db.execute("SELECT reporter, target, status FROM reports WHERE id=?", (int(rid),)) as c:
        row = await c.fetchone()
    if not row:
        await cb.answer("Жалоба не найдена", show_alert=True)
        return
    reporter, target, status = row
    if status != "open":
        await cb.answer("Уже обработана", show_alert=True)
        return
    by = cb.from_user.id
    if action == "warn":
        await add_warn(target, "жалоба пользователей", by)
        res = "выдано предупреждение"
    elif action == "ban24":
        await ban_user(target, 24, "жалоба пользователей", by)
        res = "бан на 24 часа"
    elif action == "banperm":
        await ban_user(target, 0, "жалоба пользователей", by)
        res = "бан навсегда"
    else:
        res = "жалоба отклонена"
    await db.execute("UPDATE reports SET status=? WHERE id=?", (f"done:{action}", int(rid)))
    await db.commit()
    try:
        await cb.message.edit_text(f"{cb.message.text}\n\n✅ {res} (модератор {by})")
    except TelegramBadRequest:
        pass
    await cb.answer("Готово")
    if action != "dismiss":
        await send(reporter, f"✅ Жалоба #{rid} рассмотрена. Спасибо за помощь!")


# ---------- middleware ----------
class GuardMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = event.from_user
        if user is None:
            return await handler(event, data)
        if isinstance(event, Message) and event.chat.type != "private":
            return
        await ensure_user(user.id)
        if user.id in ADMINS:
            return await handler(event, data)

        ban = await get_ban(user.id)
        if ban:
            until, reason = ban
            text = f"🔨 Вы заблокированы {fmt_until(until)}.\nПричина: {reason or 'нарушение правил'}"
            if isinstance(event, CallbackQuery):
                await event.answer("Вы заблокированы", show_alert=True)
            elif time.time() - ban_notice_at.get(user.id, 0) > 60:
                ban_notice_at[user.id] = time.time()
                await event.answer(text, reply_markup=ReplyKeyboardRemove())
            return

        if isinstance(event, Message):
            now = time.time()
            if muted_until.get(user.id, 0) > now:
                return
            dq = flood[user.id]
            dq.append(now)
            if len(dq) > FLOOD_MSGS and now - dq[0] < FLOOD_SECONDS:
                muted_until[user.id] = now + FLOOD_MUTE
                dq.clear()
                await event.answer(f"🐢 Слишком быстро! Мут на {FLOOD_MUTE} сек.")
                return
        return await handler(event, data)


# ---------- команды пользователя ----------
HELP = (
    "🎭 LumenAnonim\n\n"
    "Анонимные сообщения\n"
    "«🔗 Моя ссылка» — отправь её друзьям, и они смогут писать тебе анонимно. "
    "Ты можешь ответить, заблокировать отправителя или пожаловаться.\n\n"
    "Чат-рулетка\n"
    "«🎲 Найти собеседника» — случайный анонимный собеседник.\n\n"
    "Правила: без оскорблений, спама, рекламы и ссылок. "
    "За нарушения — предупреждения и бан.\n\n"
    "Команды: /search /next /stop /report /link /cancel"
)


async def send_link(uid: int):
    code = await ensure_user(uid)
    me = await bot.get_me()
    await send(
        uid,
        f"🔗 Твоя ссылка для анонимных сообщений:\n\nhttps://t.me/{me.username}?start={code}\n\n"
        "Разместь её в профиле или отправь друзьям.",
        reply_markup=menu_kb(),
    )


@router.message(CommandStart())
async def cmd_start(m: Message, command: CommandObject):
    uid = m.from_user.id
    arg = (command.args or "").strip()
    if arg:
        target = await user_by_code(arg)
        if not target:
            await m.answer("❌ Ссылка недействительна.", reply_markup=menu_kb())
            return
        if target == uid:
            await m.answer("Это твоя собственная ссылка 🙂\nОтправь её друзьям.", reply_markup=menu_kb())
            return
        await leave_roulette(uid, silent=True)
        states[uid] = ("send", target)
        await m.answer(
            "🕶 Напиши сообщение — я отправлю его анонимно.\n(/cancel — отмена)",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    await m.answer(
        "👋 Привет! Я LumenAnonim — анонимные сообщения и чат-рулетка.\n\nВыбери действие ниже.",
        reply_markup=menu_kb(),
    )


@router.message(Command("help"))
@router.message(F.text == BTN_HELP)
async def cmd_help(m: Message):
    await m.answer(HELP, reply_markup=menu_kb())


@router.message(Command("link"))
@router.message(F.text == BTN_LINK)
async def cmd_link(m: Message):
    await send_link(m.from_user.id)


@router.message(Command("search"))
@router.message(F.text == BTN_SEARCH)
async def cmd_search(m: Message):
    await start_search(m.from_user.id)


@router.message(Command("next"))
@router.message(F.text == BTN_NEXT)
async def cmd_next(m: Message):
    await leave_roulette(m.from_user.id, silent=True)
    await start_search(m.from_user.id)


@router.message(Command("stop"))
@router.message(F.text == BTN_STOP)
async def cmd_stop(m: Message):
    if not await leave_roulette(m.from_user.id):
        await m.answer("Ты сейчас не в диалоге.", reply_markup=menu_kb())


@router.message(Command("report"))
@router.message(F.text == BTN_REPORT)
async def cmd_report(m: Message):
    await report_roulette(m)


@router.message(Command("cancel"))
async def cmd_cancel(m: Message):
    states.pop(m.from_user.id, None)
    await m.answer("Отменено.", reply_markup=menu_kb())


# ---------- команды админов ----------
ADMIN_HELP = (
    "🛠 Админ-панель\n\n"
    "/ban <id> [часы] [причина] — бан (без часов = навсегда)\n"
    "/unban <id> — разбан\n"
    "/warn <id> [причина] — предупреждение\n"
    "/who <номер_сообщения> — узнать ID отправителя анонимного сообщения\n"
    "/info <id> — данные о пользователе\n"
    "/bans — список активных банов\n"
    "/words — стоп-слова, /addword <слово>, /delword <слово>\n"
    "/links on|off — фильтр ссылок и @упоминаний\n"
    "/broadcast <текст> — рассылка всем\n"
    "/stats — статистика"
)

is_admin = F.from_user.id.in_(ADMINS)


def parse_id(command: CommandObject):
    parts = (command.args or "").split()
    if not parts or not parts[0].lstrip("-").isdigit():
        return None, []
    return int(parts[0]), parts[1:]


@router.message(Command("admin"), is_admin)
async def adm_help(m: Message):
    await m.answer(ADMIN_HELP)


@router.message(Command("ban"), is_admin)
async def adm_ban(m: Message, command: CommandObject):
    uid, rest = parse_id(command)
    if uid is None:
        await m.answer("Использование: /ban <id> [часы] [причина]")
        return
    hours = 0
    if rest and rest[0].isdigit():
        hours = int(rest[0])
        rest = rest[1:]
    reason = " ".join(rest) or "нарушение правил"
    await ban_user(uid, hours, reason, m.from_user.id)
    await m.answer(f"🔨 {uid} забанен {'на %d ч.' % hours if hours else 'навсегда'}.")


@router.message(Command("unban"), is_admin)
async def adm_unban(m: Message, command: CommandObject):
    uid, _ = parse_id(command)
    if uid is None:
        await m.answer("Использование: /unban <id>")
        return
    await unban_user(uid)
    await m.answer(f"✅ {uid} разбанен.")


@router.message(Command("warn"), is_admin)
async def adm_warn(m: Message, command: CommandObject):
    uid, rest = parse_id(command)
    if uid is None:
        await m.answer("Использование: /warn <id> [причина]")
        return
    warns = await add_warn(uid, " ".join(rest) or "нарушение правил", m.from_user.id)
    await m.answer(f"⚠️ Предупреждение выдано (теперь {warns}/{MAX_WARNS}, при {MAX_WARNS} — автобан).")


@router.message(Command("who"), is_admin)
async def adm_who(m: Message, command: CommandObject):
    rid, _ = parse_id(command)
    row = await load_anon(rid) if rid is not None else None
    if not row:
        await m.answer("Сообщение не найдено. Использование: /who <номер>")
        return
    await m.answer(f"Сообщение #{rid}\nОт: {row[0]}\nКому: {row[1]}")


@router.message(Command("info"), is_admin)
async def adm_info(m: Message, command: CommandObject):
    uid, _ = parse_id(command)
    if uid is None:
        await m.answer("Использование: /info <id>")
        return
    async with db.execute("SELECT warns, created_at FROM users WHERE id=?", (uid,)) as c:
        u = await c.fetchone()
    if not u:
        await m.answer("Пользователь не найден.")
        return
    async with db.execute("SELECT COUNT(*) FROM anon_msgs WHERE from_id=?", (uid,)) as c:
        sent = (await c.fetchone())[0]
    async with db.execute("SELECT COUNT(*) FROM reports WHERE target=?", (uid,)) as c:
        reps = (await c.fetchone())[0]
    ban = await get_ban(uid)
    await m.answer(
        f"👤 {uid}\nВ боте с: {datetime.fromtimestamp(u[1]):%d.%m.%Y}\n"
        f"Предупреждений: {u[0]}/{MAX_WARNS}\nАнон. сообщений отправлено: {sent}\n"
        f"Жалоб на него: {reps}\nБан: {fmt_until(ban[0]) + ' — ' + str(ban[1]) if ban else 'нет'}"
    )


@router.message(Command("bans"), is_admin)
async def adm_bans(m: Message):
    now = int(time.time())
    async with db.execute(
        "SELECT user_id, until, reason FROM bans WHERE until=0 OR until>? ORDER BY created_at DESC LIMIT 50",
        (now,),
    ) as c:
        rows = await c.fetchall()
    if not rows:
        await m.answer("Активных банов нет.")
        return
    await m.answer("\n".join(f"{u} — {fmt_until(t)} — {r}" for u, t, r in rows))


@router.message(Command("words"), is_admin)
async def adm_words(m: Message):
    await m.answer("Стоп-слова: " + (", ".join(sorted(STOPWORDS)) or "пусто"))


@router.message(Command("addword"), is_admin)
async def adm_addword(m: Message, command: CommandObject):
    w = (command.args or "").strip().lower().replace("ё", "е")
    if not w:
        await m.answer("Использование: /addword <слово>")
        return
    await db.execute("INSERT OR IGNORE INTO stopwords(word) VALUES(?)", (w,))
    await db.commit()
    await load_stopwords()
    await m.answer(f"✅ Добавлено: {w}")


@router.message(Command("delword"), is_admin)
async def adm_delword(m: Message, command: CommandObject):
    w = (command.args or "").strip().lower().replace("ё", "е")
    await db.execute("DELETE FROM stopwords WHERE word=?", (w,))
    await db.commit()
    await load_stopwords()
    await m.answer(f"🗑 Удалено: {w}")


@router.message(Command("links"), is_admin)
async def adm_links(m: Message, command: CommandObject):
    arg = (command.args or "").strip().lower()
    if arg not in ("on", "off"):
        cur = await get_setting("block_links", "1")
        await m.answer(f"Фильтр ссылок: {'включён' if cur == '1' else 'выключен'}. /links on|off")
        return
    await set_setting("block_links", "1" if arg == "on" else "0")
    await m.answer(f"Фильтр ссылок {'включён' if arg == 'on' else 'выключен'}.")


@router.message(Command("broadcast"), is_admin)
async def adm_broadcast(m: Message, command: CommandObject):
    text = (command.args or "").strip()
    if not text:
        await m.answer("Использование: /broadcast <текст>")
        return
    async with db.execute("SELECT id FROM users") as c:
        ids = [r[0] for r in await c.fetchall()]
    ok = 0
    for uid in ids:
        if await send(uid, text):
            ok += 1
        await asyncio.sleep(0.05)
    await m.answer(f"📣 Доставлено: {ok}/{len(ids)}")


@router.message(Command("stats"), is_admin)
async def adm_stats(m: Message):
    async with db.execute("SELECT COUNT(*) FROM users") as c:
        users = (await c.fetchone())[0]
    async with db.execute("SELECT COUNT(*) FROM anon_msgs") as c:
        msgs = (await c.fetchone())[0]
    async with db.execute("SELECT COUNT(*) FROM reports WHERE status='open'") as c:
        open_rep = (await c.fetchone())[0]
    async with db.execute("SELECT COUNT(*) FROM bans WHERE until=0 OR until>?", (int(time.time()),)) as c:
        bans = (await c.fetchone())[0]
    await m.answer(
        f"👥 Пользователей: {users}\n✉️ Анон. сообщений: {msgs}\n"
        f"🔎 В очереди: {len(queue)}\n💬 Диалогов: {len(pairs) // 2}\n"
        f"🔨 Банов: {bans}\n🚨 Открытых жалоб: {open_rep}"
    )


# ---------- общий обработчик ----------
@router.message()
async def any_message(m: Message):
    uid = m.from_user.id
    st = states.pop(uid, None)
    if st:
        kind, target = st
        await deliver_anon(m, target, "📩 Новое анонимное сообщение:" if kind == "send" else "💌 Анонимный ответ:")
        return
    if await relay(m):
        return
    await m.answer("Выбери действие в меню 👇", reply_markup=menu_kb())


# ---------- запуск ----------
async def main():
    global bot
    if not TOKEN:
        raise SystemExit("Задай переменную окружения BOT_TOKEN")
    if not ADMINS:
        logging.warning("ADMIN_IDS не задан — жалобы и админ-команды работать не будут!")
    await init_db()
    bot = Bot(TOKEN)
    dp = Dispatcher()
    guard = GuardMiddleware()
    dp.message.outer_middleware(guard)
    dp.callback_query.outer_middleware(guard)
    dp.include_router(router)
    await bot.set_my_commands(
        [
            BotCommand(command="search", description="Найти собеседника"),
            BotCommand(command="next", description="Следующий собеседник"),
            BotCommand(command="stop", description="Выйти из диалога"),
            BotCommand(command="report", description="Пожаловаться на собеседника"),
            BotCommand(command="link", description="Моя ссылка для анонимных сообщений"),
            BotCommand(command="help", description="Помощь и правила"),
        ]
    )
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
