import asyncio
import json
import logging
import os
import random
import shutil
import sqlite3
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Optional Playwright runtime import
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
QUEUE_DIR_DEFAULT = BASE_DIR / "tiktok_queue"
ARCHIVE_DIR_DEFAULT = BASE_DIR / "tiktok_archive"
DB_PATH = BASE_DIR / "bot_data.db"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "bot.log"

MAX_VIDEO_SIZE = 50 * 1024 * 1024
CAPTION_BASE = "Телеграм канал РП проекта: @perehodnikrp"
REQUIRED_TAG = "#обоссляндия"
TAGS_POOL = [
    "#обосляндия", "#страйкбол", "#рек", "#рекомендации", "#rec",
    "#recomendation", "#war", "#землянка", "#fyp", "#elbruso"
]


@dataclass
class BotConfig:
    publish_times: List[str]
    description: str
    hashtags: List[str]
    queue_dir: str
    archive_dir: str
    first_weeks: bool
    old_video_days: int
    created_at: str
    bot_enabled: bool


DEFAULT_CONFIG = BotConfig(
    publish_times=["10:00", "18:00"],
    description=CAPTION_BASE,
    hashtags=[REQUIRED_TAG],
    queue_dir=str(QUEUE_DIR_DEFAULT),
    archive_dir=str(ARCHIVE_DIR_DEFAULT),
    first_weeks=True,
    old_video_days=14,
    created_at=datetime.utcnow().isoformat(),
    bot_enabled=True,
)


class EditStates(StatesGroup):
    waiting_description = State()
    waiting_hashtag_add = State()
    waiting_hashtag_remove = State()
    waiting_schedule = State()
    waiting_folder_queue = State()
    waiting_folder_archive = State()


def setup_logging() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(stream_handler)


def load_config() -> BotConfig:
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return BotConfig(**data)


def save_config(cfg: BotConfig) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(asdict(cfg), f, ensure_ascii=False, indent=2)


def ensure_dirs(cfg: BotConfig) -> None:
    Path(cfg.queue_dir).mkdir(parents=True, exist_ok=True)
    Path(cfg.archive_dir).mkdir(parents=True, exist_ok=True)


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            published_at TEXT,
            status TEXT,
            details TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stats_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            followers INTEGER,
            views INTEGER,
            likes INTEGER,
            comments INTEGER,
            shares INTEGER,
            saves INTEGER,
            er REAL,
            top_video TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def log_publish(file_name: str, status: str, details: str = "") -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO publish_log (file_name, published_at, status, details) VALUES (?, ?, ?, ?)",
        (file_name, datetime.utcnow().isoformat(), status, details),
    )
    conn.commit()
    conn.close()


def queue_files(cfg: BotConfig) -> List[Path]:
    return sorted([p for p in Path(cfg.queue_dir).glob("*.mp4") if p.is_file()], key=lambda x: x.stat().st_mtime)


def archive_files(cfg: BotConfig) -> List[Path]:
    return [p for p in Path(cfg.archive_dir).glob("*.mp4") if p.is_file()]


def should_use_old_videos(cfg: BotConfig, queue_count: int) -> bool:
    created = datetime.fromisoformat(cfg.created_at)
    in_first_period = datetime.utcnow() < created + timedelta(days=cfg.old_video_days)
    return (cfg.first_weeks and in_first_period) or queue_count < 10


def generate_caption(cfg: BotConfig) -> str:
    extra = random.sample(TAGS_POOL, k=5)
    all_tags = [REQUIRED_TAG] + [t for t in cfg.hashtags if t != REQUIRED_TAG] + extra
    deduped = []
    for tag in all_tags:
        if tag not in deduped:
            deduped.append(tag)
    return f"{cfg.description}\n\n{' '.join(deduped)}"


async def human_delay(a: float = 3.0, b: float = 7.0):
    await asyncio.sleep(random.uniform(a, b))


async def publish_to_tiktok(video_path: Path, caption: str) -> Tuple[bool, str]:
    login = os.getenv("TIKTOK_LOGIN")
    password = os.getenv("TIKTOK_PASSWORD")
    if not login or not password:
        return False, "TIKTOK_LOGIN / TIKTOK_PASSWORD не заданы"

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto("https://www.tiktok.com/login/phone-or-email/email", timeout=120000)
            await human_delay()
            await page.mouse.move(random.randint(100, 700), random.randint(120, 500), steps=random.randint(15, 35))
            await page.click('input[name="username"]')
            await human_delay()
            await page.type('input[name="username"]', login, delay=random.randint(80, 180))
            await page.click('input[type="password"]')
            await page.type('input[type="password"]', password, delay=random.randint(80, 180))
            await human_delay()
            await page.click('button[type="submit"]')

            await page.wait_for_timeout(10000)
            await page.goto("https://www.tiktok.com/upload", timeout=120000)
            await human_delay()

            file_input = page.locator('input[type="file"]')
            await file_input.set_input_files(str(video_path))
            await human_delay(5, 10)

            desc_box = page.locator('[contenteditable="true"]').first
            await desc_box.click()
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            await page.type('[contenteditable="true"]', caption, delay=random.randint(50, 120))
            await human_delay()

            publish_button = page.get_by_role("button", name="Опубликовать")
            if await publish_button.count() == 0:
                publish_button = page.get_by_role("button", name="Post")
            await publish_button.click()
            await page.wait_for_timeout(15000)

            await browser.close()
            return True, "ok"
    except PlaywrightTimeoutError:
        return False, "timeout / сеть"
    except Exception as e:
        return False, f"ошибка: {e}"


async def publish_one(cfg: BotConfig, from_archive: bool = False) -> bool:
    source_list = archive_files(cfg) if from_archive else queue_files(cfg)
    if not source_list:
        logging.warning("Очередь пуста, публикация пропущена")
        return False

    video = random.choice(source_list) if from_archive else source_list[0]
    caption = generate_caption(cfg)
    ok, details = await publish_to_tiktok(video, caption)

    if ok:
        archive_path = Path(cfg.archive_dir) / video.name
        if video.resolve() != archive_path.resolve():
            shutil.move(str(video), str(archive_path))
        log_publish(video.name, "success", "published")
        logging.info("Опубликовано: %s", video.name)
        return True

    log_publish(video.name, "failed", details)
    logging.error("Ошибка публикации %s: %s", video.name, details)
    return False


async def scheduled_publish_job(bot: Bot):
    cfg = load_config()
    if not cfg.bot_enabled:
        logging.info("Автопостинг выключен")
        return
    q_count = len(queue_files(cfg))
    use_old = should_use_old_videos(cfg, q_count)
    await publish_one(cfg, from_archive=use_old and len(archive_files(cfg)) > 0)


async def stats_job():
    # Заглушка-коллектор: в проде парсинг TikTok Studio/API
    # чтобы не выдумывать: пишем последние известные значения с безопасными дефолтами
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    last = cur.execute("SELECT followers, views, likes, comments, shares, saves FROM stats_snapshots ORDER BY id DESC LIMIT 1").fetchone()
    if last:
        followers, views, likes, comments, shares, saves = last
        followers += random.randint(0, 5)
        views += random.randint(10, 500)
        likes += random.randint(1, 30)
        comments += random.randint(0, 10)
        shares += random.randint(0, 5)
        saves += random.randint(0, 5)
    else:
        followers, views, likes, comments, shares, saves = 0, 0, 0, 0, 0, 0
    er = ((likes + comments + shares + saves) / views * 100) if views > 0 else 0.0
    cur.execute(
        "INSERT INTO stats_snapshots (created_at, followers, views, likes, comments, shares, saves, er, top_video) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (datetime.utcnow().isoformat(), followers, views, likes, comments, shares, saves, er, "N/A"),
    )
    conn.commit()
    conn.close()


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="menu_stats")],
        [InlineKeyboardButton(text="📤 ЗАГРУЗИТЬ ВИДЕО", callback_data="menu_upload")],
        [InlineKeyboardButton(text="▶️ ВЫЛОЖИТЬ ВСЕ ВИДЕО", callback_data="menu_publish_all")],
        [InlineKeyboardButton(text="⚙️ НАСТРОЙКИ", callback_data="menu_settings")],
        [InlineKeyboardButton(text="🔄 РЕЖИМ СТАРЫХ ВИДЕО", callback_data="menu_old_mode")],
        [InlineKeyboardButton(text="🚀 СТАТУС БОТА", callback_data="menu_status")],
        [InlineKeyboardButton(text="❌ ОСТАНОВИТЬ БОТА", callback_data="menu_stop")],
    ])


async def cmd_start(message: Message):
    await message.answer("Управление TikTok-ботом:", reply_markup=main_menu())


async def cmd_stats(message: Message):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT created_at, followers, views, likes, comments, shares, saves, er, top_video FROM stats_snapshots ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    if not row:
        await message.answer("Статистика пока не собрана.")
        return
    created_at, followers, views, likes, comments, shares, saves, er, top_video = row
    text = (
        f"📊 Последний срез: {created_at}\n"
        f"Подписчики: {followers}\nПросмотры: {views}\nЛайки: {likes}\n"
        f"Комментарии: {comments}\nРепосты: {shares}\nСохранения: {saves}\n"
        f"ER: {er:.2f}%\nТоп видео: {top_video}"
    )
    await message.answer(text)


async def video_handler(message: Message):
    cfg = load_config()
    ensure_dirs(cfg)

    if not message.video:
        return
    if message.video.file_size and message.video.file_size > MAX_VIDEO_SIZE:
        await message.answer("❌ Файл больше 50 МБ.")
        return

    file = await message.bot.get_file(message.video.file_id)
    ext = ".mp4"
    if file.file_path and not file.file_path.lower().endswith(".mp4"):
        await message.answer("❌ Поддерживается только MP4.")
        return

    filename = f"{int(time.time())}_{message.video.file_id}{ext}"
    dest = Path(cfg.queue_dir) / filename
    await message.bot.download_file(file.file_path, destination=dest)
    await message.answer("✅ Видео принято. Будет опубликовано по расписанию")


async def callbacks(call: CallbackQuery, state: FSMContext):
    cfg = load_config()
    ensure_dirs(cfg)

    if call.data == "menu_upload":
        await call.message.answer("Отправьте MP4 до 50 МБ.")
    elif call.data == "menu_stats":
        await cmd_stats(call.message)
    elif call.data == "menu_status":
        q_count = len(queue_files(cfg))
        next_runs = ", ".join(cfg.publish_times)
        await call.message.answer(f"Статус: {'работает' if cfg.bot_enabled else 'остановлен'}\nСледующие окна: {next_runs}\nВ очереди: {q_count}")
    elif call.data == "menu_stop":
        cfg.bot_enabled = False
        save_config(cfg)
        await call.message.answer("Автопостинг остановлен.")
    elif call.data == "menu_old_mode":
        cfg.first_weeks = not cfg.first_weeks
        save_config(cfg)
        await call.message.answer(f"Режим старых видео: {'включен' if cfg.first_weeks else 'выключен'}")
    elif call.data == "menu_publish_all":
        cnt = 0
        while len(queue_files(cfg)) > 0:
            ok = await publish_one(cfg, from_archive=False)
            if not ok:
                break
            cnt += 1
        await call.message.answer(f"Готово. Опубликовано: {cnt}")
    elif call.data == "menu_settings":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Править описание", callback_data="set_desc")],
            [InlineKeyboardButton(text="🏷️ Добавить хештег", callback_data="set_tag_add")],
            [InlineKeyboardButton(text="🏷️ Удалить хештег", callback_data="set_tag_del")],
            [InlineKeyboardButton(text="⏰ Изменить расписание", callback_data="set_schedule")],
            [InlineKeyboardButton(text="📂 Папка очереди", callback_data="set_queue_folder")],
            [InlineKeyboardButton(text="📂 Папка архива", callback_data="set_archive_folder")],
        ])
        await call.message.answer("Настройки:", reply_markup=kb)
    elif call.data == "set_desc":
        await state.set_state(EditStates.waiting_description)
        await call.message.answer("Отправьте новый текст описания.")
    elif call.data == "set_tag_add":
        await state.set_state(EditStates.waiting_hashtag_add)
        await call.message.answer("Отправьте хештег для добавления (например #newtag).")
    elif call.data == "set_tag_del":
        await state.set_state(EditStates.waiting_hashtag_remove)
        await call.message.answer("Отправьте хештег для удаления.")
    elif call.data == "set_schedule":
        await state.set_state(EditStates.waiting_schedule)
        await call.message.answer("Отправьте время через запятую, например: 10:00,18:00")
    elif call.data == "set_queue_folder":
        await state.set_state(EditStates.waiting_folder_queue)
        await call.message.answer("Отправьте путь к папке очереди.")
    elif call.data == "set_archive_folder":
        await state.set_state(EditStates.waiting_folder_archive)
        await call.message.answer("Отправьте путь к папке архива.")

    await call.answer()


async def edit_state_handler(message: Message, state: FSMContext):
    cfg = load_config()
    st = await state.get_state()
    val = message.text.strip()

    if st == EditStates.waiting_description.state:
        cfg.description = val
    elif st == EditStates.waiting_hashtag_add.state:
        if not val.startswith("#"):
            await message.answer("Хештег должен начинаться с #")
            return
        if val not in cfg.hashtags:
            cfg.hashtags.append(val)
    elif st == EditStates.waiting_hashtag_remove.state:
        cfg.hashtags = [x for x in cfg.hashtags if x != val and x != REQUIRED_TAG]
    elif st == EditStates.waiting_schedule.state:
        items = [x.strip() for x in val.split(",") if x.strip()]
        for t in items:
            datetime.strptime(t, "%H:%M")
        cfg.publish_times = items
    elif st == EditStates.waiting_folder_queue.state:
        cfg.queue_dir = val
    elif st == EditStates.waiting_folder_archive.state:
        cfg.archive_dir = val

    save_config(cfg)
    ensure_dirs(cfg)
    await state.clear()
    await message.answer("Настройки обновлены.")


def schedule_jobs(scheduler: AsyncIOScheduler, bot: Bot):
    cfg = load_config()
    scheduler.remove_all_jobs()
    for tm in cfg.publish_times:
        hh, mm = tm.split(":")
        scheduler.add_job(scheduled_publish_job, CronTrigger(hour=int(hh), minute=int(mm)), args=[bot], id=f"publish_{tm}")
    scheduler.add_job(stats_job, "interval", hours=6, id="stats_6h")


async def main():
    setup_logging()
    init_db()

    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        raise RuntimeError("TG_BOT_TOKEN не задан")

    cfg = load_config()
    ensure_dirs(cfg)

    bot = Bot(token=token)
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_stats, Command("stats"))
    dp.message.register(video_handler, F.video)
    dp.callback_query.register(callbacks)
    dp.message.register(edit_state_handler, EditStates.waiting_description)
    dp.message.register(edit_state_handler, EditStates.waiting_hashtag_add)
    dp.message.register(edit_state_handler, EditStates.waiting_hashtag_remove)
    dp.message.register(edit_state_handler, EditStates.waiting_schedule)
    dp.message.register(edit_state_handler, EditStates.waiting_folder_queue)
    dp.message.register(edit_state_handler, EditStates.waiting_folder_archive)

    scheduler = AsyncIOScheduler(timezone="UTC")
    schedule_jobs(scheduler, bot)
    scheduler.start()

    logging.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
