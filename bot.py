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
from typing import List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "bot_data.db"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "bot.log"
QUEUE_DIR_DEFAULT = BASE_DIR / "tiktok_queue"
ARCHIVE_DIR_DEFAULT = BASE_DIR / "tiktok_archive"

MAX_VIDEO_SIZE = 50 * 1024 * 1024
DEFAULT_DESCRIPTION = "Телеграм канал РП проекта: @perehodnikrp"
REQUIRED_TAG = "#обоссляндия"
RANDOM_TAGS = [
    "#обосляндия", "#страйкбол", "#рек", "#рекомендации", "#rec",
    "#recomendation", "#war", "#землянка", "#fyp", "#elbruso",
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


class SettingsState(StatesGroup):
    description = State()
    add_tag = State()
    remove_tag = State()
    schedule = State()
    queue_folder = State()
    archive_folder = State()


def default_config() -> BotConfig:
    env_first_weeks = os.getenv("FIRST_WEEKS", "True").lower() == "true"
    return BotConfig(
        publish_times=["10:00", "18:00"],
        description=DEFAULT_DESCRIPTION,
        hashtags=[REQUIRED_TAG],
        queue_dir=str(QUEUE_DIR_DEFAULT),
        archive_dir=str(ARCHIVE_DIR_DEFAULT),
        first_weeks=env_first_weeks,
        old_video_days=14,
        created_at=datetime.utcnow().isoformat(),
        bot_enabled=True,
    )


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    root.addHandler(stream_handler)


def load_config() -> BotConfig:
    if not CONFIG_PATH.exists():
        cfg = default_config()
        save_config(cfg)
        return cfg
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return BotConfig(**json.load(f))


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
        """CREATE TABLE IF NOT EXISTS publish_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            published_at TEXT,
            status TEXT,
            details TEXT
        )"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS stats_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            followers INTEGER,
            views INTEGER,
            likes INTEGER,
            comments INTEGER,
            shares INTEGER,
            saves INTEGER,
            topic TEXT,
            er REAL
        )"""
    )
    conn.commit()
    conn.close()


def queue_files(cfg: BotConfig) -> List[Path]:
    return sorted(Path(cfg.queue_dir).glob("*.mp4"), key=lambda p: p.stat().st_mtime)


def archive_files(cfg: BotConfig) -> List[Path]:
    return list(Path(cfg.archive_dir).glob("*.mp4"))


def should_use_old_mode(cfg: BotConfig) -> bool:
    start = datetime.fromisoformat(cfg.created_at)
    in_first_days = datetime.utcnow() < start + timedelta(days=cfg.old_video_days)
    return cfg.first_weeks and in_first_days


def make_caption(cfg: BotConfig) -> str:
    tags = [REQUIRED_TAG] + [t for t in cfg.hashtags if t != REQUIRED_TAG] + random.sample(RANDOM_TAGS, 5)
    dedup = []
    for t in tags:
        if t not in dedup:
            dedup.append(t)
    return f"{cfg.description}\n\n{' '.join(dedup)}"


async def human_delay(a=2.0, b=6.0):
    await asyncio.sleep(random.uniform(a, b))


async def publish_to_tiktok(video_path: Path, caption: str) -> Tuple[bool, str]:
    login = os.getenv("TIKTOK_LOGIN")
    password = os.getenv("TIKTOK_PASSWORD")
    if not login or not password:
        return False, "Не заданы TIKTOK_LOGIN/TIKTOK_PASSWORD"

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            # Первый вход может потребовать ручную капчу/подтверждение
            await page.goto("https://www.tiktok.com/login/phone-or-email/email", timeout=120000)
            await human_delay()
            await page.click('input[name="username"]')
            await page.type('input[name="username"]', login, delay=80)
            await page.click('input[type="password"]')
            await page.type('input[type="password"]', password, delay=80)
            await human_delay()
            await page.click('button[type="submit"]')

            # Время на ручную капчу/2FA при необходимости
            await page.wait_for_timeout(25000)

            await page.goto("https://www.tiktok.com/upload", timeout=120000)
            await human_delay(4, 8)
            await page.locator('input[type="file"]').set_input_files(str(video_path))
            await human_delay(6, 10)

            editor = page.locator('[contenteditable="true"]').first
            await editor.click()
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            await page.type('[contenteditable="true"]', caption, delay=60)
            await human_delay(3, 6)

            btn = page.get_by_role("button", name="Опубликовать")
            if await btn.count() == 0:
                btn = page.get_by_role("button", name="Post")
            await btn.click()
            await page.wait_for_timeout(15000)

            await browser.close()
            return True, "ok"
    except PlaywrightTimeoutError:
        return False, "Timeout: TikTok не ответил вовремя"
    except Exception as e:
        return False, f"Ошибка публикации: {e}"


def log_publish(file_name: str, status: str, details: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO publish_log (file_name, published_at, status, details) VALUES (?, ?, ?, ?)",
        (file_name, datetime.utcnow().isoformat(), status, details),
    )
    conn.commit()
    conn.close()


async def publish_one(cfg: BotConfig, from_archive: bool = False) -> bool:
    files = archive_files(cfg) if from_archive else queue_files(cfg)
    if not files:
        logging.warning("Очередь публикации пуста")
        return False

    video = random.choice(files) if from_archive else files[0]
    ok, msg = await publish_to_tiktok(video, make_caption(cfg))
    if ok:
        target = Path(cfg.archive_dir) / video.name
        if video.resolve() != target.resolve():
            shutil.move(str(video), str(target))
        log_publish(video.name, "success", "published")
        return True

    log_publish(video.name, "failed", msg)
    logging.error("Не удалось опубликовать %s: %s", video.name, msg)
    return False


async def scheduled_publish(bot: Bot):
    cfg = load_config()
    if not cfg.bot_enabled:
        logging.info("Автопостинг отключён")
        return

    use_old = should_use_old_mode(cfg) and len(queue_files(cfg)) < 10 and len(archive_files(cfg)) > 0
    await publish_one(cfg, from_archive=use_old)


async def stats_job():
    # Шаблон сбора статистики (без выдумывания несуществующих API).
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO stats_snapshots (created_at, followers, views, likes, comments, shares, saves, topic, er) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (datetime.utcnow().isoformat(), 0, 0, 0, 0, 0, 0, "unknown", 0.0),
    )
    conn.commit()
    conn.close()


def menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="stats")],
        [InlineKeyboardButton(text="📤 ЗАГРУЗИТЬ ВИДЕО", callback_data="upload")],
        [InlineKeyboardButton(text="▶️ ВЫЛОЖИТЬ ВСЕ ВИДЕО", callback_data="publish_all")],
        [InlineKeyboardButton(text="⚙️ НАСТРОЙКИ", callback_data="settings")],
        [InlineKeyboardButton(text="🔄 РЕЖИМ СТАРЫХ ВИДЕО", callback_data="old_mode")],
        [InlineKeyboardButton(text="🚀 СТАТУС БОТА", callback_data="status")],
        [InlineKeyboardButton(text="❌ ОСТАНОВИТЬ БОТА", callback_data="stop")],
    ])


async def start_cmd(message: Message):
    await message.answer("Панель управления TikTok-ботом", reply_markup=menu())


async def stats_cmd(message: Message):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT created_at, followers, views, likes, comments, shares, saves, topic, er FROM stats_snapshots ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    if not row:
        await message.answer("Статистика ещё не собрана")
        return
    created, followers, views, likes, comments, shares, saves, topic, er = row
    await message.answer(
        f"📊 Срез: {created}\nПодписчики: {followers}\nПросмотры: {views}\n"
        f"Лайки: {likes}\nКомментарии: {comments}\nРепосты: {shares}\n"
        f"Сохранения: {saves}\nТема: {topic}\nER: {er:.2f}%"
    )


async def on_video(message: Message):
    cfg = load_config()
    ensure_dirs(cfg)
    if not message.video:
        return
    if message.video.file_size and message.video.file_size > MAX_VIDEO_SIZE:
        await message.answer("❌ Файл больше 50 МБ")
        return

    info = await message.bot.get_file(message.video.file_id)
    if info.file_path and not info.file_path.lower().endswith(".mp4"):
        await message.answer("❌ Поддерживается только MP4")
        return

    filename = f"{int(time.time())}_{message.video.file_id}.mp4"
    dst = Path(cfg.queue_dir) / filename
    await message.bot.download_file(info.file_path, destination=dst)
    await message.answer("✅ Видео принято. Будет опубликовано по расписанию")


async def callbacks(call: CallbackQuery, state: FSMContext):
    cfg = load_config()

    if call.data == "upload":
        await call.message.answer("Отправьте MP4-файл до 50 МБ")
    elif call.data == "stats":
        await stats_cmd(call.message)
    elif call.data == "status":
        await call.message.answer(f"Статус: {'работает' if cfg.bot_enabled else 'остановлен'}\nВ очереди: {len(queue_files(cfg))}\nОкна публикаций: {', '.join(cfg.publish_times)}")
    elif call.data == "stop":
        cfg.bot_enabled = False
        save_config(cfg)
        await call.message.answer("Автопостинг остановлен")
    elif call.data == "old_mode":
        cfg.first_weeks = not cfg.first_weeks
        save_config(cfg)
        await call.message.answer(f"Режим старых видео: {'включён' if cfg.first_weeks else 'выключен'}")
    elif call.data == "publish_all":
        count = 0
        while queue_files(cfg):
            if not await publish_one(cfg):
                break
            count += 1
        await call.message.answer(f"Опубликовано: {count}")
    elif call.data == "settings":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Править описание", callback_data="s_desc")],
            [InlineKeyboardButton(text="🏷️ Добавить хештег", callback_data="s_add")],
            [InlineKeyboardButton(text="🏷️ Удалить хештег", callback_data="s_del")],
            [InlineKeyboardButton(text="⏰ Изменить расписание", callback_data="s_time")],
            [InlineKeyboardButton(text="📂 Папка очереди", callback_data="s_queue")],
            [InlineKeyboardButton(text="📂 Папка архива", callback_data="s_arch")],
        ])
        await call.message.answer("Настройки:", reply_markup=kb)
    elif call.data == "s_desc":
        await state.set_state(SettingsState.description)
        await call.message.answer("Новый текст описания:")
    elif call.data == "s_add":
        await state.set_state(SettingsState.add_tag)
        await call.message.answer("Хештег для добавления (#tag):")
    elif call.data == "s_del":
        await state.set_state(SettingsState.remove_tag)
        await call.message.answer("Хештег для удаления:")
    elif call.data == "s_time":
        await state.set_state(SettingsState.schedule)
        await call.message.answer("Новое расписание, например: 10:00,18:00")
    elif call.data == "s_queue":
        await state.set_state(SettingsState.queue_folder)
        await call.message.answer("Новый путь для очереди:")
    elif call.data == "s_arch":
        await state.set_state(SettingsState.archive_folder)
        await call.message.answer("Новый путь для архива:")

    await call.answer()


async def settings_handler(message: Message, state: FSMContext):
    cfg = load_config()
    text = (message.text or "").strip()
    st = await state.get_state()

    if st == SettingsState.description.state:
        cfg.description = text
    elif st == SettingsState.add_tag.state:
        if not text.startswith("#"):
            await message.answer("Хештег должен начинаться с #")
            return
        if text not in cfg.hashtags:
            cfg.hashtags.append(text)
    elif st == SettingsState.remove_tag.state:
        cfg.hashtags = [h for h in cfg.hashtags if h != text and h != REQUIRED_TAG]
    elif st == SettingsState.schedule.state:
        times = [x.strip() for x in text.split(",") if x.strip()]
        for t in times:
            datetime.strptime(t, "%H:%M")
        cfg.publish_times = times
    elif st == SettingsState.queue_folder.state:
        cfg.queue_dir = text
    elif st == SettingsState.archive_folder.state:
        cfg.archive_dir = text

    save_config(cfg)
    ensure_dirs(cfg)
    await state.clear()
    await message.answer("Настройки сохранены")


def schedule_jobs(scheduler: AsyncIOScheduler, bot: Bot):
    cfg = load_config()
    scheduler.remove_all_jobs()
    for t in cfg.publish_times:
        hh, mm = t.split(":")
        scheduler.add_job(scheduled_publish, CronTrigger(hour=int(hh), minute=int(mm)), args=[bot], id=f"pub_{t}")
    scheduler.add_job(stats_job, "interval", hours=6, id="stats")


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

    dp.message.register(start_cmd, Command("start"))
    dp.message.register(stats_cmd, Command("stats"))
    dp.message.register(on_video, F.video)
    dp.callback_query.register(callbacks)
    dp.message.register(settings_handler, SettingsState.description)
    dp.message.register(settings_handler, SettingsState.add_tag)
    dp.message.register(settings_handler, SettingsState.remove_tag)
    dp.message.register(settings_handler, SettingsState.schedule)
    dp.message.register(settings_handler, SettingsState.queue_folder)
    dp.message.register(settings_handler, SettingsState.archive_folder)

    scheduler = AsyncIOScheduler(timezone="UTC")
    schedule_jobs(scheduler, bot)
    scheduler.start()

    logging.info("Бот запущен без прокси")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
