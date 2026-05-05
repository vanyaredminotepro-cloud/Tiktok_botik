import asyncio
import logging
import os
import re
import socket
from dataclasses import dataclass
from typing import List, Optional, Set

import aiohttp


MT_RE = re.compile(r"(?P<host>[a-zA-Z0-9_.-]+):(?P<port>\d+)\s+секрет\s+(?P<secret>[a-fA-F0-9]+)", re.IGNORECASE)
URL_RE = re.compile(r"tg://proxy\?server=([^&]+)&port=(\d+)&secret=([a-fA-F0-9]+)", re.IGNORECASE)


@dataclass(eq=True, frozen=True)
class MTProtoProxy:
    host: str
    port: int
    secret: str


class ProxyManager:
    """
    Менеджер прокси:
    - хранит пул MTProto-прокси
    - выбирает рабочий SOCKS5-прокси
    - обновляет список из Telegram-каналов раз в 30 минут
    """

    def __init__(self, refresh_minutes: int = 30):
        self.logger = logging.getLogger("proxy_manager")
        self.refresh_minutes = refresh_minutes
        self.channels = ["https://t.me/s/TProxyRU", "https://t.me/s/ProxyMTProto"]

        # Временный стартовый пул MTProto
        self.pool: Set[MTProtoProxy] = {
            MTProtoProxy("94.183.177.111", 443, "dd0a3036756eb7227a3a938f29564f181e"),
            MTProtoProxy("94.183.177.201", 443, "dd4dfae341b3dba1516cd9cfea3196f6ed"),
            MTProtoProxy("tpr.webvirt.cloud", 443, "ee938dd87467bc49301de2e9765cf20f4374656c2e776562766972742e636c6f7564"),
            MTProtoProxy("s.rkn.tg", 853, "ee54ce330e4690cc297d2b031ff3f288b06d742e616b656e61692e636c69636b"),
        }

        self.current_socks: Optional[str] = None
        self._lock = asyncio.Lock()

    @staticmethod
    def mtproto_to_socks_fallback(mt: MTProtoProxy) -> str:
        """
        Заглушка-конвертер MTProto -> SOCKS5.
        Для реального прод-режима используйте mtproto2socks / python-mtproto-proxy локально.
        """
        return f"socks5://{mt.host}:{mt.port}"

    async def _is_socks_reachable(self, socks_url: str, timeout: float = 5.0) -> bool:
        try:
            host_port = socks_url.replace("socks5://", "")
            host, port = host_port.split(":", 1)
            with socket.create_connection((host, int(port)), timeout=timeout):
                return True
        except Exception:
            return False

    async def select_working_proxy(self) -> str:
        """Возвращает рабочий socks5://... или бросает RuntimeError."""
        async with self._lock:
            env_proxy = os.getenv("SOCKS_PROXY")
            if env_proxy:
                if await self._is_socks_reachable(env_proxy):
                    self.current_socks = env_proxy
                    self.logger.info("Используется SOCKS_PROXY из .env: %s", env_proxy)
                    return env_proxy
                self.logger.warning("SOCKS_PROXY из .env недоступен: %s", env_proxy)

            for mt in list(self.pool):
                socks_url = self.mtproto_to_socks_fallback(mt)
                if await self._is_socks_reachable(socks_url):
                    if self.current_socks != socks_url:
                        self.logger.info("Переключение прокси: %s", socks_url)
                    self.current_socks = socks_url
                    return socks_url

            raise RuntimeError("Не найден рабочий прокси (SOCKS/MTProto). Проверьте сеть/пул прокси.")

    async def refresh_from_channels(self) -> int:
        """Читает страницы каналов и добавляет найденные MTProto-прокси в пул."""
        found = 0
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in self.channels:
                try:
                    async with session.get(url) as resp:
                        html = await resp.text()
                except Exception as e:
                    self.logger.warning("Не удалось прочитать %s: %s", url, e)
                    continue

                for host, port, secret in MT_RE.findall(html):
                    proxy = MTProtoProxy(host=host, port=int(port), secret=secret)
                    if proxy not in self.pool:
                        self.pool.add(proxy)
                        found += 1

                for host, port, secret in URL_RE.findall(html):
                    proxy = MTProtoProxy(host=host, port=int(port), secret=secret)
                    if proxy not in self.pool:
                        self.pool.add(proxy)
                        found += 1

        if found:
            self.logger.info("Добавлено новых MTProto-прокси: %s", found)
        else:
            self.logger.info("Новых MTProto-прокси не найдено")
        return found

    async def background_refresh_loop(self):
        while True:
            try:
                await self.refresh_from_channels()
                # Если текущий прокси умер, найти новый
                if self.current_socks and not await self._is_socks_reachable(self.current_socks):
                    self.logger.warning("Текущий прокси недоступен, попытка переключения")
                    await self.select_working_proxy()
            except Exception as e:
                self.logger.exception("Ошибка фонового обновления прокси: %s", e)
            await asyncio.sleep(self.refresh_minutes * 60)
