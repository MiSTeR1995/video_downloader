# coding: utf-8
from urllib.parse import parse_qs, urlparse
import re
import requests
from bs4 import BeautifulSoup

def get_video_id(url):
    parsed_url = urlparse(url)
    host = parsed_url.netloc.lower()

    # youtu.be короткие ссылки
    if host in ('youtu.be', 'www.youtu.be'):
        return 'youtube', parsed_url.path.lstrip('/')

    # youtube / m.youtube / www.youtube
    if host in ('m.youtube.com', 'youtube.com', 'www.youtube.com'):
        # shorts
        if '/shorts/' in parsed_url.path:
            return 'youtube', parsed_url.path.split('/shorts/')[1].split('/')[0]
        # обычный watch?v=
        query = parse_qs(parsed_url.query)
        return 'youtube', query.get('v', [None])[0]

    # rutube
    if host in ('rutube.ru', 'www.rutube.ru'):
        match = re.search(r'/video/([a-zA-Z0-9_-]+)', parsed_url.path)
        if match:
            return 'rutube', match.group(1)

    return None, None

def get_playlist_id(url):
    parsed_url = urlparse(url)
    host = parsed_url.netloc.lower()

    # YouTube плейлисты
    if host in ('www.youtube.com', 'youtube.com', 'm.youtube.com'):
        query = parse_qs(parsed_url.query)
        playlist_id = query.get('list', [None])[0]
        return 'youtube', playlist_id if playlist_id else None

    # Rutube плейлисты
    elif host in ('rutube.ru', 'www.rutube.ru'):
        # /playlists/{id}
        path_parts = parsed_url.path.split('/')
        if len(path_parts) > 2 and path_parts[1] == 'playlists':
            return 'rutube', path_parts[2]

        # /plst/{id}
        plst_match = re.search(r'/plst/(\d+)', parsed_url.path)
        if plst_match:
            return 'rutube', plst_match.group(1)

    # не удалось определить
    return None, None

def read_video_urls(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def get_rutube_playlist_video_ids(playlist_url, logger):
    try:
        # заголовки — чтобы сайт думал, что мы браузер
        headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"),
            "Accept-Language": "ru,en;q=0.8",
        }
        response = requests.get(playlist_url, headers=headers, timeout=20)
        if response.status_code != 200:
            logger.error(f"Не удалось получить страницу плейлиста: {response.status_code}")
            return []
        logger.info(f"Страница плейлиста успешно получена: {playlist_url}")

        soup = BeautifulSoup(response.content, 'html.parser')

        video_ids = []

        # Старый селектор
        for a in soup.select('a.wdp-playlist-video-card-module__title'):
            href = a.get('href', '')
            m = re.search(r'/video/([a-zA-Z0-9_-]+)', href)
            if m:
                video_ids.append(m.group(1))
                logger.info(f"Найдено видео с ID: {m.group(1)}")

        # Альтернативные варианты
        if not video_ids:
            for a in soup.select('a.playlist-card__link, a.video-card__link'):
                href = a.get('href', '')
                m = re.search(r'/video/([a-zA-Z0-9_-]+)', href)
                if m:
                    video_ids.append(m.group(1))
                    logger.info(f"Найдено видео с ID: {m.group(1)}")

        # Fallback — data-атрибуты
        if not video_ids:
            for tag in soup.select('[data-video-id]'):
                vid = tag.get('data-video-id')
                if vid and re.fullmatch(r'[a-zA-Z0-9_-]+', vid):
                    video_ids.append(vid)
                    logger.info(f"Найдено видео с ID: {vid}")

        # уникальные в исходном порядке
        video_ids = list(dict.fromkeys(video_ids))

        if not video_ids:
            logger.error(f"Не найдено ни одного видео в плейлисте: {playlist_url}")
        return video_ids

    except Exception as e:
        logger.error(f"Ошибка при скрапинге страницы плейлиста: {e}")
        return []
