from urllib.parse import parse_qs, urlparse
import re
import requests
from bs4 import BeautifulSoup

def get_video_id(url):
    parsed_url = urlparse(url)
    if parsed_url.netloc in ('www.youtube.com', 'youtube.com'):
        query = parse_qs(parsed_url.query)
        return 'youtube', query.get('v', [None])[0]
    elif parsed_url.netloc in ('rutube.ru', 'www.rutube.ru'):
        # Обновленный паттерн для более надежного извлечения ID видео Rutube
        match = re.search(r'/video/([a-zA-Z0-9_-]+)', parsed_url.path)
        if match:
            return 'rutube', match.group(1)
    return None, None

def get_playlist_id(url):
    parsed_url = urlparse(url)

    # Обработка YouTube плейлистов
    if parsed_url.netloc in ('www.youtube.com', 'youtube.com'):
        query = parse_qs(parsed_url.query)
        playlist_id = query.get('list', [None])[0]
        return 'youtube', playlist_id if playlist_id else None

    # Обработка Rutube плейлистов
    elif parsed_url.netloc in ('rutube.ru', 'www.rutube.ru'):
        # Проверка формата /playlists/{id}
        path_parts = parsed_url.path.split('/')
        if len(path_parts) > 2 and path_parts[1] == 'playlists':
            return 'rutube', path_parts[2]

        # Проверка формата /plst/{id}
        plst_match = re.search(r'/plst/(\d+)', parsed_url.path)
        if plst_match:
            return 'rutube', plst_match.group(1)

    # Если не удалось определить платформу или ID плейлиста
    return None, None

def read_video_urls(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def get_rutube_playlist_video_ids(playlist_url, logger):
    try:
        response = requests.get(playlist_url)
        if response.status_code != 200:
            logger.error(f"Failed to retrieve playlist page: {response.status_code}")
            return []
        logger.info(f"Playlist page retrieved successfully: {playlist_url}")

        soup = BeautifulSoup(response.content, 'html.parser')

        video_ids = []
        for video_tag in soup.select('a.wdp-playlist-video-card-module__title'):
            video_id = video_tag['href'].split('/')[2]
            video_ids.append(video_id)
            logger.info(f"Found video ID: {video_id}")

        if not video_ids:
            logger.error(f"No video IDs found in playlist: {playlist_url}")
        return video_ids
    except Exception as e:
        logger.error(f"Error while scraping playlist page: {e}")
        return []
