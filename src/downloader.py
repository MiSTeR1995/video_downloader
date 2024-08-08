import os
import signal
import logging
import multiprocessing
from multiprocessing import Pool
from tqdm import tqdm
from .url_utils import get_video_id, get_playlist_id, read_video_urls, get_rutube_playlist_video_ids
from .logging_utils import setup_logging, filter_yt_dlp_output
from .file_utils import create_file_index, find_existing_file
from .subprocess_utils import subprocess_run_context
from .metadata_utils import update_metadata_to_csv, process_video_metadata, cleanup_info_json_files, get_file_metadata

# Глобальная переменная для отслеживания состояния прерывания
interrupt_event = multiprocessing.Event()

def signal_handler(signum, frame):
    logging.info("Получен сигнал прерывания. Начинаем корректное завершение...")
    interrupt_event.set()

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def download_video(args):
    video_url, output_dir, video_quality, file_index, config = args
    logger = setup_logging(config, worker=True)

    if interrupt_event.is_set():
        return False, None

    platform, video_id = get_video_id(video_url)

    if not video_id:
        logger.warning(f"Некорректный URL видео: {video_url}")
        return False, None

    logger.info(f"Обработка видео с ID: {video_id} на платформе {platform}")

    existing_file_path = find_existing_file(output_dir, video_id, file_index)

    if existing_file_path:
        existing_height = get_file_metadata(existing_file_path, config).get('height')

        if video_quality and existing_height is not None:
            target_height = int(video_quality[:-1])
            existing_height = int(existing_height)
            if existing_height == target_height:
                logger.info(f"Файл {video_id} с требуемым разрешением уже существует. Обновляем метаданные.")
                metadata = process_video_metadata(video_url, existing_file_path, output_dir, config)
                return True, metadata
            else:
                logger.info(f"Существующий файл {video_id} имеет разрешение {existing_height}p. Будет загружен файл с разрешением {target_height}p.")
    else:
        logger.info(f"Видеозаписи с ID: {video_id} не существует. Начинаем загрузку.")

    format_string = f"bestvideo[height<={video_quality[:-1]}]+bestaudio/best[height<={video_quality[:-1]}]" if video_quality else "bestvideo+bestaudio/best"
    output_template = os.path.join(output_dir, f"{video_id}.%(ext)s")

    logger.info(f"Начало загрузки видео {video_id} с {platform}")
    download_command = [
        "yt-dlp",
        video_url,
        "-f", format_string,
        "--output", output_template,
        "--force-overwrites",
        "--no-continue",
        "--no-part",
        "--merge-output-format", "mp4",
        "--no-warnings"
    ]

    try:
        with subprocess_run_context(download_command) as process:
            while True:
                if interrupt_event.is_set():
                    process.terminate()
                    logger.info(f"Загрузка видео {video_id} прервана.")
                    return False, None

                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    filtered_output = filter_yt_dlp_output(output.strip())
                    if filtered_output:
                        logger.debug(filtered_output)

            stderr = process.stderr.read()
            if stderr:
                filtered_stderr = filter_yt_dlp_output(stderr)
                if filtered_stderr:
                    logger.error(f"yt-dlp stderr для видео {video_id}:\n{filtered_stderr}")

        if interrupt_event.is_set():
            return False, None

        new_file_path = find_existing_file(output_dir, video_id, create_file_index(output_dir, logger))
        if new_file_path:
            new_size = os.path.getsize(new_file_path)
            new_size_mb = new_size / (1024 * 1024)
            logger.info(f"Загрузка видео {video_id} завершена. Размер файла: {new_size_mb:.2f} MB")

            # Обработка метаданных
            metadata = process_video_metadata(video_url, new_file_path, output_dir, config)

            return True, metadata
        else:
            logger.error(f"Ошибка: Файл не найден после попытки загрузки видео {video_id}")
    except Exception as e:
        logger.error(f"Ошибка во время загрузки видео {video_id}: {e}")
        return False, None

    return False, None

def download_playlist(playlist_url: str, output_dir: str, num_workers: int, video_quality: str, config, logger):

    platform, playlist_id = get_playlist_id(playlist_url)

    if not playlist_id:
        logger.error(f"Некорректный URL плейлиста: {playlist_url}")
        return

    if platform == 'youtube':

        list_command = [
            "yt-dlp",
            "--flat-playlist",
            "--get-id",
            playlist_url
        ]

        try:
            with subprocess_run_context(list_command) as process:
                stdout, stderr = process.communicate()
            video_ids = stdout.strip().split('\n')
        except Exception as e:
            logger.error(f"Ошибка при обработке плейлиста {playlist_url}: {e}")
            return

        video_urls = [f"https://www.youtube.com/watch?v={vid}" for vid in video_ids]

    elif platform == 'rutube':
        try:
            video_ids = get_rutube_playlist_video_ids(playlist_url, logger)
            if not video_ids:
                logger.error(f"Не удалось получить идентификаторы видео для плейлиста {playlist_url}")
                return
            video_urls = [f"https://rutube.ru/video/{vid}/" for vid in video_ids]
        except Exception as e:
            logger.error(f"Ошибка при обработке плейлиста {playlist_url}: {e}")
            return
    else:
        logger.error(f"Неподдерживаемая платформа: {platform}")
        return

    download_individual_videos(video_urls, output_dir, num_workers, video_quality, config, logger)

def download_individual_videos(video_urls: list, output_dir: str, num_workers: int, video_quality: str, config, logger):
    file_index = create_file_index(output_dir, logger)
    download_args = [(url, output_dir, video_quality, file_index, config) for url in video_urls]

    metadata_list = []
    with Pool(processes=num_workers, initializer=init_worker) as pool:
        try:
            results = []
            for success, metadata in tqdm(pool.imap_unordered(download_video, download_args), total=len(video_urls), desc="Загрузка видео", ncols=70):
                results.append(success)
                if metadata:
                    metadata_list.append(metadata)
                if interrupt_event.is_set():
                    logger.info("Прерывание загрузки...")
                    pool.terminate()
                    break
        except KeyboardInterrupt:
            logger.info("Получено прерывание клавиатуры. Завершаем работу пула процессов...")
            pool.terminate()
        finally:
            pool.close()
            pool.join()

    successful = sum(1 for result in results if result)
    logger.info(f"Успешно обработано {successful} из {len(results)} видео.")

    # Обновляем метаданные в CSV файл
    if metadata_list:
        csv_path = None  # Инициализация переменной csv_path
        for metadata in metadata_list:
            csv_path = update_metadata_to_csv(metadata, output_dir, config)
        if csv_path:
            logger.info(f"Метаданные обновлены в файле: {csv_path}")
        else:
            logger.error("Не удалось обновить метаданные в файле CSV.")

    cleanup_info_json_files(output_dir, config)

def download_videos(config):
    logger = setup_logging(config)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    output_dir = config['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Выходная директория: {output_dir}")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    video_quality = config.get('video_quality')
    if video_quality is None:
        logger.info("Качество видео не указано. Будет использовано лучшее доступное качество.")
    else:
        logger.info(f"Планируется загрузить видео в качестве: {video_quality}")

    try:
        if config['use_playlists']:
            playlists_file = os.path.join(base_dir, config['playlists_file'])
            playlist_urls = read_video_urls(playlists_file)

            for playlist_url in playlist_urls:
                if interrupt_event.is_set():
                    break
                platform, playlist_id = get_playlist_id(playlist_url)
                if not playlist_id:
                    logger.warning(f"Некорректный URL плейлиста: {playlist_url}")
                    continue

                playlist_dir = os.path.join(output_dir, playlist_id)
                os.makedirs(playlist_dir, exist_ok=True)

                logger.info(f"Обработка плейлиста: {playlist_url}")
                download_playlist(playlist_url, playlist_dir, config['num_workers'], video_quality, config, logger)
        else:
            video_urls_file = os.path.join(base_dir, config['video_urls_file'])
            video_urls = read_video_urls(video_urls_file)
            individual_videos_dir = os.path.join(output_dir, "individual_videos")
            os.makedirs(individual_videos_dir, exist_ok=True)
            logger.info(f"Создана папка для отдельных видео: {individual_videos_dir}")
            download_individual_videos(video_urls, individual_videos_dir, config['num_workers'], video_quality, config, logger)

        if interrupt_event.is_set():
            logger.info("Процесс загрузки прерван пользователем.")
        else:
            logger.info("Процесс загрузки завершен успешно.")
    except Exception as e:
        logger.error(f"Произошла ошибка во время выполнения: {e}")
    finally:
        logger.info("Завершение работы...")
