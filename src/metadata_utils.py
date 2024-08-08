import csv
import os
import json
from yt_dlp import YoutubeDL
from ffprobe import FFProbe
from .logging_utils import setup_logging
from .url_utils import get_video_id

def cleanup_info_json_files(output_dir, config):
    logger = setup_logging(config)
    for filename in os.listdir(output_dir):
        if filename.endswith('.info.json'):
            file_path = os.path.join(output_dir, filename)
            try:
                os.remove(file_path)
                logger.info(f"Удален файл {file_path}")
            except Exception as e:
                logger.error(f"Не удалось удалить файл {file_path}: {e}")

def get_file_metadata(file_path, config):
    logger = setup_logging(config, worker=True)
    try:
        metadata = FFProbe(file_path)
        video_stream = next((s for s in metadata.streams if s.is_video()), None)
        audio_stream = next((s for s in metadata.streams if s.is_audio()), None)

        def safe_get(obj, attr):
            return getattr(obj, attr, 'N/A') if obj else 'N/A'

        duration = safe_get(video_stream, 'duration')
        if duration == 'N/A' and video_stream:
            time_base = safe_get(video_stream, 'time_base')
            nb_frames = safe_get(video_stream, 'nb_frames')
            if time_base != 'N/A' and nb_frames != 'N/A':
                duration = float(time_base) * int(nb_frames)

        result = {
            'duration': duration,
            'fps': safe_get(video_stream, 'framerate'),
            'sample_rate': safe_get(audio_stream, 'sample_rate'),
            'channels': safe_get(audio_stream, 'channels'),
            'width': safe_get(video_stream, 'width'),
            'height': safe_get(video_stream, 'height')
        }

        logger.info(f"Извлеченные метаданные файла для {file_path}: {result}")

        if all(value == 'N/A' for value in result.values()):
            logger.warning(f"Все метаданные для {file_path} имеют значение 'N/A'. Возможно, проблема с извлечением данных.")

        return result
    except Exception as e:
        logger.error(f"Ошибка при получении метаданных файла {file_path}: {e}")
        return {}

def update_metadata_to_csv(metadata, output_dir, config):
    logger = setup_logging(config)
    csv_path = os.path.join(output_dir, 'video_metadata.csv')

    # Определяем точный список полей, которые нужно сохранить
    required_fields = ['id', 'file_name', 'height', 'width', 'fps', 'duration', 'sample_rate', 'audio_channels', 'file_size', 'video_url', 'title', 'platform']

    try:
        if not os.path.isfile(csv_path):
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=required_fields)
                writer.writeheader()
                writer.writerow(metadata)
        else:
            with open(csv_path, 'r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)

            updated = False
            for row in rows:
                if row['id'] == metadata['id']:
                    row.update(metadata)
                    updated = True
                    break

            if not updated:
                rows.append(metadata)

            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=required_fields)
                writer.writeheader()
                writer.writerows(rows)

        logger.info(f"Метаданные успешно обновлены в файле: {csv_path}")
        return csv_path
    except Exception as e:
        logger.error(f"Ошибка при обновлении метаданных в CSV: {e}")
        return None

def update_metadata(video_id, new_metadata, output_dir, config):
    logger = setup_logging(config, worker=True)
    json_path = os.path.join(output_dir, f"{video_id}_metadata.json")
    try:
        if os.path.exists(json_path):
            with open(json_path, 'r', encoding='utf-8') as f:
                existing_metadata = json.load(f)
            existing_metadata.update(new_metadata)
        else:
            existing_metadata = new_metadata

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(existing_metadata, f, ensure_ascii=False, indent=4)

        # logger.info(f"Метаданные обновлены для видео {video_id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении метаданных для видео {video_id}: {e}")
        return False

def get_cached_metadata(video_id, output_dir, config):

    logger = setup_logging(config, worker=True)
    json_path = os.path.join(output_dir, f"{video_id}_metadata.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка при чтении кэшированных метаданных для видео {video_id}: {e}")
    return None

def compare_metadata(old_metadata, new_metadata):

    differences = {}
    for key in set(old_metadata.keys()) | set(new_metadata.keys()):
        if key not in old_metadata:
            differences[key] = ('Добавлено', new_metadata[key])
        elif key not in new_metadata:
            differences[key] = ('Удалено', old_metadata[key])
        elif old_metadata[key] != new_metadata[key]:
            differences[key] = ('Изменено', old_metadata[key], new_metadata[key])
    return differences

def process_video_metadata(video_url, file_path, output_dir, config):
    logger = setup_logging(config, worker=True)
    # video_id = get_video_id(video_url)
    platform, video_id = get_video_id(video_url)

    # Проверяем наличие кэшированных метаданных
    cached_metadata = get_cached_metadata(video_id, output_dir, config)

    # Получаем метаданные из файла
    file_metadata = get_file_metadata(file_path, config)

    # Получаем минимальную информацию из YouTube
    ydl_opts = {'quiet': True}
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)

    new_metadata = {
        'id': video_id,
        'file_name': os.path.basename(file_path),
        'height': file_metadata.get('height', 'N/A'),
        'width': file_metadata.get('width', 'N/A'),
        'fps': file_metadata.get('fps', 'N/A'),
        'duration': file_metadata.get('duration', 'N/A'),
        'sample_rate': file_metadata.get('sample_rate', 'N/A'),
        'title': info['title'],
        'platform': platform,
        'video_url': video_url,
        'audio_channels': file_metadata.get('channels', 'N/A'),
        'file_size': os.path.getsize(file_path)
    }

    # Сравниваем новые метаданные с кэшированными, если они есть
    if cached_metadata:
        differences = compare_metadata(cached_metadata, new_metadata)
        if differences:
            logger.info(f"Обнаружены изменения в метаданных для видео {video_id}: {differences}")
        else:
            logger.info(f"Метаданные для видео {video_id} не изменились")

    # Обновляем метаданные в JSON файле
    update_metadata(video_id, new_metadata, output_dir, config)

    # Проверка наличия всех необходимых метаданных
    required_fields = ['id', 'file_name', 'height', 'width', 'fps', 'duration', 'sample_rate', 'audio_channels', 'file_size', 'video_url', 'title', 'platform']
    if all(new_metadata.get(key) != 'N/A' for key in required_fields):
        logger.info(f"Метаданные успешно извлечены для видео {video_id}")
    else:
        logger.warning(f"Не все метаданные были успешно извлечены для видео {video_id}. Проверьте лог для деталей.")

    return new_metadata
