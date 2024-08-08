import os
from src import read_config, download_videos, setup_logging

if __name__ == "__main__":
    # Получаем абсолютный путь к директории, где находится main.py
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Формируем полный путь к файлу конфигурации
    config_path = os.path.join(base_dir, 'config.yaml')

    # Читаем конфигурацию
    config = read_config(config_path)

    # Настраиваем логирование
    logger = setup_logging(config)

    # Запускаем процесс загрузки видео
    try:
        download_videos(config)
    except KeyboardInterrupt:
        logger.info("Программа была прервана пользователем.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
