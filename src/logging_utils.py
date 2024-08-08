import logging
import os
import multiprocessing
import re

def setup_logging(config, worker=False):
    log_level = logging.INFO
    log_format = '%(asctime)s - %(processName)s - %(levelname)s - %(message)s'

    if worker:
        logger = logging.getLogger(f'worker_{multiprocessing.current_process().name}')
    else:
        logger = logging.getLogger('main')

    logger.setLevel(log_level)

    if not logger.handlers:
        if config['logging']['console_logging']:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(log_format))
            logger.addHandler(console_handler)

        if config['logging']['file_logging']:
            log_dir = config['logging']['log_dir']
            os.makedirs(log_dir, exist_ok=True)
            if worker:
                log_file = os.path.join(log_dir, f'downloader_worker_{multiprocessing.current_process().name}.log')
            else:
                log_file = os.path.join(log_dir, 'downloader.log')
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(logging.Formatter(log_format))
            logger.addHandler(file_handler)

    return logger

def filter_yt_dlp_output(output):
    lines = output.split('\n')
    filtered = []
    for line in lines:
        # Пропускаем строки с предупреждениями
        if "WARNING:" in line:
            continue
        # Оставляем строки с процентами загрузки или информацией о назначении
        if re.search(r'\d+\.\d+%|Destination:', line) and 'player=' not in line:
            filtered.append(line)
        # Добавляем информацию об ошибках, если они есть
        elif "ERROR:" in line:
            filtered.append(line)
    return '\n'.join(filtered)
