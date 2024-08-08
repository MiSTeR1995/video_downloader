import os

def create_file_index(directory, logger):
    file_index = {os.path.splitext(f)[0]: f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))}
    logger.debug(f"Создан индекс файлов для директории {directory}")
    return file_index

def find_existing_file(output_dir, video_id, file_index):
    file_name = file_index.get(video_id)
    if file_name:
        full_path = os.path.join(output_dir, file_name)
        if os.path.exists(full_path):
            return full_path
    return None
