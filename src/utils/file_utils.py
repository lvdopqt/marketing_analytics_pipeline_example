import os
import logging

def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        logging.info(f"Directory created: {path}")