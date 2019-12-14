import os
import logging
from pathlib import Path
from datetime import datetime

class Logger():
    def __init__(self, path, foldername, logger):
        self.path = path
        self.logger = logger
        self.folder = foldername
        self.check_if_exists()

        
    def check_if_exists(self):
        final_path = self.path + '/' + self.folder + '/'
        self.file_path = final_path + self.folder + '_{:%Y-%m-%d}.log'.format(datetime.now())
        if not os.path.exists(final_path):
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            Path(self.file_path).touch()

    def getlogger(self):
        logger = logging.getLogger(self.logger)
        logger.setLevel(logging.DEBUG)

        # Add the log message handler to the logger
        handler = logging.FileHandler(self.file_path, mode='a', encoding=None, delay=False)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

        if (logger.hasHandlers()):
            logger.handlers.clear()

        logger.addHandler(handler)
        return logger