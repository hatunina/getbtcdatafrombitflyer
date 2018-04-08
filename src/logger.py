import logging
from logging import getLogger, StreamHandler, FileHandler, Formatter


class Logger():

    def __init__(self):
        self.logger = getLogger("Logger")
        self.logger.setLevel(logging.INFO)

        handler_format = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        stream_handler = StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(handler_format)

        file_handler = FileHandler('./log/log.log', 'a')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(handler_format)

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)
