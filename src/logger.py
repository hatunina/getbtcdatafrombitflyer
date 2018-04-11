from logging import getLogger, StreamHandler, FileHandler, Formatter, INFO


class Logger():

    def __init__(self):
        self.logger = getLogger("Logger")
        self.logger.setLevel(INFO)

        handler_format = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        stream_handler = StreamHandler()
        stream_handler.setLevel(INFO)
        stream_handler.setFormatter(handler_format)

        file_handler = FileHandler('./log/log.log', 'a')
        file_handler.setLevel(INFO)
        file_handler.setFormatter(handler_format)

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)
