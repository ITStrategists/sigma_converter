import logging
import os

class Logger:
    def __init__(self):
        pass
        

    def getLogger(self):
        logger = logging
        baseDir = '../logs'
        os.makedirs(baseDir, exist_ok=True)
        logger.basicConfig(filename='{baseDir}/converter.log'.format(baseDir = baseDir),level=logging.INFO, filemode='a', format = '%(asctime)s:%(levelname)s:%(message)s')
        return logger


