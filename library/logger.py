class Log4j(object):
    def __init__(self, session):
        log4j = session._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("newsproject")

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)