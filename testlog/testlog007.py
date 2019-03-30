import logging.config

logging.config.fileConfig(fname='testlog007.ini', disable_existing_loggers=False)
logger = logging.getLogger("sampleLogger")

logger.info("hello hello hello")
