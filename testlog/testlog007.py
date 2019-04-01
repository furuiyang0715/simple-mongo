import logging.config

logging.config.fileConfig(fname='testlog007.ini', disable_existing_loggers=False)
logger = logging.getLogger("sampleLogger")
print("logger: ", logger)

logger.info("hello hello hello")

logger2 = logging.getLogger("root")
logger2.info("happy happy happy...")
print("logger2: ", logger2)

logger3 = logging.getLogger(__name__)
print("logger3: ", logger3)
logger3.info("name name name ...")
