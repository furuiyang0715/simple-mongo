import logging
logging.basicConfig(
    filename='testlog002.log',
    filemode='w',
    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
    datefmt="%d-%M-%Y %H:%M:%S",
    level=logging.DEBUG
)
logging.debug("This is a debug message.")
logging.info("This is a info message.")
logging.warning("This is a warning message.")
logging.error("This is a error message.")
logging.critical("This is a critical message.")
