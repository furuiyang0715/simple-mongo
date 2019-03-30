import logging
import logging.handlers

logger = logging.getLogger("logger")
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.debug('This is a customer debug message')
logging.info('This is an customer info message')
# 创建了自定义的Logger对象，就不要再使用logging中的日志输出方法了
# 这些方法使用的是默认配置的 Logger 对象，否则会输出的日志信息会重复
logger.warning('This is a customer warning message')
logger.error('This is an customer error message')
logger.critical('This is a customer critical message')
