# 一个系统只有一个 Logger 对象，并且该对象不能被直接实例化，
# 没错，这里用到了单例模式，获取 Logger 对象的方法为 getLogger。


# 注意：这里的单例模式并不是说只有一个 Logger 对象，而是指整个系统只有一个根 Logger 对象，
# Logger 对象在执行 info()、error() 等方法时实际上调用都是根 Logger 对象对应的 info()、error() 等方法。


# 我们可以创造多个 Logger 对象，但是真正输出日志的是根 Logger 对象。
# 每个 Logger 对象都可以设置一个名字，如果设置logger = logging.getLogger(__name__)
# __name__ 是 Python 中的一个特殊内置变量，他代表当前模块的名称（默认为 __main__)
# 则 Logger 对象的 name 为建议使用使用以点号作为分隔符的命名空间等级制度。

# Logger 对象可以设置多个 Handler 对象和 Filter 对象，Handler 对象又可以设置 Formatter 对象。


import logging
import logging.handlers

logger = logging.getLogger("logger")

handler1 = logging.StreamHandler()
handler2 = logging.FileHandler(filename="testlog004.log")

logger.setLevel(logging.DEBUG)
handler1.setLevel(logging.WARNING)
handler2.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
handler1.setFormatter(formatter)
handler2.setFormatter(formatter)

logger.addHandler(handler1)
logger.addHandler(handler2)

# 分别为 10、30、30
# print(handler1.level)
# print(handler2.level)
# print(logger.level)

logger.debug('This is a customer debug message')
logger.info('This is an customer info message')
logger.warning('This is a customer warning message')
logger.error('This is an customer error message')
logger.critical('This is a customer critical message')
