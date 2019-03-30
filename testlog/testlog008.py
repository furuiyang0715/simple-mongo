import logging.config
# 需要安装 pyymal 库
import yaml

with open('testlog008.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger("sampleLogger")
# 省略日志输出
logger.info("test test test")
