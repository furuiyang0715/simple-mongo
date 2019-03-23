import utils
import sys
import logging.config


logging.config.fileConfig('conf/logging.conf')
logger = logging.getLogger('main')


if __name__ == '__main__':
    logger.info("  ")
    logger.info("  ")

    parser = utils.cmd_parser()

    args = parser.parse_args()

    if args.dump:
        try:
            pass   # 开启几个守护进程 运行相应格式的 rundemo 后期只开一个就好了
            pass   # 运行一个 check
        except Exception as e:
            logger.error(f'开启同步数据失败, 原因是 {e}')
            sys.exit(1)
    elif args.check:
        try:
            pass   # 只运行一个 check
        except Exception as e:
            logger.error(f'开启随机检验失败，原因是 {e}')
            sys.exit(1)
    sys.exit(0)
