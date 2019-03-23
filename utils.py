import argparse


def cmd_parser():
    parser = argparse.ArgumentParser(description='Replicate a MySQL database to MongoDB')

    parser.add_argument('--mysqldump', dest='mysqldump_complete',
                        action='store_true', help="Run mysqldump to import schema and data", default=False)

    parser.add_argument('--randomcheck', dest='ramdom_check',
                        action='store_true', help="random check", default=False)
    return parser


# class LoggerWriter:
#     def __init__(self, logger, level):
#         self.logger = logger
#         self.level = level
#
#     def write(self, message):
#         if message != '\n':
#             self.logger.log(self.level, message)
#
#     def flush(self):
#         return True
