#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: yinkaisheng@foxmail.com
import os
import sys
import time
import io
import inspect
import threading
from datetime import datetime
from typing import Any

IsWindows = sys.platform == 'win32'
IsPy38OrHigher = sys.version_info >= (3, 8)

if IsPy38OrHigher:
    current_thread_id = threading.get_native_id
else:
    current_thread_id = threading.get_ident

try:
    import colorama
    colorama.init(autoreset=True)

    class Fore:
        Black = colorama.Fore.LIGHTBLACK_EX
        Blue = colorama.Fore.LIGHTBLUE_EX
        Cyan = colorama.Fore.LIGHTCYAN_EX
        Green = colorama.Fore.LIGHTGREEN_EX
        Magenta = colorama.Fore.LIGHTMAGENTA_EX
        Red = colorama.Fore.LIGHTRED_EX
        White = colorama.Fore.LIGHTWHITE_EX
        Yellow = colorama.Fore.LIGHTYELLOW_EX

        DarkBlack = colorama.Fore.BLACK
        DarkBlue = colorama.Fore.BLUE
        DarkCyan = colorama.Fore.CYAN
        DarkGreen = colorama.Fore.GREEN
        DarkMagenta = colorama.Fore.MAGENTA
        DarkRed = colorama.Fore.RED
        DarkWhite = colorama.Fore.WHITE
        DarkYellow = colorama.Fore.YELLOW

        Reset = colorama.Fore.RESET

    class Back:
        Black = colorama.Back.LIGHTBLACK_EX
        Blue = colorama.Back.LIGHTBLUE_EX
        Cyan = colorama.Back.LIGHTCYAN_EX
        Green = colorama.Back.LIGHTGREEN_EX
        Magenta = colorama.Back.LIGHTMAGENTA_EX
        Red = colorama.Back.LIGHTRED_EX
        White = colorama.Back.LIGHTWHITE_EX
        Yellow = colorama.Back.LIGHTYELLOW_EX

        DarkBlack = colorama.Back.BLACK
        DarkBlue = colorama.Back.BLUE
        DarkCyan = colorama.Back.CYAN
        DarkGreen = colorama.Back.GREEN
        DarkMagenta = colorama.Back.MAGENTA
        DarkRed = colorama.Back.RED
        DarkWhite = colorama.Back.WHITE
        DarkYellow = colorama.Back.YELLOW

        Reset = colorama.Back.RESET

    class Style:
        Bright = colorama.Style.BRIGHT
        Dim = colorama.Style.DIM
        Normal = colorama.Style.NORMAL
        Reset = colorama.Style.RESET_ALL

except Exception:
    # Windows 7 console doesn't support colors
    if sys.platform == 'win32': # works on Windows 10
        import ctypes
        ctypes.windll.kernel32.SetConsoleMode(ctypes.windll.kernel32.GetStdHandle(-11), 7)

    class Fore:
        Black = "\033[90m"
        Red = "\033[91m"
        Green = "\033[92m"
        Yellow = "\033[93m"
        Blue = "\033[94m"
        Magenta = "\033[95m"
        Cyan = "\033[96m"
        White = "\033[97m"

        DarkBlack = "\033[30m"
        DarkRed = "\033[31m"
        DarkGreen = "\033[32m"
        DarkYellow = "\033[33m"
        DarkBlue = "\033[34m"
        DarkMagenta = "\033[35m"
        DarkCyan = "\033[36m"
        DarkWhite = "\033[37m"

        Reset = "\033[39m"

    class Back:
        Black = "\033[100m"
        Red = "\033[101m"
        Green = "\033[102m"
        Yellow = "\033[103m"
        Blue = "\033[104m"
        Magenta = "\033[105m"
        Cyan = "\033[106m"
        White = "\033[107m"

        DarkBlack = "\033[40m"
        DarkRed = "\033[41m"
        DarkGreen = "\033[42m"
        DarkYellow = "\033[43m"
        DarkBlue = "\033[44m"
        DarkMagenta = "\033[45m"
        DarkCyan = "\033[46m"
        DarkWhite = "\033[47m"

        Reset = "\033[49m"

    class Style:
        Bright = ''
        Dim = ''
        Normal = ''
        Reset = ''


try:
    # raise ImportError('use logging first')

    from loguru import logger

    def config_logger(logger, log_level = 'info', log_dir = 'logs', log_file = 'app.log',
                      backup_count = 15, log_to_stdout = True):
        def add_thread_native_id(record):
            record['extra']['thread'] = threading.get_native_id

        logger.configure(patcher=add_thread_native_id)
        if log_dir and log_dir != '.':
            os.makedirs(log_dir, exist_ok=True)
        log_level = log_level.upper()
        logger.remove()   # remove the default sink of sys.stdout
        '''
        record example:
        {
            'elapsed': datetime.timedelta(microseconds=30002),
            'exception': None,
            'extra': {},
            'file': (name='log_util.py', path='E:\\codes\\python\\automation\\log_util.py'),
            'function': 'log_test',
            'level': (name='CRITICAL', no=50, icon='☠️'),
            'line': 123,
            'message': 'hello world',
            'module': 'log_util',
            'name': '__main__',
            'process': (id=23244, name='MainProcess'),
            'thread': (id=25388, name='MainThread'),
            'time': datetime(2025, 8, 26, 10, 45, 45, 783528, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '中国标准时间'))
        }
        '''
        file_format = '{time:YYYY-MM-DD HH:mm:ss.SSS} {level} T{thread} L{line} {function}: {message}'
        if log_to_stdout:
            console_format = ('<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> <lvl>{level}</lvl> '
                '{file},{line} <light-blue>T{thread}</light-blue> <light-cyan>{function}</light-cyan>'
                ': <lvl>{message}</lvl>')
            _stdout_logger_id = logger.add(sys.stdout, level=log_level, colorize=True, format=console_format)
        logger.add(f'{log_dir}/{log_file}', level=log_level, enqueue=True, rotation=f'00:00:00',
                   retention=backup_count, compression='zip', format=file_format)

except ImportError:

    import zipfile
    import logging
    import logging.handlers

    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s %(levelname)s %(filename)s,%(lineno)d {Fore.Blue}T%(thread)d{Fore.Reset} {Fore.Cyan}%(funcName)s{Fore.Reset}: %(message)s',
        # format=f'%(asctime)s %(levelname)s %(filename)s,%(lineno)d T%(thread)d %(funcName)s: %(message)s', # no color
    )
    logger = logging.getLogger()


    class LogFormatter(logging.Formatter):
        default_time_format = '%Y-%m-%d %H:%M:%S'

        def __init__(self, fmt=None, datefmt=None, style='%', validate=True):
            super(LogFormatter, self).__init__(fmt, datefmt, style, validate)


    class ZipTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
        def doRollover(self):
            if self.stream:
                self.stream.close()
                self.stream = None

            last_rollover_time_tuple = time.localtime(self.rolloverAt - self.interval)
            old_log_suffix = time.strftime(self.suffix, last_rollover_time_tuple)
            old_log_path = f"{self.baseFilename}.{old_log_suffix}"

            super().doRollover()

            if os.path.exists(old_log_path):
                zip_filename = f"{old_log_path}.zip"
                try:
                    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zf:
                        zf.write(old_log_path, os.path.basename(old_log_path))
                    print(f"compress: {old_log_path} -> {zip_filename}")
                    os.remove(old_log_path)
                    print(f"remove: {old_log_path}")
                except Exception as ex:
                    print(f"compress failed: {old_log_path}: {ex!r}")
            else:
                print(f"can not find: {old_log_path}")


    def config_logger(logger: logging.Logger, log_level = 'info', log_dir = 'logs', log_file = 'app.log',
                      backup_count = 15, log_to_stdout = True):
        if log_dir and log_dir != '.':
            os.makedirs(log_dir, exist_ok=True)
        log_level = logging._nameToLevel[log_level.upper()]
        logging.Formatter.default_msec_format = '%s.%03d'
        file_formatter = LogFormatter('%(asctime)s %(levelname)s T%(thread)d L%(lineno)d %(funcName)s: %(message)s')
        logger.setLevel(log_level)
        file_handler = ZipTimedRotatingFileHandler(
            f'{log_dir}/{log_file}',
            when="midnight",    # midnight
            interval=1,         # every day
            backupCount=backup_count
        )
        file_handler.setFormatter(file_formatter)
        if log_to_stdout:
            pass
        else:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
        logger.addHandler(file_handler)


if sys.stdout:
    FileColor = Fore.DarkGreen
    FunctionColor = Fore.DarkCyan
    TypeColor = Fore.Cyan
    ResetColor = Fore.Reset
else:
    FileColor = ''
    FunctionColor = ''
    TypeColor = ''
    ResetColor = ''


def remove_color_of_shell_text(stdout: str) -> str:
    start = stdout.find('\x1b[')
    if start == -1:
        return stdout
    result_parts = [stdout[:start]]
    text_len = len(stdout)
    while True:
        color_pos = stdout.find('\x1b[', start) # \x1b[XXm or \x1b[XXXm
        if color_pos == -1:
            result_parts.append(stdout[start:])
            break
        mpos = -1
        if color_pos + 4 < text_len and stdout[color_pos + 4] == 'm':   # \x1b[XXm
            mpos = color_pos + 4
        elif color_pos + 5 < text_len and stdout[color_pos + 5] == 'm': # \x1b[XXXm
            mpos = color_pos + 5
        elif color_pos + 6 < text_len and stdout[color_pos + 6] == 'm': # \x1b[XXXXm
            mpos = color_pos + 6
        if mpos > 0:
            result_parts.append(stdout[start:color_pos])
            start = mpos + 1
        else:
            start = color_pos + 2
            result_parts.append(stdout[color_pos:start])
    return ''.join(result_parts)


def printx(*values, prefix: Any = '', print_id: bool = False, sep: str = ' ', end: str = None, caller: bool = True, flush: bool = False) -> None:
    now = datetime.now()
    if caller:
        frame = sys._getframe(1)
        file_name = os.path.basename(frame.f_code.co_filename)
        self_obj = frame.f_locals.get('self', None)
        if self_obj:
            timestr = f'{now.year}-{now.month:02}-{now.day:02} {now.hour:02}:{now.minute:02}:{now.second:02}.{now.microsecond // 1000:03}' \
                f' T{current_thread_id()} {FileColor}{file_name},{frame.f_lineno}{ResetColor} {FunctionColor}{self_obj.__class__.__name__}.{frame.f_code.co_name}{ResetColor}:{prefix}'
        else:
            timestr = f'{now.year}-{now.month:02}-{now.day:02} {now.hour:02}:{now.minute:02}:{now.second:02}.{now.microsecond // 1000:03}' \
                f' T{current_thread_id()} {FileColor}{file_name},{frame.f_lineno}{ResetColor} {FunctionColor}{frame.f_code.co_name}{ResetColor}:{prefix}'
    else:
        timestr = f'{now.year}-{now.month:02}-{now.day:02} {now.hour:02}:{now.minute:02}:{now.second:02}.{now.microsecond // 1000:03}:{prefix}'

    caller_frame = inspect.currentframe().f_back
    caller_locals = caller_frame.f_locals

    output_parts = []
    available_vars = []
    for name, var_value in caller_locals.items():
        if not name.startswith('__') and name != 'printx':
            available_vars.append((var_value, name))

    for val_to_find in values:
        found_name = None
        match_index = -1

        for j, (available_val, available_name) in enumerate(available_vars):
            if val_to_find is available_val: # Same memory address, usually means it's the same object
                found_name = available_name
                match_index = j
                break

        if found_name:
            if print_id:
                output_parts.append(f"{Fore.Cyan}{found_name}{Fore.Reset}={Fore.Green}{type(val_to_find)}{Fore.Reset},id={id(val_to_find)},{val_to_find!r}")
            else:
                output_parts.append(f"{Fore.Cyan}{found_name}{Fore.Reset}={Fore.Green}{type(val_to_find)}{Fore.Reset},{val_to_find!r}")
            if match_index != -1:
                del available_vars[match_index]
        else:
            output_parts.append(f"<{val_to_find!r}>")

    print(timestr, "\n  ".join(output_parts), sep=sep, end=end, flush=flush)


def log(msg: Any = '', sep: str = ' ', end: str = None, caller: bool = True, flush: bool = False, file: io.FileIO = None) -> None:
    '''console log'''
    now = datetime.now()
    if caller:
        frame = sys._getframe(1)
        file_name = os.path.basename(frame.f_code.co_filename)
        self_obj = frame.f_locals.get('self', None)
        if self_obj:
            timestr = f'{now.year}-{now.month:02}-{now.day:02} {now.hour:02}:{now.minute:02}:{now.second:02}.{now.microsecond // 1000:03}' \
                f' T{current_thread_id()} {FileColor}{file_name},{frame.f_lineno}{ResetColor} {FunctionColor}{self_obj.__class__.__name__}.{frame.f_code.co_name}{ResetColor}:'
        else:
            timestr = f'{now.year}-{now.month:02}-{now.day:02} {now.hour:02}:{now.minute:02}:{now.second:02}.{now.microsecond // 1000:03}' \
                f' T{current_thread_id()} {FileColor}{file_name},{frame.f_lineno}{ResetColor} {FunctionColor}{frame.f_code.co_name}{ResetColor}:'
    else:
        timestr = f'{now.year}-{now.month:02}-{now.day:02} {now.hour:02}:{now.minute:02}:{now.second:02}.{now.microsecond // 1000:03}:'
    print(timestr, msg, sep=sep, end=end, flush=flush, file=file)


def set_console_title(title: str) -> None:
    # need colorama.init
    if sys.stdout:
        sys.stdout.write(f'\x1b]2;{title}\x07')


if __name__ == '__main__':
    def log_test():
        logger.info('hello world')
        logger.warning('hello world')
        logger.error('hello world')
        logger.critical('hello world')

    config_logger(logger, log_to_stdout=True)
    logger.debug('hello world')
    log_test()