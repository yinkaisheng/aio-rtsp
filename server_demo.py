#!/usr/bin/env python3

import asyncio

import aio_sockets as aio
import aio_rtsp_toolkit as aiortsp
from log_util import logger, config_logger


aiortsp.server.logger = logger


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="Directory to publish recursively")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8554, help="Bind port")
    args = parser.parse_args()

    use_file_logger = 1
    if use_file_logger:
        config_logger(logger, 'info', log_dir='logs', log_file='rtsp_server.log')
    else:
        config_logger(logger, 'info')

    asyncio.run(aiortsp.serve(args.dir, args.host, args.port))


if __name__ == "__main__":
    main()
