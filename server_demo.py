#!/usr/bin/env python3

import asyncio

import aio_sockets as aio
import aio_rtsp_toolkit as aiortsp
import aio_rtsp_toolkit.server as server
from log_util import logger, config_logger


aio.aio_sockets.logger = logger
server.logger = logger


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dir", required=True, help="Directory to publish recursively")
    parser.add_argument("-H", "--host", default="0.0.0.0", help="Bind host[default 0.0.0.0]")
    parser.add_argument("-p", "--port", type=int, default=8554, help="Bind port[default 8554]")
    parser.add_argument("-cp", "--control-port", type=int, default=8553, help="Control port[default 8553]")
    parser.add_argument("-ld", "--log-dir", default=None, metavar="DIR",
        help="Write logs under DIR/rtsp_server.log; omit for stdout only")
    args = parser.parse_args()

    if args.log_dir:
        config_logger(logger, "info", log_dir=args.log_dir, log_file="rtsp_server.log")
    else:
        config_logger(logger, "info")

    asyncio.run(server.serve(args.dir, args.host, args.port, control_port=args.control_port))


if __name__ == "__main__":
    main()
