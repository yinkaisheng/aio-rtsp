import asyncio

from aio_rtsp.server import serve


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="Directory to publish recursively")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=554, help="Bind port")
    args = parser.parse_args()
    asyncio.run(serve(args.dir, args.host, args.port))


if __name__ == "__main__":
    main()
