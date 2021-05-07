import argparse
import asyncio
import logging
import os

from .client import Client
from .torrent import Torrent


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="debug log", action="store_true")
    parser.add_argument("-l", "--log-file", help="log file")
    parser.add_argument(
        "-j", "--max-connections", type=int, help="max connections", default=10
    )
    parser.add_argument("torrent", help="torrent file")

    args = parser.parse_args()

    if args.torrent is None:
        raise Exception(".torrent file is not specified")

    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode="a",
            level=level,
            format="%(levelname)s:%(filename)s:%(lineno)s:%(message)s",
        )
    else:
        logging.basicConfig(
            level=level, format="%(levelname)s:%(filename)s:%(lineno)s:%(message)s"
        )

    if not os.path.exists(args.torrent):
        raise Exception("Can't find .torrent file")

    if args.max_connections > 30:
        args.max_connections = 30

    torrent = Torrent(args.torrent)
    client = Client(torrent, args.max_connections)

    loop = asyncio.get_event_loop()
    loop.set_debug(args.verbose)
    task = loop.create_task(client.start())
    loop.run_until_complete(task)
