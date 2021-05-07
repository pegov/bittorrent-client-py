# BitTorrent-py

A simple torrent client in Python using asyncio.

## Features
- single file/multiple files download

## Usage
```bash
poetry install
poetry run python -m src [-h] [-v] [-l] [-j] <torrent>
# torrent               - .torrent file
# -v, --verbose         - DEBUG log
# -l, --log-file        - log file path 
# -j, --max-connections - max connections (<=30)
```
## References

- https://www.bittorrent.org/beps/bep_0003.html
- https://wiki.theory.org/BitTorrentSpecification
