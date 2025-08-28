#!/usr/bin/env python3
"""Movies + magnets indexer for qBittorrent archives.

- Scans movie files under /mnt/vault/movies
- Scans .torrent files under /mnt/vault/metadata
- Emits/updates MOVIES.md
- Commits and pushes if the file changed.

Assumptions:
- Your git remote/auth is already configured.
- Movie library is file- or folder-based; we detect movies via video extensions.
"""

import csv
import hashlib
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import yaml

# ---- Configuration Loading ----


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    """Load configuration from YAML file."""
    config_file = Path(config_path)

    # Try multiple config file locations
    if not config_file.exists():
        # Try in script directory
        script_dir = Path(__file__).parent
        config_file = script_dir / config_path

    if not config_file.exists():
        # Fall back to example config
        example_config = Path(__file__).parent / "config.example.yaml"
        if example_config.exists():
            logging.warning("No config.yaml found, using config.example.yaml")
            config_file = example_config
        else:
            logging.error("No configuration file found!")
            sys.exit(1)

    with open(config_file) as f:
        config = yaml.safe_load(f)

    return config


def setup_logging(config: dict[str, Any]) -> None:
    """Set up logging based on configuration."""
    log_config = config.get("logging", {})
    log_file = log_config.get("log_file")
    log_level = getattr(logging, log_config.get("log_level", "INFO").upper())

    handlers = [logging.StreamHandler()]
    if log_file:
        try:
            # Try to create parent directory if it doesn't exist
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(log_file))
        except (PermissionError, OSError) as e:
            print(f"Warning: Cannot write to log file {log_file}: {e}")
            print("Continuing with console logging only...")

    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


# Load configuration
CONFIG = load_config(os.getenv("CONFIG_FILE", "config.yaml"))
setup_logging(CONFIG)

# Extract configuration values
MOVIES_DIR = Path(CONFIG["sources"]["movies_dir"])
TORRENTS_DIR = Path(CONFIG["sources"]["torrents_dir"])
BACKUP_DIR = Path(CONFIG["output"]["backup_dir"])
OUTPUT_CSV = BACKUP_DIR / CONFIG["output"]["csv_filename"]
OUTPUT_MD = BACKUP_DIR / CONFIG["output"]["markdown_filename"]
SKIP_HIDDEN = CONFIG["options"].get("skip_hidden", True)
IGNORE_DIRS = set(CONFIG["options"].get("ignore_dirs", []))

# ---- Utilities ----


def get_directory_name(name: str) -> str:
    """Return the directory name as-is."""
    return name


def walk_movie_directories(root: Path) -> tuple[list[tuple[Path, str]], datetime]:
    """Return sorted movie directories and most recent modification time."""
    directories = []
    most_recent = datetime.min

    for item in root.iterdir():
        # Only process directories
        if not item.is_dir():
            continue

        # Skip hidden directories if configured
        if SKIP_HIDDEN and item.name.startswith("."):
            continue

        # Skip ignored directories
        if item.name in IGNORE_DIRS:
            continue

        directories.append((item, item.name))

        # Track most recent modification time
        try:
            mtime = datetime.fromtimestamp(item.stat().st_mtime)
            if mtime > most_recent:
                most_recent = mtime
        except OSError:
            # Skip if we can't get modification time
            pass

    # Sort alphabetically by name (case-insensitive)
    directories.sort(key=lambda x: x[1].lower())

    return directories, most_recent


# ---- Minimal bencode decoder (enables capturing raw 'info' slice) ----


class BencodeError(Exception):
    pass


def _bdecode(
    data: bytes, i: int = 0, capture_info: bool = False
) -> tuple[Any, int, Optional[bytes]]:
    """Decode bencode, returning (value, next_index, info_slice_bytes or None)."""
    if i >= len(data):
        raise BencodeError("Unexpected end of data")

    c = data[i : i + 1]
    if c == b"i":  # integer
        j = data.index(b"e", i)
        num = int(data[i + 1 : j])
        return num, j + 1, None
    elif c == b"l":  # list
        i += 1
        lst = []
        info_slice = None
        while data[i : i + 1] != b"e":
            v, i, info_sub = _bdecode(data, i, capture_info=capture_info)
            lst.append(v)
            info_slice = info_slice or info_sub
        return lst, i + 1, info_slice
    elif c == b"d":  # dict
        i += 1
        d = {}
        info_slice = None
        while data[i : i + 1] != b"e":
            # keys are byte strings
            k, i, _ = _bdecode(data, i, capture_info=False)
            if not isinstance(k, (bytes, bytearray)):
                raise BencodeError("Non-bytes dict key")
            k_bytes = bytes(k)
            # Special handling to capture raw 'info' bencoded slice without re-encode
            if capture_info and k_bytes == b"info":
                val_start = i
                v, i, _ = _bdecode(data, i, capture_info=False)
                val_end = i
                d[k_bytes] = v
                info_slice = data[val_start:val_end]
            else:
                v, i, sub_info = _bdecode(data, i, capture_info=capture_info)
                d[k_bytes] = v
                info_slice = info_slice or sub_info
        return d, i + 1, info_slice
    elif b"0" <= c <= b"9":  # byte string
        j = data.index(b":", i)
        length = int(data[i:j])
        start = j + 1
        end = start + length
        return data[start:end], end, None
    else:
        raise BencodeError(f"Invalid bencode prefix at {i}: {c!r}")


def decode_torrent_and_infohash(
    torrent_path: Path,
) -> tuple[dict[bytes, Any], Optional[str], str]:
    """Return dict-like metadata, infohash string (btih or btmh), and display name."""
    raw = torrent_path.read_bytes()
    val, idx, info_slice = _bdecode(raw, 0, capture_info=True)
    if not isinstance(val, dict):
        raise BencodeError("Top-level bencode is not a dict")

    info = val.get(b"info")
    name = None
    if isinstance(info, dict):
        # name could be bytes; prefer 'name' from info dict; fallback to filename
        nm = info.get(b"name")
        if isinstance(nm, (bytes, bytearray)):
            name = nm.decode("utf-8", errors="replace")

    # Compute infohash:
    # v1: SHA1(info bencoded slice) -> btih hex
    # v2: SHA256(info bencoded slice) -> multihash urn:btmh:1220<hex_digest>
    infohash = None
    if info_slice is None:
        # last resort: try whole-file (won't be correct ordering) — skip
        return val, None, name or torrent_path.stem

    # Detect v2
    is_v2 = False
    if isinstance(info, dict):
        try:
            if isinstance(info.get(b"meta version"), int) and info.get(b"meta version") == 2:
                is_v2 = True
        except Exception:
            pass

    if is_v2:
        digest = hashlib.sha256(info_slice).hexdigest()
        # multihash prefix: 0x12 (sha2-256), 0x20 (32 bytes) => "1220" + digest
        infohash = f"btmh:1220{digest}"
    else:
        digest = hashlib.sha1(info_slice).hexdigest()
        infohash = f"btih:{digest}"

    return val, infohash, name or torrent_path.stem


def magnet_from_info(
    torrent_dict: dict[bytes, Any], infohash: str, display_name: str
) -> Optional[str]:
    """Build a magnet link from an infohash tag:
      - btih:<hex>  -> xt=urn:btih:<hex>
      - btmh:1220<hex> -> xt=urn:btmh:1220<hex>
    Append dn=<name> and (optionally) trackers from announce/announce-list.
    """
    if not infohash:
        return None
    if infohash.startswith("btih:"):
        xt = f"urn:btih:{infohash.split(':', 1)[1]}"
    elif infohash.startswith("btmh:"):
        xt = f"urn:btmh:{infohash.split(':', 1)[1]}"
    else:
        return None

    params = [f"xt={quote(xt, safe=':')}"]
    if display_name:
        params.append(f"dn={quote(display_name)}")

    # Trackers (optional)
    ann = torrent_dict.get(b"announce")
    if isinstance(ann, (bytes, bytearray)):
        params.append(f"tr={quote(ann.decode('utf-8', 'ignore'))}")

    ann_list = torrent_dict.get(b"announce-list")
    if isinstance(ann_list, list):
        # flatten potential nested lists
        trackers = []
        for item in ann_list:
            if isinstance(item, list):
                trackers += [x for x in item if isinstance(x, (bytes, bytearray))]
            elif isinstance(item, (bytes, bytearray)):
                trackers.append(item)
        # dedupe
        seen = set()
        for t in trackers:
            s = t.decode("utf-8", "ignore")
            if s and s not in seen:
                params.append(f"tr={quote(s)}")
                seen.add(s)

    return "magnet:?" + "&".join(params)


def torrent_file_list(torrent_dict: dict[bytes, Any]) -> set[str]:
    """Return a set of file basenames contained in the torrent (best-effort)."""
    info = torrent_dict.get(b"info")
    names: set[str] = set()
    if not isinstance(info, dict):
        return names
    # Single-file mode
    if b"length" in info and b"name" in info:
        try:
            nm = info[b"name"].decode("utf-8", "ignore")
            names.add(Path(nm).name)
        except Exception:
            pass
    # Multi-file mode
    if b"files" in info and isinstance(info[b"files"], list):
        for f in info[b"files"]:
            if isinstance(f, dict) and b"path" in f:
                path = f[b"path"]
                # path can be list of path segments per spec
                if isinstance(path, list):
                    segs = []
                    for seg in path:
                        if isinstance(seg, (bytes, bytearray)):
                            segs.append(seg.decode("utf-8", "ignore"))
                    nm = Path(*segs).name if segs else None
                elif isinstance(path, (bytes, bytearray)):
                    nm = Path(path.decode("utf-8", "ignore")).name
                else:
                    nm = None
                if nm:
                    names.add(nm)
    # v2 (file tree) — best-effort grab leaves
    if b"file tree" in info and isinstance(info[b"file tree"], dict):

        def walk(tree: dict[bytes, Any], prefix: str = "") -> None:
            for k, v in tree.items():
                if not isinstance(k, (bytes, bytearray)):
                    continue  # type: ignore[unreachable]
                key = k.decode("utf-8", "ignore")
                if b"" in v:  # file leaf at key path
                    names.add(key)
                elif isinstance(v, dict):
                    walk(v, prefix=str(Path(prefix) / key))

        walk(info[b"file tree"])
    return names


# ---- Build torrent index ----


def load_torrents(torrents_dir: Path) -> list[dict[str, Any]]:
    index = []
    for p in torrents_dir.rglob("*.torrent"):
        try:
            meta, ih, disp = decode_torrent_and_infohash(p)
            magnet = magnet_from_info(meta, ih, disp) if ih else None
            files = torrent_file_list(meta)
            index.append(
                {
                    "path": p,
                    "display_name": disp,
                    "infohash": ih,
                    "magnet": magnet,
                    "files": {f.lower() for f in files},
                }
            )
        except Exception as e:
            # Skip malformed torrents but keep going
            logging.warning(f"Failed to parse {p}: {e}")
    return index


# ---- Match movies to torrents ----


def match_movie_to_torrent(
    movie_dir: Path, torrents: list[dict[str, Any]]
) -> Optional[dict[str, Any]]:
    dir_name = movie_dir.name.lower()

    # 1) Exact directory name match with torrent display name
    for t in torrents:
        torrent_name = (t["display_name"] or "").lower()
        if torrent_name and torrent_name == dir_name:
            return t

    # 2) Directory name contains torrent display name or vice versa
    for t in torrents:
        torrent_name = (t["display_name"] or "").lower()
        if torrent_name and (torrent_name in dir_name or dir_name in torrent_name):
            return t

    # 3) Check if any files in the torrent match directory name pattern
    for t in torrents:
        if any(dir_name in f.lower() for f in t["files"]):
            return t

    # 4) No match
    return None


# ---- Markdown I/O ----


def make_markdown(rows: list[dict[str, Any]], last_updated: datetime) -> str:
    """Create a simple markdown file with movie count and last updated info."""
    movie_count = len(rows)
    last_updated_str = last_updated.strftime("%Y-%m-%d %H:%M:%S")

    header = (
        f"# Movie Library Index\n\n"
        f"**Movies:** {movie_count}\n"
        f"**Last Updated:** {last_updated_str}\n\n"
        f"_Auto-generated movie list. See movies.csv for full details._\n\n"
    )
    lines = [header]

    # Sort by directory name and add to list
    sorted_rows = sorted(rows, key=lambda x: x["title"].lower())
    for row in sorted_rows:
        lines.append(f"- {row['title']}")

    return "\n".join(lines) + "\n"


def make_csv(rows: list[dict[str, Any]], output_path: Path) -> bool:
    """Write movie data to CSV file. Returns True if file changed."""
    # Sort rows for consistent output
    sorted_rows = sorted(rows, key=lambda x: x["title"].lower())

    # Read existing content if file exists
    old_content = ""
    if output_path.exists():
        old_content = output_path.read_text(encoding="utf-8")

    # Write to temporary string first to compare
    import io

    string_buffer = io.StringIO()
    writer = csv.DictWriter(
        string_buffer,
        fieldnames=["title", "directory", "magnet"],
        quoting=csv.QUOTE_MINIMAL,
    )
    writer.writeheader()
    for row in sorted_rows:
        writer.writerow(
            {
                "title": row["title"],
                "directory": row[
                    "file"
                ],  # Keep "file" key for consistency but map to "directory" column
                "magnet": row.get("magnet") or "",
            }
        )

    new_content = string_buffer.getvalue()

    # Only write if content changed
    if old_content != new_content:
        output_path.write_text(new_content, encoding="utf-8")
        return True
    return False


def read_file_text(p: Path) -> str:
    return p.read_text(encoding="utf-8") if p.exists() else ""


def write_if_changed(p: Path, content: str) -> bool:
    old = read_file_text(p)
    if old == content:
        return False
    p.write_text(content, encoding="utf-8")
    return True


# ---- Main ----


def main() -> None:
    """Main function to generate movie indexes."""
    # Validate directories
    if not MOVIES_DIR.exists():
        logging.error(f"Movies directory not found: {MOVIES_DIR}")
        sys.exit(1)
    if not TORRENTS_DIR.exists():
        logging.error(f"Torrents directory not found: {TORRENTS_DIR}")
        sys.exit(1)

    # Create backup directory if it doesn't exist
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    logging.info(f"Using backup directory: {BACKUP_DIR}")

    # Load torrents
    torrents = load_torrents(TORRENTS_DIR)
    torrent_count = len(torrents)
    logging.info(f"Loaded {torrent_count} torrent(s) from {TORRENTS_DIR}")

    # Get movie directories and most recent modification time
    movie_dirs, last_updated = walk_movie_directories(MOVIES_DIR)

    rows = []
    for movie_dir, name in movie_dirs:
        match = match_movie_to_torrent(movie_dir, torrents)
        magnet = match["magnet"] if match else None
        rows.append(
            {
                "title": name,
                "file": str(movie_dir),
                "magnet": magnet,
            }
        )

    # Generate outputs
    csv_changed = make_csv(rows, OUTPUT_CSV)
    md = make_markdown(rows, last_updated)
    md_changed = write_if_changed(OUTPUT_MD, md)

    # Report results
    if csv_changed:
        logging.info(f"Updated {OUTPUT_CSV} ({len(rows)} entries)")
    if md_changed:
        logging.info(f"Updated {OUTPUT_MD} ({len(rows)} entries)")

    if not csv_changed and not md_changed:
        logging.info("No changes detected in movie library")

    # Exit with appropriate code for shell script
    sys.exit(0 if (csv_changed or md_changed) else 1)


if __name__ == "__main__":
    main()
