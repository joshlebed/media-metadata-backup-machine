# Media Metadata Backup Machine

Index your movie collection by matching files with torrent metadata to generate searchable CSV and Markdown indexes.

## Quick Start

1. **Install UV and dependencies:**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

2. **Configure paths:**

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your paths
```

3. **Run:**

```bash
# Generate indexes only
uv run update_movies_index.py

# Generate indexes and commit to git
./update-and-commit.sh
```

## Configuration

Edit `config.yaml` to set your paths:

```yaml
sources:
  movies_dir: "/path/to/your/movies"
  torrents_dir: "/path/to/your/torrents"

output:
  backup_dir: "/path/to/backup/directory"
  csv_filename: "movies.csv"
  markdown_filename: "MOVIES.md"
```

## Automated Updates

Add to crontab for daily updates. Run `crontab -e` and add the following line:

```bash
# Run daily at 3 AM
0 3 * * * cd /path/to/media-metadata-backup-machine && ./update-and-commit.sh
```

## Output

- **movies.csv** - Complete data (titles, file paths, magnet links)
- **MOVIES.md** - Human-readable list

## Development

```bash
# Format and lint
uv run ruff format .
uv run ruff check .

# Type check
uv run mypy .
```
