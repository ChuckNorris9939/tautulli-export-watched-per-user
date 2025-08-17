# tautulli-export-watched-per-user
Export watched **TV shows & movies** from Tautulli for a given user — including **per-play percent**, **available episodes per show**, CSV/JSON output, and **console progress logging** with total runtime.

---

# Tautulli Watched Exporter

Export the watched history of a Tautulli user to CSV/JSON with:
- **Series progress**: unique episodes watched vs. **available episodes** → `% watched`
- **Per-play percent** (`percent_complete` or `view_offset / duration`) for accuracy
- **Console logging** (step-by-step progress) + **total runtime** summary

## Requirements
- Python 3.8+
- `pip install requests`
- Tautulli base URL + API key

## Usage
```bash
python3 tautulli_export_watched.py \
  --url https://TAUTULLI_HOST \
  --apikey YOUR_API_KEY \
  --user USERNAME
# Options:
#   --export {series,movies,both}   (default: both)
#   --watched-threshold 85          # % to count as "watched"
#   --out-series series.csv         # default: watched_series_<user>.csv
#   --out-movies movies.csv         # default: watched_movies_<user>.csv
#   --json export.json              # combined JSON
#   --log-level INFO|DEBUG|WARNING  # default: INFO
