#!/usr/bin/env python3
import argparse, csv, json, sys, time, logging
from typing import Dict, Any, List, Optional
import requests

# ---------------------------- Logging -----------------------------------------
def setup_logging(level: str = "INFO"):
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )

def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

# ---------------------------- API Helper --------------------------------------
_session = requests.Session()

def call_api(base_url: str, apikey: str, cmd: str, **params) -> Any:
    url = base_url.rstrip("/") + "/api/v2"
    payload = {"apikey": apikey, "cmd": cmd}
    payload.update({k: v for k, v in params.items() if v is not None})
    r = _session.get(url, params=payload, timeout=30)
    r.raise_for_status()
    js = r.json()
    if js.get("response", {}).get("result") != "success":
        raise RuntimeError(f"API result != success for {cmd}: {js}")
    return js["response"]["data"]

def resolve_user_id(base_url: str, apikey: str, name: str) -> int:
    name_lower = name.lower()
    logging.info("Löse user_id für '%s'…", name)
    # 1) get_users (bevorzugt)
    try:
        users = call_api(base_url, apikey, "get_users")
        for u in users:
            if str(u.get("username", "")).lower() == name_lower or str(u.get("friendly_name", "")).lower() == name_lower:
                uid = int(u["user_id"])
                logging.info("→ user_id gefunden: %s", uid)
                return uid
    except Exception:
        pass
    # 2) get_user_names (Fallback)
    users2 = call_api(base_url, apikey, "get_user_names")
    for u in users2:
        if str(u.get("friendly_name", "")).lower() == name_lower:
            uid = int(u["user_id"])
            logging.info("→ user_id gefunden: %s", uid)
            return uid
    raise KeyError(f'User "{name}" wurde nicht gefunden.')

# ----------------------- History (paginiert) ----------------------------------
def fetch_history(base_url: str, apikey: str, user_id: int, media_type: str) -> List[Dict[str, Any]]:
    assert media_type in ("episode", "movie")
    results: List[Dict[str, Any]] = []
    start = 0
    page = 1000
    logging.info("Lade %s-History…", "Episoden" if media_type == "episode" else "Film")
    while True:
        data = call_api(
            base_url, apikey, "get_history",
            user_id=user_id, media_type=media_type,
            start=start, length=page, order_column="date", order_dir="asc",
        )
        rows = data["data"] if isinstance(data, dict) and "data" in data else data
        if not rows:
            break
        results.extend(rows)
        start += len(rows)
        logging.debug("…geladen: %d Einträge", len(results))
        if len(rows) < page:
            break
    logging.info("→ %d %s-History-Einträge", len(results), "Episoden" if media_type == "episode" else "Film")
    return results

# ----------------------------- Utils ------------------------------------------
def _ts_readable(ts: Any) -> str:
    try:
        if isinstance(ts, (int, float)):
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
        return str(ts) if ts else ""
    except Exception:
        return ""

def _percent_from_row(r: Dict[str, Any]) -> Optional[float]:
    pc = r.get("percent_complete")
    if pc is not None:
        try:
            return float(pc)
        except Exception:
            pass
    off = r.get("view_offset")
    dur = r.get("duration") or r.get("media_duration")
    try:
        off = float(off)
        dur = float(dur)
        if dur > 0:
            # ms -> s Heuristik
            if off > dur * 5:
                off /= 1000.0
            if dur > 100000:
                dur /= 1000.0
            return max(0.0, min(100.0, (off / dur) * 100.0))
    except Exception:
        pass
    return None

# ---- Verfügbare Episoden pro Serie (Show-Rating-Key) -------------------------
def count_available_episodes(base_url: str, apikey: str, show_rating_key: str) -> int:
    """
    Fast Path: get_metadata -> leaf_count
    Fallback:  get_children_metadata(show)->Seasons -> je Season get_children_metadata(season)->children_count
    """
    # Fast Path
    try:
        md = call_api(base_url, apikey, "get_metadata", rating_key=show_rating_key)
        if isinstance(md, dict):
            for k in ("leaf_count", "leafCount", "episode_count"):
                if k in md and md[k] is not None:
                    return int(md[k])
    except Exception:
        pass
    # Fallback
    total_eps = 0
    try:
        seasons = call_api(base_url, apikey, "get_children_metadata",
                           rating_key=show_rating_key, media_type="show")
        season_list = []
        if isinstance(seasons, dict):
            season_list = seasons.get("children_list") or []
        else:
            season_list = seasons or []
        for s in season_list:
            season_key = s.get("rating_key") or s.get("ratingKey")
            if not season_key:
                continue
            eps = call_api(base_url, apikey, "get_children_metadata",
                           rating_key=season_key, media_type="season")
            if isinstance(eps, dict):
                cnt = eps.get("children_count")
                if cnt is None:
                    cnt = len(eps.get("children_list") or [])
            else:
                cnt = len(eps or [])
            total_eps += int(cnt or 0)
    except Exception:
        return total_eps
    return total_eps

# ---------------------- Aggregation: Serien -----------------------------------
def aggregate_series(rows: List[Dict[str, Any]], watched_threshold: float = 85.0) -> List[Dict[str, Any]]:
    """
    Aggregiert Plays zu Serien-Zeilen (ohne available_episodes; das macht compute_available_after).
    """
    series: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        show_key = str(r.get("grandparent_rating_key") or "")
        show_title = r.get("grandparent_title") or r.get("full_title") or r.get("title") or "Unbekannt"
        ep_key = str(r.get("rating_key") or "")
        tsr = _ts_readable(r.get("date") or r.get("stopped") or r.get("started") or r.get("last_played"))
        pct = _percent_from_row(r)
        is_watched = (pct is None or pct >= watched_threshold)

        bucket = series.setdefault(show_key or show_title, {
            "show_title": show_title,
            "show_rating_key": show_key,
            "unique_episodes_watched": 0,
            "episodes_partial": 0,
            "avg_episode_percent": 0.0,
            "first_watched": tsr,
            "last_watched": tsr,
            "_plays_count": 0,
            "_seen_watched": set(),
            "_seen_partial": set(),
        })

        # Durchschnitt (über Plays)
        if pct is not None:
            bucket["avg_episode_percent"] = (
                (bucket["avg_episode_percent"] * bucket["_plays_count"] + pct) / (bucket["_plays_count"] + 1)
            )
        bucket["_plays_count"] += 1

        # Unique Episoden (nach Schwelle)
        if ep_key:
            if is_watched:
                bucket["_seen_watched"].add(ep_key)
            else:
                # nur als partial zählen, wenn nicht bereits "voll" gesehen
                if ep_key not in bucket["_seen_watched"]:
                    bucket["_seen_partial"].add(ep_key)

        # Zeit
        if tsr:
            if not bucket["first_watched"] or tsr < bucket["first_watched"]:
                bucket["first_watched"] = tsr
            if not bucket["last_watched"] or tsr > bucket["last_watched"]:
                bucket["last_watched"] = tsr

    # finalize
    out: List[Dict[str, Any]] = []
    for b in series.values():
        b["unique_episodes_watched"] = len(b["_seen_watched"])
        b["episodes_partial"] = len(b["_seen_partial"])
        b["avg_episode_percent"] = round(b["avg_episode_percent"], 2)
        # Platzhalter; wird später gefüllt
        b["available_episodes"] = 0
        b["percent_watched_show"] = ""
        # Cleanup
        b.pop("_plays_count", None); b.pop("_seen_watched", None); b.pop("_seen_partial", None)
        out.append(b)

    out.sort(key=lambda x: (x["show_title"] or "").lower())
    return out

def compute_available_after(base_url: str, apikey: str, series_rows: List[Dict[str, Any]]):
    n = len(series_rows)
    if n == 0:
        return
    logging.info("Ermittle verfügbare Episoden je Serie…")
    for i, row in enumerate(series_rows, 1):
        show_key = row.get("show_rating_key") or ""
        if show_key:
            try:
                avail = count_available_episodes(base_url, apikey, show_key)
            except Exception as e:
                logging.warning("  [%d/%d] %s – Fehler: %s", i, n, row.get("show_title"), e)
                avail = 0
        else:
            avail = 0
        row["available_episodes"] = int(avail or 0)
        if row["available_episodes"] > 0:
            row["percent_watched_show"] = round(
                (row["unique_episodes_watched"] / row["available_episodes"]) * 100.0, 2
            )
        else:
            row["percent_watched_show"] = ""
        logging.info("  [Serie %d/%d] %s: available=%s, watched=%s",
                     i, n, row.get("show_title"),
                     row["available_episodes"], row["unique_episodes_watched"])

# ---------------------- Aggregation: Filme ------------------------------------
def aggregate_movies(rows: List[Dict[str, Any]], watched_threshold: float = 85.0) -> List[Dict[str, Any]]:
    movies: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = str(r.get("rating_key") or r.get("parent_rating_key") or "")
        title = r.get("title") or r.get("full_title") or "Unbekannt"
        year = r.get("year") or ""
        tsr = _ts_readable(r.get("date") or r.get("stopped") or r.get("started") or r.get("last_played"))
        pct = _percent_from_row(r)
        watched = (pct is None or pct >= watched_threshold)

        bucket = movies.setdefault(key or f"{title} ({year})", {
            "movie_title": title, "year": year, "plays": 0,
            "max_percent": 0.0, "avg_percent": 0.0, "last_percent": None,
            "completed_any": False, "first_watched": tsr, "last_watched": tsr,
        })
        bucket["plays"] += 1
        if pct is not None:
            bucket["max_percent"] = max(bucket["max_percent"], float(pct))
            bucket["avg_percent"] = ((bucket["avg_percent"] * (bucket["plays"] - 1)) + float(pct)) / bucket["plays"]
            bucket["last_percent"] = float(pct)
        if watched:
            bucket["completed_any"] = True
        if tsr:
            if not bucket["first_watched"] or tsr < bucket["first_watched"]:
                bucket["first_watched"] = tsr
            if not bucket["last_watched"] or tsr > bucket["last_watched"]:
                bucket["last_watched"] = tsr

    out = list(movies.values())
    for b in out:
        b["avg_percent"] = round(b["avg_percent"], 2)
        if b["last_percent"] is not None:
            b["last_percent"] = round(b["last_percent"], 2)
        b["max_percent"] = round(b["max_percent"], 2)
    out.sort(key=lambda x: ((x["movie_title"] or "").lower(), str(x.get("year") or "")))
    return out

# ---------------------------- Speichern ---------------------------------------
def save_csv(path: str, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in columns})

# --------------------------------- Main ---------------------------------------
def main():
    p = argparse.ArgumentParser(description="Exportiert Serien- & Film-History eines Tautulli-Users mit Fortschritts-Logging.")
    p.add_argument("--url", required=True)
    p.add_argument("--apikey", required=True)
    p.add_argument("--user", default="chucknorris99")
    p.add_argument("--export", choices=["series", "movies", "both"], default="both")
    p.add_argument("--out-series", default=None)
    p.add_argument("--out-movies", default=None)
    p.add_argument("--json", dest="json_out", default=None)
    p.add_argument("--watched-threshold", type=float, default=85.0)
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()

    setup_logging(args.log_level)

    t_total = time.time()
    logging.info("Starte Export für User '%s'…", args.user)

    # user_id
    try:
        t0 = time.time()
        user_id = resolve_user_id(args.url, args.apikey, args.user)
        logging.info("User-Auflösung: %s", fmt_duration(time.time() - t0))
    except Exception as e:
        logging.error("Konnte user_id nicht auflösen: %s", e)
        sys.exit(1)

    series_rows: List[Dict[str, Any]] = []
    movies_rows: List[Dict[str, Any]] = []

    # Serien
    if args.export in ("series", "both"):
        try:
            t1 = time.time()
            hist_eps = fetch_history(args.url, args.apikey, user_id, "episode")
            logging.info("History (Episoden) geladen in %s", fmt_duration(time.time() - t1))

            t2 = time.time()
            series_rows = aggregate_series(hist_eps, watched_threshold=args.watched_threshold)
            logging.info("Aggregation Serien: %s (Serien: %d)", fmt_duration(time.time() - t2), len(series_rows))

            t3 = time.time()
            compute_available_after(args.url, args.apikey, series_rows)
            logging.info("Verfügbare Episoden ermittelt: %s", fmt_duration(time.time() - t3))

            out_csv = args.out_series or f"watched_series_{args.user}.csv"
            save_csv(out_csv, series_rows, [
                "show_title",
                "unique_episodes_watched",
                "episodes_partial",
                "available_episodes",
                "percent_watched_show",
                "avg_episode_percent",
                "first_watched",
                "last_watched",
            ])
            logging.info("✓ Serien-CSV: %s  (Serien: %d)", out_csv, len(series_rows))
        except Exception as e:
            logging.error("Serien-Export fehlgeschlagen: %s", e)

    # Filme
    if args.export in ("movies", "both"):
        try:
            t4 = time.time()
            hist_mov = fetch_history(args.url, args.apikey, user_id, "movie")
            logging.info("History (Filme) geladen in %s", fmt_duration(time.time() - t4))

            t5 = time.time()
            movies_rows = aggregate_movies(hist_mov, watched_threshold=args.watched_threshold)
            logging.info("Aggregation Filme: %s (Filme: %d)", fmt_duration(time.time() - t5), len(movies_rows))

            out_csv = args.out_movies or f"watched_movies_{args.user}.csv"
            save_csv(out_csv, movies_rows, [
                "movie_title", "year", "plays", "max_percent", "avg_percent",
                "last_percent", "completed_any", "first_watched", "last_watched"
            ])
            logging.info("✓ Filme-CSV: %s  (Filme: %d)", out_csv, len(movies_rows))
        except Exception as e:
            logging.error("Film-Export fehlgeschlagen: %s", e)

    # JSON (optional)
    if args.json_out:
        try:
            payload = {"user": args.user, "series": series_rows, "movies": movies_rows}
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            logging.info("✓ JSON exportiert: %s", args.json_out)
        except Exception as e:
            logging.error("JSON-Export fehlgeschlagen: %s", e)

    logging.info("Fertig in %s ✅", fmt_duration(time.time() - t_total))

if __name__ == "__main__":
    main()
