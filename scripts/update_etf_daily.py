#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import exchange_calendars as xcals
import pandas as pd
from tickflow import TickFlow

REQUIRED_KLINE_COLUMNS = [
    "symbol",
    "name",
    "timestamp",
    "trade_date",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]

TRADE_TIMEZONE = ZoneInfo("Asia/Shanghai")


@dataclass
class SyncStats:
    total: int = 0
    updated_symbols: int = 0
    skipped_symbols: int = 0
    error_symbols: int = 0
    fetched_rows: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incrementally sync ETF daily bars from TickFlow into DuckDB."
    )
    parser.add_argument(
        "--etf-list",
        type=Path,
        default=Path("data/etf_list.csv"),
        help="ETF list CSV path",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/etf_daily.duckdb"),
        help="DuckDB file path",
    )
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Each TickFlow request row count"
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Min interval between TickFlow requests (seconds)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=6,
        help="Max retries for transient API errors",
    )
    parser.add_argument(
        "--retry-seconds",
        type=float,
        default=3.0,
        help="Default retry interval seconds",
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of symbols for testing"
    )
    return parser.parse_args()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def normalize_symbol(raw_code: str) -> str:
    code = str(raw_code).strip().upper()
    code = re.sub(r"\s+", "", code)
    if not code:
        return ""
    if "." in code and code.endswith((".SH", ".SZ")):
        return code

    match = re.match(r"^(SH|SZ)(\d{6})$", code)
    if match:
        return f"{match.group(2)}.{match.group(1)}"

    digits = "".join(ch for ch in code if ch.isdigit())
    if len(digits) != 6:
        return code

    suffix = "SH" if digits.startswith(("5", "6")) else "SZ"
    return f"{digits}.{suffix}"


def load_symbols(
    etf_list: Path, limit: int | None
) -> list[str]:
    if not etf_list.exists():
        raise FileNotFoundError(f"ETF list CSV not found: {etf_list}")

    df = pd.read_csv(etf_list, dtype=str)
    if df.empty:
        return []

    df.columns = [str(c).strip() for c in df.columns]

    symbol_col = "代码"
    if symbol_col not in df.columns:
        raise ValueError(
            "ETF list must contain fixed column '代码'. "
            f"Current columns: {list(df.columns)}"
        )

    symbols: list[str] = []
    for _, row in df.iterrows():
        raw = row.get(symbol_col)
        if pd.isna(raw):
            continue
        symbol = normalize_symbol(str(raw))
        if symbol:
            symbols.append(symbol)

    deduped = list(dict.fromkeys(symbols))
    if limit is not None:
        return deduped[:limit]
    return deduped


def create_client(api_key: str | None) -> TickFlow:
    key = api_key or os.getenv("TICKFLOW_API_KEY")
    if key:
        return TickFlow(api_key=key)
    return TickFlow.free()


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS etf_daily (
            symbol VARCHAR NOT NULL,
            name VARCHAR,
            timestamp BIGINT NOT NULL,
            trade_date DATE NOT NULL,
            trade_time TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(symbol, trade_date)
        );
        """
    )


def get_last_timestamp(conn: duckdb.DuckDBPyConnection, symbol: str) -> int | None:
    row = conn.execute(
        "SELECT MAX(timestamp) FROM etf_daily WHERE symbol = ?", [symbol]
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def get_market_latest_timestamp_from_calendar() -> tuple[int, str]:
    cal = xcals.get_calendar("XSHG")
    now = pd.Timestamp.now(tz=str(TRADE_TIMEZONE))
    latest_session = cal.date_to_session(now.date(), direction="previous")
    latest_date = latest_session.date()
    latest_date_str = latest_date.isoformat()
    latest_dt = datetime(
        latest_date.year,
        latest_date.month,
        latest_date.day,
        tzinfo=TRADE_TIMEZONE,
    )
    return int(latest_dt.timestamp() * 1000), latest_date_str


def fetch_since(
    client: TickFlow,
    symbol: str,
    start_time_ms: int,
    batch_size: int,
    sleep_seconds: float,
    max_retries: int,
    retry_seconds: float,
    throttle_state: dict,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    end_time: int | None = None

    while True:
        kwargs = {
            "period": "1d",
            "count": batch_size,
            "start_time": start_time_ms,
            "as_dataframe": True,
        }
        if end_time is not None:
            kwargs["end_time"] = end_time

        chunk = get_chunk_with_retry(
            client=client,
            symbol=symbol,
            kwargs=kwargs,
            max_retries=max_retries,
            retry_seconds=retry_seconds,
            min_interval_seconds=sleep_seconds,
            throttle_state=throttle_state,
        )
        if chunk is None or chunk.empty:
            break

        chunk = chunk.copy()
        chunk = chunk[REQUIRED_KLINE_COLUMNS]
        chunk["timestamp"] = chunk["timestamp"].astype("int64")
        frames.append(chunk)

        oldest_ts = int(chunk["timestamp"].min())
        if len(chunk) < batch_size:
            break

        next_end = oldest_ts - 1
        if end_time is not None and next_end >= end_time:
            break
        end_time = next_end

    if not frames:
        return pd.DataFrame(columns=REQUIRED_KLINE_COLUMNS)

    data = pd.concat(frames, ignore_index=True)
    data = data[data["timestamp"] >= start_time_ms]
    data = data.drop_duplicates(subset=["symbol", "trade_date"], keep="last")
    data = data.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    return data


def is_retryable_error(message: str) -> bool:
    msg = message.lower()
    hints = [
        "rate limit",
        "请求过于频繁",
        "too many requests",
        "timed out",
        "connection",
        "temporary",
        "nodename nor servname",
    ]
    return any(hint in msg for hint in hints)


def extract_retry_seconds(message: str, fallback: float) -> float:
    patterns = [
        r"(\d+)\s*毫秒后重试",
        r"(\d+)\s*ms",
        r"retry(?:\s+after)?\s*(\d+(?:\.\d+)?)\s*seconds?",
    ]
    for pattern in patterns:
        match = re.search(pattern, message, flags=re.IGNORECASE)
        if not match:
            continue
        value = float(match.group(1))
        if "毫秒" in pattern or "ms" in pattern:
            return max(0.2, value / 1000.0 + 1.0)
        return max(0.2, value)
    return max(0.2, fallback)


def get_chunk_with_retry(
    client: TickFlow,
    symbol: str,
    kwargs: dict,
    max_retries: int,
    retry_seconds: float,
    min_interval_seconds: float,
    throttle_state: dict,
):
    for attempt in range(max_retries + 1):
        try:
            if min_interval_seconds > 0:
                last_call = throttle_state.get("last_call_at")
                if last_call is not None:
                    wait_seconds = min_interval_seconds - (time.monotonic() - last_call)
                    if wait_seconds > 0:
                        time.sleep(wait_seconds)
            return client.klines.get(symbol, **kwargs)
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            if attempt >= max_retries or not is_retryable_error(message):
                raise
            sleep_for = extract_retry_seconds(
                message, fallback=retry_seconds * (attempt + 1)
            )
            print(
                f"{symbol} retry {attempt + 1}/{max_retries} after {sleep_for:.1f}s "
                f"because: {message}",
                file=sys.stderr,
            )
            time.sleep(sleep_for)
        finally:
            throttle_state["last_call_at"] = time.monotonic()
    raise RuntimeError("Unreachable retry state")


def upsert_rows(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    if df.empty:
        return

    conn.register("incoming_klines", df)
    conn.execute(
        """
        DELETE FROM etf_daily
        USING incoming_klines
        WHERE etf_daily.symbol = incoming_klines.symbol
          AND etf_daily.trade_date = CAST(incoming_klines.trade_date AS DATE);
        """
    )
    conn.execute(
        """
        INSERT INTO etf_daily (
            symbol,
            name,
            timestamp,
            trade_date,
            trade_time,
            open,
            high,
            low,
            close,
            volume,
            amount,
            updated_at
        )
        SELECT
            symbol,
            name,
            CAST(timestamp AS BIGINT),
            CAST(trade_date AS DATE),
            CAST(trade_time AS TIMESTAMP),
            CAST(open AS DOUBLE),
            CAST(high AS DOUBLE),
            CAST(low AS DOUBLE),
            CAST(close AS DOUBLE),
            CAST(volume AS DOUBLE),
            CAST(amount AS DOUBLE),
            CURRENT_TIMESTAMP
        FROM incoming_klines;
        """
    )
    conn.unregister("incoming_klines")


def main() -> int:
    args = parse_args()

    symbols = load_symbols(args.etf_list, args.limit)
    if not symbols:
        print("No symbols found in ETF list.", file=sys.stderr)
        return 1

    ensure_parent(args.db_path)
    conn = duckdb.connect(str(args.db_path))
    init_db(conn)

    client = create_client(args.api_key)
    throttle_state: dict = {"last_call_at": None}
    market_latest_ts, market_latest_date = get_market_latest_timestamp_from_calendar()
    print(f"Market latest trade date (calendar): {market_latest_date}")

    stats = SyncStats(total=len(symbols))
    interrupted = False

    try:
        for idx, symbol in enumerate(symbols, start=1):
            last_ts = get_last_timestamp(conn, symbol)
            start_ts = 0 if last_ts is None else (last_ts + 1)
            if last_ts is not None and last_ts >= market_latest_ts:
                stats.skipped_symbols += 1
                print(
                    f"[{idx}/{stats.total}] {symbol} no new rows (already at market latest)"
                )
                continue

            try:
                new_data = fetch_since(
                    client=client,
                    symbol=symbol,
                    start_time_ms=start_ts,
                    batch_size=args.batch_size,
                    sleep_seconds=args.sleep_seconds,
                    max_retries=args.max_retries,
                    retry_seconds=args.retry_seconds,
                    throttle_state=throttle_state,
                )
            except Exception as exc:  # noqa: BLE001
                stats.error_symbols += 1
                print(f"[{idx}/{stats.total}] {symbol} ERROR: {exc}", file=sys.stderr)
                continue

            if new_data.empty:
                stats.skipped_symbols += 1
                print(f"[{idx}/{stats.total}] {symbol} no new rows")
            else:
                upsert_rows(conn, new_data)
                stats.updated_symbols += 1
                stats.fetched_rows += len(new_data)

                first_date = str(new_data["trade_date"].iloc[0])
                last_date = str(new_data["trade_date"].iloc[-1])
                mode = "init" if last_ts is None else "incr"
                print(
                    f"[{idx}/{stats.total}] {symbol} {mode} +{len(new_data)} rows ({first_date} -> {last_date})"
                )
    except KeyboardInterrupt:
        interrupted = True
        print("\nInterrupted by user.", file=sys.stderr)
    finally:
        conn.close()

    print("=" * 72)
    print(
        "SYNC DONE | "
        f"total={stats.total}, updated={stats.updated_symbols}, "
        f"skipped={stats.skipped_symbols}, errors={stats.error_symbols}, rows={stats.fetched_rows}"
    )
    print(f"DuckDB: {args.db_path}")
    if interrupted:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
