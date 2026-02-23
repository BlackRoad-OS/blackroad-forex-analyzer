#!/usr/bin/env python3
"""BlackRoad Forex Analyzer - Production Module.

Currency pair analysis, exchange rate tracking, spread monitoring,
and position management with persistent SQLite storage.
"""

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

RED     = "\033[0;31m"
GREEN   = "\033[0;32m"
YELLOW  = "\033[1;33m"
CYAN    = "\033[0;36m"
BLUE    = "\033[0;34m"
MAGENTA = "\033[0;35m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
NC      = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "forex_analyzer.db"

# Pip decimal places by quote currency
PIP_DECIMALS = {"JPY": 2, "HUF": 2, "KRW": 2}
DEFAULT_PIP_DECIMALS = 4


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class CurrencyPair:
    base_currency: str   # e.g. EUR
    quote_currency: str  # e.g. USD
    pair_code: str = ""  # e.g. EURUSD  (auto-computed)
    description: str = ""
    created_at: str = ""
    id: Optional[int] = None

    def __post_init__(self):
        if not self.pair_code:
            self.pair_code = f"{self.base_currency}{self.quote_currency}"
        if not self.created_at:
            self.created_at = datetime.now().isoformat()

    def pip_size(self) -> float:
        decimals = PIP_DECIMALS.get(self.quote_currency, DEFAULT_PIP_DECIMALS)
        return 10 ** -decimals


@dataclass
class RateRecord:
    pair_code: str
    bid: float
    ask: float
    source: str = "manual"
    recorded_at: str = ""
    id: Optional[int] = None

    @property
    def mid(self) -> float:
        return round((self.bid + self.ask) / 2.0, 6)

    @property
    def spread_pips(self) -> float:
        """Spread in pips (assumes 4-decimal pair unless JPY etc.)."""
        raw_spread = self.ask - self.bid
        return round(raw_spread * 10000, 2)  # normalised to 4-dp pip

    def __post_init__(self):
        if not self.recorded_at:
            self.recorded_at = datetime.now().isoformat()


@dataclass
class Position:
    pair_code: str
    direction: str       # long | short
    lot_size: float      # standard lots; 1 lot = 100 000 base units
    open_rate: float
    stop_loss: float = 0.0
    take_profit: float = 0.0
    status: str = "open"   # open | closed
    close_rate: float = 0.0
    pnl_pips: float = 0.0
    opened_at: str = ""
    closed_at: str = ""
    id: Optional[int] = None

    def __post_init__(self):
        if not self.opened_at:
            self.opened_at = datetime.now().isoformat()


# ---------------------------------------------------------------------------
# Database / Business Logic
# ---------------------------------------------------------------------------

class ForexAnalyzer:
    """Production forex currency pair analyzer with position tracking."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS pairs (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    base_currency  TEXT NOT NULL,
                    quote_currency TEXT NOT NULL,
                    pair_code      TEXT UNIQUE NOT NULL,
                    description    TEXT DEFAULT '',
                    created_at     TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rates (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_code   TEXT NOT NULL,
                    bid         REAL NOT NULL,
                    ask         REAL NOT NULL,
                    source      TEXT DEFAULT 'manual',
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS positions (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_code    TEXT NOT NULL,
                    direction    TEXT NOT NULL,
                    lot_size     REAL NOT NULL,
                    open_rate    REAL NOT NULL,
                    stop_loss    REAL DEFAULT 0.0,
                    take_profit  REAL DEFAULT 0.0,
                    status       TEXT DEFAULT 'open',
                    close_rate   REAL DEFAULT 0.0,
                    pnl_pips     REAL DEFAULT 0.0,
                    opened_at    TEXT NOT NULL,
                    closed_at    TEXT DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_rates_pair ON rates(pair_code, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_pos_pair   ON positions(pair_code, status);
            """)

    def add_pair(self, base: str, quote: str,
                 description: str = "") -> CurrencyPair:
        """Register a currency pair for rate tracking."""
        pair = CurrencyPair(base_currency=base.upper(),
                            quote_currency=quote.upper(),
                            description=description)
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO pairs "
                "(base_currency, quote_currency, pair_code, description, created_at) "
                "VALUES (?,?,?,?,?)",
                (pair.base_currency, pair.quote_currency, pair.pair_code,
                 pair.description, pair.created_at)
            )
        return pair

    def record_rate(self, pair_code: str, bid: float, ask: float,
                    source: str = "manual") -> RateRecord:
        """Record a live bid/ask rate for a currency pair."""
        rec = RateRecord(pair_code=pair_code.upper(), bid=bid, ask=ask, source=source)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO rates (pair_code, bid, ask, source, recorded_at) "
                "VALUES (?,?,?,?,?)",
                (rec.pair_code, rec.bid, rec.ask, rec.source, rec.recorded_at)
            )
        return rec

    def analyze_pair(self, pair_code: str, lookback: int = 50) -> dict:
        """Analyse recent rates: trend, average spread, volatility, bias."""
        pair_code = pair_code.upper()
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM rates WHERE pair_code=? "
                "ORDER BY recorded_at DESC LIMIT ?",
                (pair_code, lookback)
            ).fetchall()
        if not rows:
            return {"pair_code": pair_code, "error": "No rate data available."}

        mids    = [round((r["bid"] + r["ask"]) / 2.0, 6) for r in rows]
        spreads = [round((r["ask"] - r["bid"]) * 10000, 2) for r in rows]
        latest  = mids[0]
        oldest  = mids[-1]
        change  = latest - oldest
        change_pct = (change / oldest * 100) if oldest else 0.0

        # Simple trend: compare first-half vs second-half averages
        half = max(1, len(mids) // 2)
        avg_recent = sum(mids[:half]) / half
        avg_older  = sum(mids[half:]) / max(1, len(mids) - half)
        trend = "BULLISH" if avg_recent > avg_older else (
                "BEARISH" if avg_recent < avg_older else "NEUTRAL")

        avg_spread = sum(spreads) / len(spreads)
        n          = len(mids)
        mean_mid   = sum(mids) / n
        variance   = sum((x - mean_mid) ** 2 for x in mids) / n
        volatility = round(variance ** 0.5, 6)

        return {
            "pair_code":      pair_code,
            "data_points":    n,
            "latest_mid":     latest,
            "oldest_mid":     oldest,
            "change":         round(change, 6),
            "change_pct":     f"{change_pct:+.4f}%",
            "trend":          trend,
            "avg_spread_pips": round(avg_spread, 2),
            "volatility":     volatility,
            "latest_bid":     rows[0]["bid"],
            "latest_ask":     rows[0]["ask"],
        }

    def open_position(self, pair_code: str, direction: str, lot_size: float,
                      open_rate: float, stop_loss: float = 0.0,
                      take_profit: float = 0.0) -> Position:
        """Open a new forex trading position."""
        pos = Position(pair_code=pair_code.upper(), direction=direction.lower(),
                       lot_size=lot_size, open_rate=open_rate,
                       stop_loss=stop_loss, take_profit=take_profit)
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO positions (pair_code, direction, lot_size, open_rate, "
                "stop_loss, take_profit, status, close_rate, pnl_pips, opened_at, closed_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (pos.pair_code, pos.direction, pos.lot_size, pos.open_rate,
                 pos.stop_loss, pos.take_profit, pos.status, pos.close_rate,
                 pos.pnl_pips, pos.opened_at, pos.closed_at)
            )
            pos.id = cur.lastrowid
        return pos

    def list_pairs(self) -> List[dict]:
        """List all tracked currency pairs with latest rate snapshot."""
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT p.*,
                       r.bid         AS latest_bid,
                       r.ask         AS latest_ask,
                       r.recorded_at AS rate_updated
                FROM pairs p
                LEFT JOIN rates r
                  ON p.pair_code = r.pair_code
                 AND r.recorded_at = (
                     SELECT MAX(recorded_at) FROM rates r2
                     WHERE r2.pair_code = p.pair_code
                 )
                ORDER BY p.pair_code
            """).fetchall()
        return [dict(r) for r in rows]

    def get_summary(self) -> dict:
        """Portfolio-level summary across all positions."""
        with self._conn() as conn:
            total_pos  = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
            open_pos   = conn.execute(
                "SELECT COUNT(*) FROM positions WHERE status='open'"
            ).fetchone()[0]
            closed_pos = conn.execute(
                "SELECT COUNT(*) FROM positions WHERE status='closed'"
            ).fetchone()[0]
            total_pnl  = conn.execute(
                "SELECT COALESCE(SUM(pnl_pips),0) FROM positions WHERE status='closed'"
            ).fetchone()[0]
            total_pairs = conn.execute("SELECT COUNT(*) FROM pairs").fetchone()[0]
            total_rates = conn.execute("SELECT COUNT(*) FROM rates").fetchone()[0]
        return {
            "tracked_pairs":   total_pairs,
            "rate_records":    total_rates,
            "total_positions": total_pos,
            "open_positions":  open_pos,
            "closed_positions": closed_pos,
            "total_pnl_pips":  f"{total_pnl:+.1f} pips",
        }

    def export_report(self, output_path: str = "forex_report.json") -> str:
        """Export full forex analysis report to JSON."""
        with self._conn() as conn:
            positions = [dict(r) for r in conn.execute(
                "SELECT * FROM positions ORDER BY opened_at DESC"
            ).fetchall()]
            rates = [dict(r) for r in conn.execute(
                "SELECT * FROM rates ORDER BY recorded_at DESC LIMIT 200"
            ).fetchall()]
        data = {
            "exported_at": datetime.now().isoformat(),
            "generator":   "BlackRoad Forex Analyzer v1.0",
            "summary":     self.get_summary(),
            "pairs":       self.list_pairs(),
            "positions":   positions,
            "recent_rates": rates,
        }
        Path(output_path).write_text(json.dumps(data, indent=2))
        return output_path


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def _header(title: str):
    w = 64
    print(f"\n{BOLD}{BLUE}{'━' * w}{NC}")
    print(f"{BOLD}{BLUE}  {title}{NC}")
    print(f"{BOLD}{BLUE}{'━' * w}{NC}")


def _trend_color(trend: str) -> str:
    return {"BULLISH": GREEN, "BEARISH": RED, "NEUTRAL": YELLOW}.get(trend, NC)


def _dir_color(direction: str) -> str:
    return GREEN if direction == "long" else RED


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

def cmd_list(args, analyzer: ForexAnalyzer):
    pairs = analyzer.list_pairs()
    _header("FOREX ANALYZER — Tracked Currency Pairs")
    if not pairs:
        print(f"  {YELLOW}No pairs tracked. Use 'add' to register pairs.{NC}\n")
        return
    for p in pairs:
        bid_str = f"{CYAN}{p['latest_bid']:.5f}{NC}" if p.get("latest_bid") else DIM + "—" + NC
        ask_str = f"{CYAN}{p['latest_ask']:.5f}{NC}" if p.get("latest_ask") else DIM + "—" + NC
        spread  = ""
        if p.get("latest_bid") and p.get("latest_ask"):
            sp = round((p["latest_ask"] - p["latest_bid"]) * 10000, 2)
            spread = f"  Spread: {YELLOW}{sp:.1f} pips{NC}"
        print(f"  {BOLD}{p['pair_code']:<10}{NC} "
              f"{DIM}{p['base_currency']}/{p['quote_currency']}{NC}  "
              f"Bid: {bid_str}  Ask: {ask_str}{spread}")
    print()


def cmd_add(args, analyzer: ForexAnalyzer):
    pair = analyzer.add_pair(args.base, args.quote,
                             getattr(args, "description", ""))
    print(f"\n{GREEN}✓ Pair registered:{NC} {BOLD}{pair.pair_code}{NC} "
          f"({pair.base_currency}/{pair.quote_currency})\n")


def cmd_rate(args, analyzer: ForexAnalyzer):
    rec = analyzer.record_rate(args.pair, args.bid, args.ask, args.source)
    mid = rec.mid
    print(f"\n{CYAN}✓ Rate recorded:{NC} {BOLD}{args.pair.upper()}{NC}  "
          f"Bid:{YELLOW}{rec.bid:.5f}{NC}  Ask:{YELLOW}{rec.ask:.5f}{NC}  "
          f"Mid:{GREEN}{mid:.5f}{NC}  "
          f"Spread:{MAGENTA}{rec.spread_pips:.1f}p{NC}\n")


def cmd_analyze(args, analyzer: ForexAnalyzer):
    result = analyzer.analyze_pair(args.pair, getattr(args, "lookback", 50))
    if "error" in result:
        print(f"\n{RED}✗ {result['error']}{NC}\n")
        return
    tc = _trend_color(result["trend"])
    _header(f"PAIR ANALYSIS — {result['pair_code']}")
    print(f"  {DIM}Data Points:{NC}       {result['data_points']}")
    print(f"  {DIM}Latest Mid:{NC}        {YELLOW}{result['latest_mid']:.5f}{NC}")
    print(f"  {DIM}Change:{NC}            {result['change']:+.6f}  ({result['change_pct']})")
    print(f"  {DIM}Trend:{NC}             {tc}{result['trend']}{NC}")
    print(f"  {DIM}Avg Spread:{NC}        {MAGENTA}{result['avg_spread_pips']:.2f} pips{NC}")
    print(f"  {DIM}Volatility (σ):{NC}    {result['volatility']:.6f}")
    print(f"  {DIM}Latest Bid/Ask:{NC}    {result['latest_bid']:.5f} / {result['latest_ask']:.5f}\n")


def cmd_position(args, analyzer: ForexAnalyzer):
    pos = analyzer.open_position(args.pair, args.direction, args.lots,
                                 args.rate, args.sl, args.tp)
    dc = _dir_color(pos.direction)
    print(f"\n{GREEN}✓ Position opened{NC}")
    print(f"  {BOLD}ID:{NC}        {pos.id}")
    print(f"  {BOLD}Pair:{NC}      {pos.pair_code}")
    print(f"  {BOLD}Direction:{NC} {dc}{pos.direction.upper()}{NC}")
    print(f"  {BOLD}Lots:{NC}      {pos.lot_size}")
    print(f"  {BOLD}Open Rate:{NC} {pos.open_rate:.5f}")
    if pos.stop_loss:
        print(f"  {BOLD}Stop Loss:{NC} {RED}{pos.stop_loss:.5f}{NC}")
    if pos.take_profit:
        print(f"  {BOLD}Take Profit:{NC} {GREEN}{pos.take_profit:.5f}{NC}")
    print()


def cmd_status(args, analyzer: ForexAnalyzer):
    s = analyzer.get_summary()
    _header("FOREX ANALYZER — SUMMARY")
    for key, val in s.items():
        label = key.replace("_", " ").title()
        color = GREEN if "+" in str(val) else (RED if "-" in str(val) else CYAN)
        print(f"  {DIM}{label:<25}{NC}  {color}{val}{NC}")
    print()


def cmd_export(args, analyzer: ForexAnalyzer):
    path = analyzer.export_report(args.output)
    print(f"\n{GREEN}✓ Report exported to:{NC} {BOLD}{path}{NC}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    analyzer = ForexAnalyzer()
    parser = argparse.ArgumentParser(
        prog="forex-analyzer",
        description=f"{BOLD}BlackRoad Forex Analyzer{NC}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s add --base EUR --quote USD\n"
            "  %(prog)s rate --pair EURUSD --bid 1.08521 --ask 1.08534\n"
            "  %(prog)s analyze --pair EURUSD\n"
            "  %(prog)s position --pair EURUSD --direction long "
            "--lots 1.0 --rate 1.08521 --sl 1.0800 --tp 1.0950\n"
        ),
    )
    subs = parser.add_subparsers(dest="command", metavar="COMMAND")
    subs.required = True

    subs.add_parser("list", help="List tracked currency pairs")

    p = subs.add_parser("add", help="Register a currency pair")
    p.add_argument("--base",        required=True, metavar="EUR")
    p.add_argument("--quote",       required=True, metavar="USD")
    p.add_argument("--description", default="")

    p = subs.add_parser("rate", help="Record a live bid/ask rate")
    p.add_argument("--pair",   required=True, metavar="EURUSD")
    p.add_argument("--bid",    required=True, type=float)
    p.add_argument("--ask",    required=True, type=float)
    p.add_argument("--source", default="manual")

    p = subs.add_parser("analyze", help="Analyse a currency pair's rate history")
    p.add_argument("--pair",     required=True, metavar="EURUSD")
    p.add_argument("--lookback", default=50, type=int, metavar="N")

    p = subs.add_parser("position", help="Open a new trading position")
    p.add_argument("--pair",      required=True, metavar="EURUSD")
    p.add_argument("--direction", required=True, choices=["long", "short"])
    p.add_argument("--lots",      required=True, type=float)
    p.add_argument("--rate",      required=True, type=float)
    p.add_argument("--sl",        default=0.0,   type=float, metavar="STOP_LOSS")
    p.add_argument("--tp",        default=0.0,   type=float, metavar="TAKE_PROFIT")

    subs.add_parser("status", help="Show portfolio summary")

    p = subs.add_parser("export", help="Export full analysis report")
    p.add_argument("--output", default="forex_report.json", metavar="FILE")

    args = parser.parse_args()
    {"list": cmd_list, "add": cmd_add, "rate": cmd_rate, "analyze": cmd_analyze,
     "position": cmd_position, "status": cmd_status, "export": cmd_export
     }[args.command](args, analyzer)


if __name__ == "__main__":
    main()
