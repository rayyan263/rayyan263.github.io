import json
import time
import re
import requests
from collections import defaultdict, deque
from datetime import datetime

WALLET = "0x63ce342161250d705dc0b16df89036c8e5f9ba9a"

ACTIVITY_URL = "https://data-api.polymarket.com/activity"
GAMMA_URL = "https://gamma-api.polymarket.com/markets?slug={slug}"

WINDOW_SECS = 3
MIN_TOTAL_SIZE = 80.0
MIN_TRADES = 4
MIN_UNIQUE_MARKETS = 1
MIN_UNIQUE_ASSETS = 1
MAX_ENTRY_PRICE = 0.70
MIN_ENTRY_PRICE = 0.08

PAPER_SIZE = 20.0
POLL_SECS = 1.0

JSON_LOG = "runs/cluster_trades.json"
TEXT_LOG = "runs/cluster_bot.log"

seen_trade_keys = set()
recent_events = deque()
open_positions = {}
closed_positions = []
cash = 10000.0


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log_line(msg: str) -> None:
    line = f"[{now_str()}] {msg}"
    print(line)
    with open(TEXT_LOG, "a") as f:
        f.write(line + "\n")


def save_json() -> None:
    payload = {
        "cash": round(cash, 2),
        "open_positions": list(open_positions.values()),
        "closed_positions": closed_positions,
    }
    with open(JSON_LOG, "w") as f:
        json.dump(payload, f, indent=2)


def fetch_recent_wallet_trades(limit: int = 120):
    try:
        r = requests.get(
            ACTIVITY_URL,
            params={"user": WALLET, "limit": limit, "type": "TRADE"},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        return [t for t in data if isinstance(t, dict)]
    except Exception as e:
        log_line(f"ERR fetch_recent_wallet_trades: {e}")
        return []


def trade_key(t: dict) -> str:
    return f"{t.get('timestamp')}_{t.get('price')}_{t.get('usdcSize')}_{t.get('outcome')}_{t.get('conditionId')}"


def gamma_market(slug: str):
    try:
        r = requests.get(GAMMA_URL.format(slug=slug), timeout=6)
        r.raise_for_status()
        data = r.json()
        return data[0] if data else None
    except Exception:
        return None


def parse_outcome_map(market: dict):
    try:
        prices = json.loads(market.get("outcomePrices", "[]"))
        outcomes = json.loads(market.get("outcomes", "[]"))
        if not outcomes or len(outcomes) != len(prices):
            return {}
        return {o: float(prices[i]) for i, o in enumerate(outcomes)}
    except Exception:
        return {}


def infer_asset(title: str, slug: str) -> str:
    text = f"{title} {slug}".lower()
    if "bitcoin" in text or re.search(r"\bbtc\b", text):
        return "BTC"
    if "ethereum" in text or re.search(r"\beth\b", text):
        return "ETH"
    if "solana" in text or re.search(r"\bsol\b", text):
        return "SOL"
    if "xrp" in text:
        return "XRP"
    return "OTHER"


def seed_seen():
    trades = fetch_recent_wallet_trades(limit=120)
    for t in trades:
        if isinstance(t, dict) and t.get("side") == "BUY":
            seen_trade_keys.add(trade_key(t))
    log_line(f"Seeded with {len(seen_trade_keys)} recent wallet BUY trades")


def register_new_wallet_trades():
    new_count = 0
    trades = fetch_recent_wallet_trades()
    trades = sorted(trades, key=lambda x: x.get("timestamp", 0))

    for t in trades:
        if t.get("side") != "BUY":
            continue

        k = trade_key(t)
        if k in seen_trade_keys:
            continue
        seen_trade_keys.add(k)
        new_count += 1

        title = t.get("title", "")
        slug = t.get("slug", "")
        asset = infer_asset(title, slug)

        rec = {
            "timestamp": int(t["timestamp"]),
            "time": datetime.fromtimestamp(int(t["timestamp"])).strftime("%H:%M:%S"),
            "conditionId": t.get("conditionId", ""),
            "slug": slug,
            "title": title,
            "asset": asset,
            "outcome": t.get("outcome", ""),
            "price": float(t.get("price", 0)),
            "size": float(t.get("usdcSize", 0)),
        }

        recent_events.append(rec)
        log_line(
            f"NEW  {rec['outcome']:4} @{rec['price']:.3f} ${rec['size']:.2f} "
            f"{rec['asset']:>4} | {rec['title'][:58]}"
        )

    trim_recent_events()
    return new_count
def trim_recent_events():
    if not recent_events:
        return
    newest_ts = recent_events[-1]["timestamp"]
    cutoff = newest_ts - WINDOW_SECS
    while recent_events and recent_events[0]["timestamp"] < cutoff:
        recent_events.popleft()


def cluster_groups():
    groups = defaultdict(list)
    for e in recent_events:
        groups[e["outcome"]].append(e)
    return groups


def maybe_open_cluster_positions():
    global cash

    groups = cluster_groups()

    for outcome, events in groups.items():
        if len(events) < MIN_TRADES:
            continue

        total_size = sum(e["size"] for e in events)
        unique_markets = len(set(e["conditionId"] for e in events))
        unique_assets = len(set(e["asset"] for e in events if e["asset"] != "OTHER"))

        reasons = []
        if total_size < MIN_TOTAL_SIZE:
            reasons.append(f"total={total_size:.2f}")
        if unique_markets < MIN_UNIQUE_MARKETS:
            reasons.append(f"mkts={unique_markets}")
        if unique_assets < MIN_UNIQUE_ASSETS:
            reasons.append(f"assets={unique_assets}")
        if reasons:
            log_line(
                f"SKIP_CLUSTER {outcome:4} " +
                ", ".join(reasons) +
                f" trades={len(events)} | {events[-1]['title'][:58]}"
            )
            continue

        eligible = [e for e in events if MIN_ENTRY_PRICE <= e["price"] <= MAX_ENTRY_PRICE]
        if not eligible:
            log_line(
                f"SKIP_CLUSTER {outcome:4} no eligible trigger in price band | "
                f"trades={len(events)} total=${total_size:.2f}"
            )
            continue

        trigger = max(eligible, key=lambda x: x["size"])
        cid = trigger["conditionId"]
        pos_key = f"{cid}:{outcome}"

        if pos_key in open_positions:
            continue
        if any(k.startswith(f"{cid}:") for k in open_positions):
            continue

        price = trigger["price"]
        shares = PAPER_SIZE / price
        cash -= PAPER_SIZE

        open_positions[pos_key] = {
            "opened_at": now_str(),
            "entry_time": trigger["time"],
            "timestamp": trigger["timestamp"],
            "conditionId": cid,
            "slug": trigger["slug"],
            "title": trigger["title"],
            "asset": trigger["asset"],
            "outcome": outcome,
            "entry_price": price,
            "paper_size": PAPER_SIZE,
            "shares": round(shares, 6),
            "status": "OPEN",
            "mark_price": price,
            "mark_pnl": 0.0,
            "source_bot": "cluster",
            "cluster_total_size": round(total_size, 2),
            "cluster_trade_count": len(events),
            "cluster_unique_markets": unique_markets,
            "cluster_unique_assets": unique_assets,
            "trigger_size": round(trigger["size"], 2),
        }

        log_line(
            f"ENTER {outcome:4} @{price:.3f} size=${PAPER_SIZE:.2f} "
            f"cluster=${total_size:.2f} trades={len(events)} "
            f"mkts={unique_markets} assets={unique_assets} "
            f"trigger=${trigger['size']:.2f} {trigger['asset']} | {trigger['title'][:58]}"
        )
        save_json()


def mark_and_resolve_positions():
    global cash
    to_close = []

    for pos_key, pos in open_positions.items():
        market = gamma_market(pos["slug"])
        if not market:
            continue

        outcome_map = parse_outcome_map(market)
        if not outcome_map:
            continue

        cur = outcome_map.get(pos["outcome"], pos["entry_price"])
        pos["mark_price"] = round(cur, 4)
        pos["mark_pnl"] = round((cur - pos["entry_price"]) * pos["shares"], 4)

        if not market.get("closed", False):
            continue

        won = cur > 0.5
        payout = pos["shares"] if won else 0.0
        realized = payout - pos["paper_size"]
        cash += payout

        pos["status"] = "CLOSED"
        pos["won"] = won
        pos["final_price"] = round(cur, 4)
        pos["realized_pnl"] = round(realized, 4)
        pos["closed_at"] = now_str()

        closed_positions.append(pos)
        to_close.append(pos_key)

        log_line(
            f"EXIT  {pos['outcome']:4} @{pos['entry_price']:.3f} -> {cur:.3f} "
            f"{'WIN' if won else 'LOSS'} pnl=${realized:+.2f} | {pos['title'][:58]}"
        )

    for k in to_close:
        del open_positions[k]

    if to_close:
        save_json()
def print_status():
    open_pnl = sum(p.get("mark_pnl", 0.0) for p in open_positions.values())
    realized = sum(p.get("realized_pnl", 0.0) for p in closed_positions)
    log_line(
        f"STATUS open={len(open_positions)} closed={len(closed_positions)} "
        f"cash=${cash:.2f} open_pnl=${open_pnl:+.2f} realized=${realized:+.2f}"
    )


def main():
    open(TEXT_LOG, "a").close()
    seed_seen()
    save_json()

    loops = 0
    while True:
        register_new_wallet_trades()
        maybe_open_cluster_positions()
        mark_and_resolve_positions()

        loops += 1
        if loops % 20 == 0:
            print_status()

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
