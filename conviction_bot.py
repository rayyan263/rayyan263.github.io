import json
import time
import requests
from collections import defaultdict, deque
from datetime import datetime

WALLET = "0x63ce342161250d705dc0b16df89036c8e5f9ba9a"

ACTIVITY_URL = "https://data-api.polymarket.com/activity"
GAMMA_URL = "https://gamma-api.polymarket.com/markets?slug={slug}"

# --- strategy knobs ---
PRICE_MIN = 0.50
PRICE_MAX = 0.80
MIN_SIZE_USD = 5.0
SPIKE_MULT = 1.5
MIN_STREAK = 1
LOOKBACK_TRADES = 8
PAPER_SIZE = 20.0
POLL_SECS = 1.0

# --- files ---
JSON_LOG = "runs/conviction_trades.json"
TEXT_LOG = "runs/conviction_bot.log"

# --- runtime state ---
seen_trade_keys = set()
market_trades = defaultdict(list)         # conditionId -> list of recent wallet buys
open_positions = {}                       # pos_key -> position dict
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


def fetch_recent_wallet_trades(limit: int = 80):
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


def register_new_wallet_trades():
    new_count = 0
    trades = fetch_recent_wallet_trades()

    # oldest -> newest
    trades = sorted(trades, key=lambda x: x.get("timestamp", 0))

    for t in trades:
        if t.get("side") != "BUY":
            continue

        k = trade_key(t)
        if k in seen_trade_keys:
            continue
        seen_trade_keys.add(k)
        new_count += 1
        log_line(f"NEW  {t.get('outcome','?'):4} @{float(t.get('price',0)):.3f} ${float(t.get('usdcSize',0)):.2f} | {t.get('title','')[:60]}")

        cid = t.get("conditionId")
        if not cid:
            continue

        rec = {
            "timestamp": int(t["timestamp"]),
            "time": datetime.fromtimestamp(int(t["timestamp"])).strftime("%H:%M:%S"),
            "conditionId": cid,
            "slug": t.get("slug", ""),
            "title": t.get("title", ""),
            "outcome": t.get("outcome", ""),
            "price": float(t.get("price", 0)),
            "size": float(t.get("usdcSize", 0)),
        }

        market_trades[cid].append(rec)
        market_trades[cid] = market_trades[cid][-LOOKBACK_TRADES:]

        maybe_open_conviction_position(cid)

    return new_count


def same_side_streak(trades: list, outcome: str) -> int:
    streak = 0
    for t in reversed(trades):
        if t["outcome"] == outcome:
            streak += 1
        else:
            break
    return streak


def maybe_open_conviction_position(cid: str):
    global cash

    trades = market_trades[cid]
    if len(trades) < 3:
        return

    cur = trades[-1]
    slug = cur["slug"]
    outcome = cur["outcome"]
    price = cur["price"]
    size = cur["size"]

    pos_key = f"{cid}:{outcome}"
    if pos_key in open_positions:
        return

    # one side only per market for v1
    if any(k.startswith(f"{cid}:") for k in open_positions):
        return

    if not (PRICE_MIN <= price <= PRICE_MAX):
        return

    if size < MIN_SIZE_USD:
        return

    buys_same_market = trades
    sizes = [t["size"] for t in buys_same_market]
    recent_avg = sum(sizes[:-1]) / max(1, len(sizes[:-1]))
    max_prior = max(sizes[:-1]) if len(sizes) > 1 else 0.0
    streak = same_side_streak(buys_same_market, outcome)

    recent_same = [t for t in buys_same_market if t["outcome"] == outcome]
    recent_opp = [t for t in buys_same_market if t["outcome"] != outcome]

    no_recent_flip = True
    if len(buys_same_market) >= 2 and buys_same_market[-2]["outcome"] != outcome:
        no_recent_flip = False

    # core conviction filters
    reasons = []
    if size <= max_prior:
        reasons.append(f"not_max size={size:.2f} max_prior={max_prior:.2f}")
    if recent_avg > 0 and size < recent_avg * SPIKE_MULT:
        reasons.append(f"no_spike size={size:.2f} avg={recent_avg:.2f}")
    if streak < MIN_STREAK:
        reasons.append(f"streak={streak}")
    if not no_recent_flip:
        reasons.append("recent_flip")
    if len(recent_same) < 2:
        reasons.append(f"same_count={len(recent_same)}")

    if reasons:
        log_line(
            f"SKIP {outcome:4} @{price:.3f} ${size:.2f} | " + ", ".join(reasons) +
            f" | {cur['title'][:60]}"
        )
        return

    shares = PAPER_SIZE / price
    cash -= PAPER_SIZE

    open_positions[pos_key] = {
        "opened_at": now_str(),
        "entry_time": cur["time"],
        "timestamp": cur["timestamp"],
        "conditionId": cid,
        "slug": slug,
        "title": cur["title"],
        "outcome": outcome,
        "entry_price": price,
        "paper_size": PAPER_SIZE,
        "shares": round(shares, 6),
        "wallet_trigger_size": size,
        "recent_avg_size": round(recent_avg, 4),
        "max_prior_size": round(max_prior, 4),
        "streak": streak,
        "status": "OPEN",
        "mark_price": price,
        "mark_pnl": 0.0,
    }

    log_line(
        f"ENTER {outcome:4} @{price:.3f} size=${PAPER_SIZE:.2f} "
        f"trigger=${size:.2f} avg=${recent_avg:.2f} max_prior=${max_prior:.2f} "
        f"streak={streak} | {cur['title'][:70]}"
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

        closed = market.get("closed", False)
        if not closed:
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
            f"{'WIN' if won else 'LOSS'} pnl=${realized:+.2f} | {pos['title'][:70]}"
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


def seed_seen():
    trades = fetch_recent_wallet_trades(limit=80)
    for t in trades:
        if isinstance(t, dict) and t.get("side") == "BUY":
            seen_trade_keys.add(trade_key(t))
    log_line(f"Seeded with {len(seen_trade_keys)} recent wallet BUY trades")


def main():
    open(TEXT_LOG, "a").close()
    seed_seen()
    save_json()

    loops = 0
    while True:
        register_new_wallet_trades()
        mark_and_resolve_positions()

        loops += 1
        if loops % 20 == 0:
            print_status()

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
