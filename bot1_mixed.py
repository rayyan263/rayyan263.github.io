import requests, json, time
from datetime import datetime, timezone

WALLET = "0x63ce342161250d705dc0b16df89036c8e5f9ba9a"

CHEAP_MIN, CHEAP_MAX = 0.02, 0.20
MOM_MIN, MOM_MAX = 0.55, 0.88
MIN_TL, MAX_TL = 60, 3600
SIZE = 20.0
MAX_MKT = 200

STATE_PATH = "runs/live_bot_state.json"
LOG_PATH = "runs/live_bot_run.log"

entered = {}
open_positions = {}
closed_positions = []
bal = 10000.0
trades = 0


def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def save_state():
    payload = {
        "bal": round(bal, 2),
        "trades": trades,
        "entered": entered,
        "open_positions": list(open_positions.values()),
        "closed_positions": closed_positions[-200:],
    }
    with open(STATE_PATH, "w") as f:
        json.dump(payload, f, indent=2)


def load_state():
    global bal, trades, entered, open_positions, closed_positions
    try:
        with open(STATE_PATH, "r") as f:
            d = json.load(f)
        bal = float(d.get("bal", 10000.0))
        trades = int(d.get("trades", 0))
        entered = d.get("entered", {})
        open_positions = {p["key"]: p for p in d.get("open_positions", []) if "key" in p}
        closed_positions = d.get("closed_positions", [])
        log(
            f"Loaded state bal=${bal:.2f} open={len(open_positions)} "
            f"closed={len(closed_positions)} trades={trades}"
        )
    except FileNotFoundError:
        log("No prior state, starting fresh")
    except Exception as e:
        log(f"State load failed: {e}")


def get_active_markets():
    active = {}
    try:
        r = requests.get(
            "https://data-api.polymarket.com/activity",
            params={"user": WALLET, "limit": 50, "type": "TRADE"},
            timeout=5,
        ).json()
        for t in r:
            if not isinstance(t, dict):
                continue
            cid = t.get("conditionId")
            slug = t.get("slug", "")
            if cid and cid not in active:
                active[cid] = {"slug": slug, "cid": cid}
    except Exception as e:
        log(f"activity fetch failed: {e}")
        return []

    results = []
    for cid, info in active.items():
        try:
            clob = requests.get(f"https://clob.polymarket.com/markets/{cid}", timeout=3).json()
            if not clob.get("accepting_orders"):
                continue

            tokens = clob.get("tokens", [])
            if not tokens:
                continue

            prices = {}
            for tok in tokens:
                try:
                    mid = requests.get(
                        f"https://clob.polymarket.com/midpoint?token_id={tok['token_id']}",
                        timeout=2,
                    ).json()
                    prices[tok["outcome"]] = {
                        "price": float(mid.get("mid", 0.5)),
                        "token_id": tok["token_id"],
                    }
                except Exception:
                    prices[tok["outcome"]] = {
                        "price": 0.5,
                        "token_id": tok["token_id"],
                    }

            slug = info["slug"]
            m = requests.get(
                f"https://gamma-api.polymarket.com/markets?slug={slug}",
                timeout=3
            ).json()
            if not m:
                continue
            m = m[0]

            end = m.get("endDate") or m.get("end_date")
            if not end:
                continue

            try:
                end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
            except Exception:
                continue

            tl = int((end_dt - datetime.now(timezone.utc)).total_seconds())
            if tl < MIN_TL or tl > MAX_TL:
                continue

            title = m.get("question") or m.get("title") or slug

            results.append({
                "cid": cid,
                "slug": slug,
                "title": title,
                "tl": tl,
                "prices": prices,
                "closed": bool(m.get("closed", False)),
                "outcomes": json.loads(m.get("outcomes", "[]")) if isinstance(m.get("outcomes"), str) else m.get("outcomes", []),
                "outcomePrices": json.loads(m.get("outcomePrices", "[]")) if isinstance(m.get("outcomePrices"), str) else m.get("outcomePrices", []),
            })
        except Exception:
            continue

    return results[:MAX_MKT]


def outcome_price_map(m):
    outcomes = m.get("outcomes", []) or []
    prices = m.get("outcomePrices", []) or []
    if not outcomes or not prices or len(outcomes) != len(prices):
        return {}
    try:
        return {outcomes[i]: float(prices[i]) for i in range(len(outcomes))}
    except Exception:
        return {}


def settle_positions(markets):
    global bal, open_positions, closed_positions

    m_by_cid = {m["cid"]: m for m in markets}
    to_remove = []

    for key, pos in list(open_positions.items()):
        m = m_by_cid.get(pos["cid"])
        if not m:
            try:
                r = requests.get(
                    f"https://gamma-api.polymarket.com/markets?slug={pos['slug']}",
                    timeout=3
                ).json()
                if not r:
                    continue
                gm = r[0]
                m = {
                    "cid": pos["cid"],
                    "slug": pos["slug"],
                    "title": pos["title"],
                    "closed": bool(gm.get("closed", False)),
                    "outcomes": json.loads(gm.get("outcomes", "[]")) if isinstance(gm.get("outcomes"), str) else gm.get("outcomes", []),
                    "outcomePrices": json.loads(gm.get("outcomePrices", "[]")) if isinstance(gm.get("outcomePrices"), str) else gm.get("outcomePrices", []),
                }
            except Exception:
                continue

        pm = outcome_price_map(m)
        cur = pm.get(pos["outcome"], pos["entry_price"])
        pos["mark_price"] = round(cur, 4)
        pos["mark_value"] = round(pos["shares"] * cur, 4)
        pos["mark_pnl"] = round(pos["shares"] * cur - pos["cost"], 4)

        if not m.get("closed", False):
            continue

        won = cur > 0.5
        payout = pos["shares"] if won else 0.0
        realized = payout - pos["cost"]
        bal += payout

        pos["status"] = "CLOSED"
        pos["won"] = won
        pos["final_price"] = round(cur, 4)
        pos["payout_realized"] = round(payout, 4)
        pos["realized_pnl"] = round(realized, 4)
        pos["closed_at"] = datetime.now().strftime("%H:%M:%S")

        closed_positions.append(pos)
        to_remove.append(key)

        log(
            f"EXIT {pos['label']} #{pos['trade_no']} {pos['outcome']} "
            f"@{pos['entry_price']:.3f} -> {cur:.3f} "
            f"{'WIN' if won else 'LOSS'} pnl=${realized:+.2f} bal=${bal:,.0f} | "
            f"{pos['title'][:70]}"
        )

    for key in to_remove:
        del open_positions[key]

    if to_remove:
        save_state()


def maybe_enter(m):
    global bal, trades

    cid = m["cid"]
    title = m["title"]
    tl = m["tl"]
    prices = m["prices"]

    for outcome, d in prices.items():
        p = float(d["price"])
        if p <= 0:
            continue

        label = None
        if CHEAP_MIN <= p <= CHEAP_MAX:
            label = "CHEAP"
        elif MOM_MIN <= p <= MOM_MAX:
            label = "MOMENTUM"
        else:
            continue

        key = f"{cid}:{outcome}:{label}"
        if key in entered or key in open_positions:
            continue

        if bal < SIZE:
            continue

        shares = SIZE / p
        payout = shares

        bal -= SIZE
        trades += 1
        entered[key] = True

        pos = {
            "key": key,
            "trade_no": trades,
            "label": label,
            "cid": cid,
            "slug": m["slug"],
            "title": title,
            "outcome": outcome,
            "entry_price": round(p, 4),
            "shares": round(shares, 4),
            "cost": round(SIZE, 2),
            "potential_payout": round(payout, 4),
            "time_left_sec": tl,
            "opened_at": datetime.now().strftime("%H:%M:%S"),
            "status": "OPEN",
            "mark_price": round(p, 4),
            "mark_value": round(SIZE, 4),
            "mark_pnl": 0.0,
        }
        open_positions[key] = pos

        u_other = None
        try:
            other = [k for k in prices.keys() if k != outcome]
            if other:
                u_other = prices[other[0]]["price"]
        except Exception:
            pass

        utxt = f" U={u_other:.2f}" if u_other is not None else ""
        log(
            f"{label} #{trades} {outcome} @{p:.3f} | {title[:65]}{utxt}\n"
            f"       {shares:.0f}sh | cost=${SIZE:.0f} | payout=${payout:.2f} | "
            f"TL={int(tl/60)}m | bal=${bal:,.0f}"
        )
        save_state()


def print_status():
    open_pnl = sum(p.get("mark_pnl", 0.0) for p in open_positions.values())
    realized = sum(p.get("realized_pnl", 0.0) for p in closed_positions)
    log(
        f"STATUS open={len(open_positions)} closed={len(closed_positions)} "
        f"cash=${bal:.2f} open_pnl=${open_pnl:+.2f} realized=${realized:+.2f}"
    )


def main():
    open(LOG_PATH, "a").close()
    load_state()

    loops = 0
    while True:
        markets = get_active_markets()
        settle_positions(markets)

        for m in markets:
            maybe_enter(m)

        loops += 1
        if loops % 12 == 0:
            print_status()

        time.sleep(5)


if __name__ == "__main__":
    main()
