#!/usr/bin/env python3
"""
MULTI-COIN EXPLOSION RIDER v4 (PAPER)
Trades: AINUSDT, YALAUSDT, TANSSIUSDT simultaneously
Backtested edge:
  AIN:    51.4% WR $1.88/day at 20x
  YALA:   48.9% WR $1.56/day at 20x
  TANSSI: 43.8% WR $1.04/day at 20x
  TOTAL:  ~$4.48/day on $150 deployed
"""

import asyncio
import csv
import json
import os
import time
import datetime
import signal
from collections import deque
from dataclasses import dataclass, field
from typing import Dict

import aiohttp
import websockets
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel


# ==============================
# CONFIG
# ==============================
SYMBOLS = ["ainusdt"]

@dataclass
class Config:
    initial_balance: float = 500.0
    trade_size: float = 50.0        # per coin
    taker_fee_rate: float = 0.0004
    leverage: int = 20

    vol_thr: float = 4.0
    imb_thr: float = 0.15
    bar_seconds: int = 300
    vol_window_bars: int = 20
    min_score: int = 2

    tp_pct: float = 0.008
    sl_pct: float = 0.004
    max_hold_s: int = 1800

    metrics_poll_s: int = 30
    ob_poll_s: float = 1.0
    ob_wall_min: float = 30000
    ob_clear_pct: float = 0.006

    trade_hours_only: bool = False
    trade_hours: str = "6,11,15,16,18"
    trade_log: str = "explosion_trades.csv"


# ==============================
# PER-COIN STATE
# ==============================
def make_coin_state(cfg):
    return {
        "bid": 0.0, "ask": 0.0, "mid": 0.0,
        "bar_start": time.time(),
        "bar_buy_vol": 0.0, "bar_sell_vol": 0.0,
        "bar_high": 0.0, "bar_low": float("inf"),
        "bar_open": 0.0, "bar_close": 0.0,
        "bar_volumes": deque(maxlen=cfg.vol_window_bars),
        "ob_bids": [], "ob_asks": [],
        "ob_bid_wall": None, "ob_ask_wall": None,
        "ob_bid_ratio": 0.5,
        "ob_clear_long": False, "ob_clear_short": False,
        "ob_last_ts": 0.0,
        "oi": 0.0, "oi_change_pct": 0.0,
        "funding_rate": 0.0,
        "ls_ratio": 1.0, "ls_long_pct": 0.5, "ls_short_pct": 0.5,
        "taker_ratio": 1.0,
        "metrics_last_ts": 0.0,
        "last_score": 0,
        "last_signals": {},
        "active_trade": None,
        "wins": 0, "losses": 0,
        "last_exit_time": 0.0,
        "ws_trade": "INIT",
        "history": deque(maxlen=5),
        "skip_log": deque(maxlen=5),
        "vol_ratio": 0.0,
        "vol_imb": 0.0,
    }


# ==============================
# GLOBAL STATE
# ==============================
lock = asyncio.Lock()
coins: Dict[str, dict] = {}
global_state = {
    "balance": 500.0,
    "fees_paid": 0.0,
    "telemetry": {},
    "brain_log": [],
    "brain_wr_recent": 0.0,
    "brain_wr_all": 0.0,
    "brain_n": 0,
}


def now_ts():
    return time.time()


def telemetry_hit(k):
    global_state["telemetry"][k] = global_state["telemetry"].get(k, 0) + 1


def _append_csv(path, row):
    exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)


# ==============================
# ORDER BOOK
# ==============================
def analyze_ob(cfg, cs):
    bids = cs["ob_bids"]
    asks = cs["ob_asks"]
    mid = cs["mid"]
    if not bids or not asks or mid <= 0:
        return

    total_bid = sum(s for _, s in bids[:10])
    total_ask = sum(s for _, s in asks[:10])
    total = total_bid + total_ask
    cs["ob_bid_ratio"] = total_bid / total if total > 0 else 0.5

    ask_wall = None
    for price, size in asks:
        if size >= cfg.ob_wall_min:
            ask_wall = (price, size, (price - mid) / mid)
            break
    cs["ob_ask_wall"] = ask_wall

    bid_wall = None
    for price, size in bids:
        if size >= cfg.ob_wall_min:
            bid_wall = (price, size, (mid - price) / mid)
            break
    cs["ob_bid_wall"] = bid_wall

    cs["ob_clear_long"] = (ask_wall is None or ask_wall[2] >= cfg.ob_clear_pct)
    cs["ob_clear_short"] = (bid_wall is None or bid_wall[2] >= cfg.ob_clear_pct)


# ==============================
# SIGNALS
# ==============================
def compute_signals(cfg, cs, side, vol_ratio, vol_imb):
    signals = {}
    score = 0

    signals["1_vol"] = f"YES {vol_ratio:.1f}x"
    score += 1

    if (side == "LONG" and vol_imb > cfg.imb_thr) or (side == "SHORT" and vol_imb < -cfg.imb_thr):
        signals["2_flow"] = f"YES {vol_imb:+.2f}"
        score += 1
    else:
        signals["2_flow"] = f"NO  {vol_imb:+.2f}"

    ob_ok = cs["ob_clear_long"] if side == "LONG" else cs["ob_clear_short"]
    if cs["ob_last_ts"] == 0:
        signals["3_ob"] = "--- loading"
    elif ob_ok:
        signals["3_ob"] = "YES clear"
        score += 1
    else:
        signals["3_ob"] = "NO  blocked"

    ls = cs["ls_ratio"]
    if side == "LONG" and ls < 0.7:
        signals["4_crowd"] = f"YES ls={ls:.2f} short squeeze"
        score += 1
    elif side == "SHORT" and ls > 1.4:
        signals["4_crowd"] = f"YES ls={ls:.2f} long squeeze"
        score += 1
    else:
        signals["4_crowd"] = f"NO  ls={ls:.2f}"

    tr = cs["taker_ratio"]
    if (side == "LONG" and tr > 1.1) or (side == "SHORT" and tr < 0.9):
        signals["5_taker"] = f"YES {tr:.2f}"
        score += 1
    else:
        signals["5_taker"] = f"NO  {tr:.2f}"

    return score, signals


# ==============================
# BAR CLOSE
# ==============================
def close_bar(cfg, sym):
    cs = coins[sym]
    total_vol = cs["bar_buy_vol"] + cs["bar_sell_vol"]
    if total_vol <= 0:
        _reset_bar(cs); return

    cs["bar_volumes"].append(total_vol)
    vol_imb = (cs["bar_buy_vol"] - cs["bar_sell_vol"]) / total_vol
    cs["vol_imb"] = vol_imb

    if len(cs["bar_volumes"]) < 4:
        _reset_bar(cs); return

    vol_ma = sum(cs["bar_volumes"]) / len(cs["bar_volumes"])
    vol_ratio = total_vol / vol_ma if vol_ma > 0 else 0
    cs["vol_ratio"] = vol_ratio

    # Hour filter
    current_hour = datetime.datetime.utcnow().hour
    allowed = [int(h.strip()) for h in cfg.trade_hours.split(",")]
    if cfg.trade_hours_only and current_hour not in allowed:
        _reset_bar(cs); return

    if (cs["active_trade"] is None and
            vol_ratio >= cfg.vol_thr and
            abs(vol_imb) >= cfg.imb_thr and
            now_ts() - cs["last_exit_time"] > 60):

        side = "LONG" if vol_imb > 0 else "SHORT"
        entry = float(cs["bar_close"])
        score, signals = compute_signals(cfg, cs, side, vol_ratio, vol_imb)
        cs["last_score"] = score
        cs["last_signals"] = signals

        if score >= cfg.min_score:
            tp_pct = cfg.tp_pct
            if side == "LONG" and cs["ob_ask_wall"]:
                dist = cs["ob_ask_wall"][2]
                if dist < cfg.tp_pct * 1.5:
                    tp_pct = max(dist * 0.85, cfg.sl_pct * 1.5)
            elif side == "SHORT" and cs["ob_bid_wall"]:
                dist = cs["ob_bid_wall"][2]
                if dist < cfg.tp_pct * 1.5:
                    tp_pct = max(dist * 0.85, cfg.sl_pct * 1.5)

            tp = entry * (1 + tp_pct) if side == "LONG" else entry * (1 - tp_pct)
            sl = entry * (1 - cfg.sl_pct) if side == "LONG" else entry * (1 + cfg.sl_pct)
            liq = entry * (1 - 1/cfg.leverage) if side == "LONG" else entry * (1 + 1/cfg.leverage)

            cs["active_trade"] = {
                "sym": sym, "side": side, "entry": entry,
                "tp": tp, "sl": sl, "liq": liq, "tp_pct": tp_pct,
                "entry_time": now_ts(), "vol_ratio": vol_ratio,
                "vol_imb": vol_imb, "score": score, "signals": signals,
                "ls_ratio": cs["ls_ratio"], "taker_ratio": cs["taker_ratio"],
                "oi_change": cs["oi_change_pct"], "funding": cs["funding_rate"],
            }
            cs["history"].append({
                "time": datetime.datetime.utcnow().strftime("%H:%M:%S"),
                "side": side, "entry": entry,
                "vol_ratio": round(vol_ratio, 1), "score": score,
            })
            telemetry_hit(f"entry_{sym[:3]}_{side.lower()}")
        else:
            cs["skip_log"].append(
                f"{datetime.datetime.utcnow().strftime('%H:%M')} {side} vol={vol_ratio:.1f}x score={score}/{cfg.min_score}"
            )
            telemetry_hit("low_score")

    _reset_bar(cs)


def _reset_bar(cs):
    cs.update({
        "bar_start": now_ts(), "bar_buy_vol": 0.0, "bar_sell_vol": 0.0,
        "bar_high": 0.0, "bar_low": float("inf"),
        "bar_open": 0.0, "bar_close": 0.0,
    })


# ==============================
# TRADE MANAGEMENT
# ==============================
def manage_trades(cfg):
    for sym, cs in coins.items():
        t = cs["active_trade"]
        if not t:
            continue

        side = t["side"]
        bid, ask = float(cs["bid"]), float(cs["ask"])
        hold_s = now_ts() - float(t["entry_time"])
        entry = float(t["entry"])
        ex = bid if side == "LONG" else ask

        liq_hit = (side == "LONG" and bid <= t["liq"]) or (side == "SHORT" and ask >= t["liq"])
        tp_hit = (side == "LONG" and ask >= t["tp"]) or (side == "SHORT" and bid <= t["tp"])
        sl_hit = (side == "LONG" and bid <= t["sl"]) or (side == "SHORT" and ask >= t["sl"])

        reason = None
        if liq_hit: ex = float(t["liq"]); reason = "LIQUIDATED"
        elif tp_hit: ex = float(t["tp"]); reason = "TP"
        elif sl_hit: ex = float(t["sl"]); reason = "SL"
        elif hold_s >= cfg.max_hold_s: reason = "TIMEOUT"

        if reason:
            raw = (ex - entry) / entry if side == "LONG" else (entry - ex) / entry
            levered = raw * cfg.leverage
            fee = cfg.taker_fee_rate * 2
            net = max(levered - fee, -1.0)
            pnl = net * cfg.trade_size

            global_state["balance"] += pnl
            global_state["fees_paid"] += fee * cfg.trade_size
            if pnl > 0: cs["wins"] += 1
            else: cs["losses"] += 1
            cs["last_exit_time"] = now_ts()

            _append_csv(cfg.trade_log, {
                "ts_utc": datetime.datetime.utcnow().isoformat(),
                "symbol": sym.upper(),
                "side": side, "entry": entry, "exit": ex,
                "exit_reason": reason, "hold_s": round(hold_s),
                "leverage": cfg.leverage,
                "raw_pct": round(raw * 100, 4),
                "levered_pct": round(levered * 100, 4),
                "net_pnl": round(net, 6), "pnl_$": round(pnl, 4),
                "fees_$": round(fee * cfg.trade_size, 4),
                "score": t["score"],
                "vol_ratio": round(float(t["vol_ratio"]), 2),
                "vol_imb": round(float(t["vol_imb"]), 3),
                "ls_ratio": round(float(t["ls_ratio"]), 3),
                "taker_ratio": round(float(t["taker_ratio"]), 3),
            })
            cs["active_trade"] = None
            adjust_brain(cfg)


# ==============================
# BRAIN
# ==============================
def adjust_brain(cfg):
    try:
        if not os.path.exists(cfg.trade_log):
            return
        import pandas as pd
        df = pd.read_csv(cfg.trade_log)
        if len(df) < 5:
            return
        df["win"] = df["pnl_$"] > 0
        recent = df.tail(10)
        recent_wr = recent["win"].mean()
        all_wr = df["win"].mean()
        n = len(df)
        brain_log = []

        if recent_wr < 0.35 and cfg.vol_thr < 8.0:
            old = cfg.vol_thr
            cfg.vol_thr = min(cfg.vol_thr + 0.5, 8.0)
            brain_log.append(f"WR={recent_wr*100:.0f}%<35% → vol_thr {old}→{cfg.vol_thr}")
            telemetry_hit("brain_raise_vol")
        elif recent_wr > 0.65 and cfg.vol_thr > 3.0:
            old = cfg.vol_thr
            cfg.vol_thr = max(cfg.vol_thr - 0.5, 3.0)
            brain_log.append(f"WR={recent_wr*100:.0f}%>65% → vol_thr {old}→{cfg.vol_thr}")
            telemetry_hit("brain_lower_vol")

        if n >= 15 and "ts_utc" in df.columns:
            df["hour"] = pd.to_datetime(df["ts_utc"]).dt.hour
            hour_stats = df.groupby("hour")["win"].agg(["count", "mean"])
            bad_hours = hour_stats[(hour_stats["count"] >= 3) & (hour_stats["mean"] < 0.30)].index.tolist()
            current_allowed = [int(h) for h in cfg.trade_hours.split(",")]
            new_allowed = [h for h in current_allowed if h not in bad_hours]
            if len(new_allowed) >= 2 and new_allowed != current_allowed:
                removed = [h for h in current_allowed if h not in new_allowed]
                cfg.trade_hours = ",".join(str(h) for h in new_allowed)
                brain_log.append(f"Removed bad hours {removed}")
                telemetry_hit("brain_remove_hour")

        global_state["brain_log"] = brain_log
        global_state["brain_wr_recent"] = round(recent_wr * 100, 1)
        global_state["brain_wr_all"] = round(all_wr * 100, 1)
        global_state["brain_n"] = n
    except Exception:
        telemetry_hit("brain_err")


# ==============================
# WEBSOCKETS
# ==============================
async def aggtrade_ws(sym, stop):
    url = f"wss://fstream.binance.com/ws/{sym}@aggTrade"
    backoff = 1
    cs = coins[sym]
    while not stop.is_set():
        try:
            cs["ws_trade"] = "CONNECTING"
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                cs["ws_trade"] = "LIVE"; backoff = 1
                while not stop.is_set():
                    d = json.loads(await ws.recv())
                    price, qty, taker_buy = float(d["p"]), float(d["q"]), not bool(d["m"])
                    async with lock:
                        if cs["bar_open"] == 0.0: cs["bar_open"] = price
                        cs["bar_close"] = price
                        cs["bar_high"] = max(cs["bar_high"], price)
                        cs["bar_low"] = min(cs["bar_low"], price)
                        if taker_buy: cs["bar_buy_vol"] += qty
                        else: cs["bar_sell_vol"] += qty
        except Exception:
            cs["ws_trade"] = f"ERR({backoff}s)"
            await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)


async def depth_ws(sym, stop):
    # Try WebSocket first, fall back to REST polling
    url = f"wss://fstream.binance.com/ws/{sym}@bookTicker"
    rest_url = f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={sym.upper()}"
    cs = coins[sym]
    backoff = 1
    ws_failed = 0
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                ws_failed = 0; backoff = 1
                while not stop.is_set():
                    d = json.loads(await ws.recv())
                    async with lock:
                        cs["bid"] = float(d["b"])
                        cs["ask"] = float(d["a"])
                        cs["mid"] = (cs["bid"] + cs["ask"]) / 2.0
        except Exception:
            ws_failed += 1
            if ws_failed >= 3:
                # Fall back to REST polling
                while not stop.is_set():
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(rest_url, timeout=aiohttp.ClientTimeout(total=3)) as r:
                                d = await r.json()
                                async with lock:
                                    cs["bid"] = float(d["bidPrice"])
                                    cs["ask"] = float(d["askPrice"])
                                    cs["mid"] = (cs["bid"] + cs["ask"]) / 2.0
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)
            await asyncio.sleep(backoff); backoff = min(backoff * 2, 10)


async def ob_poller(cfg, sym, stop):
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={sym.upper()}&limit=20"
    cs = coins[sym]
    while not stop.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as r:
                    data = await r.json()
                    bids = sorted([(float(p), float(s)) for p, s in data.get("bids", [])],
                                  key=lambda x: x[0], reverse=True)
                    asks = sorted([(float(p), float(s)) for p, s in data.get("asks", [])],
                                  key=lambda x: x[0])
                    async with lock:
                        cs["ob_bids"] = bids
                        cs["ob_asks"] = asks
                        cs["ob_last_ts"] = now_ts()
                        analyze_ob(cfg, cs)
        except Exception:
            telemetry_hit(f"ob_err_{sym[:3]}")
        await asyncio.sleep(cfg.ob_poll_s)


async def metrics_poller(cfg, sym, stop):
    s = sym.upper()
    urls = {
        "oi": f"https://fapi.binance.com/fapi/v1/openInterest?symbol={s}",
        "funding": f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={s}&limit=1",
        "ls": f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={s}&period=5m&limit=1",
        "taker": f"https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={s}&period=5m&limit=1",
    }
    cs = coins[sym]
    while not stop.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(urls["oi"], timeout=aiohttp.ClientTimeout(total=5)) as r:
                    d = await r.json()
                    new_oi = float(d["openInterest"])
                    async with lock:
                        prev = cs["oi"] if cs["oi"] > 0 else new_oi
                        cs["oi_change_pct"] = (new_oi - prev) / prev if prev > 0 else 0
                        cs["oi"] = new_oi

                async with session.get(urls["funding"], timeout=aiohttp.ClientTimeout(total=5)) as r:
                    d = await r.json()
                    if d:
                        async with lock:
                            cs["funding_rate"] = float(d[0]["fundingRate"])

                async with session.get(urls["ls"], timeout=aiohttp.ClientTimeout(total=5)) as r:
                    d = await r.json()
                    if d:
                        async with lock:
                            cs["ls_ratio"] = float(d[0]["longShortRatio"])
                            cs["ls_long_pct"] = float(d[0]["longAccount"])
                            cs["ls_short_pct"] = float(d[0]["shortAccount"])

                async with session.get(urls["taker"], timeout=aiohttp.ClientTimeout(total=5)) as r:
                    d = await r.json()
                    if d:
                        async with lock:
                            cs["taker_ratio"] = float(d[0]["buySellRatio"])
                            cs["metrics_last_ts"] = now_ts()
        except Exception:
            telemetry_hit(f"metrics_err_{sym[:3]}")
        await asyncio.sleep(cfg.metrics_poll_s)


async def bar_timer(cfg, stop):
    while not stop.is_set():
        n = datetime.datetime.utcnow()
        mins = n.minute % (cfg.bar_seconds // 60)
        wait = (cfg.bar_seconds // 60 - mins) * 60 - n.second
        if wait <= 0: wait = cfg.bar_seconds
        await asyncio.sleep(min(wait, cfg.bar_seconds))
        async with lock:
            for sym in SYMBOLS:
                close_bar(cfg, sym)


async def engine_loop(cfg, stop):
    while not stop.is_set():
        try:
            async with lock:
                manage_trades(cfg)
        except Exception:
            telemetry_hit("engine_err")
        await asyncio.sleep(0.5)


# ==============================
# PREWARM
# ==============================
async def prewarm(cfg):
    import glob as g
    import pandas as pd
    for sym in SYMBOLS:
        SYM = sym.upper()
        files = sorted(g.glob(f"{SYM}-aggTrades-*.csv") +
                       g.glob(f"ain_data/{SYM}-aggTrades-*.csv") +
                       g.glob(f"other_coins/{SYM}-aggTrades-*.csv"))
        if not files:
            print(f"[PREWARM] {SYM}: no data, warming live")
            continue
        latest = files[-1]
        try:
            df = pd.read_csv(latest, header=0)
            df.columns = ["agg_trade_id","price","qty","first_trade_id",
                          "last_trade_id","timestamp","is_buyer_maker"]
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            candles = df["qty"].resample(f"{cfg.bar_seconds}s").sum()
            vols = list(candles.dropna().values[-cfg.vol_window_bars:])
            for v in vols:
                coins[sym]["bar_volumes"].append(float(v))
            print(f"[PREWARM] {SYM}: {len(vols)} bars loaded")
        except Exception as e:
            print(f"[PREWARM] {SYM}: failed - {e}")


# ==============================
# DASHBOARD
# ==============================
def make_dashboard(cfg):
    layout = Layout()
    layout.split_column(
        Layout(name="h", size=3),
        Layout(name="coins", ratio=1),
        Layout(name="status", size=8),
        Layout(name="f", size=6),
    )

    total_wins = sum(coins[s]["wins"] for s in SYMBOLS)
    total_losses = sum(coins[s]["losses"] for s in SYMBOLS)
    total = total_wins + total_losses
    wr = total_wins / total * 100 if total > 0 else 0
    bal = global_state["balance"]

    layout["h"].update(Panel(
        f"[cyan]MULTI-COIN EXPLOSION RIDER v4[/] | "
        f"Balance: [yellow]${bal:.2f}[/] | "
        f"WR: [bold]{wr:.1f}%[/] | Trades: {total} | "
        f"[magenta]{cfg.leverage}x[/] | "
        f"Coins: {len(SYMBOLS)} | "
        f"Fees: [red]${global_state['fees_paid']:.3f}[/]",
        title="PAPER TRADING"
    ))

    # One panel per coin
    layout["coins"].split_row(*[Layout(name=sym) for sym in SYMBOLS])

    for sym in SYMBOLS:
        cs = coins[sym]
        SYM = sym.upper()[:6]
        t = Table(expand=True, box=None)

        tv = cs["bar_buy_vol"] + cs["bar_sell_vol"]
        vi = (cs["bar_buy_vol"] - cs["bar_sell_vol"]) / tv if tv > 0 else 0
        vm = sum(cs["bar_volumes"]) / len(cs["bar_volumes"]) if cs["bar_volumes"] else 1
        vr = tv / vm if vm > 0 else 0
        bars_ready = len(cs["bar_volumes"])
        bar_age = now_ts() - cs["bar_start"]

        vc = "green" if vr >= cfg.vol_thr else ("yellow" if vr >= cfg.vol_thr * 0.6 else "white")
        ic = "green" if abs(vi) >= cfg.imb_thr else "white"

        total_c = cs["wins"] + cs["losses"]
        wr_c = cs["wins"] / total_c * 100 if total_c > 0 else 0
        wrc = "green" if wr_c >= 50 else ("yellow" if wr_c >= 40 else "red")

        t.add_row("WS", f"{cs['ws_trade']}")
        t.add_row("Bid/Ask", f"{cs['bid']:.5f}/{cs['ask']:.5f}")
        t.add_row("WR", f"[{wrc}]{wr_c:.0f}%[/] ({cs['wins']}W/{cs['losses']}L)")
        t.add_row("Bar", f"{bar_age:.0f}s | {bars_ready}/{cfg.vol_window_bars}")
        t.add_row("VolR", f"[{vc}]{vr:.2f}x[/] need {cfg.vol_thr}x")
        t.add_row("Imb", f"[{ic}]{vi:+.3f}[/]")
        t.add_row("Score", f"{cs['last_score']}/5")
        t.add_row("L/S", f"{cs['ls_ratio']:.2f} {cs['ls_long_pct']*100:.0f}%L")
        t.add_row("Taker", f"{cs['taker_ratio']:.2f}")

        ob_col = "green" if cs["ob_clear_long"] else "red"
        t.add_row("OB Long", f"[{ob_col}]{'CLEAR' if cs['ob_clear_long'] else 'BLOCKED'}[/]")

        at = cs["active_trade"]
        if at:
            hold = now_ts() - float(at["entry_time"])
            mid = cs["mid"]
            raw = (mid - float(at["entry"])) / float(at["entry"]) if at["side"] == "LONG" else (float(at["entry"]) - mid) / float(at["entry"])
            unreal = (raw * cfg.leverage - cfg.taker_fee_rate * 2) * cfg.trade_size
            col = "green" if unreal > 0 else "red"
            t.add_row("TRADE", f"{at['side']} {at['entry']:.5f}")
            t.add_row("PnL", f"[{col}]${unreal:+.3f}[/] {hold:.0f}s")
            t.add_row("TP/SL", f"{at['tp']:.5f}/{at['sl']:.5f}")
        else:
            t.add_row("TRADE", "NONE")

        layout[sym].update(Panel(t, title=f"{SYM}"))

    # Status
    sl = []
    current_hour = datetime.datetime.utcnow().hour
    allowed = [int(h) for h in cfg.trade_hours.split(",")]
    in_window = current_hour in allowed
    next_window = min((h for h in allowed if h > current_hour), default=allowed[0])
    mins_to_next = ((next_window - current_hour) * 60) - datetime.datetime.utcnow().minute

    sl.append(f"[green]HUNTING 24/7[/] — hour {current_hour} UTC | Watching {len(SYMBOLS)} coins for {cfg.vol_thr}x+ explosions")

    sl.append(f"   Allowed hours: {cfg.trade_hours} | vol_thr={cfg.vol_thr}x | min_score={cfg.min_score}/5")

    # Active trades summary
    active = [(sym, coins[sym]["active_trade"]) for sym in SYMBOLS if coins[sym]["active_trade"]]
    if active:
        for sym, at in active:
            col = "green" if at["side"] == "LONG" else "red"
            sl.append(f"   [{col}]IN TRADE {sym.upper()[:6]} {at['side']}[/] entry={at['entry']:.5f} score={at['score']}/5")

    # Brain
    bn = global_state["brain_n"]
    bwr = global_state["brain_wr_recent"]
    bwa = global_state["brain_wr_all"]
    bc = "green" if bwr >= 50 else ("yellow" if bwr >= 35 else "red")
    sl.append(f"   [cyan]🧠 BRAIN[/] n={bn} | recent=[{bc}]{bwr}%[/] | all={bwa}% | vol_thr={cfg.vol_thr}x")
    for bl in global_state.get("brain_log", [])[-1:]:
        sl.append(f"   [yellow]🧠 ADJUSTED: {bl}[/]")

    # Recent skips
    all_skips = []
    for sym in SYMBOLS:
        for sk in list(coins[sym]["skip_log"])[-1:]:
            all_skips.append(f"{sym[:3].upper()}: {sk}")
    if all_skips:
        sl.append(f"   [dim]Skipped: {' | '.join(all_skips[-3:])}[/]")

    layout["status"].update(Panel("\n".join(sl), title="STATUS"))

    # History table
    h = Table(expand=True, box=None)
    h.add_row("[bold]Time[/]","[bold]Coin[/]","[bold]Side[/]","[bold]Entry[/]","[bold]VolR[/]","[bold]Score[/]")
    all_history = []
    for sym in SYMBOLS:
        for e in coins[sym]["history"]:
            all_history.append({**e, "sym": sym.upper()[:6]})
    all_history = sorted(all_history, key=lambda x: x["time"], reverse=True)[:5]
    for e in all_history:
        col = "green" if e["side"] == "LONG" else "red"
        h.add_row(e["time"], e["sym"], f"[{col}]{e['side']}[/]",
                  f"{e['entry']:.5f}", f"{e['vol_ratio']}x", f"{e['score']}/5")
    layout["f"].update(Panel(h, title="Recent Entries (all coins)"))
    return layout


# ==============================
# MAIN
# ==============================
async def run_app(cfg):
    global coins
    for sym in SYMBOLS:
        coins[sym] = make_coin_state(cfg)
    global_state["balance"] = cfg.initial_balance

    await prewarm(cfg)
    stop = asyncio.Event()

    def request_stop(*_): stop.set()
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try: loop.add_signal_handler(sig, request_stop)
            except NotImplementedError: pass
    except Exception: pass

    tasks = []
    for sym in SYMBOLS:
        tasks.append(asyncio.create_task(aggtrade_ws(sym, stop)))
        tasks.append(asyncio.create_task(depth_ws(sym, stop)))
        tasks.append(asyncio.create_task(ob_poller(cfg, sym, stop)))
        tasks.append(asyncio.create_task(metrics_poller(cfg, sym, stop)))

    tasks.append(asyncio.create_task(bar_timer(cfg, stop)))
    tasks.append(asyncio.create_task(engine_loop(cfg, stop)))

    try:
        with Live(make_dashboard(cfg), refresh_per_second=1, screen=True) as live:
            while not stop.is_set():
                await asyncio.sleep(0.5)
                async with lock:
                    live.update(make_dashboard(cfg))
    finally:
        stop.set()
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print(f"\nDone. Balance:${global_state['balance']:.2f}")


if __name__ == "__main__":
    cfg = Config()
    print(f"Multi-Coin Explosion Rider v4")
    print(f"Coins: {', '.join(s.upper() for s in SYMBOLS)}")
    print(f"Entry: vol>{cfg.vol_thr}x + score>={cfg.min_score}/5 | Hours: {cfg.trade_hours}")
    print(f"TP={cfg.tp_pct*100}% SL={cfg.sl_pct*100}% @ {cfg.leverage}x")
    asyncio.run(run_app(cfg))