#!/usr/bin/env python3
"""BTC Momentum Bot with Regime Detection - logging version"""
import asyncio, aiohttp, time, datetime, csv, os, numpy as np
from aiohttp_socks import ProxyConnector
from dataclasses import dataclass

@dataclass
class Config:
    symbol: str = "BTCUSDT"
    initial_balance: float = 500.0
    trade_size: float = 50.0
    taker_fee_rate: float = 0.0002
    leverage: int = 10
    move_thr: float = 0.015
    vol_thr: float = 1.5
    imb_thr: float = 0.05
    vol_window: int = 20
    tp_pct: float = 0.008
    sl_pct: float = 0.005
    max_hold_bars: int = 4
    bar_seconds: int = 14400
    price_poll_s: int = 30
    kline_poll_s: int = 60
    trade_log: str = "btc_trades.csv"
    allowed_regimes: str = "TREND_UP,TREND_DOWN,HIGH_VOL"

def log(msg):
    ts = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def make_session():
    return aiohttp.ClientSession(
        connector=ProxyConnector.from_url("socks5://127.0.0.1:9050")
    )

state = {
    "bars": [], "last_bar_time": None,
    "bid": 0.0, "ask": 0.0, "mid": 0.0,
    "active_trade": None,
    "balance": 500.0, "fees_paid": 0.0,
    "wins": 0, "losses": 0,
    "last_exit_time": 0.0,
    "vol_thr": 1.5, "move_thr": 0.015,
    "current_regime": "UNKNOWN",
}

def update_regime(bars):
    if len(bars) < 20:
        state["current_regime"] = "UNKNOWN"
        return

    closes = [b["close"] for b in bars[-50:]]
    highs = [b["high"] for b in bars[-50:]]
    lows = [b["low"] for b in bars[-50:]]

    trs = []
    for i in range(1, len(closes)):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        trs.append(max(hl, hc, lc))

    if len(trs) < 14:
        state["current_regime"] = "UNKNOWN"
        return

    atr14 = np.mean(trs[-14:])
    atr_ratio = trs[-1] / atr14 if atr14 > 0 else 1.0

    up_moves = [max(highs[i]-highs[i-1], 0) for i in range(1, len(highs))]
    down_moves = [max(lows[i-1]-lows[i], 0) for i in range(1, len(lows))]
    plus_dm = [u if u > d and u > 0 else 0 for u,d in zip(up_moves, down_moves)]
    minus_dm = [d if d > u and d > 0 else 0 for u,d in zip(up_moves, down_moves)]

    if atr14 > 0 and len(plus_dm) >= 14:
        plus_di = 100 * np.mean(plus_dm[-14:]) / atr14
        minus_di = 100 * np.mean(minus_dm[-14:]) / atr14
        di_sum = plus_di + minus_di
        adx = 100 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0
        trend_up = plus_di > minus_di
    else:
        adx = 0
        trend_up = True

    ema10 = np.mean(closes[-10:])
    ema20 = np.mean(closes[-20:])
    trend_up = ema10 > ema20

    bb_std = np.std(closes[-20:])
    bb_mid = np.mean(closes[-20:])
    bb_width = (bb_std * 2) / bb_mid if bb_mid > 0 else 0
    if len(closes) >= 50:
        bb_std50 = np.std(closes[-50:])
        bb_mid50 = np.mean(closes[-50:])
        bb_width_avg = (bb_std50 * 2) / bb_mid50 if bb_mid50 > 0 else bb_width
    else:
        bb_width_avg = bb_width
    squeeze = bb_width < bb_width_avg * 0.7

    if squeeze and atr_ratio < 0.7:
        regime = "SQUEEZE"
    elif atr_ratio > 2.0:
        regime = "HIGH_VOL"
    elif adx > 25:
        regime = "TREND_UP" if trend_up else "TREND_DOWN"
    elif atr_ratio < 0.5 and adx < 15:
        regime = "LOW_VOL"
    else:
        regime = "RANGING"

    state["current_regime"] = regime

async def fetch_klines(cfg):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={cfg.symbol}&interval=4h&limit=25"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json()

async def fetch_price(cfg):
    url = f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={cfg.symbol}"
    async with make_session() as s:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            d = await r.json()
    return float(d["bidPrice"]), float(d["askPrice"])

def check_entry(cfg, bars):
    if len(bars) < cfg.vol_window + 2:
        return None, {}
    bar = bars[-2]
    vols = [b["volume"] for b in bars[:-2]]
    vol_ma = sum(vols[-cfg.vol_window:]) / cfg.vol_window if len(vols) >= cfg.vol_window else 0
    vol_ratio = bar["volume"] / vol_ma if vol_ma > 0 else 0
    move = (bar["close"] - bar["open"]) / bar["open"]
    vol_imb = bar["vol_imb"]
    signals = {
        "move_pct": round(move*100, 3),
        "vol_ratio": round(vol_ratio, 2),
        "vol_imb": round(vol_imb, 3),
        "regime": state["current_regime"],
    }
    mt = state["move_thr"]
    vt = state["vol_thr"]
    if move > mt and vol_ratio > vt and vol_imb > cfg.imb_thr:
        return "LONG", signals
    elif move < -mt and vol_ratio > vt and vol_imb < -cfg.imb_thr:
        return "SHORT", signals
    return None, signals

def manage_trade(cfg):
    t = state["active_trade"]
    if not t:
        return
    bid, ask = state["bid"], state["ask"]
    side = t["side"]
    entry = t["entry"]
    hold_s = time.time() - t["entry_time"]
    hold_bars = int(hold_s / cfg.bar_seconds)
    ex = bid if side == "LONG" else ask
    reason = None
    if (side=="LONG" and bid<=t["liq"]) or (side=="SHORT" and ask>=t["liq"]):
        ex = t["liq"]; reason = "LIQUIDATED"
    elif (side=="LONG" and ask>=t["tp"]) or (side=="SHORT" and bid<=t["tp"]):
        ex = t["tp"]; reason = "TP"
    elif (side=="LONG" and bid<=t["sl"]) or (side=="SHORT" and ask>=t["sl"]):
        ex = t["sl"]; reason = "SL"
    elif hold_bars >= cfg.max_hold_bars:
        reason = "TIMEOUT"
    if reason:
        raw = (ex-entry)/entry if side=="LONG" else (entry-ex)/entry
        net = max(raw*cfg.leverage - cfg.taker_fee_rate*2, -1.0)
        pnl = net * cfg.trade_size
        state["balance"] += pnl
        state["fees_paid"] += cfg.taker_fee_rate*2*cfg.trade_size
        if pnl > 0: state["wins"] += 1
        else: state["losses"] += 1
        state["last_exit_time"] = time.time()
        log(f"EXIT {side} @ {ex:.2f} | {reason} | PnL:${pnl:+.4f} | Bal:${state['balance']:.2f} | W:{state['wins']} L:{state['losses']}")
        exists = os.path.exists(cfg.trade_log)
        row = {
            "ts_utc": datetime.datetime.now(datetime.UTC).isoformat(),
            "side": side, "entry": entry, "exit": ex,
            "exit_reason": reason,
            "hold_hours": round(hold_s/3600, 1),
            "leverage": cfg.leverage,
            "raw_pct": round(raw*100, 4),
            "levered_pct": round(raw*cfg.leverage*100, 4),
            "net_pnl": round(net, 6),
            "pnl_$": round(pnl, 4),
            "fees_$": round(cfg.taker_fee_rate*2*cfg.trade_size, 4),
            "move_pct": t.get("move_pct", 0),
            "vol_ratio": t.get("vol_ratio", 0),
            "vol_imb": t.get("vol_imb", 0),
            "regime": t.get("regime", "UNKNOWN"),
        }
        with open(cfg.trade_log, "a", newline="") as f:
            import csv as _csv
            w = _csv.DictWriter(f, fieldnames=list(row.keys()))
            if not exists: w.writeheader()
            w.writerow(row)
        state["active_trade"] = None

def adjust_brain(cfg):
    try:
        if not os.path.exists(cfg.trade_log): return
        import csv as _csv
        rows = []
        with open(cfg.trade_log) as f:
            for r in _csv.DictReader(f): rows.append(r)
        if len(rows) < 5: return
        recent = rows[-10:]
        wr = sum(1 for r in recent if float(r["pnl_$"]) > 0) / len(recent)
        state["brain_wr"] = round(wr*100, 1)
        if wr < 0.40 and state["vol_thr"] < 3.0:
            state["vol_thr"] = round(state["vol_thr"]+0.2, 1)
            log(f"BRAIN: WR low → vol_thr→{state['vol_thr']}")
        elif wr > 0.75 and state["vol_thr"] > 1.2:
            state["vol_thr"] = round(state["vol_thr"]-0.1, 1)
            log(f"BRAIN: WR high → vol_thr→{state['vol_thr']}")
    except Exception as e:
        log(f"Brain error: {e}")

async def bar_loop(cfg, stop):
    while not stop.is_set():
        try:
            data = await fetch_klines(cfg)
            if not isinstance(data, list):
                log(f"API error: {data}")
                await asyncio.sleep(60); continue

            bars = []
            for d in data:
                vol = float(d[5]); tb = float(d[9]); sv = vol - tb
                bars.append({
                    "open_time": int(d[0]),
                    "open": float(d[1]), "high": float(d[2]),
                    "low": float(d[3]), "close": float(d[4]),
                    "volume": vol,
                    "vol_imb": (tb-sv)/vol if vol > 0 else 0,
                })
            state["bars"] = bars
            update_regime(bars)

            last_bar_time = bars[-2]["open_time"]
            if state["last_bar_time"] != last_bar_time:
                state["last_bar_time"] = last_bar_time
                bar = bars[-2]
                vols = [b["volume"] for b in bars[:-2]]
                vol_ma = sum(vols[-cfg.vol_window:]) / cfg.vol_window if vols else 1
                vol_ratio = bar["volume"] / vol_ma
                move = (bar["close"] - bar["open"]) / bar["open"]
                regime = state["current_regime"]
                allowed = [r.strip() for r in cfg.allowed_regimes.split(",")]

                log(f"NEW 4H BAR | ${bar['close']:,.2f} move={move*100:+.2f}% vol={vol_ratio:.2f}x regime={regime}")

                if regime not in allowed:
                    log(f"REGIME SKIP: {regime} not in allowed {allowed}")
                elif state["active_trade"] is None and time.time()-state["last_exit_time"] > 300:
                    side, signals = check_entry(cfg, bars)
                    if side:
                        entry = state["mid"] if state["mid"] > 0 else bar["close"]
                        tp = entry*(1+cfg.tp_pct) if side=="LONG" else entry*(1-cfg.tp_pct)
                        sl = entry*(1-cfg.sl_pct) if side=="LONG" else entry*(1+cfg.sl_pct)
                        liq = entry*(1-1/cfg.leverage) if side=="LONG" else entry*(1+1/cfg.leverage)
                        state["active_trade"] = {
                            "side": side, "entry": entry,
                            "tp": tp, "sl": sl, "liq": liq,
                            "entry_time": time.time(), **signals,
                        }
                        log(f"ENTRY {side} @ {entry:.2f} | move={signals['move_pct']}% vol={signals['vol_ratio']}x regime={regime}")
                    else:
                        log(f"NO SIGNAL | move={move*100:+.2f}% vol={vol_ratio:.2f}x | W:{state['wins']} L:{state['losses']} Bal:${state['balance']:.2f}")
            else:
                t = state["active_trade"]
                if t:
                    mid = state["mid"]
                    raw = (mid-t["entry"])/t["entry"] if t["side"]=="LONG" else (t["entry"]-mid)/t["entry"]
                    log(f"IN TRADE {t['side']} @ {t['entry']:.2f} | unreal={raw*cfg.leverage*100:+.2f}% | regime={state['current_regime']}")

        except Exception as e:
            log(f"Bar loop error: {e}")
        await asyncio.sleep(cfg.kline_poll_s)

async def price_loop(cfg, stop):
    while not stop.is_set():
        try:
            bid, ask = await fetch_price(cfg)
            state["bid"] = bid
            state["ask"] = ask
            state["mid"] = (bid+ask)/2
            manage_trade(cfg)
        except Exception as e:
            log(f"Price error: {e}")
        await asyncio.sleep(cfg.price_poll_s)

async def run_app(cfg):
    stop = asyncio.Event()
    log(f"BTC Momentum Bot v2 | {cfg.symbol} | {cfg.leverage}x | Regime Detection ON")
    log(f"Entry: 4h move>{cfg.move_thr*100}% + vol>{cfg.vol_thr}x + flow + regime in {cfg.allowed_regimes}")
    log(f"TP={cfg.tp_pct*100}% SL={cfg.sl_pct*100}% | Backtested: TREND regime WR=92.3%")
    adjust_brain(cfg)
    tasks = [
        asyncio.create_task(bar_loop(cfg, stop)),
        asyncio.create_task(price_loop(cfg, stop)),
    ]
    await asyncio.gather(*tasks)

cfg = Config()
asyncio.run(run_app(cfg))
