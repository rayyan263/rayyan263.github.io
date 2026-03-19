#!/usr/bin/env python3
"""
BTC 1H Dual Strategy Bot
Strategy 1: Momentum (move>0.8% + vol>1.5x + TREND regime)
Strategy 2: Mean Reversion (RSI<35 + BB oversold + fade big move)
Both with Kelly sizing and Brain
Backtested 750 days:
  Momentum: WR=67.3% $1,488 profit with Kelly
  Mean Rev: WR=63.9% $2,881 profit with Kelly
  Combined: $4,369 profit, 2.16 trades/day
Data: Kraken REST
"""
import asyncio, aiohttp, time, datetime, csv, os, numpy as np
from dataclasses import dataclass

@dataclass
class Config:
    kraken_pair: str = "XBTUSD"
    initial_balance: float = 500.0
    trade_size: float = 50.0
    taker_fee_rate: float = 0.0002
    leverage: int = 10
    # Momentum params
    mom_move_thr: float = 0.008
    mom_vol_thr: float = 1.5
    mom_imb_thr: float = 0.05
    mom_tp: float = 0.003
    mom_sl: float = 0.002
    mom_hold: int = 6
    # Mean reversion params
    rev_move_thr: float = 0.003
    rev_rsi_long: float = 35.0
    rev_rsi_short: float = 65.0
    rev_bb_long: float = 0.15
    rev_bb_short: float = 0.85
    rev_vol_thr: float = 1.2
    rev_tp: float = 0.003
    rev_sl: float = 0.002
    rev_hold: int = 8
    # General
    vol_window: int = 20
    bar_seconds: int = 3600
    price_poll_s: int = 30
    kline_poll_s: int = 60
    trade_log: str = "btc_trades.csv"
    allowed_regimes: str = "TREND_UP,TREND_DOWN,HIGH_VOL"

def log(msg):
    ts = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

state = {
    "bars": [], "last_bar_time": None,
    "bid": 0.0, "ask": 0.0, "mid": 0.0,
    "active_trade": None,
    "balance": 500.0, "fees_paid": 0.0,
    "wins": 0, "losses": 0,
    "last_exit_time": 0.0,
    "mom_vol_thr": 1.5,
    "current_regime": "UNKNOWN",
    # Indicators
    "rsi": 50.0,
    "bb_pct": 0.5,
    "vol_ratio": 0.0,
}

def update_indicators(bars):
    if len(bars) < 20:
        return
    closes = [b["close"] for b in bars[-51:]]
    highs = [b["high"] for b in bars[-51:]]
    lows = [b["low"] for b in bars[-51:]]
    vols = [b["volume"] for b in bars[-21:]]

    # ATR + ADX for regime
    trs = []
    for i in range(1, len(closes)):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        trs.append(max(hl, hc, lc))
    if len(trs) < 14:
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
    else:
        adx = 0
        plus_di = minus_di = 0
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
    bb_upper = bb_mid + bb_std * 2
    bb_lower = bb_mid - bb_std * 2
    bb_pct = (closes[-1] - bb_lower) / (bb_upper - bb_lower) if bb_upper != bb_lower else 0.5

    # RSI
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(d, 0) for d in deltas[-14:]]
    losses = [abs(min(d, 0)) for d in deltas[-14:]]
    avg_gain = np.mean(gains) if gains else 0
    avg_loss = np.mean(losses) if losses else 0.0001
    rsi = 100 - (100 / (1 + avg_gain/avg_loss))

    # Volume ratio
    vol_ma = np.mean(vols[:-1]) if len(vols) > 1 else 1
    vol_ratio = vols[-1] / vol_ma if vol_ma > 0 else 1.0

    # Regime
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
    state["rsi"] = round(rsi, 1)
    state["bb_pct"] = round(bb_pct, 3)
    state["vol_ratio"] = round(vol_ratio, 2)

def kelly_size(trade_log, balance, tp, sl, default=50.0,
               min_pct=0.05, max_pct=0.20):
    try:
        if not os.path.exists(trade_log): return default
        import csv as _csv
        rows = []
        with open(trade_log) as f:
            for r in _csv.DictReader(f): rows.append(r)
        if len(rows) < 10: return default
        recent = rows[-20:]
        wr = sum(1 for r in recent if float(r["pnl_$"]) > 0) / len(recent)
        rr = tp / sl
        kelly = wr - (1-wr)/rr
        half_kelly = max(0, kelly*0.5)
        pct = min(max(half_kelly, min_pct), max_pct)
        return round(balance * pct, 2)
    except:
        return default

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
        if wr < 0.40 and state["mom_vol_thr"] < 3.0:
            state["mom_vol_thr"] = round(state["mom_vol_thr"]+0.2, 1)
            log(f"BRAIN: WR={wr*100:.0f}% low → mom_vol_thr→{state['mom_vol_thr']}")
        elif wr > 0.80 and state["mom_vol_thr"] > 1.2:
            state["mom_vol_thr"] = round(state["mom_vol_thr"]-0.1, 1)
            log(f"BRAIN: WR={wr*100:.0f}% high → mom_vol_thr→{state['mom_vol_thr']}")
    except Exception as e:
        log(f"Brain error: {e}")

async def fetch_klines(cfg, session):
    url = f"https://api.kraken.com/0/public/OHLC?pair={cfg.kraken_pair}&interval=60"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
        d = await r.json()
    raw = d["result"]["XXBTZUSD"]
    bars = []
    for b in raw:
        ts, o, h, l, c, vwap, vol, count = b
        bars.append({
            "open_time": int(ts),
            "open": float(o), "high": float(h),
            "low": float(l), "close": float(c),
            "volume": float(vol),
        })
    return bars

async def fetch_price(cfg, session):
    url = f"https://api.kraken.com/0/public/Ticker?pair={cfg.kraken_pair}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        d = await r.json()
    ticker = d["result"]["XXBTZUSD"]
    return float(ticker["b"][0]), float(ticker["a"][0])

def check_momentum(cfg, bars):
    if len(bars) < cfg.vol_window + 2:
        return None, {}
    bar = bars[-2]
    vols = [b["volume"] for b in bars[:-2]]
    vol_ma = sum(vols[-cfg.vol_window:]) / cfg.vol_window if len(vols) >= cfg.vol_window else 0
    vol_ratio = bar["volume"] / vol_ma if vol_ma > 0 else 0
    move = (bar["close"] - bar["open"]) / bar["open"]
    taker_imb = state["vol_ratio"]
    signals = {
        "move_pct": round(move*100, 3),
        "vol_ratio": round(vol_ratio, 2),
        "strategy": "MOMENTUM",
        "regime": state["current_regime"],
    }
    mt = cfg.mom_move_thr
    vt = state["mom_vol_thr"]
    allowed = [r.strip() for r in cfg.allowed_regimes.split(",")]
    if state["current_regime"] not in allowed:
        return None, signals
    if move > mt and vol_ratio > vt:
        return "LONG", signals
    elif move < -mt and vol_ratio > vt:
        return "SHORT", signals
    return None, signals

def check_mean_reversion(cfg, bars):
    if len(bars) < 22:
        return None, {}
    bar = bars[-2]
    move = (bar["close"] - bar["open"]) / bar["open"]
    rsi = state["rsi"]
    bb_pct = state["bb_pct"]
    vol_ratio = state["vol_ratio"]
    signals = {
        "move_pct": round(move*100, 3),
        "rsi": rsi,
        "bb_pct": bb_pct,
        "vol_ratio": vol_ratio,
        "strategy": "MEAN_REV",
        "regime": state["current_regime"],
    }
    if vol_ratio < cfg.rev_vol_thr:
        return None, signals
    if (move < -cfg.rev_move_thr and
        rsi < cfg.rev_rsi_long and
        bb_pct < cfg.rev_bb_long):
        return "LONG", signals
    elif (move > cfg.rev_move_thr and
          rsi > cfg.rev_rsi_short and
          bb_pct > cfg.rev_bb_short):
        return "SHORT", signals
    return None, signals

def save_trade(cfg, t, ex, reason, hold_s):
    side = t["side"]
    entry = t["entry"]
    trade_size = t.get("trade_size", cfg.trade_size)
    raw = (ex-entry)/entry if side=="LONG" else (entry-ex)/entry
    net = max(raw*cfg.leverage - cfg.taker_fee_rate*2, -1.0)
    pnl = net * trade_size
    state["balance"] += pnl
    state["fees_paid"] += cfg.taker_fee_rate*2*trade_size
    if pnl > 0: state["wins"] += 1
    else: state["losses"] += 1
    state["last_exit_time"] = time.time()
    strategy = t.get("strategy", "UNKNOWN")
    log(f"EXIT {side} [{strategy}] @ {ex:.2f} | {reason} | PnL:${pnl:+.4f} | Bal:${state['balance']:.2f} | W:{state['wins']} L:{state['losses']}")
    exists = os.path.exists(cfg.trade_log)
    row = {
        "ts_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "strategy": strategy,
        "side": side, "entry": entry, "exit": ex,
        "exit_reason": reason,
        "hold_hours": round(hold_s/3600, 1),
        "leverage": cfg.leverage,
        "raw_pct": round(raw*100, 4),
        "levered_pct": round(raw*cfg.leverage*100, 4),
        "net_pnl": round(net, 6),
        "pnl_$": round(pnl, 4),
        "fees_$": round(cfg.taker_fee_rate*2*trade_size, 4),
        "trade_size": round(trade_size, 2),
        "move_pct": t.get("move_pct", 0),
        "vol_ratio": t.get("vol_ratio", 0),
        "rsi": t.get("rsi", 0),
        "bb_pct": t.get("bb_pct", 0),
        "regime": t.get("regime", "UNKNOWN"),
    }
    with open(cfg.trade_log, "a", newline="") as f:
        import csv as _csv
        w = _csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists: w.writeheader()
        w.writerow(row)
    return pnl

def manage_trade(cfg):
    t = state["active_trade"]
    if not t: return
    bid, ask = state["bid"], state["ask"]
    side = t["side"]
    entry = t["entry"]
    hold_s = time.time() - t["entry_time"]
    hold_bars = int(hold_s / cfg.bar_seconds)
    max_hold = cfg.rev_hold if t.get("strategy") == "MEAN_REV" else cfg.mom_hold
    ex = bid if side=="LONG" else ask
    reason = None
    if (side=="LONG" and bid<=t["liq"]) or (side=="SHORT" and ask>=t["liq"]):
        ex=t["liq"]; reason="LIQUIDATED"
    elif (side=="LONG" and ask>=t["tp"]) or (side=="SHORT" and bid<=t["tp"]):
        ex=t["tp"]; reason="TP"
    elif (side=="LONG" and bid<=t["sl"]) or (side=="SHORT" and ask>=t["sl"]):
        ex=t["sl"]; reason="SL"
    elif hold_bars >= max_hold:
        reason="TIMEOUT"
    if reason:
        save_trade(cfg, t, ex, reason, hold_s)
        state["active_trade"] = None
        adjust_brain(cfg)

async def bar_loop(cfg, stop):
    async with aiohttp.ClientSession() as session:
        while not stop.is_set():
            try:
                bars = await fetch_klines(cfg, session)
                state["bars"] = bars
                update_indicators(bars)

                last_bar_time = bars[-2]["open_time"]
                if state["last_bar_time"] != last_bar_time:
                    state["last_bar_time"] = last_bar_time
                    bar = bars[-2]
                    move = (bar["close"] - bar["open"]) / bar["open"]
                    regime = state["current_regime"]
                    log(f"NEW 1H BAR | ${bar['close']:,.2f} move={move*100:+.2f}% vol={state['vol_ratio']}x RSI={state['rsi']} BB%={state['bb_pct']} regime={regime}")

                    if state["active_trade"] is None and time.time()-state["last_exit_time"] > 120:
                        # Try momentum first
                        side, signals = check_momentum(cfg, bars)
                        strategy_type = "MOMENTUM"

                        # If no momentum signal try mean reversion
                        if side is None:
                            side, signals = check_mean_reversion(cfg, bars)
                            strategy_type = "MEAN_REV"
                            if side:
                                tp = cfg.rev_tp
                                sl = cfg.rev_sl
                            else:
                                tp = cfg.mom_tp
                                sl = cfg.mom_sl
                        else:
                            tp = cfg.mom_tp
                            sl = cfg.mom_sl

                        if side:
                            entry = state["mid"] if state["mid"] > 0 else bar["close"]
                            tp_p = entry*(1+tp) if side=="LONG" else entry*(1-tp)
                            sl_p = entry*(1-sl) if side=="LONG" else entry*(1+sl)
                            liq = entry*(1-1/cfg.leverage) if side=="LONG" else entry*(1+1/cfg.leverage)
                            trade_size = kelly_size(cfg.trade_log, state["balance"], tp, sl)
                            state["active_trade"] = {
                                "side": side, "entry": entry,
                                "tp": tp_p, "sl": sl_p, "liq": liq,
                                "entry_time": time.time(),
                                "trade_size": trade_size,
                                "strategy": strategy_type,
                                **signals,
                            }
                            log(f"ENTRY [{strategy_type}] {side} @ {entry:.2f} | size=${trade_size:.2f} | {signals}")
                        else:
                            log(f"NO SIGNAL | mom_vol_thr={state['mom_vol_thr']} | W:{state['wins']} L:{state['losses']} Bal:${state['balance']:.2f}")
                else:
                    t = state["active_trade"]
                    if t:
                        mid = state["mid"]
                        raw = (mid-t["entry"])/t["entry"] if t["side"]=="LONG" else (t["entry"]-mid)/t["entry"]
                        hold_bars = int((time.time()-t["entry_time"])/cfg.bar_seconds)
                        log(f"IN TRADE [{t['strategy']}] {t['side']} @ {t['entry']:.2f} | unreal={raw*cfg.leverage*100:+.2f}% | {hold_bars}bars")
            except Exception as e:
                log(f"Bar loop error: {e}")
            await asyncio.sleep(cfg.kline_poll_s)

async def price_loop(cfg, stop):
    async with aiohttp.ClientSession() as session:
        while not stop.is_set():
            try:
                bid, ask = await fetch_price(cfg, session)
                state["bid"] = bid
                state["ask"] = ask
                state["mid"] = (bid+ask)/2
                manage_trade(cfg)
            except Exception as e:
                log(f"Price error: {e}")
            await asyncio.sleep(cfg.price_poll_s)

async def run_app(cfg):
    stop = asyncio.Event()
    log(f"BTC Dual Strategy Bot | Kraken | {cfg.leverage}x leverage")
    log(f"Strategy 1 MOMENTUM: move>{cfg.mom_move_thr*100}% vol>{cfg.mom_vol_thr}x TP={cfg.mom_tp*100}% SL={cfg.mom_sl*100}%")
    log(f"Strategy 2 MEAN_REV: RSI<{cfg.rev_rsi_long} BB%<{cfg.rev_bb_long} TP={cfg.rev_tp*100}% SL={cfg.rev_sl*100}%")
    log(f"Kelly sizing ON | Brain ON | Backtested 750 days combined PnL=$4,369")
    adjust_brain(cfg)
    tasks = [
        asyncio.create_task(bar_loop(cfg, stop)),
        asyncio.create_task(price_loop(cfg, stop)),
    ]
    await asyncio.gather(*tasks)

cfg = Config()
asyncio.run(run_app(cfg))
