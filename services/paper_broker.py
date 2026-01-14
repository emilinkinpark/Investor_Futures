# services/paper_broker.py

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Order:
    order_id: str
    symbol: str
    side: str  # "BUY" / "SELL"
    order_type: str  # "MARKET" / "LIMIT" / "STOP_MARKET" / "TAKE_PROFIT_MARKET"
    qty: float

    limit_price: Optional[float] = None
    stop_price: Optional[float] = None

    reduce_only: bool = False
    oco_group: Optional[str] = None

    status: str = "OPEN"
    filled_qty: float = 0.0
    avg_fill_price: float = 0.0
    created_ts: float = field(default_factory=lambda: time.time())


@dataclass
class Position:
    amt: float = 0.0
    entry: float = 0.0
    realized: float = 0.0
    fees: float = 0.0
    leverage: float = 1.0
    last_mark: float = 0.0
    isolated_margin_usdt: float = 0.0   # ADD


class PaperBroker:
    def __init__(self, fee_bps: float = 4.0):
        self.fee_bps = float(fee_bps)
        self.open_orders: dict[str, list[Order]] = {}
        self.positions: dict[str, Position] = {}
        self.last_price: dict[str, float] = {}
        self._seq = 0

    def _next_id(self) -> str:
        self._seq += 1
        return f"SIM-{self._seq}"

    def _cancel_oco_siblings(self, symbol: str, filled_order: Order):
        if not filled_order.oco_group:
            return
        orders = self.open_orders.get(symbol, [])
        for o in orders:
            if (
                o.status == "OPEN"
                and o.oco_group == filled_order.oco_group
                and o.order_id != filled_order.order_id
            ):
                o.status = "CANCELED"

    def on_price(self, symbol: str, last: float):
        if not symbol:
            return
        try:
            last = float(last)
        except Exception:
            return
        if last <= 0:
            return

        symbol = symbol.upper()
        self.last_price[symbol] = last

        pos = self.positions.get(symbol)
        if pos:
            pos.last_mark = last

        orders = self.open_orders.get(symbol, [])
        if not orders:
            return

        remaining: list[Order] = []

        for o in orders:
            if o.status != "OPEN":
                continue

            # 1) LIMIT fills
            if o.order_type == "LIMIT":
                if o.limit_price is None:
                    remaining.append(o)
                    continue

                if o.side == "BUY" and last <= o.limit_price:
                    self._fill(o, fill_price=o.limit_price, fill_qty=o.qty)
                    self._cancel_oco_siblings(symbol, o)
                elif o.side == "SELL" and last >= o.limit_price:
                    self._fill(o, fill_price=o.limit_price, fill_qty=o.qty)
                    self._cancel_oco_siblings(symbol, o)
                else:
                    remaining.append(o)
                continue

            # 2) STOP/TP triggers -> market fill at last
            if o.order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
                if o.stop_price is None:
                    remaining.append(o)
                    continue

                triggered = False

                if o.side == "SELL":
                    if o.order_type == "STOP_MARKET" and last <= o.stop_price:
                        triggered = True
                    if o.order_type == "TAKE_PROFIT_MARKET" and last >= o.stop_price:
                        triggered = True
                elif o.side == "BUY":
                    if o.order_type == "STOP_MARKET" and last >= o.stop_price:
                        triggered = True
                    if o.order_type == "TAKE_PROFIT_MARKET" and last <= o.stop_price:
                        triggered = True

                if triggered:
                    self._fill(o, fill_price=last, fill_qty=o.qty)
                    self._cancel_oco_siblings(symbol, o)
                else:
                    remaining.append(o)
                continue

            # 3) MARKET should never rest
            remaining.append(o)

        self.open_orders[symbol] = [o for o in remaining if o.status == "OPEN"]

    def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        qty: float,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        reduce_only: bool = False,
        leverage: float = 1.0,
        oco_group: Optional[str] = None,
        isolated_margin_usdt: float = 0.0,
    ) -> dict:
        symbol = (symbol or "").upper()
        side = (side or "").upper()
        order_type = (order_type or "").upper()

        qty = float(qty)
        if qty <= 0:
            raise ValueError("Quantity must be positive")
        if side not in ("BUY", "SELL"):
            raise ValueError("Side must be BUY or SELL")

        allowed = ("MARKET", "LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET")
        if order_type not in allowed:
            raise ValueError(f"Order type must be one of {allowed}")

        if order_type == "LIMIT":
            if limit_price is None or float(limit_price) <= 0:
                raise ValueError("Limit price must be positive")

        if order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            if stop_price is None or float(stop_price) <= 0:
                raise ValueError("Stop/TP trigger price must be positive")

        o = Order(
            order_id=self._next_id(),
            symbol=symbol,
            side=side,
            order_type=order_type,
            qty=qty,
            limit_price=float(limit_price) if limit_price is not None else None,
            stop_price=float(stop_price) if stop_price is not None else None,
            reduce_only=bool(reduce_only),
            oco_group=oco_group,
        )

        # ensure position exists and leverage stored
        pos = self.positions.get(symbol)
        if not pos:
            pos = Position(leverage=float(leverage or 1.0))
            self.positions[symbol] = pos
        else:
            pos.leverage = float(leverage or pos.leverage or 1.0)
        if not reduce_only and float(isolated_margin_usdt or 0.0) > 0:
            pos.isolated_margin_usdt = float(isolated_margin_usdt)

        # MARKET fills immediately at last known price
        if order_type == "MARKET":
            last = float(self.last_price.get(symbol, 0.0))
            if last <= 0:
                raise ValueError("No price available for MARKET fill")
            self._fill(o, fill_price=last, fill_qty=qty)
            return self._order_to_dict(o)

        # Resting order
        self.open_orders.setdefault(symbol, []).append(o)

        # If we already have a last price, allow immediate trigger/fill check
        last = float(self.last_price.get(symbol, 0.0))
        if last > 0:
            self.on_price(symbol, last)

        return self._order_to_dict(o)

    def close_position_market(self, symbol: str) -> Optional[dict]:
        symbol = (symbol or "").upper()
        pos = self.positions.get(symbol)
        if not pos or pos.amt == 0:
            return None

        side = "SELL" if pos.amt > 0 else "BUY"
        qty = abs(pos.amt)
        return self.place_order(symbol, side, "MARKET", qty, reduce_only=True, leverage=pos.leverage)

    def _fill(self, o: Order, fill_price: float, fill_qty: float):
        fill_price = float(fill_price)
        fill_qty = float(fill_qty)

        pos = self.positions.get(o.symbol)
        if not pos:
            pos = Position()
            self.positions[o.symbol] = pos

        current_amt = pos.amt
        fill_signed = fill_qty if o.side == "BUY" else -fill_qty

        # reduce-only enforcement
        if o.reduce_only:
            if current_amt == 0:
                o.status = "REJECTED"
                return
            # reject if it increases exposure
            if current_amt > 0 and fill_signed > 0:
                o.status = "REJECTED"
                return
            if current_amt < 0 and fill_signed < 0:
                o.status = "REJECTED"
                return
            # clamp to available position size
            if abs(fill_signed) > abs(current_amt):
                fill_signed = -abs(current_amt) if fill_signed < 0 else abs(current_amt)

        if fill_signed == 0:
            o.status = "CANCELED"
            return

        # fee
        fee = abs(fill_signed) * fill_price * (self.fee_bps / 10000.0)
        pos.fees += fee

        # one-way netting
        if pos.amt == 0:
            pos.amt = fill_signed
            pos.entry = fill_price
        else:
            same_dir = (pos.amt > 0 and fill_signed > 0) or (pos.amt < 0 and fill_signed < 0)
            if same_dir:
                new_amt = pos.amt + fill_signed
                pos.entry = (pos.entry * abs(pos.amt) + fill_price * abs(fill_signed)) / max(abs(new_amt), 1e-12)
                pos.amt = new_amt
            else:
                closing_qty = min(abs(pos.amt), abs(fill_signed))
                if pos.amt > 0:
                    pos.realized += (fill_price - pos.entry) * closing_qty
                else:
                    pos.realized += (pos.entry - fill_price) * closing_qty

                pos.amt = pos.amt + fill_signed
                if pos.amt == 0:
                    pos.entry = 0.0
                else:
                    pos.entry = fill_price  # flipped

        pos.last_mark = float(self.last_price.get(o.symbol, fill_price))

        o.filled_qty = abs(fill_signed)
        o.avg_fill_price = fill_price
        o.status = "FILLED"

    def get_positions(self) -> list[dict]:
        out = []
        for sym, pos in self.positions.items():
            if pos.amt == 0:
                continue

            mark = float(self.last_price.get(sym, pos.last_mark or 0.0))
            entry = float(pos.entry or 0.0)
            amt = float(pos.amt)

            if amt > 0:
                side = "LONG"
                pnl_unreal = (mark - entry) * amt
            else:
                side = "SHORT"
                pnl_unreal = (entry - mark) * abs(amt)

            pnl = pnl_unreal + float(pos.realized) - float(pos.fees)

            lev = float(pos.leverage or 1.0)
            notional = abs(amt) * entry
            margin = notional / max(lev, 1.0)
            roe = (pnl / margin * 100.0) if margin > 0 else 0.0

            size = abs(amt)
            m = float(pos.isolated_margin_usdt or 0.0)

            liq = 0.0
            if size > 0 and entry > 0 and m > 0:
                if amt > 0:  # long
                    liq = max(0.0, entry - (m / size))
                else:        # short
                    liq = entry + (m / size)

            out.append({
                    "symbol": sym,
                    "side": side,
                    "size": abs(amt),
                    "entry": entry,
                    "mark": mark,
                    "pnl": pnl,
                    "roe": roe,
                    "leverage": lev,
                    "liquidation": liq,   
            })
        return out

    def _order_to_dict(self, o: Order) -> dict:
        return {
            "orderId": o.order_id,
            "symbol": o.symbol,
            "side": o.side,
            "type": o.order_type,
            "status": o.status,
            "origQty": o.qty,
            "executedQty": o.filled_qty,
            "avgPrice": o.avg_fill_price,
            "stopPrice": o.stop_price,
            "price": o.limit_price,
            "reduceOnly": o.reduce_only,
            "ocoGroup": o.oco_group,
        }
