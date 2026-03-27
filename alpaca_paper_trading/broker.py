import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest


@dataclass
class AccountSnapshot:
    equity: float
    cash: float
    buying_power: float


@dataclass
class PositionSnapshot:
    symbol: str
    qty: float
    avg_entry_price: float
    market_value: float


@dataclass
class MarketClockSnapshot:
    timestamp: datetime
    is_open: bool
    next_open: datetime
    next_close: datetime


class AlpacaPaperBroker:
    def __init__(self, api_key: str, secret_key: str, paper: bool = True) -> None:
        self.client = TradingClient(api_key, secret_key, paper=paper)

    def get_account_snapshot(self) -> AccountSnapshot:
        account = self.client.get_account()
        return AccountSnapshot(
            equity=float(account.equity),
            cash=float(account.cash),
            buying_power=float(account.buying_power),
        )

    def get_positions(self) -> Dict[str, PositionSnapshot]:
        positions: Dict[str, PositionSnapshot] = {}
        for position in self.client.get_all_positions():
            positions[position.symbol] = PositionSnapshot(
                symbol=position.symbol,
                qty=float(position.qty),
                avg_entry_price=float(position.avg_entry_price),
                market_value=float(position.market_value),
            )
        return positions

    def get_market_clock(self) -> MarketClockSnapshot:
        clock = self.client.get_clock()
        return MarketClockSnapshot(
            timestamp=clock.timestamp,
            is_open=bool(clock.is_open),
            next_open=clock.next_open,
            next_close=clock.next_close,
        )

    def get_open_orders(self) -> List[object]:
        request = GetOrdersRequest(status=QueryOrderStatus.OPEN, nested=True, limit=100)
        return list(self.client.get_orders(filter=request))

    def assert_no_open_orders(self, symbols: Optional[List[str]] = None) -> None:
        symbols = symbols or []
        open_orders = self.get_open_orders()
        conflicts = [order for order in open_orders if not symbols or order.symbol in symbols]
        if conflicts:
            details = ", ".join(f"{order.symbol}:{order.side}:{order.qty}" for order in conflicts)
            raise RuntimeError(
                f"Refusing to trade because open Alpaca orders already exist: {details}"
            )

    def submit_market_order(
        self,
        symbol: str,
        side: OrderSide,
        qty: int,
        client_order_id: str,
        wait_for_fill: bool = True,
        timeout_seconds: int = 90,
    ) -> object:
        if qty <= 0:
            raise ValueError(f"Order quantity must be positive. Received {qty} for {symbol}.")

        order = self.client.submit_order(
            order_data=MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce.DAY,
                client_order_id=client_order_id,
            )
        )
        if not wait_for_fill:
            return order
        return self.wait_for_fill(order.client_order_id, timeout_seconds=timeout_seconds)

    def wait_for_fill(self, client_order_id: str, timeout_seconds: int = 90) -> object:
        deadline = time.time() + timeout_seconds
        terminal_statuses = {"filled", "canceled", "expired", "rejected"}
        while time.time() < deadline:
            order = self.client.get_order_by_client_id(client_order_id)
            status = str(order.status).lower()
            if status in terminal_statuses:
                if status != "filled":
                    raise RuntimeError(
                        f"Order {client_order_id} ended with status '{order.status}'."
                    )
                return order
            time.sleep(2)
        raise TimeoutError(f"Timed out waiting for Alpaca order {client_order_id} to fill.")

    def rebalance_to_target_shares(
        self,
        target_shares: Dict[str, int],
        reference_prices: Dict[str, float],
        execution_tag: str,
        symbols_in_scope: Optional[List[str]] = None,
    ) -> List[object]:
        symbols = symbols_in_scope or sorted(target_shares.keys())
        self.assert_no_open_orders(symbols)

        submitted_orders: List[object] = []
        positions = self.get_positions()
        current_shares = {
            symbol: int(round(positions.get(symbol, PositionSnapshot(symbol, 0.0, 0.0, 0.0)).qty))
            for symbol in symbols
        }

        for symbol in symbols:
            target_qty = target_shares.get(symbol, 0)
            current_qty = current_shares.get(symbol, 0)
            shares_to_sell = max(0, current_qty - target_qty)
            if shares_to_sell == 0:
                continue
            client_order_id = self._client_order_id(execution_tag, symbol, "sell")
            order = self.submit_market_order(
                symbol=symbol,
                side=OrderSide.SELL,
                qty=shares_to_sell,
                client_order_id=client_order_id,
            )
            submitted_orders.append(order)

        positions = self.get_positions()
        current_shares = {
            symbol: int(round(positions.get(symbol, PositionSnapshot(symbol, 0.0, 0.0, 0.0)).qty))
            for symbol in symbols
        }

        for symbol in symbols:
            target_qty = target_shares.get(symbol, 0)
            current_qty = current_shares.get(symbol, 0)
            shares_to_buy = max(0, target_qty - current_qty)
            if shares_to_buy == 0:
                continue

            account = self.get_account_snapshot()
            price_proxy = float(reference_prices[symbol])
            if price_proxy <= 0:
                raise ValueError(f"Invalid reference price for {symbol}: {price_proxy}")
            affordable_qty = math.floor(account.cash / price_proxy)
            shares_to_buy = min(shares_to_buy, max(0, affordable_qty))
            if shares_to_buy == 0:
                continue

            client_order_id = self._client_order_id(execution_tag, symbol, "buy")
            order = self.submit_market_order(
                symbol=symbol,
                side=OrderSide.BUY,
                qty=shares_to_buy,
                client_order_id=client_order_id,
            )
            submitted_orders.append(order)
            positions = self.get_positions()
            current_shares[symbol] = current_shares.get(symbol, 0) + shares_to_buy

        return submitted_orders

    def rotate_spy_stop_to_shy(
        self,
        shy_target_shares: int,
        shy_reference_price: float,
        execution_tag: str,
    ) -> List[object]:
        self.assert_no_open_orders(["SPY", "SHY"])
        submitted_orders: List[object] = []
        positions = self.get_positions()
        spy_qty = int(round(positions.get("SPY", PositionSnapshot("SPY", 0.0, 0.0, 0.0)).qty))
        if spy_qty > 0:
            sell_order = self.submit_market_order(
                symbol="SPY",
                side=OrderSide.SELL,
                qty=spy_qty,
                client_order_id=self._client_order_id(execution_tag, "SPY", "sell"),
            )
            submitted_orders.append(sell_order)

        positions = self.get_positions()
        shy_qty = int(round(positions.get("SHY", PositionSnapshot("SHY", 0.0, 0.0, 0.0)).qty))
        shy_to_buy = max(0, shy_target_shares - shy_qty)
        if shy_reference_price <= 0:
            raise ValueError(f"Invalid SHY reference price: {shy_reference_price}")
        if shy_to_buy > 0:
            account = self.get_account_snapshot()
            affordable_qty = math.floor(account.cash / shy_reference_price)
            shy_to_buy = min(shy_to_buy, max(0, affordable_qty))
        if shy_to_buy > 0:
            buy_order = self.submit_market_order(
                symbol="SHY",
                side=OrderSide.BUY,
                qty=shy_to_buy,
                client_order_id=self._client_order_id(execution_tag, "SHY", "buy"),
            )
            submitted_orders.append(buy_order)

        return submitted_orders

    @staticmethod
    def _client_order_id(execution_tag: str, symbol: str, side: str) -> str:
        suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"{execution_tag}-{symbol.lower()}-{side}-{suffix}"[:48]
