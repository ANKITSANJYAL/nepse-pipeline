"""Wire contracts. Validation happens at every layer boundary, never after.

Field names mirror NEPSE's JSON exactly via aliases so a rename upstream
surfaces as a loud ValidationError instead of a column of nulls.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    # extra="forbid" is the schema-drift tripwire: a NEW upstream field raises
    # rather than being silently dropped. Noisy on purpose.
    model_config = ConfigDict(extra="forbid", populate_by_name=True, str_strip_whitespace=True)


class FloorsheetContract(StrictModel):
    """One executed trade. contract_id is globally unique and monotonic per day."""

    contract_id: int = Field(alias="contractId")
    stock_symbol: str = Field(alias="stockSymbol")
    stock_id: int = Field(alias="stockId")
    security_name: str | None = Field(default=None, alias="securityName")
    buyer_member_id: str = Field(alias="buyerMemberId")
    seller_member_id: str = Field(alias="sellerMemberId")
    buyer_broker_name: str | None = Field(default=None, alias="buyerBrokerName")
    seller_broker_name: str | None = Field(default=None, alias="sellerBrokerName")
    contract_quantity: int = Field(alias="contractQuantity", gt=0)
    contract_rate: Decimal = Field(alias="contractRate", gt=0)
    contract_amount: Decimal = Field(alias="contractAmount", gt=0)
    business_date: date = Field(alias="businessDate")
    trade_book_id: int | None = Field(default=None, alias="tradeBookId")
    trade_time: datetime = Field(alias="tradeTime")

    @model_validator(mode="after")
    def _amount_reconciles(self) -> "FloorsheetContract":
        expected = self.contract_rate * self.contract_quantity
        # NEPSE rounds to paisa; anything past 1 rupee of drift is a real defect.
        if abs(expected - self.contract_amount) > Decimal("1.0"):
            raise ValueError(
                f"amount mismatch: {self.contract_quantity} x {self.contract_rate} "
                f"= {expected}, got {self.contract_amount}"
            )
        return self

    @model_validator(mode="after")
    def _self_trade_is_suspicious_not_fatal(self) -> "FloorsheetContract":
        # Cross-trades through one broker are legal and do happen. Kept as a
        # documented non-error so nobody "fixes" it into a raise later.
        return self


class EodQuote(StrictModel):
    """End-of-day OHLCV for one security. Natural key: (symbol, business_date)."""

    id: int | None = None
    business_date: date = Field(alias="businessDate")
    security_id: int = Field(alias="securityId")
    symbol: str
    security_name: str | None = Field(default=None, alias="securityName")
    open_price: Decimal | None = Field(default=None, alias="openPrice")
    high_price: Decimal | None = Field(default=None, alias="highPrice")
    low_price: Decimal | None = Field(default=None, alias="lowPrice")
    close_price: Decimal | None = Field(default=None, alias="closePrice")
    total_traded_quantity: int | None = Field(default=None, alias="totalTradedQuantity", ge=0)
    total_traded_value: Decimal | None = Field(default=None, alias="totalTradedValue", ge=0)
    previous_day_close_price: Decimal | None = Field(default=None, alias="previousDayClosePrice")
    fifty_two_week_high: Decimal | None = Field(default=None, alias="fiftyTwoWeekHigh")
    fifty_two_week_low: Decimal | None = Field(default=None, alias="fiftyTwoWeekLow")
    last_updated_time: datetime | None = Field(default=None, alias="lastUpdatedTime")
    last_updated_price: Decimal | None = Field(default=None, alias="lastUpdatedPrice")
    total_trades: int | None = Field(default=None, alias="totalTrades", ge=0)
    average_traded_price: Decimal | None = Field(default=None, alias="averageTradedPrice")
    market_capitalization: Decimal | None = Field(default=None, alias="marketCapitalization")

    # Not from the wire: populated by validation, persisted alongside the row.
    quality_flags: str | None = None

    @field_validator("symbol")
    @classmethod
    def _symbol_shape(cls, v: str) -> str:
        if not v or len(v) > 24:
            raise ValueError(f"implausible symbol: {v!r}")
        return v.upper()

    @model_validator(mode="after")
    def _ohlc_coherent(self) -> "EodQuote":
        o, h, l, c = self.open_price, self.high_price, self.low_price, self.close_price

        # high < low is unambiguously corrupt - nothing downstream can use it.
        if h is not None and l is not None and h < l:
            raise ValueError(f"high {h} < low {l}")

        # close outside [low, high] is a real NEPSE quirk on thinly-traded
        # debentures, where closePrice is a carried/adjusted figure rather than
        # a traded one. Rejecting drops legitimate securities; accepting
        # silently hides it. Flag it and let downstream decide.
        flags: list[str] = []
        for name, v in (("open", o), ("close", c)):
            if v is not None and h is not None and l is not None and not (l <= v <= h):
                flags.append(f"{name}_outside_range")
        if flags:
            object.__setattr__(self, "quality_flags", ",".join(flags))
        return self


class MarketStatus(StrictModel):
    is_open: str = Field(alias="isOpen")
    as_of: datetime = Field(alias="asOf")
    id: int

    @property
    def open(self) -> bool:
        return self.is_open.strip().upper() == "OPEN"


class IndexPoint(StrictModel):
    """One point of the intraday index series: [epoch_seconds, value]."""

    ts: datetime
    value: Decimal
