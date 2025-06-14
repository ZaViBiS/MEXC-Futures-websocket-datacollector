from os import write
import time
import logging
import sqlalchemy
from sqlalchemy.orm import (
    Mapped,
    declarative_base,
    mapped_column,
    sessionmaker,
)
from sqlalchemy import BigInteger, Integer, Float, Boolean, String

from shared.queue import q
from shared.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

Base = declarative_base()


class Trade(Base):
    __tablename__ = "trades"  # Вказуємо динамічну назву таблиці
    time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    side: Mapped[bool] = mapped_column(Boolean, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)


class FundingRate(Base):
    __tablename__ = "funding_rate"
    time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rate: Mapped[float] = mapped_column(Float, nullable=False)
    next_settle_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)


DATABASE_URL = "sqlite:////mnt/sd/mexc_futures_data.db"
# DATABASE_URL = "sqlite:///mexc_futures_data.db"

engine = sqlalchemy.create_engine(DATABASE_URL)

Base.metadata.create_all(engine)


class DB:
    def __init__(self) -> None:
        self.session = sessionmaker(bind=engine)

    def data_to_model(self, data: dict):
        data["data"]["T"] = True if data["data"]["T"] == 1 else False
        return Trade(
            time=data["data"]["t"],
            price=data["data"]["p"],
            side=data["data"]["T"],
            volume=data["data"]["v"],
            symbol=data["symbol"],
        )

    def data_to_funding_model(self, data: dict) -> FundingRate:
        return FundingRate(
            time=data["ts"],
            rate=data["data"]["rate"],
            next_settle_time=data["data"]["nextSettleTime"],
            symbol=data["symbol"],
        )

    def loop(self):
        while True:
            try:
                if not q.empty():
                    start = time.time()
                    writes_cloc = 0
                    with self.session.begin() as session:
                        while True:
                            if not q.empty():
                                data = q.get()
                                if data["channel"] == "push.deal":
                                    session.add(self.data_to_model(data))
                                    writes_cloc += 1
                                elif data["channel"] == "push.funding.rate":
                                    session.add(self.data_to_funding_model(data))
                                    writes_cloc += 1
                                else:
                                    log.debug(f"зайві дані: {data}")
                            else:
                                break
                    log.info(
                        f"час на запис в базу: {time.time() - start:.4f}s | було записано {writes_cloc} записів"
                    )
                    time.sleep(60)
                else:
                    time.sleep(10)
            except Exception as e:
                log.error(e)
                time.sleep(1)
