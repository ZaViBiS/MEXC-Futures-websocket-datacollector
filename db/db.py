import time
import sqlalchemy
from sqlalchemy.orm import (
    Mapped,
    declarative_base,
    mapped_column,
    sessionmaker,
)
from sqlalchemy import BigInteger, Integer, Float, Boolean

from shared.queue import q

Base = declarative_base()


class Trades(Base):
    __tablename__ = "trades"
    time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    side: Mapped[bool] = mapped_column(Boolean, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)


class FundingRate(Base):
    __tablename__ = "funding_rate"
    time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rate: Mapped[float] = mapped_column(Float, nullable=False)
    next_settle_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)


DATABASE_URL = "sqlite:///mexc_futures_data.db"

engine = sqlalchemy.create_engine(DATABASE_URL)

Base.metadata.create_all(engine)


class DB:
    def __init__(self) -> None:
        self.session = sessionmaker(bind=engine)

    def add(self, data: dict) -> None:
        data["T"] = True if data["T"] == 1 else False
        data = Trades(
            time=data["t"],
            price=data["p"],
            side=data["T"],
            volume=data["v"],
        )
        with self.session.begin() as session:
            session.add(data)

    def add_rate(self, data: dict) -> None:
        data = FundingRate(
            time=data["ts"],
            rate=data["data"]["rate"],
            next_settle_time=data["data"]["nextSettleTime"],
        )
        with self.session.begin() as session:
            session.add(data)

    def loop(self):
        while True:
            try:
                if not q.empty():
                    data = q.get()
                    if data["channel"] == "push.deal":
                        self.add(data["data"])
                    if data["channel"] == "push.funding.rate":
                        self.add_rate(data)
                    else:
                        print(f"зайві дані: {data}")
            except Exception as e:
                print(e)
                time.sleep(1)
