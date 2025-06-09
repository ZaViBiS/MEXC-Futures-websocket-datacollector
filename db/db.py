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


DATABASE_URL = "sqlite:////home/pasta/data_saver/data/mexc_futures_data.db"
# DATABASE_URL = "sqlite:///mexc_futures_data.db"

engine = sqlalchemy.create_engine(DATABASE_URL)

Base.metadata.create_all(engine)


class DB:
    def __init__(self) -> None:
        self.session = sessionmaker(bind=engine)

    def add_all(self, data: list[dict]) -> None:
        def convert(d):
            d["data"]["T"] = True if d["data"]["T"] == 1 else False
            return Trades(
                time=d["data"]["t"],
                price=d["data"]["p"],
                side=d["data"]["T"],
                volume=d["data"]["v"],
            )

        for x in data:
            data[data.index(x)] = convert(x)

        with self.session.begin() as session:
            session.add_all(data)

    def add_rate(self, data: list[dict]) -> None:
        def convert(d):
            return FundingRate(
                time=d["ts"],
                rate=d["data"]["rate"],
                next_settle_time=d["data"]["nextSettleTime"],
            )

        for x in data:
            data[data.index(x)] = convert(x)

        with self.session.begin() as session:
            session.add_all(data)

    def loop(self):
        while True:
            try:
                if not q.empty():
                    data = {"push.deal": [], "push.funding.rate": []}
                    while True:
                        if not q.empty():
                            a = q.get()
                            if a["channel"] == "push.deal":
                                data["push.deal"].append(a)
                            elif a["channel"] == "push.funding.rate":
                                data["push.funding.rate"].append(a)
                            else:
                                print(f"зайві дані: {a}")
                        else:
                            break
                    self.add_all(data["push.deal"])
                    self.add_rate(data["push.funding.rate"])

                    # for x in data:
                    #     if ["channel"] == "push.deal":
                    #         self.add(data["data"])
                    #     elif data["channel"] == "push.funding.rate":
                    #         self.add_rate(data)
                    #     else:
                    #         print(f"зайві дані: {data}")
                else:
                    time.sleep(10)
            except Exception as e:
                print(e)
                time.sleep(1)
