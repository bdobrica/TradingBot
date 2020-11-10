from sqlalchemy import Table, Column, Index
from sqlalchemy.types import BigInteger, Float, Integer, String, DateTime, Float

class OrderStatus:
    PENDING = 0
    PARTIAL = 1
    FULFILED = 2

class DatabaseSchema:
    TRANSACTIONS = 'transactions'
    BUDGET = 'budget'
    PORTFOLIO = 'portfolio'
    ORDERS = 'orders'
    USED = 'used'
    
    def __init__(self, meta):
        # the `transactions` table, we've played with this before
        self.transactions = Table(
            self.TRANSACTIONS, meta, 
            Column('id', BigInteger, primary_key = True),
            Column('price', Float), 
            Column('symbol', String(32)),
            Column('time', DateTime),
            Column('stamp', BigInteger),
            Column('volume', Float)
        )
        _ = Index('symbol', self.transactions.c.symbol)
        _ = Index('symbol_stamp', self.transactions.c.symbol, self.transactions.c.stamp, unique = True)

        # the `budget` table; the current budget is the last line from the table (time wise)
        self.budget = Table(
            self.BUDGET, meta,
            Column('id', BigInteger, primary_key = True),
            Column('amount', Float),
            Column('time', DateTime),
            Column('stamp', BigInteger)
        )

        # the `portfolio` table: this is kind of a `transactions` table, but with transactions done on my behalf
        self.portfolio = Table(
            self.PORTFOLIO, meta,
            Column('id', BigInteger, primary_key = True),
            Column('transaction', BigInteger),
            Column('price', Float),
            Column('commission', Float),
            Column('symbol', String(32)),
            Column('time', DateTime),
            Column('stamp', BigInteger),
            Column('volume', Float)
        )
        _ = Index('symbol', self.portfolio.c.symbol)
        _ = Index('symbol_stamp', self.portfolio.c.symbol, self.portfolio.c.stamp, unique = True)

        # the `orders` table: this is kind of a `transactions` table, but with pending orders
        self.orders = Table(
            self.ORDERS, meta,
            Column('id', BigInteger, primary_key = True),
            Column('price', Float), 
            Column('symbol', String(32)),
            Column('time', DateTime),
            Column('stamp', BigInteger),
            Column('volume', Float),
            Column('status', Integer)
        )
        _ = Index('symbol', self.orders.c.symbol)
        _ = Index('status', self.orders.c.status)
        _ = Index('symbol_stamp', self.orders.c.symbol, self.orders.c.stamp, unique = True)

        # the `used_log` table: this will keep only the `id`s and used volume for transactions used to fulfil orders
        self.used = Table(
            self.USED, meta,
            Column('id', BigInteger, primary_key = True),
            Column('transaction', BigInteger),
            Column('stamp', BigInteger),
            Column('volume', Float)
        )