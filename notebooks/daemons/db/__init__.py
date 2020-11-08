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
    
    def init(self, meta):
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
        _ = Index('symbol', transactions.c.symbol)
        _ = Index('symbol_stamp', transactions.c.symbol, transactions.c.stamp, unique = True)

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
        _ = Index('symbol', portfolio.c.symbol)
        _ = Index('symbol_stamp', portfolio.c.symbol, portfolio.c.stamp, unique = True)

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
        _ = Index('symbol', orders.c.symbol)
        _ = Index('status', orders.c.status)
        _ = Index('symbol_stamp', orders.c.symbol, orders.c.stamp, unique = True)

        # the `used_log` table: this will keep only the `id`s and used volume for transactions used to fulfil orders
        self.used = Table(
            self.USED, meta,
            Column('id', BigInteger, primary_key = True),
            Column('transaction', BigInteger),
            Column('stamp', BigInteger),
            Column('volume', Float)