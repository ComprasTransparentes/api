from peewee import fn, Expression
from playhouse.postgres_ext import OP

__author__ = 'lbenitez'


# TODO Make this compatible with peewee upgrades
def ts_match(field, query, regconfig='spanish'):
    return Expression(fn.to_tsvector(regconfig, field), OP.TS_MATCH, fn.plainto_tsquery(regconfig, query))

# TODO Use this
def ts_rank(field, query, regconfig='spanish'):
    # return Expression(fn.to_tsvector(regconfig, field), OP.TS_MATCH, fn.plainto_tsquery(regconfig, query))
    return Expression(fn.ts_rank_cd(fn.to_tsvector(regconfig, field), fn.plainto_tsquery(regconfig, query)))
