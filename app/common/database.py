import abc
from contextlib import contextmanager

from pypika.terms import Criterion, builder
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.orm.session import sessionmaker
from urllib.parse import quote_plus


class DateBetweenCriterion(Criterion):
    def __init__(self, term, start, end, alias=None):
        super(DateBetweenCriterion, self).__init__(alias)
        self.term = term
        self.start = start
        self.end = end

    @property
    def tables_(self):
        return self.term.tables_

    @builder
    def for_(self, table):
        self.term = self.term.for_(table)

    def get_sql(self, **kwargs):
        return "{term} BETWEEN date '{start}' AND date '{end}'".format(
            term=self.term.get_sql(**kwargs),
            start=self.start,
            end=self.end,
        )

    def fields(self):
        return self.term.fields() if self.term.fields else []


class AthenaService(abc.ABC):
    engine = None

    def __init__(self):
        conn_str = 'awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/' \
                   '{schema_name}?s3_staging_dir={s3_staging_dir}'

        self.engine = create_engine(conn_str.format(
            aws_access_key_id=quote_plus('AKIAIBTZL2HUG4PRHLRA'),
            aws_secret_access_key=quote_plus('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'),
            region_name='us-east-1',
            schema_name='analytics',
            s3_staging_dir=quote_plus('s3://artnew-analytics/athena-query-results')), echo=True)

    @contextmanager
    def db_session(self):
        session = scoped_session(sessionmaker(bind=self.engine))()
        yield session

        session.close()


class BaseService(AthenaService):

    @abc.abstractmethod
    def get_table(self):
        pass
