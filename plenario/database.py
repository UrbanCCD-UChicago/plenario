import boto3

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base

from plenario.settings import DATABASE_CONN, AWS_ACCESS_KEY, AWS_SECRET_KEY


app_engine = create_engine(DATABASE_CONN, convert_unicode=True)

session = scoped_session(sessionmaker(bind=app_engine,
                                      autocommit=False,
                                      autoflush=False, expire_on_commit=False))
Base = declarative_base(bind=app_engine)
Base.query = session.query_property()

client = boto3.client(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
