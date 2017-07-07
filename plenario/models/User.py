from uuid import uuid4

from sqlalchemy import Column, String
from sqlalchemy.orm import synonym

from plenario.database import postgres_base, postgres_session
from plenario.models import bcrypt


def get_uuid():
    return str(uuid4())


class User(postgres_base):
    __tablename__ = 'plenario_user'

    id = Column(String(36), default=get_uuid, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    email = Column(String, nullable=False)
    _password = Column('password', String(60), nullable=False)

    def _get_password(self):
        return self._password

    def _set_password(self, value):
        self._password = bcrypt.generate_password_hash(value).decode('utf-8')

    password = property(_get_password, _set_password)
    password = synonym('_password', descriptor=password)

    def __init__(self, name, password, email):
        self.name = name
        self.password = password
        self.email = email

    @classmethod
    def get_by_username(cls, name):
        return postgres_session.query(cls).filter(cls.name == name).first()

    @classmethod
    def check_password(cls, name, value):
        user = cls.get_by_username(name)
        if not user:
            return False
        return bcrypt.check_password_hash(user.password, value)

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id
