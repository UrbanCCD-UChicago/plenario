from sqlalchemy import Column, Integer, String
from plenario.database import Base


class ETLTask(Base):
    """Store information about completed jobs pertaining to ETL actions."""

    __tablename__ = 'etl_task'
    id = Column(Integer, primary_key=True)
    dataset_name = Column(String, nullable=False)
    status = Column(String)
    error = Column(String)
    type = Column(String)


if __name__ == '__main__':
    from plenario.database import app_engine
    ETLTask.create(bind=app_engine)
