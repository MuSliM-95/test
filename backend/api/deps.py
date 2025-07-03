from collections.abc import Generator

from database.db import engine
from sqlalchemy.orm import Session


def get_db() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session
