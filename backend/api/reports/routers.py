from fastapi import APIRouter
from sqlalchemy import select, func, desc, case
from database.db import database
from . import schemas


router = APIRouter(tags=["reports"])