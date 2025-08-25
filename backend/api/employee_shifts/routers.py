from fastapi import APIRouter, Header, HTTPException, status
from sqlalchemy import and_, desc, select
from datetime import datetime, timedelta
from typing import List

from database.db import database, employee_shifts, users_cboxes_relation
from functions.users import get_user_id_cashbox_id_by_token
from .schemas import (
    StartShiftRequest,
    EndShiftRequest, 
    CreateBreakRequest,
    ShiftResponse,
    ShiftStatusResponse,
    ShiftStatus
)

router = APIRouter(tags=["employee_shifts"], prefix="/employee_shifts")


@router.post("/start", response_model=ShiftResponse)
async def start_shift(
    request: StartShiftRequest,
    token: str = Header(..., description="Токен авторизации")
):
    """Начать смену"""
    # Получаем текущего пользователя
    user_id, cashbox_id = await get_user_id_cashbox_id_by_token(token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный токен авторизации"
        )
    
    # Проверяем, нет ли уже активной смены
    existing_shift_query = employee_shifts.select().where(
        and_(
            employee_shifts.c.user_id == user_id,
            employee_shifts.c.status.in_(["on_shift", "on_break"]),
            employee_shifts.c.shift_end.is_(None)
        )
    )
    existing_shift = await database.fetch_one(existing_shift_query)
    
    if existing_shift:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="У вас уже есть активная смена"
        )
    
    shift_data = {
        "user_id": user_id,
        "cashbox_id": cashbox_id,
        "shift_start": datetime.utcnow(),
        "status": ShiftStatus.on_shift,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    
    created_shift = await database.fetch_one(
        employee_shifts.insert().values(shift_data).returning(*employee_shifts.c)
    )
    
    return ShiftResponse(**dict(created_shift))


@router.post("/end", response_model=ShiftResponse)
async def end_shift(
    request: EndShiftRequest,
    token: str = Header(..., description="Токен авторизации")
):
    """Завершить смену"""
    # Получаем текущего пользователя
    user_id, cashbox_id = await get_user_id_cashbox_id_by_token(token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный токен авторизации"
        )
    
    # Находим активную смену
    active_shift_query = employee_shifts.select().where(
        and_(
            employee_shifts.c.user_id == user_id,
            employee_shifts.c.status.in_(["on_shift", "on_break"]),
            employee_shifts.c.shift_end.is_(None)
        )
    )
    active_shift = await database.fetch_one(active_shift_query)
    
    if not active_shift:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="У вас нет активной смены"
        )
    
    update_data = {
        "shift_end": datetime.utcnow(),
        "status": ShiftStatus.off_shift,
        "break_start": None,  # Сбрасываем перерыв при завершении смены
        "break_duration": None,
        "updated_at": datetime.utcnow()
    }
    
    updated_shift = await database.fetch_one(
        employee_shifts.update()
        .where(employee_shifts.c.id == active_shift.id)
        .values(update_data)
        .returning(*employee_shifts.c)
    )
    
    return ShiftResponse(**dict(updated_shift))


@router.post("/break", response_model=ShiftResponse)
async def create_break(
    request: CreateBreakRequest,
    token: str = Header(..., description="Токен авторизации")
):
    """Создать перерыв на N минут"""
    # Получаем текущего пользователя
    user_id, cashbox_id = await get_user_id_cashbox_id_by_token(token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный токен авторизации"
        )
    
    if request.duration_minutes <= 0 or request.duration_minutes > 480:  # Нужно поменять на нужное количество
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Продолжительность перерыва должна быть от 1 до 480 минут"
        )
    
    active_shift_query = employee_shifts.select().where(
        and_(
            employee_shifts.c.user_id == user_id,
            employee_shifts.c.status == ShiftStatus.on_shift,
            employee_shifts.c.shift_end.is_(None)
        )
    )
    active_shift = await database.fetch_one(active_shift_query)
    
    if not active_shift:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="У вас нет активной смены или вы уже на перерыве"
        )
    
    update_data = {
        "status": ShiftStatus.on_break,
        "break_start": datetime.utcnow(),
        "break_duration": request.duration_minutes,
        "updated_at": datetime.utcnow()
    }
    
    updated_shift = await database.fetch_one(
        employee_shifts.update()
        .where(employee_shifts.c.id == active_shift.id)
        .values(update_data)
        .returning(*employee_shifts.c)
    )
    
    return ShiftResponse(**dict(updated_shift))


@router.post("/break/end", response_model=ShiftResponse)
async def end_break(
    token: str = Header(..., description="Токен авторизации")
):
    """Завершить перерыв и вернуться на смену"""
    # Получаем текущего пользователя
    user_id, cashbox_id = await get_user_id_cashbox_id_by_token(token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный токен авторизации"
        )
    
    # Находим смену на перерыве
    break_shift_query = employee_shifts.select().where(
        and_(
            employee_shifts.c.user_id == user_id,
            employee_shifts.c.status == ShiftStatus.on_break,
            employee_shifts.c.shift_end.is_(None)
        )
    )
    break_shift = await database.fetch_one(break_shift_query)
    
    if not break_shift:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="У вас нет активного перерыва"
        )
    
    update_data = {
        "status": ShiftStatus.on_shift,
        "break_start": None,
        "break_duration": None,
        "updated_at": datetime.utcnow()
    }
    
    updated_shift = await database.fetch_one(
        employee_shifts.update()
        .where(employee_shifts.c.id == break_shift.id)
        .values(update_data)
        .returning(*employee_shifts.c)
    )
    
    return ShiftResponse(**dict(updated_shift))


@router.get("/status", response_model=ShiftStatusResponse)
async def get_shift_status(
    token: str = Header(..., description="Токен авторизации")
):
    """Получить текущий статус смены"""
    # Получаем текущего пользователя
    user_id, cashbox_id = await get_user_id_cashbox_id_by_token(token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный токен авторизации"
        )
    
    # Ищем активную смену
    active_shift_query = employee_shifts.select().where(
        and_(
            employee_shifts.c.user_id == user_id,
            employee_shifts.c.shift_end.is_(None)
        )
    ).order_by(desc(employee_shifts.c.created_at))
    
    active_shift = await database.fetch_one(active_shift_query)
    
    if not active_shift:
        return ShiftStatusResponse(
            is_on_shift=False,
            status=ShiftStatus.off_shift,
            current_shift=None,
            message="Вы не на смене"
        )
    
    # Проверяем, не истек ли перерыв
    if (active_shift.status == ShiftStatus.on_break and 
        active_shift.break_start and 
        active_shift.break_duration):
        
        break_end_time = active_shift.break_start + timedelta(minutes=active_shift.break_duration)
        if datetime.utcnow() >= break_end_time:
            update_data = {
                "status": ShiftStatus.on_shift,
                "break_start": None,
                "break_duration": None,
                "updated_at": datetime.utcnow()
            }
            
            active_shift = await database.fetch_one(
                employee_shifts.update()
                .where(employee_shifts.c.id == active_shift.id)
                .values(update_data)
                .returning(*employee_shifts.c)
            )
    
    is_on_shift = active_shift.status in [ShiftStatus.on_shift, ShiftStatus.on_break]
    
    status_messages = {
        ShiftStatus.on_shift: "Вы на смене",
        ShiftStatus.on_break: f"Вы на перерыве (осталось {_get_remaining_break_time(active_shift)} мин.)",
        ShiftStatus.off_shift: "Вы не на смене"
    }
    
    return ShiftStatusResponse(
        is_on_shift=is_on_shift,
        status=active_shift.status,
        current_shift=ShiftResponse(**dict(active_shift)),
        message=status_messages.get(active_shift.status, "Неизвестный статус")
    )


def _get_remaining_break_time(shift) -> int:
    """Вспомогательная функция для подсчета оставшегося времени перерыва"""
    if not shift.break_start or not shift.break_duration:
        return 0
    
    break_end_time = shift.break_start + timedelta(minutes=shift.break_duration)
    remaining = break_end_time - datetime.utcnow()
    
    return max(0, int(remaining.total_seconds() / 60))
