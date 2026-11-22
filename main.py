"""SPbGU dorm booking — PostgreSQL-backed API, no preview or mocked records."""
from __future__ import annotations

import base64
import csv
import calendar
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import io
import json
import os
import re
from pathlib import Path
import secrets
from typing import Any, Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, HTTPException, Request, Response, UploadFile, File
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / '.env')


def require_env(name):
    value = os.environ.get(name)
    if value is None:
        raise RuntimeError('Не задана переменная %s в backend/.env' % name)
    return value


DATABASE_URL = require_env('DATABASE_URL')
SECRET_KEY = require_env('SECRET_KEY')
APP_ENV = require_env('APP_ENV')
PRIMARY_ORGANIZATION_ID = int(os.environ.get('PRIMARY_ORGANIZATION_ID') or 0)
OFDATA_API_KEY = os.environ.get('OFDATA_API_KEY', '').strip()
OFDATA_TIMEOUT_SECONDS = float(os.environ.get('OFDATA_TIMEOUT_SECONDS', '8'))
COOKIE = require_env('COOKIE_NAME')
COOKIE_SECURE = require_env('COOKIE_SECURE').lower() in ('1', 'true', 'yes')
COOKIE_SAMESITE = require_env('COOKIE_SAMESITE')
SESSION_TTL = int(require_env('SESSION_TTL_SECONDS'))
FRONTEND_DIST = Path(require_env('FRONTEND_DIST')).expanduser()
if not FRONTEND_DIST.is_absolute():
    FRONTEND_DIST = (ROOT / FRONTEND_DIST).resolve()
MEDIA_DIR = Path(require_env('MEDIA_DIR')).expanduser()
if not MEDIA_DIR.is_absolute():
    MEDIA_DIR = (ROOT / MEDIA_DIR).resolve()
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
MAX_UPLOAD_BYTES = int(require_env('MAX_UPLOAD_MB')) * 1024 * 1024
MAX_IMPORT_ROWS = int(require_env('MAX_IMPORT_ROWS'))
CORS_ORIGINS = [item.strip() for item in require_env('CORS_ORIGINS').split(',') if item.strip()]
if len(SECRET_KEY) < 32:
    raise RuntimeError('SECRET_KEY должен содержать минимум 32 символа')
serializer = URLSafeTimedSerializer(SECRET_KEY, salt='dorm-mvp-auth-v1')
engine = None

app = FastAPI(title='Сервис бронирования мест в общежитиях', version='2.0.0')

@app.exception_handler(RequestValidationError)
async def precise_request_validation_error(request: Request, exc: RequestValidationError):
    """Explain incorrect field instead of opaque HTTP 422 or an SQL exception."""
    ru_fields = {
        'building_id':'общежитие', 'floor_id':'этаж', 'block_id':'блок',
        'room_number':'номер комнаты', 'capacity':'вместимость комнаты',
        'gender_policy':'ограничение по полу', 'category_id':'категория комфортности',
        'room_id':'комната', 'bed_id':'койко-место', 'bed_number':'номер койки',
        'quantity':'количество мест', 'status':'статус', 'name':'название',
        'check_in':'дата заезда', 'check_out':'дата выезда',
        'tariff_id':'тариф', 'amount':'стоимость', 'reason':'причина',
    }
    problems=[]
    for error in exc.errors()[:5]:
        loc=[str(part) for part in error.get('loc',()) if part not in ('body','query','path')]
        field=ru_fields.get(loc[-1],loc[-1]) if loc else 'запрос'
        kind=error.get('type','')
        if kind=='missing': explanation='обязательное поле не заполнено'
        elif kind in ('int_parsing','int_type','float_parsing','float_type'):
            explanation='должно содержать число'
        elif kind=='string_too_short': explanation='слишком короткое значение'
        elif kind=='string_too_long': explanation='слишком длинное значение'
        elif kind in ('greater_than_equal','greater_than'):
            explanation='значение меньше допустимого'
        elif kind in ('less_than_equal','less_than'):
            explanation='значение больше допустимого'
        elif kind in ('date_parsing','date_from_datetime_parsing'):
            explanation='укажите корректную дату в формате ГГГГ-ММ-ДД'
        else: explanation='проверьте формат и допустимое значение'
        problems.append(f'{field}: {explanation}')
    detail = 'Не удалось выполнить действие: ' + '; '.join(problems)
    request.state.failure_reason = detail
    return JSONResponse(status_code=422,content={'detail':detail})

@app.exception_handler(HTTPException)
async def audited_http_error(request: Request, exc: HTTPException):
    # The failure event is persisted by a separate transaction in middleware.
    request.state.failure_reason = str(exc.detail)[:400]
    return JSONResponse(status_code=exc.status_code, content={'detail': exc.detail}, headers=exc.headers)


app.add_middleware(CORSMiddleware, allow_origins=CORS_ORIGINS, allow_credentials=True,
                   allow_methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'], allow_headers=['*'])
app.mount('/media', StaticFiles(directory=MEDIA_DIR), name='media')


@app.middleware('http')
async def no_cache_personal_inventory(request: Request, call_next):
    """Availability depends on authenticated profile and booking dates.

    Clients and reverse proxies must not reuse a response across users.
    """
    response = await call_next(request)
    path = request.url.path
    if (path == '/api/catalog' or path == '/api/quote' or
        path.startswith('/api/floors/') or path.startswith('/api/rooms/') or
        (path.startswith('/api/buildings/') and path.endswith('/floor-availability'))):
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['Vary'] = 'Cookie'
    return response



def audit_failed_api_request(request: Request, status: int, reason: str):
    # Persist a separate error record after the failed business transaction.
    if not request.url.path.startswith('/api/'):
        return
    import logging
    global engine
    try:
        if engine is None:
            engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
        actor_id = None
        org_id = None
        cookie = request.cookies.get(COOKIE)
        if cookie:
            try:
                signed = serializer.loads(cookie, max_age=SESSION_TTL)
                actor_id = int(signed['id'])
            except (BadSignature, SignatureExpired, KeyError, ValueError, TypeError):
                pass
        # Use a new committed connection; otherwise the failed request's ROLLBACK
        # would erase the ERROR event along with its changes.
        with engine.begin() as connection:
            if actor_id is not None:
                identity = connection.execute(text(
                    'SELECT id,organization_id FROM dorm_booking.users WHERE id=:id'),
                    {'id': actor_id}).mappings().first()
                actor_id = identity['id'] if identity else None
                org_id = identity['organization_id'] if identity else None
            connection.execute(text('''
                INSERT INTO dorm_booking.activity_logs
                  (organization_id,actor_user_id,event_type,entity_type,result,message,details)
                VALUES (:org,:actor,'HTTP_REQUEST_FAILED','api_request','ERROR',:reason,
                        CAST(:details AS jsonb))
            '''), {
                'org': org_id, 'actor': actor_id,
                'reason': str(reason or 'Ошибка выполнения операции')[:400],
                'details': json.dumps({
                    'method':request.method, 'path':request.url.path[:180],
                    'status_code':status, **(getattr(request.state, 'db_error', None) or {})}, ensure_ascii=False)
            })
    except Exception:
        # Logging must never replace the original API error with a second error.
        logging.getLogger('dorm_booking.audit').exception('Failed to persist HTTP error audit')


@app.middleware('http')
async def audit_failed_requests(request: Request, call_next):
    try:
        response = await call_next(request)
    except Exception as exc:
        audit_failed_api_request(request, 500, 'Внутренняя ошибка сервера (' + type(exc).__name__ + ')')
        raise
    if response.status_code >= 400:
        reason = getattr(request.state, 'failure_reason', '') or (
            'Ошибка HTTP ' + str(response.status_code) + ' при выполнении ' + request.method)
        audit_failed_api_request(request, response.status_code, reason)
    return response


class JavaScriptStaticFiles(StaticFiles):
    """MIME fix for Windows when serving the React production build."""
    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        if path.lower().endswith(('.js', '.mjs')):
            response.headers['content-type'] = 'text/javascript; charset=utf-8'
        return response


if (FRONTEND_DIST / 'assets').is_dir():
    app.mount('/assets', JavaScriptStaticFiles(directory=FRONTEND_DIST / 'assets'), name='react-assets')


def db_session(request: Request):
    global engine
    if engine is None:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
    with Session(engine) as session:
        yield session


def rows(db: Session, query: str, **params: Any) -> list[dict]:
    return [dict(row) for row in db.execute(text(query), params).mappings().all()]


def row(db: Session, query: str, **params: Any) -> Optional[dict]:
    result = db.execute(text(query), params).mappings().first()
    return dict(result) if result else None


def scalar(db: Session, query: str, **params: Any):
    return db.execute(text(query), params).scalar_one_or_none()


def json_data(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(type(value).__name__)


def fail(message: str, status: int = 400):
    raise HTTPException(status_code=status, detail=message)


def wrap_db_error(exc):
    message = str(getattr(exc, 'orig', exc)).split('\n')[0]
    if isinstance(exc, IntegrityError):
        # Diagnose the actual database constraint instead of only reporting a
        # generic insertion failure. Never remove/disable financial checks.
        diagnostic = getattr(getattr(exc, 'orig', None), 'diag', None)
        constraint_name = getattr(diagnostic, 'constraint_name', None)
        if constraint_name == 'uq_organizations_inn_normalized':
            return HTTPException(409, 'Организация с таким ИНН уже зарегистрирована. Проверьте реестр организаций.')
        sqlstate = getattr(getattr(exc, 'orig', None), 'pgcode', None) or getattr(getattr(exc, 'orig', None), 'sqlstate', None)
        if constraint_name in ('ex_period_no_overlap', 'ex_quota_slot_no_overlap') or (
            sqlstate == '23P01' and ('ex_period_no_overlap' in message or 'ex_quota_slot_no_overlap' in message)
        ):
            if constraint_name == 'ex_quota_slot_no_overlap' or 'ex_quota_slot_no_overlap' in message:
                return HTTPException(409, 'На выбранные даты все места этого пула уже забронированы. Выберите другой период или вариант проживания.')
            return HTTPException(409, 'Это койко-место уже забронировано на выбранные даты. Выберите другое место или период проживания.')
        if constraint_name == 'ck_booking_fee_snapshot' or 'ck_booking_fee_snapshot' in message:
            return HTTPException(409,
                'Сумма комиссии не совпадает с её снимком в PostgreSQL. '
                'Проверьте, выполнена ли миграция '
                'backend/migrations/2026_11_12_v6_9_repair_booking_fee_snapshot.sql '
                'после v6.7. Если миграция уже выполнена, проверьте '
                'fee_base_amount_snapshot и формулу комиссии.')
        # CSV imports with explicit PKs do not advance IDENTITY/SERIAL sequences.
        constraint = str(getattr(getattr(exc, 'orig', None), 'diag', None) or '')
        if (
            ('нарушает ограничение уникальности' in message and '_pkey' in message)
            or ('duplicate key value' in message.lower() and '_pkey' in (message + constraint))
        ):
            return HTTPException(409,
                "Конфликт первичного ключа PostgreSQL. После импорта CSV проверьте "
                "последовательности ID (SQL-скрипт 01_синхронизация_ID_всех_таблиц.sql).")
        # Frequent conflicts from administrator forms: return field-level errors.
        constraint_messages = {
            'uq_room_in_block':'Комната с таким номером уже существует в выбранном блоке. Укажите другой номер.',
            'uq_room_without_block':'Комната с таким номером уже существует на выбранном этаже. Укажите другой номер.',
            'uq_bed_room_number':'Место с таким номером уже существует в этой комнате.',
            'uq_floor_building_number':'Этаж с таким номером уже существует в выбранном общежитии.',
            'uq_block_building_code':'Блок с таким кодом уже существует в этом общежитии.',
            'ex_period_no_overlap':'Койка уже занята на выбранные даты. Выберите другое место или период.',
            'ex_quota_slot_no_overlap':'Этот слот количественного пула уже забронирован на выбранные даты.',
            'ck_room_active_category':'Для активной комнаты требуется указать категорию комфортности.',
        }
        if constraint_name in constraint_messages:
            return HTTPException(409, constraint_messages[constraint_name])
        if sqlstate == '23505':
            return HTTPException(409, 'Такая запись уже существует. Проверьте номер или название и выберите уникальное значение.')
        if sqlstate == '23503':
            return HTTPException(409, 'Запись связана с другими объектами. Сначала удалите или измените зависимые записи.')
        if sqlstate == '23514':
            return HTTPException(409, 'Не выполнено ограничение данных: проверьте статус, вместимость, пол и обязательные параметры.')
        return HTTPException(409, 'Операция конфликтует с существующими данными. Проверьте уникальность номера и связанные объекты.')
    # Expected business-rule exceptions from PostgreSQL must be clear to the person.
    business_errors = (
        ('Новое правило этажа конфликтует', 'Ограничение по полу этажа противоречит характеристикам здания или комнат.'),
        ('Конфликт распределения по полу', 'Ограничения по полу здания, этажа, блока и комнаты несовместимы. Исправьте их перед сохранением.'),
        ('Конфликт правил размещения по полу', 'Ограничение по полу противоречит параметрам вышестоящего уровня.'),
        ('Пол пользователя несовместим', 'Это место не подходит для вашего пола. Выберите другое место.'),
        ('Несовместим пол размещения', 'Это место не подходит для вашего пола. Выберите другое место.'),
        ('У пользователя есть пересекающееся бронирование', 'У вас уже есть бронирование на эти даты.'),
        ('Это койко-место уже забронировано', 'Это койко-место уже забронировано на выбранные даты. Выберите другое место или период проживания.'),
        ('Нет подходящего тарифа', 'На выбранные даты для вашей категории нет подходящего тарифа.'),
        ('Нет тарифа проживания', 'Для этого места не настроен подходящий тариф.'),
        ('Для этого койко-места не назначен подходящий активный тариф', 'Это место сейчас недоступно: для вашей категории и даты не назначен подходящий активный тариф.'),
        ('Выбранный тариф не соответствует общежитию', 'Выбранный тариф не соответствует характеристикам места.'),
        ('Место не опубликовано', 'Место больше недоступно для бронирования.'),
        ('Пол койки противоречит', 'Ограничение по полу противоречит характеристикам общежития или комнаты.'),
        ('Заезд возможен через', 'Выбранная дата заезда не соответствует правилам организации.'),
        ('Койка не соответствует пулу', 'Выбранное место не соответствует общежитию, комфортности, вместимости или статусу физического пула.'),
        ('Неактивное здание/комната/этаж/блок', 'Невозможно назначить место: его общежитие, этаж, блок или комната не активны.'),
        ('Резерв количественного слота не активен', 'Нельзя назначить койку: резерв в количественном пуле уже отменён или завершён.'),
        ('Это не нераспределённый QUOTA-резерв', 'Назначение невозможно: заявка отменена, завершена или место уже назначено.'),
        ('Назначенная койка не принадлежит количественному пулу', 'Эта койка не закреплена за выбранным физическим пулом.'),
        ('Назначенная койка имеет другой период/организацию', 'Даты или организация выбранной койки не совпадают с заявкой.'),
        ('Место закреплено за активным пулом', 'Место уже зарезервировано для количественного пула.'),
        ('Нельзя выключить пул с действующими', 'В пуле остались текущие или будущие бронирования. Сначала урегулируйте их.'),
        ('Менять состав можно только', 'Состав активированного пула нельзя менять напрямую. Используйте безопасное исключение коек.'),
        ('Койка закреплена за количественным пулом', 'Сначала исключите койку из количественного пула, затем опубликуйте её отдельно.'),
        ('нет подходящего', 'Не найдены подходящие параметры для этого действия. Проверьте тариф, доступность и статус места.'),
    )
    for needle, friendly in business_errors:
        if needle.casefold() in message.casefold():
            return HTTPException(409, friendly)
    # PostgreSQL's explicit RAISE EXCEPTION uses SQLSTATE P0001. It contains a
    # business explanation, not SQL syntax. Return a sanitized version rather
    # than a generic phrase. Internal DDL/constraint errors remain server-side.
    origin = getattr(exc, 'orig', exc)
    state = getattr(origin, 'pgcode', None) or getattr(origin, 'sqlstate', None)
    if state == 'P0001':
        import re
        detail = re.sub(r'(?i)^(?:ERROR|ОШИБКА)\s*:\s*', '', message).strip()
        # Never leak SQL identifiers, source snippets, or untrusted huge text.
        if (0 < len(detail) <= 250 and not any(tok in detail.lower() for tok in
            ('select ', 'update ', 'insert ', 'delete ', 'constraint ', 'sqlstate', 'pg_', 'dorm_booking.'))):
            return HTTPException(409, detail)
    if state == '23503':
        return HTTPException(409, 'Объект связан с другими данными и не может быть изменён или удалён до удаления этих связей.')
    if state == '23505':
        return HTTPException(409, 'Запись с такими параметрами уже существует. Проверьте номер комнаты, места или название.')
    if state == '23514':
        return HTTPException(409, 'Значения не соответствуют ограничениям данных. Проверьте пол, статус, вместимость и связанные объекты.')
    return HTTPException(400, 'Операция отклонена базой данных. Уточните параметры и обратитесь к администратору, указав время операции.')



def organization_creation_db_error(exc: SQLAlchemyError, stage: str) -> HTTPException:
    """Explain a failed creation without exposing SQL or connection details.

    PostgreSQL SQLSTATE is stable; localized text and SQLAlchemy exception
    formatting are not. Unknown failures carry a reference for server-side logs.
    """
    source = getattr(exc, 'orig', exc)
    diag = getattr(source, 'diag', None)
    state = getattr(source, 'sqlstate', None) or getattr(source, 'pgcode', None)
    constraint = getattr(diag, 'constraint_name', None)
    column = getattr(diag, 'column_name', None)
    if constraint == 'uq_organizations_inn_normalized':
        return HTTPException(409, 'Организация с таким ИНН уже зарегистрирована. Проверьте реестр организаций.')
    if constraint == 'organizations_pkey':
        return HTTPException(409,
            'Не удалось создать организацию: счётчик идентификаторов организаций '
            'не соответствует существующим записям. Администратору БД необходимо '
            'проверить последовательность organizations.id после импорта.')
    field_checks = {
        'organizations_inn_check': 'ИНН должен содержать 10 или 12 цифр.',
        'organizations_kpp_check': 'КПП должен содержать 9 цифр.',
        'organizations_ogrn_check': 'ОГРН должен содержать 13 или 15 цифр.',
        'organizations_organization_type_check': 'Выбран недопустимый тип организации.',
        'organizations_status_check': 'Выбран недопустимый статус организации.',
    }
    if constraint in field_checks:
        return HTTPException(422, 'Не удалось создать организацию: ' + field_checks[constraint])
    columns = {
        'name': 'полное наименование', 'short_name': 'краткое наименование',
        'organization_type': 'тип организации', 'inn': 'ИНН', 'city': 'город',
        'id': 'идентификатор организации',
    }
    if state == '23502':
        field = columns.get(column, 'одно из обязательных полей')
        return HTTPException(422, f'Не удалось создать организацию: не заполнено поле «{field}».')
    if state == '23505':
        return HTTPException(409,
            'Не удалось создать организацию: в базе уже есть запись с такими уникальными данными. '
            'Проверьте ИНН и реестр организаций.')
    if state == '22001':
        return HTTPException(422, 'Не удалось создать организацию: одно из текстовых полей превышает допустимую длину.')
    if state == '23503':
        return HTTPException(409,
            'Не удалось создать организацию: нарушена связь с другой записью базы данных '
            f'на этапе «{stage}».')
    if state in ('42P08', '42P18'):
        return HTTPException(503,
            'Не удалось создать организацию: PostgreSQL не смог определить тип параметра '
            f'на этапе «{stage}» (код {state}). Обновите backend до версии v6.24.')
    if state == '42501':
        return HTTPException(503, 'Не удалось создать организацию: у подключения PostgreSQL недостаточно прав на запись.')
    if state in ('42P01', '42703', '42883', '42704'):
        return HTTPException(503,
            'Не удалось создать организацию: структура PostgreSQL не соответствует установленной '
            f'версии приложения (код {state}, этап «{stage}»). Проверьте применённые миграции.')
    if state == '23514':
        return HTTPException(422,
            'Не удалось создать организацию: значение одного из полей нарушает ограничение БД. '
            'Проверьте реквизиты, название и тип организации.')
    if state == 'P0001':
        # Business rules may raise this state without a named constraint.
        primary = (getattr(diag, 'message_primary', '') or '').strip()
        if primary and len(primary) <= 240 and not any(part in primary.lower() for part in
            ('select ', 'insert ', 'update ', 'delete ', 'dorm_booking.', 'sqlstate', 'constraint ')):
            return HTTPException(409, primary)
    import secrets
    ref = secrets.token_hex(4).upper()
    import logging
    logging.getLogger('dorm_booking.organizations').error(
        'Organization creation failed ref=%s stage=%s sqlstate=%s constraint=%s column=%s cause=%s',
        ref, stage, state, constraint, column,
        str(getattr(diag, 'message_primary', '') or type(exc).__name__)[:300])
    return HTTPException(500,
        f'Не удалось создать организацию на этапе «{stage}». '
        f'Код PostgreSQL: {state or "не определён"}. Номер ошибки: {ref}. '
        'Передайте эти данные администратору для поиска причины в журнале сервера.')

def log(db: Session, actor: Optional[dict], event: str, entity: Optional[str] = None,
        entity_id: Optional[int] = None, message: str = '', details: Optional[dict] = None,
        org_id: Optional[int] = None):
    db.execute(text("""
        INSERT INTO dorm_booking.activity_logs
        (organization_id, actor_user_id, event_type, entity_type, entity_id, result, message, details)
        VALUES (:org, :actor, :evt, :entity, :id, 'SUCCESS', :message, CAST(:details AS jsonb))
    """), dict(org=org_id if org_id is not None else (actor or {}).get('organization_id'),
                actor=(actor or {}).get('id'), evt=event, entity=entity, id=entity_id,
                message=message, details=json.dumps(details or {}, ensure_ascii=False)))


def make_hash(password: str) -> str:
    """scrypt (stdlib); never persist Excel plaintext passwords."""
    salt = secrets.token_bytes(16)
    key = hashlib.scrypt(password.encode('utf-8'), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return 'scrypt$16384$8$1$' + base64.b64encode(salt).decode() + '$' + base64.b64encode(key).decode()


def split_full_name(full_name: str):
    parts=(full_name or '').strip().split()
    if len(parts)<2:
        fail('Укажите фамилию и имя раздельно, через пробел')
    return parts[0],parts[1],(' '.join(parts[2:]) if len(parts)>2 else None)


def verify_hash(password: str, password_hash: Optional[str]) -> bool:
    if not password_hash:
        return False
    try:
        alg, n, r, p, salt, digest = password_hash.split('$')
        if alg != 'scrypt':
            return False
        actual = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt),
                                n=int(n), r=int(r), p=int(p), dklen=32)
        return secrets.compare_digest(actual, base64.b64decode(digest))
    except (ValueError, TypeError):
        return False


def current_user(request: Request, db: Session = Depends(db_session)) -> dict:
    token = request.cookies.get(COOKIE)
    if not token:
        fail('Требуется вход в систему', 401)
    try:
        identity = serializer.loads(token, max_age=SESSION_TTL)
    except (BadSignature, SignatureExpired):
        fail('Сессия недействительна или истекла', 401)
    u = row(db, 'SELECT * FROM dorm_booking.users WHERE id=:id', id=int(identity['id']))
    if u and u['status'] == 'BLOCKED':
        fail('Данный аккаунт заблокирован', 403)
    if not u or u['status'] != 'ACTIVE':
        fail('Доступ к аккаунту отключён', 403)
    if identity.get('password_stamp') != hashlib.sha256((u.get('password_hash') or '').encode()).hexdigest()[:24]:
        fail('Сессия устарела. Войдите снова', 401)
    if u['organization_id']:
        org = row(db, 'SELECT * FROM dorm_booking.organizations WHERE id=:id', id=u['organization_id'])
        if not org or org['platform_blocked_at'] or (org['status'] != 'ACTIVE' and u['role'] != 'ORG_ADMIN'):
            fail('Организация недоступна', 403)
    return u


def roles(user: dict, *permitted: str):
    if user['role'] not in permitted:
        fail('Недостаточно прав', 403)


def own_org(user: dict, organization_id: int):
    roles(user, 'ORG_ADMIN')
    if user['organization_id'] != organization_id:
        fail('Операция с чужой организацией запрещена', 403)


def clock_date(db: Session):
    """The transaction's effective date, consistent with PostgreSQL booking checks."""
    return scalar(db, "SELECT (now() AT TIME ZONE 'Europe/Moscow')::date")


def local_date(org_tz=None):
    if org_tz is None:
        org_tz = require_env('DEFAULT_TIMEZONE')
    try:
        return datetime.now(ZoneInfo(org_tz)).date()
    except Exception:
        return date.today()


class LoginInput(BaseModel):
    login: str
    password: str
    organization_id: Optional[int] = None


@app.get('/api/health')
def health(db: Session = Depends(db_session)):
    """Real DB health; no fake healthy response when PostgreSQL is offline."""
    try:
        version = scalar(db, """SELECT CASE WHEN EXISTS (
          SELECT 1 FROM information_schema.columns WHERE table_schema='dorm_booking'
          AND table_name='tariffs' AND column_name='billing_unit')
          AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='dorm_booking'
          AND table_name='users' AND column_name='citizenship_category')
          THEN 'v2.0' ELSE 'unsupported' END""")
        if version != 'v2.0':
            fail('База не соответствует согласованной DDL v2.0',503)
        return {'status':'ok','database':'postgresql','schema':version}
    except HTTPException:
        raise
    except SQLAlchemyError:
        fail('PostgreSQL недоступен',503)


def choose_primary_org(organizations):
    if PRIMARY_ORGANIZATION_ID:
        return next((o for o in organizations if o['id']==PRIMARY_ORGANIZATION_ID), None)
    return next((o for o in organizations if
        ('спбгу' in (o.get('short_name') or '').casefold() or 'spbgu' in (o.get('short_name') or '').casefold()) or
        'санкт-петербургский государственный университет' in (o.get('name') or '').casefold()), None) or next(iter(organizations), None)


@app.get('/api/organizations/{organization_id}/theme')
def organization_public_theme(organization_id: int,
                              db: Session = Depends(db_session)):
    theme = row(db, '''SELECT id,name,short_name,city,brand_logo_url,
        brand_primary_color,brand_accent_color,brand_background_color,brand_font
        FROM dorm_booking.organizations
        WHERE id=:id AND status='ACTIVE' AND platform_blocked_at IS NULL''',
        id=organization_id)
    if not theme:
        fail('Организация не найдена', 404)
    return theme


@app.get('/api/organizations')
def public_orgs(db: Session = Depends(db_session)):
    organizations = rows(db, """SELECT o.id,o.name,o.short_name,o.organization_type,o.status,
      o.brand_logo_url AS logo_url,o.brand_logo_url,o.city FROM dorm_booking.organizations o
      WHERE o.status='ACTIVE' AND o.platform_blocked_at IS NULL ORDER BY o.short_name""")
    primary = choose_primary_org(organizations)
    for item in organizations:
        item['is_primary'] = bool(primary and item['id']==primary['id'])
    return sorted(organizations, key=lambda o:(not o['is_primary'],(o.get('short_name') or '').casefold()))


@app.get('/api/catalog/cities')
def public_catalog_cities(db:Session=Depends(db_session)):
    return [item['city'] for item in rows(db,"""SELECT DISTINCT o.city
      FROM dorm_booking.organizations o
      WHERE o.status='ACTIVE' AND o.platform_blocked_at IS NULL
        AND NULLIF(btrim(o.city),'') IS NOT NULL
        AND EXISTS (SELECT 1 FROM dorm_booking.buildings b
                    WHERE b.organization_id=o.id AND b.status='ACTIVE')
      ORDER BY o.city""")]


@app.post('/api/login')
def login(body: LoginInput, response: Response, db: Session = Depends(db_session)):
    # Organization scoped login; if multiple organizations share login, explicit selection is required.
    # Check the password first; never disclose that an account is blocked to someone
    # who does not know its credentials. BLOCKED users must receive a useful reason.
    choices = rows(db, """SELECT * FROM dorm_booking.users WHERE lower(login)=lower(:login)
      AND ((CAST(:org AS BIGINT) IS NULL AND organization_id IS NULL) OR organization_id=:org)""",
                   login=body.login.strip(), org=body.organization_id)
    if len(choices) != 1 or not verify_hash(body.password, choices[0].get('password_hash')):
        fail('Неверный логин, пароль или организация', 401)
    u = choices[0]
    if u['status'] == 'BLOCKED':
        fail('Данный аккаунт заблокирован', 403)
    if u['status'] != 'ACTIVE' and not (u['status'] == 'PENDING' and u['role'] == 'STUDENT'):
        fail('Вход для данной учётной записи пока недоступен', 403)
    if u['organization_id']:
        org = row(db, 'SELECT * FROM dorm_booking.organizations WHERE id=:id', id=u['organization_id'])
        if not org or org['status'] != 'ACTIVE' or org['platform_blocked_at']:
            fail('Организация заблокирована', 403)
    if u['status'] == 'PENDING' and u['role'] == 'STUDENT':
        db.execute(text("UPDATE dorm_booking.users SET status='ACTIVE' WHERE id=:id AND status='PENDING'"), {'id':u['id']})
        u['status'] = 'ACTIVE'
    response.set_cookie(COOKIE, serializer.dumps({'id': u['id'], 'password_stamp': hashlib.sha256(u['password_hash'].encode()).hexdigest()[:24]}), httponly=True,
                        secure=COOKIE_SECURE, samesite=COOKIE_SAMESITE, max_age=SESSION_TTL, path='/')
    log(db, u, 'LOGIN', 'users', u['id'], 'Успешный вход')
    db.commit()
    return {'id': u['id'], 'role': u['role'], 'organization_id': u['organization_id'], 'name': u.get('full_name') or ' '.join(str(u.get(k) or '') for k in ('last_name','first_name','middle_name')).strip()}


@app.post('/api/logout')
def logout(response: Response):
    response.delete_cookie(COOKIE, path='/')
    return {'ok': True}


@app.get('/api/me')
def me(u: dict = Depends(current_user),db: Session = Depends(db_session)):
    profile = {k: json_data(v) if isinstance(v,(date,datetime,Decimal)) else v for k,v in u.items()
            if k not in ('password_hash','external_user_id')}
    if not profile.get('full_name'):
        profile['full_name'] = ' '.join(str(profile.get(k) or '') for k in ('last_name','first_name','middle_name')).strip()
    profile['organization_name'] = scalar(db, 'SELECT name FROM dorm_booking.organizations WHERE id=:id',id=u['organization_id']) if u['organization_id'] else None
    profile['organization_short_name'] = scalar(db, 'SELECT short_name FROM dorm_booking.organizations WHERE id=:id',id=u['organization_id']) if u['organization_id'] else None
    return profile


@app.get('/api/account/booking-rules')
def my_booking_rules(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'STUDENT','GUEST')
    value=row(db,"""SELECT start_min_days,start_max_days,paid_max_months,guest_max_days,
        budget_requires_study_end FROM dorm_booking.booking_rules
        WHERE organization_id=:org""",org=u['organization_id'])
    if not value:
        fail('Правила бронирования ещё не настроены',409)
    return value


def guest_permitted_view_period(db: Session, visitor: dict, start: Optional[date],
                                finish: Optional[date], rules: Optional[dict]):
    """Resolve an actual approved guest stay in the organization's timezone.

    An explicitly selected interval must lie in ONE permission. Without dates
    (e.g. a direct /rooms/ URL), use the next still-bookable permission instead
    of assuming tomorrow. Checkout is an exclusive date and never gains a day.
    """
    earliest = clock_date(db) + timedelta(days=int((rules or {}).get('start_min_days', 1)))
    limit_days = int((rules or {}).get('guest_max_days') or 7)
    args = dict(user=visitor['id'], org=visitor['organization_id'],
                tz=require_env('DEFAULT_TIMEZONE'))
    if start is None:
        access = row(db, """SELECT ga.id,
                (ga.starts_at AT TIME ZONE :tz)::date AS starts,
                (ga.expires_at AT TIME ZONE :tz)::date AS ends
            FROM dorm_booking.guest_accesses ga
            WHERE ga.guest_user_id=:user AND ga.host_organization_id=:org
              AND ga.status='ACTIVE' AND ga.admin_approved_at IS NOT NULL
              AND (ga.expires_at AT TIME ZONE :tz)::date > CAST(:earliest AS date)
            ORDER BY ga.starts_at,ga.id LIMIT 1""", **args, earliest=earliest)
        if not access:
            return earliest, earliest + timedelta(days=limit_days), False
        start = max(earliest, access['starts'])
    else:
        access = row(db, """SELECT ga.id,
                (ga.starts_at AT TIME ZONE :tz)::date AS starts,
                (ga.expires_at AT TIME ZONE :tz)::date AS ends
            FROM dorm_booking.guest_accesses ga
            WHERE ga.guest_user_id=:user AND ga.host_organization_id=:org
              AND ga.status='ACTIVE' AND ga.admin_approved_at IS NOT NULL
              AND (ga.starts_at AT TIME ZONE :tz)::date <= CAST(:start AS date)
              AND (ga.expires_at AT TIME ZONE :tz)::date > CAST(:start AS date)
              AND (CAST(:finish AS date) IS NULL OR
                   (ga.expires_at AT TIME ZONE :tz)::date >= CAST(:finish AS date))
            ORDER BY ga.expires_at,ga.id LIMIT 1""", **args,
            start=start,finish=finish)
        if not access:
            return start, finish or start + timedelta(days=limit_days), False
    actual_finish = finish or min(access['ends'], start + timedelta(days=limit_days))
    eligible = (start >= earliest and start >= access['starts']
                and actual_finish <= access['ends']
                and actual_finish <= start + timedelta(days=limit_days)
                and actual_finish > start)
    return start, actual_finish, eligible


def personal_view_period(db: Session, visitor: Optional[dict], start: Optional[date] = None,
                         finish: Optional[date] = None) -> tuple[date, date, bool]:
    """Use a single authorized date interval across catalog, floors and rooms.

    The catalog accepts optional explicit dates; room and floor views use the
    same interval when supplied, otherwise derive it from the guest permission.
    """
    rules = None
    if visitor and visitor.get('role') in ('STUDENT', 'GUEST'):
        rules = row(db, """SELECT start_min_days,paid_max_months,guest_max_days
                      FROM dorm_booking.booking_rules WHERE organization_id=:org""",
                    org=visitor['organization_id'])
    profile_ok = not (visitor and visitor.get('role') in ('STUDENT','GUEST')) or bool(
        visitor.get('gender') in ('MALE','FEMALE') and
        visitor.get('citizenship_category') in ('RUSSIAN','FOREIGN') and
        (visitor['role'] == 'GUEST' or
         (visitor.get('education_basis') in ('PAID','BUDGET') and visitor.get('study_status') == 'STUDYING')))
    if visitor and visitor.get('role') == 'GUEST':
        check_in, check_out, access_ok = guest_permitted_view_period(db, visitor, start, finish, rules)
        return check_in, check_out, bool(profile_ok and rules and access_ok)
    start = start or clock_date(db) + timedelta(days=int((rules or {}).get('start_min_days', 1)))
    if visitor and visitor.get('role') == 'STUDENT':
        if profile_ok and rules:
            try:
                finish, _ = booking_period_for_person(db, visitor, start, None)
            except HTTPException:
                profile_ok = False
                finish = start + timedelta(days=30)
        else:
            profile_ok = False
            finish = start + timedelta(days=30)
    else:
        finish = finish or start + timedelta(days=30)
    return start, finish, bool(profile_ok and finish > start)


class SearchFilters(BaseModel):
    organization_id: Optional[int] = None
    city: Optional[str] = None
    district: Optional[str] = None
    building_id: Optional[int] = None
    category_id: Optional[int] = None
    capacity: Optional[int] = None
    min_capacity: Optional[int] = Field(default=None, ge=1, le=50)
    max_capacity: Optional[int] = Field(default=None, ge=1, le=50)
    gender: Optional[str] = None
    min_price: float = Field(default=0, ge=0)
    max_price: float = Field(default=20000, ge=0)
    amenities: list[str] = Field(default_factory=list)
    check_in: Optional[date] = None
    check_out: Optional[date] = None
    sort: str = 'price_asc'
    mode: Optional[str] = None
    available_only: bool = False


# Query actual inventory by selected dates (not only currently free beds).
# For an unauthenticated visitor, show available published tariff values without guessing nationality.
# Before booking, tariff and fee are recalculated for the authenticated person in PostgreSQL.
CATALOG_EXACT = """
 SELECT b.id AS bed_id, bu.id AS building_id,bu.organization_id,bu.name AS building_name,
 bu.address,bu.district,bu.latitude,bu.longitude,bu.description AS building_description,
 r.id AS room_id,r.room_number,r.capacity,f.id AS floor_id,f.floor_number,
 r.block_id,bl.name AS block_name,c.id AS category_id,c.name AS category_name,
 b.bed_number,'EXACT_BED' AS mode,b.id AS offer_id,
 t.amount AS monthly_amount,t.billing_unit,t.id AS tariff_id,
 b.gender_policy AS gender_policy
 FROM dorm_booking.beds b
 JOIN dorm_booking.rooms r ON r.id=b.room_id AND r.status='ACTIVE'
 JOIN dorm_booking.floors f ON f.id=r.floor_id AND f.status='ACTIVE'
 LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
 JOIN dorm_booking.buildings bu ON bu.id=r.building_id AND bu.status='ACTIVE'
 JOIN dorm_booking.organizations o ON o.id=bu.organization_id
 JOIN dorm_booking.comfort_categories c ON c.id=r.comfort_category_id AND c.is_active
 JOIN LATERAL (SELECT t0.* FROM dorm_booking.bed_tariff_links assigned
    JOIN dorm_booking.tariffs t0 ON t0.id=assigned.tariff_id
    WHERE assigned.bed_id=b.id AND assigned.organization_id=b.organization_id
      AND t0.status='ACTIVE' AND t0.organization_id=b.organization_id
      AND t0.building_id=r.building_id
    AND t0.comfort_category_id=r.comfort_category_id AND t0.room_capacity=r.capacity
    AND (CAST(:occupant AS varchar) IS NULL OR (t0.occupant_category IN (:occupant,'ANY') AND (CAST(:occupant AS varchar) <> 'GUEST' OR t0.occupant_category='GUEST')))
    AND (CAST(:citizenship AS varchar) IS NULL OR t0.citizenship_category IN (:citizenship,'ANY'))
    AND (t0.season_id IS NULL OR EXISTS (
      SELECT 1 FROM dorm_booking.tariff_seasons ts
      JOIN dorm_booking.tariff_season_months sm ON sm.season_id=ts.id
      WHERE ts.id=t0.season_id AND ts.is_active
        AND sm.month_number=EXTRACT(MONTH FROM CAST(:start AS date))))
    ORDER BY CASE WHEN t0.occupant_category=:occupant THEN 0 ELSE 1 END,
      CASE WHEN t0.citizenship_category=:citizenship THEN 0 ELSE 1 END,
      CASE WHEN t0.season_id IS NOT NULL THEN 0 ELSE 1 END,t0.amount,t0.id LIMIT 1) t ON TRUE
 WHERE b.status='ACTIVE'
 AND o.status='ACTIVE' AND o.platform_blocked_at IS NULL
 AND (bl.id IS NULL OR bl.status='ACTIVE')
 AND COALESCE(b.gender_policy,'CONFLICT')<>'CONFLICT'
 AND (CAST(:organization_id AS BIGINT) IS NULL OR bu.organization_id=:organization_id)
 AND (CAST(:city AS TEXT) IS NULL OR o.city=:city)
 AND (CAST(:guest_only AS BOOLEAN) = FALSE OR bu.allow_guests)
 AND (CAST(:building_id AS BIGINT) IS NULL OR bu.id=:building_id)
 AND (CAST(:district AS TEXT) IS NULL OR bu.district=:district)
 AND (CAST(:category_id AS BIGINT) IS NULL OR c.id=:category_id)
 AND (CAST(:capacity AS SMALLINT) IS NULL OR r.capacity=:capacity)
 AND (CAST(:min_capacity AS SMALLINT) IS NULL OR r.capacity >= :min_capacity)
 AND (CAST(:max_capacity AS SMALLINT) IS NULL OR r.capacity <= :max_capacity)
 AND (CAST(:gender AS varchar) IS NULL OR (
   COALESCE(b.gender_policy,'ANY') IN ('ANY',:gender)
   AND COALESCE(r.gender_policy,'ANY') IN ('ANY',:gender)
   AND COALESCE(f.gender_policy,'ANY') IN ('ANY',:gender)
   AND COALESCE(bl.gender_policy,'ANY') IN ('ANY',:gender)
   AND COALESCE(bu.gender_policy,'ANY') IN ('ANY',:gender)))
 AND (CAST(:occupant AS varchar) IS NULL OR EXISTS (
   SELECT 1 FROM dorm_booking.booking_fee_policies fp
   WHERE fp.organization_id=o.id AND (
      (fp.occupant_category=:occupant AND fp.citizenship_category=:citizenship)
      OR (fp.occupant_category='ANY' AND fp.citizenship_category='ANY'))))
 AND t.amount BETWEEN :min_price AND :max_price
 AND NOT EXISTS(SELECT 1 FROM dorm_booking.bed_periods bp WHERE bp.bed_id=b.id
   AND (bp.status='ACTIVE' OR EXISTS (
     SELECT 1 FROM dorm_booking.bookings bk WHERE bk.bed_period_id=bp.id
       AND bk.status IN ('PENDING_ADMIN','CONFIRMED')))
   AND daterange(bp.check_in_date,bp.check_out_date,'[)')
   && daterange(CAST(:start AS date),CAST(:finish AS date),'[)'))
"""

CATALOG_QUOTA = """
 SELECT q.id AS offer_id,q.id AS quota_pool_id,q.building_id,bu.organization_id,
 bu.name AS building_name,bu.address,bu.district,bu.latitude,bu.longitude,
 bu.description AS building_description,c.id AS category_id,c.name AS category_name,
 q.room_capacity AS capacity,q.name AS pool_name,q.gender_policy,
 t.amount AS monthly_amount,t.billing_unit,t.id AS tariff_id,'QUOTA' AS mode,q.quantity,
 LEAST(
 (SELECT count(*) FROM dorm_booking.quota_slots sl
  WHERE sl.pool_id=q.id AND sl.status='ACTIVE'
  AND NOT EXISTS(SELECT 1 FROM dorm_booking.quota_slot_periods qp
    WHERE qp.slot_id=sl.id AND qp.status='ACTIVE'
    AND daterange(qp.check_in_date,qp.check_out_date,'[)')
    && daterange(CAST(:start AS date),CAST(:finish AS date),'[)'))),
 CASE WHEN q.inventory_mode = 'PHYSICAL' THEN
  (SELECT count(*) FROM dorm_booking.quota_pool_beds member
   JOIN dorm_booking.beds linked ON linked.id=member.bed_id
   JOIN dorm_booking.rooms linked_room ON linked_room.id=linked.room_id AND linked_room.status='ACTIVE'
   JOIN dorm_booking.floors linked_floor ON linked_floor.id=linked_room.floor_id AND linked_floor.status='ACTIVE'
   LEFT JOIN dorm_booking.blocks linked_block ON linked_block.id=linked_room.block_id
   WHERE member.pool_id=q.id AND linked.status IN ('DRAFT','INACTIVE')
     AND (linked_block.id IS NULL OR linked_block.status='ACTIVE')
     AND (CAST(:gender AS varchar) IS NULL OR dorm_booking.bed_allowed_for_gender(linked.id,:gender))
     AND NOT EXISTS(SELECT 1 FROM dorm_booking.bed_periods blocked
       WHERE blocked.bed_id=linked.id AND blocked.status='ACTIVE'
         AND daterange(blocked.check_in_date,blocked.check_out_date,'[)')
           && daterange(CAST(:start AS date),CAST(:finish AS date),'[)')))
 ELSE q.quantity END)::integer AS free_quantity
 FROM dorm_booking.quota_pools q
 JOIN dorm_booking.buildings bu ON bu.id=q.building_id AND bu.status='ACTIVE'
 JOIN dorm_booking.organizations o ON o.id=q.organization_id
 JOIN dorm_booking.comfort_categories c ON c.id=q.comfort_category_id AND c.is_active
 JOIN LATERAL (
  SELECT t1.id,t1.amount,t1.billing_unit FROM dorm_booking.tariffs t1
  WHERE EXISTS (SELECT 1 FROM dorm_booking.quota_pool_tariff_links assigned
                WHERE assigned.pool_id=q.id AND assigned.tariff_id=t1.id)
    AND t1.organization_id=q.organization_id
    AND t1.building_id=bu.id AND t1.comfort_category_id=c.id
    AND t1.room_capacity=q.room_capacity AND t1.status='ACTIVE'
    AND (CAST(:occupant AS varchar) IS NULL OR (t1.occupant_category IN (:occupant,'ANY') AND (CAST(:occupant AS varchar) <> 'GUEST' OR t1.occupant_category='GUEST')))
    AND (CAST(:citizenship AS varchar) IS NULL OR t1.citizenship_category IN (:citizenship,'ANY'))
    AND (t1.season_id IS NULL OR EXISTS (
      SELECT 1 FROM dorm_booking.tariff_seasons ts
      JOIN dorm_booking.tariff_season_months sm ON sm.season_id=ts.id
      WHERE ts.id=t1.season_id AND ts.is_active
      AND sm.month_number=EXTRACT(MONTH FROM CAST(:start AS date))))
  ORDER BY CASE WHEN CAST(:occupant AS varchar) IS NULL THEN t1.amount ELSE 0 END,
    CASE WHEN t1.occupant_category=:occupant THEN 0 ELSE 1 END,
    CASE WHEN t1.citizenship_category=:citizenship THEN 0 ELSE 1 END,
    CASE WHEN t1.season_id IS NOT NULL THEN 0 ELSE 1 END,t1.amount,t1.id LIMIT 1
 ) t ON TRUE
 WHERE q.status='ACTIVE' AND o.status='ACTIVE' AND o.platform_blocked_at IS NULL
 AND (CAST(:organization_id AS BIGINT) IS NULL OR q.organization_id=:organization_id)
 AND (CAST(:city AS TEXT) IS NULL OR o.city=:city)
 AND (CAST(:guest_only AS BOOLEAN) = FALSE OR bu.allow_guests)
 AND (CAST(:building_id AS BIGINT) IS NULL OR bu.id=:building_id)
 AND (CAST(:district AS TEXT) IS NULL OR bu.district=:district)
 AND (CAST(:category_id AS BIGINT) IS NULL OR c.id=:category_id)
 AND (CAST(:capacity AS SMALLINT) IS NULL OR q.room_capacity=:capacity)
 AND (CAST(:min_capacity AS SMALLINT) IS NULL OR q.room_capacity >= :min_capacity)
 AND (CAST(:max_capacity AS SMALLINT) IS NULL OR q.room_capacity <= :max_capacity)
 AND (CAST(:gender AS varchar) IS NULL OR q.gender_policy='ANY' OR q.gender_policy=:gender)
 AND (CAST(:gender AS varchar) IS NULL OR bu.gender_policy IS NULL OR bu.gender_policy='ANY' OR bu.gender_policy=:gender)
 AND (q.inventory_mode <> 'PHYSICAL' OR CAST(:gender AS varchar) IS NULL OR EXISTS (
   SELECT 1 FROM dorm_booking.quota_pool_beds linked
   WHERE linked.pool_id=q.id AND dorm_booking.bed_allowed_for_gender(linked.bed_id,:gender)
 ))
 AND (CAST(:occupant AS varchar) IS NULL OR EXISTS (
   SELECT 1 FROM dorm_booking.booking_fee_policies fp
   WHERE fp.organization_id=o.id AND (
      (fp.occupant_category=:occupant AND fp.citizenship_category=:citizenship)
      OR (fp.occupant_category='ANY' AND fp.citizenship_category='ANY'))))
 AND t.amount BETWEEN :min_price AND :max_price
"""


def context_user(request: Request, db: Session):
    if not isinstance(request, Request):
        return None
    try:
        return current_user(request, db)
    except HTTPException:
        return None


@app.post('/api/catalog')
def catalog(f: SearchFilters, request: Request, db: Session = Depends(db_session)):
    visitor = context_user(request, db)
    start, finish, profile_ok = personal_view_period(db, visitor, f.check_in, f.check_out)
    if finish <= start:
        fail('Дата выезда должна быть позже заселения')
    if f.min_capacity and f.max_capacity and f.min_capacity > f.max_capacity:
        fail('Неверный диапазон вместимости', 422)
    occupant = (visitor.get('education_basis') if visitor and visitor['role'] == 'STUDENT' else 'GUEST' if visitor and visitor['role'] == 'GUEST' else None)
    # Staff see the unsegmented inventory. User-facing gender/citizenship
    # restrictions apply only to STUDENT/GUEST, never ORG_ADMIN/PLATFORM_ADMIN.
    gender = visitor.get('gender') if visitor and visitor['role'] in ('STUDENT','GUEST') else (f.gender if not visitor and f.gender in ('MALE','FEMALE') else None)
    is_guest = bool(visitor and visitor['role'] == 'GUEST')
    # Organization administrators must see ONLY their own university in both
    # the map and the counts, regardless of client-supplied city/org filters.
    if visitor and visitor['role'] == 'ORG_ADMIN':
        selected_org = visitor['organization_id']
        selected_city = None
    else:
        selected_org = visitor['organization_id'] if visitor and visitor['role'] in ('STUDENT', 'GUEST') else (None if f.city else f.organization_id)
        selected_city = f.city
    args = dict(organization_id=selected_org, city=selected_city, guest_only=is_guest, district=f.district, building_id=f.building_id,
                category_id=f.category_id, capacity=f.capacity,
                min_capacity=f.min_capacity, max_capacity=f.max_capacity,
                gender=gender, min_price=f.min_price,
                max_price=f.max_price, occupant=occupant, citizenship=(visitor.get('citizenship_category') if visitor and visitor['role'] in ('STUDENT','GUEST') else None), start=start, finish=finish)
    exact = rows(db, CATALOG_EXACT, **args)
    quota = [x for x in rows(db, CATALOG_QUOTA, **args) if x['free_quantity'] > 0]
    offers = exact + quota if profile_ok else []
    if f.mode in ('QUOTA', 'EXACT_BED'):
        offers = [o for o in offers if o['mode'] == f.mode]
    # Amenities are inherited from category, building, or room; require all selected definitions.
    if f.amenities:
        definitions = rows(db, """SELECT id,lower(code) AS code FROM dorm_booking.amenity_definitions
           WHERE lower(code)=ANY(:codes) AND is_active""", codes=[x.lower() for x in f.amenities])
        by_code = {x['code']:x['id'] for x in definitions}
        def has_amenity(offer, wanted):
            did = by_code.get(wanted.lower())
            if did is None:
                return False
            return scalar(db, """SELECT 1 FROM dorm_booking.amenity_values av
                WHERE av.amenity_id=:a AND av.is_present AND av.organization_id=:o
                  AND (av.building_id=:building OR av.comfort_category_id=:category OR
                       (CAST(:room AS BIGINT) IS NOT NULL AND av.room_id=:room)) LIMIT 1""",
                          a=did,o=offer['organization_id'], building=offer['building_id'],
                          category=offer['category_id'], room=offer.get('room_id')) == 1
        offers = [o for o in offers if all(has_amenity(o,a) for a in f.amenities)]
    sort = f.sort if f.sort in ('price_asc','price_desc','free_asc','free_desc') else 'price_asc'
    counts = {}
    for offer in offers:
        counts[offer['building_id']] = counts.get(offer['building_id'],0) + (offer.get('free_quantity') or 1)
    offers.sort(key=lambda x: float(x['monthly_amount']) if sort.startswith('price') else counts[x['building_id']],
                reverse=sort.endswith('desc'))
    buildings = {}
    for o in offers:
        building = buildings.setdefault(o['building_id'], dict(id=o['building_id'],organization_id=o['organization_id'],
            name=o['building_name'],address=o.get('address'),district=o.get('district'),
            latitude=o.get('latitude'),longitude=o.get('longitude'),description=o.get('building_description'),
            free=0,min_price=float(o['monthly_amount']),price_unit=o.get('billing_unit','MONTH'),offers=[]))
        building['free'] += o.get('free_quantity') or 1
        building['min_price'] = min(building['min_price'],float(o['monthly_amount']))
        building['offers'].append(o)
    # Show genuine published SPbGU buildings even before rooms/beds/tariffs are imported.
    # No synthetic availability: such buildings have free=0 and min_price=None.
    additional = []
    if not f.available_only:
        additional = rows(db,"""SELECT bu.id,bu.organization_id,bu.name,bu.address,bu.district,
          bu.latitude,bu.longitude,bu.description FROM dorm_booking.buildings bu
          JOIN dorm_booking.organizations org ON org.id=bu.organization_id
          WHERE bu.status='ACTIVE' AND org.status='ACTIVE' AND org.platform_blocked_at IS NULL
          AND (CAST(:organization_id AS BIGINT) IS NULL OR bu.organization_id=:organization_id)
          AND (CAST(:city AS TEXT) IS NULL OR org.city=:city)
          AND (CAST(:guest_only AS BOOLEAN) = FALSE OR bu.allow_guests)
          AND (CAST(:building_id AS BIGINT) IS NULL OR bu.id=:building_id)
          AND (CAST(:district AS TEXT) IS NULL OR bu.district=:district)
          ORDER BY bu.name""",organization_id=selected_org,city=selected_city,guest_only=is_guest,building_id=f.building_id,district=f.district)
    for b in additional:
        if (b['id'] not in buildings
                and f.category_id is None and f.capacity is None
                and f.min_capacity is None and f.max_capacity is None and not f.amenities):
            buildings[b['id']] = dict(**b,free=0,min_price=None,offers=[],price_unit='MONTH')
    result = list(buildings.values())
    # Real photographs only. Never substitute images of a different building.
    if result:
        photos = rows(db, """SELECT DISTINCT ON (building_id) building_id,relative_path
          FROM dorm_booking.media_files WHERE building_id=ANY(:ids)
          ORDER BY building_id,is_primary DESC,sort_order,id""", ids=[b['id'] for b in result])
        by_building = {p['building_id']:p['relative_path'] for p in photos}
        for item in result:
            path = by_building.get(item['id'])
            item['photo_url'] = (path if path.startswith('/media/') else '/media/' + path.lstrip('/')) if path else None
    result.sort(key=lambda b: (b['min_price'] if b['min_price'] is not None else float('inf')) if sort.startswith('price') else b['free'],reverse=sort.endswith('desc'))
    return {'buildings':result,'offers_count':len(offers),'total_free':sum(b['free'] for b in result),
            'check_in':start,'check_out':finish,'sort':sort}


@app.get('/api/catalog/filters')
def catalog_filters(organization_id: Optional[int] = None, city: Optional[str] = None,
                    request: Request = None, db: Session = Depends(db_session)):
    visitor = context_user(request, db)
    if visitor and visitor['role'] == 'ORG_ADMIN':
        organization_id = visitor['organization_id']
        city = None
    return {
        'districts':[r['district'] for r in rows(db, """SELECT DISTINCT b.district
             FROM dorm_booking.buildings b
             JOIN dorm_booking.organizations o ON o.id=b.organization_id
             WHERE b.district IS NOT NULL
               AND (CAST(:o AS BIGINT) IS NULL OR b.organization_id=:o)
               AND (CAST(:city AS TEXT) IS NULL OR o.city=:city)
             ORDER BY b.district""",o=organization_id,city=city)],
        'categories':rows(db, """SELECT c.id,c.name,c.organization_id
             FROM dorm_booking.comfort_categories c
             JOIN dorm_booking.organizations o ON o.id=c.organization_id
             WHERE c.is_active AND (CAST(:o AS BIGINT) IS NULL OR c.organization_id=:o)
             AND (CAST(:city AS TEXT) IS NULL OR o.city=:city)
             ORDER BY c.name""",o=organization_id,city=city),
        'buildings':rows(db, """SELECT b.id,b.name,b.organization_id
             FROM dorm_booking.buildings b
             JOIN dorm_booking.organizations o ON o.id=b.organization_id
             WHERE b.status='ACTIVE' AND (CAST(:o AS BIGINT) IS NULL OR b.organization_id=:o)
             AND (CAST(:city AS TEXT) IS NULL OR o.city=:city)
             ORDER BY b.name""",o=organization_id,city=city),
        'amenities':rows(db, """SELECT id,code,name,amenity_group FROM dorm_booking.amenity_definitions
             WHERE is_active AND (organization_id IS NULL OR organization_id=:o OR
                (CAST(:city AS TEXT) IS NOT NULL AND organization_id IN (SELECT id FROM dorm_booking.organizations WHERE city=:city)))
             ORDER BY name""",o=organization_id,city=city),
    }


@app.post('/api/forecast')
def forecast(f: SearchFilters, request: Request, db: Session = Depends(db_session)):
    # Advisory only: future release is not a booking guarantee. Only currently occupied exact beds.
    # Keep the forecast restricted to the administrator's own organization too.
    visitor = context_user(request, db)
    scoped_org = visitor['organization_id'] if visitor and visitor['role'] == 'ORG_ADMIN' else (None if f.city else f.organization_id)
    scoped_city = None if visitor and visitor['role'] == 'ORG_ADMIN' else f.city
    args = dict(org=scoped_org,city=scoped_city,district=f.district,category=f.category_id,
                building=f.building_id,capacity=f.capacity,min_price=f.min_price,max_price=f.max_price)
    data = rows(db, """SELECT bp.check_out_date AS release_date, c.id AS category_id,c.name AS category,
      count(DISTINCT bp.bed_id)::INTEGER AS places FROM dorm_booking.bed_periods bp
      JOIN dorm_booking.beds b ON b.id=bp.bed_id
      JOIN dorm_booking.rooms r ON r.id=b.room_id
      JOIN dorm_booking.buildings bu ON bu.id=r.building_id
      JOIN dorm_booking.organizations org ON org.id=bu.organization_id
      JOIN dorm_booking.comfort_categories c ON c.id=r.comfort_category_id
      WHERE bp.status='ACTIVE' AND bp.check_in_date<=(now() AT TIME ZONE 'Europe/Moscow')::date AND bp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date
        AND bp.check_out_date<=(now() AT TIME ZONE 'Europe/Moscow')::date+INTERVAL '7 days' AND b.status='ACTIVE'
        AND (CAST(:org AS BIGINT) IS NULL OR bu.organization_id=:org)
        AND (CAST(:city AS TEXT) IS NULL OR org.city=:city)
        AND (CAST(:district AS TEXT) IS NULL OR bu.district=:district)
        AND (CAST(:building AS BIGINT) IS NULL OR bu.id=:building)
        AND (CAST(:category AS BIGINT) IS NULL OR c.id=:category)
        AND (CAST(:capacity AS SMALLINT) IS NULL OR r.capacity=:capacity)
        AND EXISTS (SELECT 1 FROM dorm_booking.bed_tariff_links assigned JOIN dorm_booking.tariffs t ON t.id=assigned.tariff_id WHERE assigned.bed_id=b.id AND assigned.organization_id=b.organization_id
          AND t.building_id=bu.id AND t.comfort_category_id=c.id AND t.room_capacity=r.capacity
          AND t.status='ACTIVE' AND t.amount BETWEEN :min_price AND :max_price)
      GROUP BY bp.check_out_date,c.id,c.name ORDER BY bp.check_out_date,c.name""",**args)
    # Quantitative release forecasts in the same 7 day window.
    qdata = rows(db, """SELECT sp.check_out_date AS release_date,c.id AS category_id,c.name AS category,
      count(DISTINCT sp.slot_id)::INTEGER AS places FROM dorm_booking.quota_slot_periods sp
      JOIN dorm_booking.quota_slots s ON s.id=sp.slot_id
      JOIN dorm_booking.quota_pools q ON q.id=s.pool_id
      JOIN dorm_booking.buildings bu ON bu.id=q.building_id
      JOIN dorm_booking.organizations org ON org.id=bu.organization_id
      JOIN dorm_booking.comfort_categories c ON c.id=q.comfort_category_id
      WHERE sp.status='ACTIVE' AND sp.check_in_date<=(now() AT TIME ZONE 'Europe/Moscow')::date AND sp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date
        AND sp.check_out_date<=(now() AT TIME ZONE 'Europe/Moscow')::date+INTERVAL '7 days' AND q.status='ACTIVE'
        AND (CAST(:org AS BIGINT) IS NULL OR q.organization_id=:org)
        AND (CAST(:city AS TEXT) IS NULL OR org.city=:city)
        AND (CAST(:district AS TEXT) IS NULL OR bu.district=:district)
        AND (CAST(:building AS BIGINT) IS NULL OR bu.id=:building)
        AND (CAST(:category AS BIGINT) IS NULL OR c.id=:category)
        AND (CAST(:capacity AS SMALLINT) IS NULL OR q.room_capacity=:capacity)
        AND EXISTS (SELECT 1 FROM dorm_booking.quota_pool_tariff_links assigned
          JOIN dorm_booking.tariffs t ON t.id=assigned.tariff_id
          WHERE assigned.pool_id=q.id AND assigned.organization_id=q.organization_id
          AND t.building_id=q.building_id
          AND t.comfort_category_id=q.comfort_category_id AND t.room_capacity=q.room_capacity
          AND t.status='ACTIVE' AND t.amount BETWEEN :min_price AND :max_price)
      GROUP BY sp.check_out_date,c.id,c.name ORDER BY sp.check_out_date,c.name""",**args)
    aggregate={}
    for r in data+qdata:
        key=(r['release_date'],r['category_id'],r['category'])
        aggregate[key]=aggregate.get(key,0)+r['places']
    return [{'release_date':k[0],'category_id':k[1],'category':k[2], 'places':v}
             for k,v in sorted(aggregate.items())]


def require_guest_building(request: Request, db: Session, building_id: int):
    """Guests must never see buildings outside their organization or the permitted inventory."""
    if not hasattr(request, 'cookies'):
        # Also supports direct calls to this helper from internal validation tests.
        return
    visitor = context_user(request, db)
    if not visitor or visitor['role'] != 'GUEST':
        return
    allowed = scalar(db, """SELECT 1 FROM dorm_booking.buildings
        WHERE id=:id AND organization_id=:org AND allow_guests=TRUE""",
        id=building_id, org=visitor['organization_id'])
    if not allowed:
        fail('Это общежитие недоступно для размещения гостей', 403)


def require_guest_offer(db: Session, user: dict, mode: str, offer_id: int):
    """Protect quote and booking endpoints, not just the visual catalog."""
    if user['role'] != 'GUEST':
        return
    if mode == 'EXACT_BED':
        building_id = scalar(db, """SELECT r.building_id FROM dorm_booking.beds b
            JOIN dorm_booking.rooms r ON r.id=b.room_id WHERE b.id=:id""", id=offer_id)
    elif mode == 'QUOTA':
        building_id = scalar(db, """SELECT building_id FROM dorm_booking.quota_pools
            WHERE id=:id""", id=offer_id)
    else:
        fail('Неверный тип размещения')
    allowed = scalar(db, """SELECT 1 FROM dorm_booking.buildings
        WHERE id=:id AND organization_id=:org AND allow_guests=TRUE""",
        id=building_id, org=user['organization_id'])
    if not allowed:
        fail('Для гостей это общежитие недоступно', 403)


@app.get('/api/buildings/{building_id}')
def building_detail(building_id:int, request: Request, db: Session=Depends(db_session)):
    require_guest_building(request, db, building_id)
    b=row(db,"""SELECT b.*,o.city AS city FROM dorm_booking.buildings b
        JOIN dorm_booking.organizations o ON o.id=b.organization_id
        WHERE b.id=:id""",id=building_id)
    if not b:
        fail('Общежитие не найдено',404)
    # Exact-bed navigation is relevant only when independently published beds exist.
    has_exact=bool(scalar(db,'''SELECT EXISTS (
        SELECT 1 FROM dorm_booking.beds bed
        JOIN dorm_booking.rooms r ON r.id=bed.room_id
        JOIN dorm_booking.floors f ON f.id=r.floor_id
        LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
        WHERE r.building_id=:building AND bed.status='ACTIVE' AND r.status='ACTIVE'
          AND f.status='ACTIVE' AND (bl.id IS NULL OR bl.status='ACTIVE')
          AND NOT EXISTS (SELECT 1 FROM dorm_booking.quota_pool_beds qb WHERE qb.bed_id=bed.id))''',
        building=building_id))
    fs=rows(db,"""SELECT f.id,f.floor_number,f.name,f.gender_policy,f.status,
      (SELECT count(*) FROM dorm_booking.rooms r WHERE r.floor_id=f.id)::INTEGER AS rooms_count,
      (SELECT count(*) FROM dorm_booking.available_now_beds a WHERE a.floor_id=f.id)::INTEGER AS exact_free
      FROM dorm_booking.floors f WHERE f.building_id=:id ORDER BY floor_number""",id=building_id)
    media=rows(db,"""SELECT relative_path,original_filename FROM dorm_booking.media_files
            WHERE building_id=:id ORDER BY is_primary DESC,sort_order,id""",id=building_id)
    amenities=rows(db, """SELECT a.id,a.name,a.code,av.quantity,a.unit
       FROM dorm_booking.amenity_values av
       JOIN dorm_booking.amenity_definitions a ON a.id=av.amenity_id
       WHERE av.building_id=:id AND av.is_present AND a.is_active ORDER BY a.name""", id=building_id)
    return {'has_exact_inventory':has_exact,'building':b,'floors':fs,'photos':media,'amenities':amenities}


@app.get('/api/buildings/{building_id}/floor-availability')
def floor_availability(building_id: int, request: Request, db: Session = Depends(db_session),
                       check_in: Optional[date] = None, check_out: Optional[date] = None):
    """Physical floor availability for the same stay as the signed-in person can book."""
    require_guest_building(request, db, building_id)
    building = row(db, 'SELECT id,status FROM dorm_booking.buildings WHERE id=:id', id=building_id)
    if not building:
        fail('Общежитие не найдено', 404)
    visitor = context_user(request, db)
    start, finish, profile_ok = personal_view_period(db, visitor, check_in, check_out)
    gender = visitor.get('gender') if visitor and visitor['role'] in ('STUDENT','GUEST') else None
    category = ('GUEST' if visitor['role']=='GUEST' else visitor.get('education_basis')) if gender else None
    citizenship = visitor.get('citizenship_category') if gender else None
    args = dict(id=building_id, start=start, finish=finish, profile_ok=profile_ok,
                gender=gender, org=visitor['organization_id'] if gender else None,
                guest=bool(visitor and visitor['role']=='GUEST'),
                category=category, citizenship=citizenship)
    data = rows(db, """WITH room_stats AS (
        SELECT r.floor_id,r.comfort_category_id,
               count(*)::integer AS rooms_count,
               COALESCE(sum(r.capacity),0)::integer AS capacity
        FROM dorm_booking.rooms r WHERE r.status='ACTIVE'
        GROUP BY r.floor_id,r.comfort_category_id
    ), free_stats AS (
        SELECT r.floor_id,r.comfort_category_id,count(b.id)::integer AS exact_free
        FROM dorm_booking.beds b
        JOIN dorm_booking.rooms r ON r.id=b.room_id AND r.status='ACTIVE'
        JOIN dorm_booking.floors f ON f.id=r.floor_id AND f.status='ACTIVE'
        LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
        JOIN dorm_booking.buildings bu ON bu.id=r.building_id AND bu.status='ACTIVE'
        JOIN dorm_booking.organizations o ON o.id=bu.organization_id
        JOIN dorm_booking.comfort_categories c ON c.id=r.comfort_category_id AND c.is_active
        WHERE b.status='ACTIVE' AND o.status='ACTIVE' AND o.platform_blocked_at IS NULL
          AND (bl.id IS NULL OR bl.status='ACTIVE') AND CAST(:profile_ok AS boolean)
          AND (CAST(:gender AS varchar) IS NULL OR (
   COALESCE(b.gender_policy,'ANY') IN ('ANY',:gender)
   AND COALESCE(r.gender_policy,'ANY') IN ('ANY',:gender)
   AND COALESCE(f.gender_policy,'ANY') IN ('ANY',:gender)
   AND COALESCE(bl.gender_policy,'ANY') IN ('ANY',:gender)
   AND COALESCE(bu.gender_policy,'ANY') IN ('ANY',:gender)))
          AND (CAST(:org AS BIGINT) IS NULL OR b.organization_id=:org)
          AND (CAST(:guest AS boolean)=FALSE OR bu.allow_guests)
          AND EXISTS (SELECT 1 FROM dorm_booking.bed_tariff_links assigned JOIN dorm_booking.tariffs t ON t.id=assigned.tariff_id WHERE assigned.bed_id=b.id AND assigned.organization_id=b.organization_id
            AND t.building_id=bu.id AND t.comfort_category_id=r.comfort_category_id AND t.room_capacity=r.capacity
            AND t.status='ACTIVE'
            AND (CAST(:category AS varchar) IS NULL OR (t.occupant_category IN (:category,'ANY') AND (CAST(:category AS varchar) <> 'GUEST' OR t.occupant_category='GUEST')))
            AND (CAST(:citizenship AS varchar) IS NULL OR t.citizenship_category IN (:citizenship,'ANY'))
            AND (t.season_id IS NULL OR EXISTS (
               SELECT 1 FROM dorm_booking.tariff_seasons ts
               JOIN dorm_booking.tariff_season_months sm ON sm.season_id=ts.id
               WHERE ts.id=t.season_id AND ts.is_active
                 AND sm.month_number=EXTRACT(MONTH FROM CAST(:start AS date)))))
          AND (CAST(:category AS varchar) IS NULL OR EXISTS (
            SELECT 1 FROM dorm_booking.booking_fee_policies fp WHERE fp.organization_id=b.organization_id
              AND ((fp.occupant_category=:category AND fp.citizenship_category=:citizenship)
               OR (fp.occupant_category='ANY' AND fp.citizenship_category='ANY'))))
          AND NOT EXISTS (SELECT 1 FROM dorm_booking.bed_periods bp WHERE bp.bed_id=b.id
            AND (bp.status='ACTIVE' OR EXISTS (SELECT 1 FROM dorm_booking.bookings bk
                 WHERE bk.bed_period_id=bp.id AND bk.status IN ('PENDING_ADMIN','CONFIRMED')))
            AND daterange(bp.check_in_date,bp.check_out_date,'[)')
                && daterange(CAST(:start AS date),CAST(:finish AS date),'[)'))
        GROUP BY r.floor_id,r.comfort_category_id
    )
    SELECT f.id AS floor_id,f.floor_number,f.gender_policy,
           c.id AS category_id,c.name AS category_name,
           COALESCE(rs.rooms_count,0) AS rooms_count,COALESCE(rs.capacity,0) AS capacity,
           COALESCE(fs.exact_free,0) AS free
    FROM dorm_booking.floors f
    LEFT JOIN room_stats rs ON rs.floor_id=f.id
    LEFT JOIN free_stats fs ON fs.floor_id=f.id AND fs.comfort_category_id IS NOT DISTINCT FROM rs.comfort_category_id
    LEFT JOIN dorm_booking.comfort_categories c ON c.id=rs.comfort_category_id
    WHERE f.building_id=:id AND f.status='ACTIVE'
    ORDER BY f.floor_number,c.name NULLS LAST""", **args)
    floors = {}
    for item in data:
        floor = floors.setdefault(item['floor_id'], {'id':item['floor_id'],
            'floor_number':item['floor_number'], 'gender_policy':item['gender_policy'],
            'total_free':0, 'total_capacity':0,'rooms_count':0,'categories':[]})
        floor['rooms_count'] += item['rooms_count']
        floor['total_capacity'] += item['capacity']
        floor['total_free'] += item['free']
        if item['category_id'] is not None:
            floor['categories'].append({'category_id':item['category_id'],
                'category_name':item['category_name'], 'free':item['free'],
                'capacity':item['capacity'], 'rooms_count':item['rooms_count']})
    return {'building_id':building_id, 'metric':'personal_stay_exact_beds',
            'check_in':start,'check_out':finish,'floors':list(floors.values())}


@app.get('/api/floors/{floor_id}/plan')
def floor_plan(floor_id: int, request: Request, db: Session = Depends(db_session),
               check_in: Optional[date] = None, check_out: Optional[date] = None):
    require_guest_building(request, db, scalar(db,
       'SELECT building_id FROM dorm_booking.floors WHERE id=:id',id=floor_id))
    floor = row(db,'SELECT * FROM dorm_booking.floors WHERE id=:id',id=floor_id)
    if not floor:
        fail('Этаж не найден',404)
    visitor = context_user(request, db)
    start, finish, profile_ok = personal_view_period(db, visitor, check_in, check_out)
    gender = visitor.get('gender') if visitor and visitor['role'] in ('STUDENT','GUEST') else None
    category = ('GUEST' if visitor['role']=='GUEST' else visitor.get('education_basis')) if gender else None
    citizenship = visitor.get('citizenship_category') if gender else None
    rs=rows(db,"""SELECT r.id,r.room_number,r.capacity,r.gender_policy,r.block_id,
         bl.name AS block_name,c.name AS category_name,c.id AS category_id,
         (SELECT count(*) FROM dorm_booking.beds bed
          JOIN dorm_booking.buildings bu ON bu.id=r.building_id AND bu.status='ACTIVE'
          JOIN dorm_booking.organizations o ON o.id=bu.organization_id
          WHERE bed.room_id=r.id AND bed.status='ACTIVE'
          AND (bl.id IS NULL OR bl.status='ACTIVE')
          AND o.status='ACTIVE' AND o.platform_blocked_at IS NULL
          AND CAST(:profile_ok AS boolean)
          AND (CAST(:gender AS varchar) IS NULL OR dorm_booking.bed_allowed_for_gender(bed.id,:gender))
          AND (CAST(:org AS BIGINT) IS NULL OR bed.organization_id=:org)
          AND (CAST(:guest AS boolean)=FALSE OR bu.allow_guests)
          AND EXISTS(SELECT 1 FROM dorm_booking.bed_tariff_links assigned
              JOIN dorm_booking.tariffs t ON t.id=assigned.tariff_id
              WHERE assigned.bed_id=bed.id AND assigned.organization_id=bed.organization_id AND t.building_id=bu.id AND t.comfort_category_id=r.comfort_category_id
              AND t.room_capacity=r.capacity AND t.status='ACTIVE'
              AND (CAST(:category AS varchar) IS NULL OR (t.occupant_category IN (:category,'ANY') AND (CAST(:category AS varchar) <> 'GUEST' OR t.occupant_category='GUEST')))
              AND (CAST(:citizenship AS varchar) IS NULL OR t.citizenship_category IN (:citizenship,'ANY'))
              AND (t.season_id IS NULL OR EXISTS (
                SELECT 1 FROM dorm_booking.tariff_seasons ts
                JOIN dorm_booking.tariff_season_months sm ON sm.season_id=ts.id
                WHERE ts.id=t.season_id AND ts.is_active
                  AND sm.month_number=EXTRACT(MONTH FROM CAST(:start AS date)))))
          AND (CAST(:category AS varchar) IS NULL OR EXISTS (
              SELECT 1 FROM dorm_booking.booking_fee_policies fp
              WHERE fp.organization_id=bed.organization_id AND
              ((fp.occupant_category=:category AND fp.citizenship_category=:citizenship)
               OR (fp.occupant_category='ANY' AND fp.citizenship_category='ANY'))))
          AND NOT EXISTS(SELECT 1 FROM dorm_booking.bed_periods bp
            WHERE bp.bed_id=bed.id
              AND (bp.status='ACTIVE' OR EXISTS (
                 SELECT 1 FROM dorm_booking.bookings bk WHERE bk.bed_period_id=bp.id
                   AND bk.status IN ('PENDING_ADMIN','CONFIRMED')))
              AND daterange(bp.check_in_date,bp.check_out_date,'[)')
                  && daterange(CAST(:start AS date),CAST(:finish AS date),'[)'))
          )::INTEGER AS free,
         (SELECT count(*) FROM dorm_booking.beds b WHERE b.room_id=r.id)::INTEGER AS physical
         FROM dorm_booking.rooms r
         JOIN dorm_booking.floors f ON f.id=r.floor_id AND f.status='ACTIVE'
         LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
         LEFT JOIN dorm_booking.comfort_categories c ON c.id=r.comfort_category_id
         WHERE r.floor_id=:id AND r.status='ACTIVE'
         ORDER BY r.room_number""",id=floor_id,start=start,finish=finish,
         gender=gender,org=visitor['organization_id'] if gender else None,
         guest=bool(visitor and visitor['role']=='GUEST'),category=category,
         citizenship=citizenship,profile_ok=profile_ok)
    return {'floor':floor,'rooms':rs,'check_in':start,'check_out':finish}


@app.get('/api/rooms/{room_id}/plan')
def room_plan(room_id: int, request: Request, db: Session = Depends(db_session),
              check_in: Optional[date] = None, check_out: Optional[date] = None):
    require_guest_building(request, db, scalar(db,
       'SELECT building_id FROM dorm_booking.rooms WHERE id=:id',id=room_id))
    room = row(db,"""SELECT r.*,c.name AS category_name,f.floor_number,
        bu.name AS building_name,bu.district,bu.address FROM dorm_booking.rooms r
        JOIN dorm_booking.floors f ON f.id=r.floor_id
        JOIN dorm_booking.buildings bu ON bu.id=r.building_id
        LEFT JOIN dorm_booking.comfort_categories c ON c.id=r.comfort_category_id WHERE r.id=:id""",id=room_id)
    if not room:
        fail('Комната не найдена',404)
    visitor = context_user(request, db)
    start, finish, profile_ok = personal_view_period(db, visitor, check_in, check_out)
    gender = visitor.get('gender') if visitor and visitor['role'] in ('STUDENT','GUEST') else None
    category = ('GUEST' if visitor['role']=='GUEST' else visitor.get('education_basis')) if gender else None
    citizenship = visitor.get('citizenship_category') if gender else None
    beds=rows(db,"""SELECT b.id,b.bed_number,b.status,b.gender_policy,
         COALESCE((SELECT json_agg(json_build_object('relative_path',m.relative_path,
                  'original_filename',m.original_filename) ORDER BY m.is_primary DESC,m.sort_order,m.id)
            FROM dorm_booking.media_files m WHERE m.bed_id=b.id), '[]'::json) AS photos,
         EXISTS(SELECT 1 FROM dorm_booking.bed_periods bp WHERE bp.bed_id=b.id
             AND (bp.status='ACTIVE' OR EXISTS (
               SELECT 1 FROM dorm_booking.bookings bk WHERE bk.bed_period_id=bp.id
                 AND bk.status IN ('PENDING_ADMIN','CONFIRMED')))
             AND daterange(bp.check_in_date,bp.check_out_date,'[)')
               && daterange(CAST(:start AS date),CAST(:finish AS date),'[)')) AS occupied,
         dorm_booking.effective_bed_gender_policy(b.id) AS effective_gender_policy,
         (b.status='ACTIVE' AND CAST(:profile_ok AS boolean)
           AND EXISTS(SELECT 1 FROM dorm_booking.rooms ar
             JOIN dorm_booking.floors af ON af.id=ar.floor_id AND af.status='ACTIVE'
             JOIN dorm_booking.buildings bu ON bu.id=ar.building_id AND bu.status='ACTIVE'
             JOIN dorm_booking.organizations o ON o.id=bu.organization_id
             LEFT JOIN dorm_booking.blocks abl ON abl.id=ar.block_id
             WHERE ar.id=b.room_id AND ar.status='ACTIVE'
               AND o.status='ACTIVE' AND o.platform_blocked_at IS NULL
               AND (abl.id IS NULL OR abl.status='ACTIVE')
               AND (CAST(:guest AS boolean)=FALSE OR bu.allow_guests))
           AND (CAST(:gender AS varchar) IS NULL OR dorm_booking.bed_allowed_for_gender(b.id,:gender))
           AND (CAST(:org AS BIGINT) IS NULL OR b.organization_id=:org)
           AND EXISTS(SELECT 1 FROM dorm_booking.bed_tariff_links assigned
              JOIN dorm_booking.tariffs t ON t.id=assigned.tariff_id
              WHERE assigned.bed_id=b.id AND assigned.organization_id=b.organization_id AND t.organization_id=b.organization_id
                AND t.building_id=:building AND t.comfort_category_id=:comfort
                AND t.room_capacity=CAST(:capacity AS smallint) AND t.status='ACTIVE'
                AND (CAST(:category AS varchar) IS NULL OR (t.occupant_category IN (:category,'ANY') AND (CAST(:category AS varchar) <> 'GUEST' OR t.occupant_category='GUEST')))
                AND (CAST(:citizenship AS varchar) IS NULL OR t.citizenship_category IN (:citizenship,'ANY'))
                AND (t.season_id IS NULL OR EXISTS (
                  SELECT 1 FROM dorm_booking.tariff_seasons ts
                  JOIN dorm_booking.tariff_season_months sm ON sm.season_id=ts.id
                  WHERE ts.id=t.season_id AND ts.is_active
                    AND sm.month_number=EXTRACT(MONTH FROM CAST(:start AS date)))))
           AND (CAST(:category AS varchar) IS NULL OR EXISTS (
              SELECT 1 FROM dorm_booking.booking_fee_policies fp WHERE fp.organization_id=b.organization_id
                AND ((fp.occupant_category=:category AND fp.citizenship_category=:citizenship)
                 OR (fp.occupant_category='ANY' AND fp.citizenship_category='ANY'))))) AS eligible
         FROM dorm_booking.beds b WHERE b.room_id=:id ORDER BY b.bed_number""",id=room_id,
         start=start,finish=finish,gender=gender,
         org=visitor['organization_id'] if gender else None,
         guest=bool(visitor and visitor['role']=='GUEST'),building=room['building_id'],
         comfort=room['comfort_category_id'],capacity=room['capacity'],
         category=category,citizenship=citizenship,profile_ok=profile_ok)
    photos=rows(db,"""SELECT relative_path,original_filename FROM dorm_booking.media_files
        WHERE room_id=:id ORDER BY is_primary DESC,sort_order,id""",id=room_id)
    amenities=rows(db,"""SELECT a.id,a.name,a.code,av.quantity,a.unit FROM dorm_booking.amenity_values av
        JOIN dorm_booking.amenity_definitions a ON a.id=av.amenity_id
        WHERE av.room_id=:id AND av.is_present AND a.is_active ORDER BY a.name""",id=room_id)
    return {'room':room,'beds':beds,'photos':photos,'amenities':amenities,
            'check_in':start,'check_out':finish}


def add_calendar_months(value: date, months: int) -> date:
    """Same capped-month calculation as PostgreSQL date + interval 'N months'."""
    month_index = value.year * 12 + (value.month - 1) + months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def validate_guest_booking_period(db: Session, user: dict, start: date, finish: date,
                                  guest_access_id: Optional[int] = None) -> int:
    """Authoritative guest access check for quote, booking and page availability.

    expires_at is the exclusive end of an access, and checkout must not exceed
    its displayed calendar date. There is deliberately no extra grace day.
    """
    if finish <= start:
        fail('Дата выезда должна быть позже даты заезда', 409)
    access = row(db, """SELECT id FROM dorm_booking.guest_accesses
        WHERE guest_user_id=:uid AND host_organization_id=:org AND status='ACTIVE'
          AND admin_approved_at IS NOT NULL
          AND (CAST(:aid AS BIGINT) IS NULL OR id=:aid)
          AND (starts_at AT TIME ZONE :timezone)::date <= CAST(:start AS date)
          AND (expires_at AT TIME ZONE :timezone)::date >= CAST(:finish AS date)
        ORDER BY expires_at DESC LIMIT 1""",
        uid=user['id'], org=user['organization_id'], aid=guest_access_id,
        timezone=require_env('DEFAULT_TIMEZONE'), start=start, finish=finish)
    if not access:
        fail('Период бронирования выходит за пределы подтверждённого гостевого доступа. '
             'Выберите даты в пределах срока действия разрешения.', 409)
    return access['id']


def booking_period_for_person(
    db: Session,
    user: dict,
    check_in: date,
    guest_check_out: Optional[date],
) -> tuple[date, str]:
    """The student chooses ARRIVAL only; checkout is internal, not editable.

    The period is still mandatory for occupancy exclusion and tariff calculation.
    Budget: day after actual study end; paid: policy's maximum month count.
    Guests continue to explicitly specify checkout.
    """
    if user['role'] == 'GUEST':
        if guest_check_out is None:
            fail('Укажите дату выезда гостя')
        finish, selection = guest_check_out, 'FIXED_DATES'
        validate_guest_booking_period(db, user, check_in, finish)
    elif user['role'] == 'STUDENT':
        if user.get('education_basis') == 'BUDGET':
            study_end = user.get('study_end_date')
            if not study_end:
                fail('У студента не указана дата окончания обучения. Обратитесь к администратору.', 409)
            if isinstance(study_end, str):
                study_end = date.fromisoformat(study_end[:10])
            finish, selection = study_end + timedelta(days=1), 'STUDY_END'
        elif user.get('education_basis') == 'PAID':
            rules = row(db, "SELECT paid_max_months FROM dorm_booking.booking_rules WHERE organization_id=:org", org=user['organization_id'])
            if not rules:
                fail('Для организации не настроены правила бронирования', 409)
            finish = add_calendar_months(check_in, int(rules['paid_max_months']))
            selection = 'FIXED_DATES'
        else:
            fail('Не указана основа обучения студента', 409)
    else:
        fail('Оформление бронирования недоступно для этой роли', 403)

    if finish <= check_in:
        fail('Техническая дата завершения проживания должна быть позже даты прибытия', 409)
    return finish, selection


class BookingIn(BaseModel):
    mode: str
    offer_id: int
    check_in: date
    check_out: Optional[date] = None  # Guests only; ignored for students
    until_study_end: bool = False  # Kept for compatibility with existing clients
    guest_access_id: Optional[int] = None


@app.post('/api/bookings')
def book(body: BookingIn, db: Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'STUDENT','GUEST')
    if body.mode not in ('EXACT_BED','QUOTA'):
        fail('Неверный способ бронирования')
    finish, selection = booking_period_for_person(db, u, body.check_in, body.check_out)
    if u['role'] == 'GUEST':
        validate_guest_booking_period(db, u, body.check_in, finish, body.guest_access_id)
    require_guest_offer(db, u, body.mode, body.offer_id)
    if body.mode == 'EXACT_BED' and not scalar(db,
            'SELECT dorm_booking.bed_allowed_for_gender(:bed,:gender)',
            bed=body.offer_id, gender=u.get('gender')):
        fail('Это койко-место не предназначено для вашего пола', 409)
    func='submit_exact_booking' if body.mode=='EXACT_BED' else 'submit_quota_booking'
    try:
        bid=scalar(db,f"SELECT dorm_booking.{func}(:offer,:user,:start,:finish,:selection,:guest)",
                   offer=body.offer_id,user=u['id'],start=body.check_in,finish=finish,
                   selection=selection,guest=body.guest_access_id)
        log(db,u,'BOOKING_CREATED','bookings',bid,f'Заявка {body.mode}',{'check_in':str(body.check_in),'check_out':str(finish)})
        db.commit()
        return {'id':bid,'status':'PENDING_ADMIN','message':'Заявка отправлена администратору'}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


BOOKINGS_QUERY="""
 SELECT bk.id,bk.organization_id,bk.booking_mode,bk.person_user_id,
 CASE WHEN bk.status='CANCELLED' AND bk.cancelled_by_user_id IS NOT NULL
      THEN 'CANCELLED_BY_ADMIN' ELSE bk.status END AS status,
 bk.cancel_reason,bk.cancelled_by_user_id,
 bk.booking_amount,bk.tariff_amount_snapshot AS monthly_tariff_snapshot,bk.created_at,bk.admin_confirmed_at,
 bk.student_user_id,bk.guest_access_id,bk.tariff_id,
 pool.inventory_mode AS pool_inventory_mode,
 (SELECT count(*) FROM dorm_booking.quota_pool_beds member WHERE member.pool_id=pool.id)::INTEGER AS pool_linked_beds,
 COALESCE(bp.check_in_date,qp.check_in_date) AS check_in,
 COALESCE(bp.check_out_date,qp.check_out_date) AS check_out,
 bp.bed_id, b.bed_number,r.room_number,f.floor_number, bu.name AS building_name,
 bu.id AS building_id,bu.address, c.name AS category_name, concat_ws(' ',u.last_name,u.first_name,u.middle_name) AS full_name,u.gender,u.education_basis,u.login AS person_login
 FROM dorm_booking.bookings bk
 LEFT JOIN dorm_booking.bed_periods bp ON bp.id=bk.bed_period_id
 LEFT JOIN dorm_booking.quota_slot_periods qp ON qp.id=bk.quota_slot_period_id
 LEFT JOIN dorm_booking.quota_slots qs ON qs.id=qp.slot_id
 LEFT JOIN dorm_booking.quota_pools pool ON pool.id=qs.pool_id
 LEFT JOIN dorm_booking.beds b ON b.id=bp.bed_id
 LEFT JOIN dorm_booking.rooms r ON r.id=b.room_id
 LEFT JOIN dorm_booking.floors f ON f.id=r.floor_id
 JOIN dorm_booking.tariffs t ON t.id=bk.tariff_id
 JOIN dorm_booking.comfort_categories c ON c.id=t.comfort_category_id
 JOIN dorm_booking.buildings bu ON bu.id=t.building_id
 JOIN dorm_booking.users u ON u.id=bk.person_user_id
"""


@app.get('/api/bookings/mine')
def my_bookings(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'STUDENT','GUEST')
    return rows(db,BOOKINGS_QUERY+' WHERE bk.person_user_id=:uid ORDER BY bk.created_at DESC',uid=u['id'])


@app.post('/api/bookings/{booking_id}/cancel')
def student_cancel(booking_id:int,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'STUDENT','GUEST')
    try:
        scalar(db,'SELECT dorm_booking.cancel_booking_by_student(:bid,:uid)',bid=booking_id,uid=u['id'])
        log(db,u,'BOOKING_CANCELLED','bookings',booking_id,'Отмена пользователем')
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


def org_admin(u):
    roles(u,'ORG_ADMIN')
    return u['organization_id']


@app.get('/api/admin/dashboard')
def admin_dashboard(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    return {
      'stats':row(db,"""SELECT
      (SELECT count(*) FROM dorm_booking.buildings WHERE organization_id=:org)::INTEGER AS buildings,
      (SELECT count(*) FROM dorm_booking.beds WHERE organization_id=:org AND status<>'ARCHIVED')::INTEGER AS beds,
      (SELECT count(*) FROM dorm_booking.beds WHERE organization_id=:org AND status='ACTIVE')::INTEGER AS published,
      (SELECT count(*) FROM dorm_booking.available_now_beds WHERE organization_id=:org)::INTEGER AS free_exact,
      (SELECT coalesce(sum(currently_free_quantity),0) FROM dorm_booking.available_now_quota_pools WHERE organization_id=:org)::INTEGER AS free_quota,
      (SELECT count(*) FROM dorm_booking.bookings WHERE organization_id=:org AND status='PENDING_ADMIN')::INTEGER AS pending,
      (SELECT count(*) FROM dorm_booking.bookings WHERE organization_id=:org AND status='CONFIRMED')::INTEGER AS confirmed,
      (SELECT count(*) FROM dorm_booking.quota_pools WHERE organization_id=:org AND status='ACTIVE')::INTEGER AS quota_pools,
      (SELECT count(*) FROM dorm_booking.guest_accesses WHERE host_organization_id=:org
         AND status IN ('WAITING_STUDENT','WAITING_ADMIN'))::INTEGER AS guest_requests""",org=oid),
      'buildings':rows(db,"""SELECT b.id,b.name,o.city AS city,b.address,b.district,b.status,b.allow_guests,b.building_layout,
      (SELECT count(*)::integer FROM dorm_booking.beds bed
        JOIN dorm_booking.rooms room ON room.id=bed.room_id
        WHERE room.building_id=b.id AND bed.status<>'ARCHIVED') AS total_beds,
      (SELECT count(*)::integer FROM dorm_booking.beds bed
        JOIN dorm_booking.rooms room ON room.id=bed.room_id
        WHERE room.building_id=b.id AND bed.status='ACTIVE') AS published_beds,
      (SELECT count(*)::integer FROM dorm_booking.beds bed
        JOIN dorm_booking.rooms room ON room.id=bed.room_id
        WHERE room.building_id=b.id AND EXISTS (
          SELECT 1 FROM dorm_booking.bed_periods bp
          WHERE bp.bed_id=bed.id AND bp.status='ACTIVE'
            AND bp.check_in_date<=(now() AT TIME ZONE 'Europe/Moscow')::date AND bp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date)) AS occupied_beds,
      (SELECT count(*)::integer FROM dorm_booking.available_now_beds a
        WHERE a.building_id=b.id) AS free_beds,
      (SELECT count(*)::integer FROM dorm_booking.quota_pools pool
        WHERE pool.building_id=b.id AND pool.status='ACTIVE') AS active_pools,
      (SELECT CASE WHEN m.relative_path LIKE '/media/%' THEN m.relative_path
              ELSE '/media/' || ltrim(m.relative_path,'/') END
       FROM dorm_booking.media_files m WHERE m.building_id=b.id
       ORDER BY m.is_primary DESC,m.sort_order,m.id LIMIT 1) AS photo_url
      FROM dorm_booking.buildings b
      JOIN dorm_booking.organizations o ON o.id=b.organization_id
      WHERE b.organization_id=:o ORDER BY b.id""",o=oid)
    }


@app.get('/api/admin/buildings/{building_id}/rooms')
def admin_building_rooms(building_id:int,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if not row(db,'SELECT id FROM dorm_booking.buildings WHERE id=:id AND organization_id=:org',
               id=building_id,org=oid):
        fail('Общежитие не найдено',404)
    return rows(db,'''SELECT r.id,r.room_number,f.floor_number
         FROM dorm_booking.rooms r JOIN dorm_booking.floors f ON f.id=r.floor_id
         WHERE r.building_id=:building AND r.organization_id=:org
         ORDER BY f.floor_number,r.room_number,r.id''',building=building_id,org=oid)


@app.get('/api/admin/buildings/{building_id}/beds')
def admin_building_beds(building_id: int, db: Session = Depends(db_session),
                        u: dict = Depends(current_user)):
    org_id = org_admin(u)
    return rows(db, """SELECT b.id, b.bed_number, r.room_number, f.floor_number,
          b.status FROM dorm_booking.beds b
          JOIN dorm_booking.rooms r ON r.id=b.room_id
          JOIN dorm_booking.floors f ON f.id=r.floor_id
          WHERE r.building_id=:building AND b.organization_id=:org
          AND b.status<>'ARCHIVED'
          ORDER BY f.floor_number, r.room_number, b.bed_number""",
          building=building_id, org=org_id)


@app.get('/api/admin/blocks')
def admin_blocks(building_id: Optional[int] = None, floor_id: Optional[int] = None,
                 db: Session = Depends(db_session), u: dict = Depends(current_user)):
    """All persisted blocks in the tenant, for the room editor; no inferred blocks."""
    oid = org_admin(u)
    return rows(db, """SELECT bl.id,bl.code,bl.name,bl.gender_policy,bl.floor_id,bl.building_id,bl.status,
          f.floor_number,(SELECT count(*) FROM dorm_booking.rooms r WHERE r.block_id=bl.id)::INTEGER AS rooms_count
          FROM dorm_booking.blocks bl JOIN dorm_booking.floors f ON f.id=bl.floor_id
          WHERE bl.organization_id=:org
          AND (CAST(:building AS BIGINT) IS NULL OR bl.building_id=:building)
          AND (CAST(:floor AS BIGINT) IS NULL OR bl.floor_id=:floor)
          ORDER BY f.floor_number,bl.code,bl.id""",
          org=oid, building=building_id, floor=floor_id)


@app.get('/api/admin/floors/{floor_id}/rooms')
def admin_floor_rooms(floor_id: int, db: Session = Depends(db_session),
                      u: dict = Depends(current_user)):
    org_id = org_admin(u)
    floor = row(db, '''SELECT id FROM dorm_booking.floors
        WHERE id=:id AND organization_id=:org''', id=floor_id, org=org_id)
    if not floor:
        fail('Этаж не найден', 404)
    result = rows(db, '''SELECT id,room_number,capacity,status,block_id,gender_policy,
        comfort_category_id FROM dorm_booking.rooms
        WHERE floor_id=:floor AND organization_id=:org
        ORDER BY room_number,id''', floor=floor_id, org=org_id)
    return {'rooms':result}


@app.get('/api/admin/rooms/list')
def admin_room_list(db: Session = Depends(db_session),
                    u: dict = Depends(current_user)):
    org_id = org_admin(u)
    return rows(db, '''SELECT r.id,r.room_number,r.capacity,r.status,r.block_id,r.floor_id,r.building_id,
        bl.name AS block_name, f.floor_number,bu.name AS building_name
        FROM dorm_booking.rooms r
        JOIN dorm_booking.floors f ON f.id=r.floor_id
        JOIN dorm_booking.buildings bu ON bu.id=r.building_id
        LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
        WHERE r.organization_id=:org
        ORDER BY bu.name,f.floor_number,bl.name NULLS FIRST,r.room_number''', org=org_id)


@app.get('/api/admin/inventory')
def inventory(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    # v6.8 introduced physical-to-quota-pool links. Never mask a missing migration
    # as 'no linked beds': that could incorrectly expose reserved inventory.
    if not scalar(db, "SELECT to_regclass('dorm_booking.quota_pool_beds') IS NOT NULL"):
        fail('Отсутствует таблица dorm_booking.quota_pool_beds. Выполните '
             'backend/migrations/2026_11_12_v6_8_quota_physical_beds.sql '
             'в используемой PostgreSQL базе и перезапустите приложение.', 503)
    if not scalar(db, """SELECT EXISTS(SELECT 1 FROM information_schema.columns
          WHERE table_schema='dorm_booking' AND table_name='beds' AND column_name='tariff_id')"""):
        fail('Выполните миграцию v6.12 для сохранения базовых тарифов у койко-мест',503)
    if not scalar(db, """SELECT EXISTS(SELECT 1 FROM information_schema.columns
          WHERE table_schema='dorm_booking' AND table_name='tariffs' AND column_name='tariff_number')"""):
        fail('Не выполнена миграция v6.21: отсутствует номер тарифа внутри организации.',503)
    physical=rows(db,"""SELECT b.id,b.bed_number,b.status,b.gender_policy AS bed_gender_policy,
         b.tariff_id,
         COALESCE((SELECT jsonb_agg(jsonb_build_object('id',lt.id,'tariff_number',lt.tariff_number,
           'amount',lt.amount,'billing_unit',lt.billing_unit,
           'occupant_category',lt.occupant_category,'citizenship_category',lt.citizenship_category,
           'season_id',lt.season_id,'status',lt.status) ORDER BY lt.tariff_number)
           FROM dorm_booking.bed_tariff_links link
           JOIN dorm_booking.tariffs lt ON lt.id=link.tariff_id
           WHERE link.bed_id=b.id), '[]'::jsonb) AS tariff_links, t.tariff_number, t.amount AS tariff_amount, t.billing_unit AS tariff_unit,
         t.occupant_category AS tariff_occupant_category,
         t.citizenship_category AS tariff_citizenship_category, t.season_id AS tariff_season_id,
         t.status AS tariff_status,
         r.room_number,r.id AS room_id,
         r.capacity,r.status AS room_status,r.comfort_category_id,
         (SELECT pb.pool_id FROM dorm_booking.quota_pool_beds pb WHERE pb.bed_id=b.id) AS pool_id,
         f.floor_number,bu.name AS building_name,bu.id AS building_id,c.name AS category_name,
         b.gender_policy AS gender_policy,
         EXISTS(SELECT 1 FROM dorm_booking.bed_periods bp WHERE bp.bed_id=b.id AND bp.status='ACTIVE'
             AND bp.check_in_date<=(now() AT TIME ZONE 'Europe/Moscow')::date AND bp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date) AS occupied,
         EXISTS(SELECT 1 FROM dorm_booking.bed_periods bp WHERE bp.bed_id=b.id AND bp.status='ACTIVE'
             AND bp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date) AS future_occupied
       FROM dorm_booking.beds b JOIN dorm_booking.rooms r ON r.id=b.room_id
       JOIN dorm_booking.buildings bu ON bu.id=r.building_id
       JOIN dorm_booking.floors f ON f.id=r.floor_id
       LEFT JOIN dorm_booking.comfort_categories c ON c.id=r.comfort_category_id
       LEFT JOIN dorm_booking.tariffs t ON t.id=b.tariff_id AND t.organization_id=b.organization_id
       WHERE b.organization_id=:o ORDER BY bu.name,f.floor_number,r.room_number,b.bed_number""",o=oid)
    # One catalog of eligible options for each bed, not N per-bed tariff selections.
    options=rows(db,"""SELECT t.id,t.tariff_number,t.building_id,t.comfort_category_id,t.room_capacity,
           t.amount,t.billing_unit,t.occupant_category,t.citizenship_category,t.season_id
        FROM dorm_booking.tariffs t WHERE t.organization_id=:o AND t.status='ACTIVE'
        ORDER BY t.building_id,t.comfort_category_id,t.room_capacity,t.amount DESC,t.id""",o=oid)
    quotas=rows(db,"""SELECT p.*,bu.name AS building_name,c.name AS category_name,
       COALESCE((SELECT jsonb_agg(jsonb_build_object('id',lt.id,'tariff_number',lt.tariff_number,
           'amount',lt.amount,'billing_unit',lt.billing_unit,'occupant_category',lt.occupant_category,
           'citizenship_category',lt.citizenship_category,'season_id',lt.season_id,
           'status',lt.status) ORDER BY lt.tariff_number)
           FROM dorm_booking.quota_pool_tariff_links link
           JOIN dorm_booking.tariffs lt ON lt.id=link.tariff_id WHERE link.pool_id=p.id),
          '[]'::jsonb) AS tariff_links,
       (SELECT count(*) FROM dorm_booking.quota_slots s WHERE s.pool_id=p.id)::INTEGER AS slots,
       (SELECT count(*) FROM dorm_booking.quota_pool_beds pb WHERE pb.pool_id=p.id)::INTEGER AS linked_beds,
       (SELECT count(*) FROM dorm_booking.quota_slots s JOIN dorm_booking.quota_slot_periods sp ON sp.slot_id=s.id
          WHERE s.pool_id=p.id AND sp.status='ACTIVE' AND sp.check_in_date<=(now() AT TIME ZONE 'Europe/Moscow')::date AND sp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date)::INTEGER AS occupied,
       (SELECT count(*) FROM dorm_booking.quota_slots s
          JOIN dorm_booking.quota_slot_periods sp ON sp.slot_id=s.id
          JOIN dorm_booking.bookings bk ON bk.quota_slot_period_id=sp.id
          WHERE s.pool_id=p.id AND s.status='ACTIVE' AND sp.status='ACTIVE'
            AND sp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date
            AND bk.status IN ('PENDING_ADMIN','CONFIRMED'))::INTEGER AS booked,
       (SELECT count(DISTINCT s.id) FROM dorm_booking.quota_slots s
          JOIN dorm_booking.quota_slot_periods sp ON sp.slot_id=s.id
          JOIN dorm_booking.bookings bk ON bk.quota_slot_period_id=sp.id
          WHERE s.pool_id=p.id AND s.status='ACTIVE' AND sp.status='ACTIVE'
            AND sp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date
            AND bk.status IN ('PENDING_ADMIN','CONFIRMED'))::INTEGER AS booked_slots
       FROM dorm_booking.quota_pools p JOIN dorm_booking.buildings bu ON bu.id=p.building_id
       JOIN dorm_booking.comfort_categories c ON c.id=p.comfort_category_id
       WHERE p.organization_id=:o AND p.deleted_at IS NULL ORDER BY p.id DESC""",o=oid)
    readiness_results=rows(db, """SELECT
        EXISTS(SELECT 1 FROM dorm_booking.booking_rules br WHERE br.organization_id=:o) AS has_booking_rules,
        EXISTS(SELECT 1 FROM dorm_booking.booking_fee_policies fp WHERE fp.organization_id=:o) AS has_booking_fee_policy
        """,o=oid)
    readiness=readiness_results[0] if readiness_results else {'has_booking_rules':False,'has_booking_fee_policy':False}
    return {'beds':physical,'quotas':quotas,'tariff_options':options,'catalog_readiness':readiness}


@app.get('/api/admin/bookings')
def admin_bookings(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    return rows(db,BOOKINGS_QUERY+' WHERE bk.organization_id=:oid ORDER BY bk.created_at DESC',oid=org_admin(u))


class PublishRequest(BaseModel):
    bed_ids: list[int]
    action: str = 'PUBLISH'


@app.post('/api/admin/publish')
def direct_bed_publication(
    body: PublishRequest,
    db: Session = Depends(db_session),
    u: dict = Depends(current_user),
):
    """Publish selected beds and activate their rooms in the same transaction.

    Room status changes are committed only after all the selected beds pass the
    existing PostgreSQL checks (tariffs, building, category, fees, etc.).
    """
    oid = org_admin(u)
    if body.action not in ('PUBLISH', 'WITHDRAW') or not body.bed_ids or len(body.bed_ids) > 5000:
        fail('Выберите места (не более 5000) и действие')

    ids = sorted(set(body.bed_ids))
    try:
        # Lock exactly the organization's requested beds, in a stable order.
        beds_to_update = rows(db, """
            SELECT id, room_id, status
            FROM dorm_booking.beds
            WHERE organization_id = :org AND id = ANY(:ids)
            ORDER BY id
            FOR UPDATE
        """, org=oid, ids=ids)
        if len(beds_to_update) != len(ids):
            fail('Некоторые места не принадлежат вашей организации', 403)

        target_status = 'ACTIVE' if body.action == 'PUBLISH' else 'INACTIVE'
        pending_beds = [bed for bed in beds_to_update if bed['status'] != target_status]
        activated_room_ids = []

        if body.action == 'PUBLISH' and pending_beds:
            unpriced = rows(db,"""SELECT b.id FROM dorm_booking.beds b
              WHERE b.id=ANY(:ids) AND NOT EXISTS(
                SELECT 1 FROM dorm_booking.bed_tariff_links l
                JOIN dorm_booking.tariffs t ON t.id=l.tariff_id AND t.status='ACTIVE'
                WHERE l.bed_id=b.id AND l.organization_id=b.organization_id)""",
              ids=[bed['id'] for bed in pending_beds])
            if unpriced:
                fail('Сначала назначьте активный тариф для каждого публикуемого места. Первый ID: '+str(unpriced[0]['id']),409)
            room_ids = sorted({bed['room_id'] for bed in pending_beds})
            inactive_rooms = rows(db, """
                SELECT r.id, r.comfort_category_id, cc.is_active AS category_active
                FROM dorm_booking.rooms r
                LEFT JOIN dorm_booking.comfort_categories cc
                    ON cc.id = r.comfort_category_id
                    AND cc.organization_id = r.organization_id
                WHERE r.organization_id = :org
                  AND r.id = ANY(:room_ids)
                  AND r.status = 'INACTIVE'
                ORDER BY r.id
                FOR UPDATE OF r
            """, org=oid, room_ids=room_ids)

            for room in inactive_rooms:
                if room['comfort_category_id'] is None:
                    fail('Для публикации сначала назначьте комнате категорию комфортности', 409)
                if not room['category_active']:
                    fail('Категория комфортности комнаты неактивна', 409)

                db.execute(text("""
                    UPDATE dorm_booking.rooms
                    SET status = 'ACTIVE'
                    WHERE id = :room_id AND organization_id = :org
                """), {'room_id': room['id'], 'org': oid})
                activated_room_ids.append(room['id'])
                log(db, u, 'ROOM_ACTIVATED', 'rooms', room['id'],
                    'Комната автоматически активирована при публикации места')

        for bed in pending_beds:
            # Original stored procedure retains all the validation and audit rules.
            scalar(db, 'SELECT dorm_booking.set_bed_publication(:bid,:actor,:publish)',
                   bid=bed['id'], actor=u['id'], publish=body.action == 'PUBLISH')

        log(db, u, 'BEDS_PUBLISHED' if body.action == 'PUBLISH' else 'BEDS_WITHDRAWN',
            'beds', None, details={
                'ids': ids,
                'changed': len(pending_beds),
                'unchanged': len(ids) - len(pending_beds),
                'auto_activated_room_ids': activated_room_ids,
                'action': body.action,
            })
        db.commit()
        return {
            'ok': True,
            'count': len(pending_beds),
            'unchanged': len(ids) - len(pending_beds),
            'activated_rooms': len(activated_room_ids),
            'status': 'APPLIED',
        }
    except HTTPException:
        db.rollback()
        raise
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class QuotaIn(BaseModel):
    building_id:int
    category_id:int
    room_capacity:int=Field(ge=1,le=20)
    gender_policy:str='ANY'
    quantity:int=Field(ge=1,le=5000)
    name:str=Field(min_length=2,max_length=255)
    inventory_mode:str='VIRTUAL'
    tariff_ids: list[int] = Field(min_length=1, max_length=100)


@app.post('/api/admin/quotas')
def quota_create(body:QuotaIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.gender_policy not in ('MALE','FEMALE','ANY'):
        fail('Некорректное правило пола')
    if body.inventory_mode not in ('VIRTUAL','PHYSICAL'):
        fail('Некорректный тип пула',422)
    tariff_ids = validate_assigned_tariffs(db, oid, body.building_id,
                                          body.category_id, body.room_capacity, body.tariff_ids)
    try:
        qid=scalar(db,"""INSERT INTO dorm_booking.quota_pools
          (organization_id,building_id,comfort_category_id,room_capacity,gender_policy,quantity,name,created_by,inventory_mode)
          VALUES (:org,:b,:c,:cap,:g,:n,:name,:actor,:mode) RETURNING id""",
          org=oid,b=body.building_id,c=body.category_id,cap=body.room_capacity,
          g=body.gender_policy,n=body.quantity,name=body.name,actor=u['id'],mode=body.inventory_mode)
        replace_inventory_tariffs(db,'pool',qid,oid,tariff_ids)
        log(db,u,'QUOTA_DRAFT','quota_pools',qid,'Создан количественный пул')
        db.commit()
        return {'id':qid,'status':'DRAFT'}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


def validate_assigned_tariffs(db: Session, org_id: int, building_id: int,
                              comfort_id: int, capacity: int, tariff_ids: list[int]) -> list[int]:
    """Validate explicit tariff IDs, with an actionable cause for each mismatch.

    `tariff_number` is an organization-local display number. The API sends the
    global tariff id to the link table; no comma-delimited legacy values.
    """
    try:
        ids = [int(value) for value in tariff_ids]
    except (ValueError, TypeError):
        fail('Переданы некорректные идентификаторы тарифов', 422)
    if not ids or len(ids) != len(set(ids)) or len(ids) > 100 or any(i <= 0 for i in ids):
        fail('Выберите от 1 до 100 различных тарифов без повторов', 422)

    selected = rows(db, """SELECT id,tariff_number,organization_id,building_id,
                  comfort_category_id,room_capacity,status
               FROM dorm_booking.tariffs
               WHERE id=ANY(CAST(:ids AS bigint[])) AND organization_id=:org""",
               ids=ids, org=org_id)
    found = {int(t['id']): t for t in selected}
    problems = []
    for tariff_id in ids:
        tariff = found.get(tariff_id)
        if not tariff:
            problems.append(f'ID {tariff_id}: тариф не найден в вашей организации')
            continue
        label = f"№{tariff['tariff_number'] or tariff_id} (ID {tariff_id})"
        if tariff['status'] != 'ACTIVE':
            problems.append(f'{label}: тариф неактивен')
        if int(tariff['building_id']) != int(building_id):
            problems.append(f"{label}: тариф для общежития ID {tariff['building_id']}, "
                            f'а комната находится в общежитии ID {building_id}')
        if tariff['comfort_category_id'] is None or comfort_id is None or \
           int(tariff['comfort_category_id']) != int(comfort_id):
            problems.append(f"{label}: категория комфортности тарифа "
                            f"ID {tariff['comfort_category_id']} не совпадает с категорией комнаты ID {comfort_id}")
        if int(tariff['room_capacity']) != int(capacity):
            problems.append(f"{label}: тариф рассчитан на {tariff['room_capacity']}-местную "
                            f'комнату, фактическая вместимость — {capacity}')
    if problems:
        fail('Нельзя назначить выбранные тарифы: ' + '; '.join(problems[:5]) +
             (f'; и ещё {len(problems)-5} несоответствий' if len(problems)>5 else '') +
             '. Измените набор тарифов или характеристики комнаты.', 409)
    return ids


def replace_inventory_tariffs(db: Session, target: str, item_id: int,
                              org_id: int, tariff_ids: list[int]) -> None:
    if target not in ('bed', 'pool'):
        raise ValueError('Unsupported inventory kind')
    table = 'bed_tariff_links' if target == 'bed' else 'quota_pool_tariff_links'
    key = 'bed_id' if target == 'bed' else 'pool_id'
    db.execute(text(f'DELETE FROM dorm_booking.{table} WHERE {key}=:id AND organization_id=:org'),
               {'id':item_id, 'org':org_id})
    for tariff_id in tariff_ids:
        db.execute(text(f"""INSERT INTO dorm_booking.{table}
            (organization_id,{key},tariff_id) VALUES (:org,:id,:tariff)"""),
            {'org':org_id,'id':item_id,'tariff':tariff_id})


class QuotaBedSelection(BaseModel):
    bed_ids: list[int] = Field(min_length=1, max_length=5000)


@app.put('/api/admin/quotas/{quota_id}/beds')
def quota_attach_existing_beds(
    quota_id: int,
    body: QuotaBedSelection,
    db: Session = Depends(db_session),
    u: dict = Depends(current_user),
):
    """Reserve existing physical beds for a DRAFT pool, without deleting physical inventory."""
    org_id = org_admin(u)
    ids = list(dict.fromkeys(body.bed_ids))
    if len(ids) != len(body.bed_ids):
        fail('В списке повторяются койко-места', 422)
    try:
        pool = row(db, """SELECT id,status,building_id,comfort_category_id,room_capacity,gender_policy
            FROM dorm_booking.quota_pools
            WHERE id=:id AND organization_id=:org FOR UPDATE""", id=quota_id, org=org_id)
        if not pool:
            fail('Пул не найден', 404)
        if pool['status'] != 'DRAFT':
            fail('Состав физических мест изменяется только в черновике пула', 409)
        chosen = rows(db, """SELECT b.id,b.status,r.building_id,r.comfort_category_id,r.capacity,
                   dorm_booking.effective_bed_gender_policy(b.id) AS effective_gender
                   FROM dorm_booking.beds b JOIN dorm_booking.rooms r ON r.id=b.room_id
                   WHERE b.id=ANY(:ids) AND b.organization_id=:org FOR UPDATE OF b""",
                   ids=ids, org=org_id)
        if len(chosen) != len(ids):
            fail('Одно или несколько мест не найдены в организации', 422)
        existing = rows(db, """SELECT bed_id,pool_id FROM dorm_booking.quota_pool_beds
                   WHERE bed_id=ANY(:ids) AND pool_id<>:pool""", ids=ids, pool=quota_id)
        if existing:
            fail('Одно из мест уже связано с другим количественным пулом', 409)
        for bed in chosen:
            if bed['status'] == 'ARCHIVED' or bed['effective_gender'] == 'CONFLICT':
                fail(f"Место {bed['id']} архивировано или имеет конфликт ограничений по полу", 409)
            if (bed['building_id'] != pool['building_id'] or
                bed['comfort_category_id'] != pool['comfort_category_id'] or
                bed['capacity'] != pool['room_capacity']):
                fail(f"Место {bed['id']} не соответствует зданию, категории или вместимости пула", 422)
            if pool['gender_policy'] in ('MALE', 'FEMALE') and bed['effective_gender'] not in ('ANY', pool['gender_policy']):
                fail(f"Место {bed['id']} не подходит по ограничению пола", 422)
        overlapping = scalar(db, """SELECT count(*) FROM dorm_booking.bed_periods
            WHERE bed_id=ANY(:ids) AND status='ACTIVE' AND check_out_date > (now() AT TIME ZONE 'Europe/Moscow')::date""", ids=ids)
        if overlapping:
            fail('Нельзя переносить места с действующим или будущим проживанием/бронированием', 409)
        # Existing DRAFT pools from older versions may still say VIRTUAL.
        # Attaching a physical set explicitly switches them to PHYSICAL.
        db.execute(text("UPDATE dorm_booking.quota_pools SET inventory_mode='PHYSICAL' WHERE id=:id"), {'id':quota_id})
        # A previous assignment to the same DRAFT pool can be replaced atomically.
        db.execute(text('DELETE FROM dorm_booking.quota_pool_beds WHERE pool_id=:pool'), {'pool': quota_id})
        db.execute(text("UPDATE dorm_booking.beds SET status='INACTIVE' WHERE id=ANY(:ids) AND status='ACTIVE'"), {'ids': ids})
        db.execute(text("""INSERT INTO dorm_booking.quota_pool_beds(organization_id,pool_id,bed_id)
            SELECT :org,:pool,unnest(CAST(:ids AS bigint[]))"""), {'org': org_id, 'pool': quota_id, 'ids': ids})
        db.execute(text('UPDATE dorm_booking.quota_pools SET quantity=:count WHERE id=:id'), {'count': len(ids), 'id': quota_id})
        log(db,u,'QUOTA_BEDS_ATTACHED','quota_pools',quota_id,details={'bed_ids':ids})
        db.commit()
        return {'ok': True, 'pool_id': quota_id, 'linked_beds': len(ids), 'quantity': len(ids)}
    except HTTPException:
        db.rollback()
        raise
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class InventoryTariffsIn(BaseModel):
    tariff_ids: list[int] = Field(min_length=1, max_length=100)


@app.put('/api/admin/quotas/{quota_id}/tariffs')
def admin_update_quota_tariffs(quota_id: int, body: InventoryTariffsIn,
                               db: Session = Depends(db_session), u: dict = Depends(current_user)):
    oid=org_admin(u)
    pool=row(db, """SELECT id,organization_id,building_id,comfort_category_id,
        room_capacity,status FROM dorm_booking.quota_pools
        WHERE id=:id AND organization_id=:org FOR UPDATE""", id=quota_id,org=oid)
    if not pool:
        fail('Количественный пул не найден',404)
    if pool['status']=='ACTIVE':
        fail('Перед изменением тарифов отключите пул; действующие бронирования должны завершиться.',409)
    ids=validate_assigned_tariffs(db,oid,pool['building_id'],
                                  pool['comfort_category_id'],pool['room_capacity'],body.tariff_ids)
    try:
        replace_inventory_tariffs(db,'pool',quota_id,oid,ids)
        log(db,u,'QUOTA_TARIFFS_UPDATED','quota_pools',quota_id,details={'tariff_ids':ids})
        db.commit()
        return {'ok':True,'tariff_ids':ids}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.post('/api/admin/quotas/{quota_id}/{step}')
def quota_step(quota_id:int,step:str,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if step!='activate':
        fail('Подтверждение не требуется; используйте activate')
    if not row(db,'SELECT id FROM dorm_booking.quota_pools WHERE id=:id AND organization_id=:org',id=quota_id,org=oid):
        fail('Пул не найден',404)
    if not scalar(db, 'SELECT 1 FROM dorm_booking.quota_pool_tariff_links WHERE pool_id=:pool',pool=quota_id):
        fail('Перед публикацией пула назначьте хотя бы один тариф',409)
    linked = scalar(db, 'SELECT count(*) FROM dorm_booking.quota_pool_beds WHERE pool_id=:id', id=quota_id)
    pool = row(db, 'SELECT quantity,inventory_mode FROM dorm_booking.quota_pools WHERE id=:id', id=quota_id)
    if pool['inventory_mode']=='PHYSICAL' and (not linked or linked!=pool['quantity']):
        fail(f'Для физического пула требуется {pool["quantity"]} привязанных коек. Сейчас {linked}.',409)
    func='activate_quota_pool'
    try:
        count=scalar(db,f'SELECT dorm_booking.{func}(:id,:actor)',id=quota_id,actor=u['id'])
        log(db,u,'QUOTA_'+step.upper(),'quota_pools',quota_id,'Подтверждён количественный пул',{'count':count})
        db.commit()
        return {'ok':True,'count':count}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


class QuotaQuantityIn(BaseModel):
    quantity: int = Field(ge=1, le=50000)


@app.put('/api/admin/quotas/{quota_id}/quantity')
def change_quota_quantity(quota_id:int,body:QuotaQuantityIn,
                          db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    q=row(db,'SELECT id,status FROM dorm_booking.quota_pools WHERE id=:id AND organization_id=:o',id=quota_id,o=oid)
    if not q:
        fail('Количественный пул не найден',404)
    linked = scalar(db, 'SELECT count(*) FROM dorm_booking.quota_pool_beds WHERE pool_id=:id', id=quota_id)
    if linked and body.quantity != linked:
        fail(f'Пул связан с {linked} физическими местами. Для изменения количества сначала измените состав в черновике.',409)
    try:
        if q['status']=='DRAFT':
            db.execute(text('UPDATE dorm_booking.quota_pools SET quantity=:n WHERE id=:id'),
                       {'n':body.quantity,'id':quota_id})
        elif q['status']=='ACTIVE':
            scalar(db,'SELECT dorm_booking.resize_quota_pool(:id,:actor,:n)',
                   id=quota_id,actor=u['id'],n=body.quantity)
        else:
            fail('Изменить количество можно только в черновике или активном пуле',409)
        log(db,u,'QUOTA_RESIZED','quota_pools',quota_id,details={'quantity':body.quantity})
        db.commit()
        return {'ok':True,'quantity':body.quantity}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class QuotaBedsRelease(BaseModel):
    bed_ids: list[int] = Field(min_length=1,max_length=5000)


@app.put('/api/admin/quotas/{quota_id}/release-beds')
def quota_release_beds(quota_id:int,body:QuotaBedsRelease,
                       db:Session=Depends(db_session),u:dict=Depends(current_user)):
    """Unlink selected beds from PHYSICAL pools; no automatic publication.

    SQL function performs the tenant/auth, period, quota-slot and capacity checks
    under row locks. Requires v6.11 migration.
    """
    oid=org_admin(u)
    if not row(db,'SELECT id FROM dorm_booking.quota_pools WHERE id=:id AND organization_id=:org',
               id=quota_id,org=oid):
        fail('Количественный пул не найден',404)
    if len(body.bed_ids)!=len(set(body.bed_ids)):
        fail('Список содержит повторяющиеся места',422)
    try:
        result=row(db,"SELECT * FROM dorm_booking.release_quota_pool_beds(:pool,:actor,CAST(:ids AS bigint[]))",pool=quota_id,actor=u['id'],ids=body.bed_ids)
        log(db,u,'QUOTA_BEDS_RELEASED','quota_pools',quota_id,
            'Физические койки исключены из количественного пула',
            {'bed_ids':body.bed_ids,'linked_beds':result['linked_beds'],
             'new_status':result['new_status']})
        db.commit()
        return {'ok':True,**result}
    except HTTPException:
        db.rollback()
        raise
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.put('/api/admin/quotas/{quota_id}/disable')
def disable_quota(quota_id:int,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    q=row(db,'SELECT id,status FROM dorm_booking.quota_pools WHERE id=:id AND organization_id=:o',id=quota_id,o=oid)
    if not q:
        fail('Количественный пул не найден',404)
    if q['status']!='ACTIVE':
        fail('Отключать можно только активный пул',409)
    try:
        db.execute(text("UPDATE dorm_booking.quota_pools SET status='INACTIVE' WHERE id=:id"),{'id':quota_id})
        log(db,u,'QUOTA_DISABLED','quota_pools',quota_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.delete('/api/admin/quotas/{quota_id}')
def archive_quota(quota_id:int,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    """Soft delete. Keep slots and reservations for financial/audit history."""
    oid=org_admin(u)
    try:
        pool=row(db,'''SELECT id,name,status,inventory_mode,deleted_at
            FROM dorm_booking.quota_pools WHERE id=:id AND organization_id=:org
            FOR UPDATE''',id=quota_id,org=oid)
        if not pool or pool['deleted_at']:
            fail('Количественный пул не найден или уже удалён',404)
        linked=scalar(db,'SELECT count(*) FROM dorm_booking.quota_pool_beds WHERE pool_id=:id',id=quota_id)
        if linked:
            fail(f'Нельзя удалить пул «{pool["name"]}»: за ним закреплено {linked} физических коек. '
                 'Сначала исключите их из пула через «Состав коек» или «Освободить все койки».',409)
        pending=scalar(db,'''SELECT count(*) FROM dorm_booking.quota_slots qs
            JOIN dorm_booking.quota_slot_periods qp ON qp.slot_id=qs.id
            JOIN dorm_booking.bookings bk ON bk.quota_slot_period_id=qp.id
            WHERE qs.pool_id=:id AND qp.status='ACTIVE' AND qp.check_out_date>(now() AT TIME ZONE 'Europe/Moscow')::date
              AND bk.status IN ('PENDING_ADMIN','CONFIRMED')''',id=quota_id)
        if pending:
            fail(f'Нельзя удалить пул «{pool["name"]}»: есть {pending} текущих или будущих бронирований. '
                 'Историю бронирований необходимо сохранить; сначала урегулируйте заявки.',409)
        db.execute(text('''UPDATE dorm_booking.quota_pools
            SET status='INACTIVE',deleted_at=now(),deleted_by=:actor
            WHERE id=:id AND organization_id=:org'''),
            {'actor':u['id'],'id':quota_id,'org':oid})
        log(db,u,'QUOTA_ARCHIVED','quota_pools',quota_id,
            f'Пул «{pool["name"]}» удалён из рабочего списка с сохранением истории')
        db.commit()
        return {'ok':True,'message':'Пул удалён из рабочего списка, история сохранена'}
    except HTTPException:
        db.rollback(); raise
    except SQLAlchemyError as e:
        db.rollback(); raise wrap_db_error(e)


class NewBuilding(BaseModel):
    name:str
    address:str=''
    district:str=''
    latitude:Optional[float]=None
    longitude:Optional[float]=None
    building_layout:str='CORRIDOR'
    building_type:str='DORMITORY'
    total_floors:Optional[int]=Field(default=None,ge=1)
    description:Optional[str]=None
    gender_policy:str='ANY'
    allow_guests: bool=False


def six_decimal_coordinate(value: Optional[float], limit: int):
    if value is None:
        return None
    if not -limit <= value <= limit:
        fail('Координата вне допустимого диапазона', 422)
    return float(Decimal(str(value)).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))


@app.post('/api/admin/buildings')
def add_building(body:NewBuilding,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    try:
        if body.building_layout not in ('CORRIDOR','BLOCK','APARTMENT','MIXED'):
            fail('Недопустимая планировка общежития',422)
        if body.gender_policy not in ('ANY','MALE','FEMALE'):
            fail('Недопустимое правило размещения по полу',422)
        if body.building_type not in ('DORMITORY','APARTMENT'):
            fail('Недопустимый тип здания',422)
        # Temporary collision-proof code during INSERT; after IDENTITY allocation the final code
        # is derived from the real ID. Legacy user codes remain unchanged.
        temporary_code = 'SYS_NEW_' + secrets.token_hex(12).upper()
        bid=scalar(db,"""INSERT INTO dorm_booking.buildings(organization_id,code,name,address,district,
            latitude,longitude,building_layout,building_type,total_floors,description,gender_policy,allow_guests,status)
          VALUES (:org,:code,:name,:addr,:district,:lat,:lon,:layout,:type,:floors,:desc,:g,:guests,'DRAFT') RETURNING id""",
          org=oid,code=temporary_code,name=body.name.strip(),addr=body.address,district=body.district,
          lat=six_decimal_coordinate(body.latitude,90),lon=six_decimal_coordinate(body.longitude,180),
          layout=body.building_layout,type=body.building_type,floors=body.total_floors,
          desc=body.description,g=body.gender_policy,guests=body.allow_guests)
        generated_code = f'SYS_BUILDING_{bid}'
        if scalar(db, 'SELECT 1 FROM dorm_booking.buildings WHERE organization_id=:org AND code=:code',
                  org=oid, code=generated_code):
            generated_code = 'SYS_BUILDING_' + secrets.token_hex(10).upper()
        db.execute(text('UPDATE dorm_booking.buildings SET code=:code WHERE id=:id AND organization_id=:org'),
                   {'code':generated_code,'id':bid,'org':oid})
        log(db,u,'BUILDING_CREATED','buildings',bid,body.name)
        db.commit()
        return {'id':bid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


class NewFloor(BaseModel):
    building_id:int
    floor_number:int
    gender_policy:str='ANY'


@app.post('/api/admin/floors')
def add_floor(body:NewFloor,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    bid=row(db,'SELECT id FROM dorm_booking.buildings WHERE id=:id AND organization_id=:o',id=body.building_id,o=oid)
    if not bid:
        fail('Здание не найдено',404)
    try:
        fid=scalar(db,"""INSERT INTO dorm_booking.floors(organization_id,building_id,floor_number,gender_policy)
            VALUES(:o,:b,:n,:g) RETURNING id""",o=oid,b=body.building_id,n=body.floor_number,g=body.gender_policy)
        log(db,u,'FLOOR_CREATED','floors',fid)
        db.commit()
        return {'id':fid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


# Blocks with four rooms are present in the actual SPbGU beds_import workbook.
# Use the same limit in manual room editing, block activation and Excel import.
MAX_ROOMS_PER_BLOCK = 4


class NewRoom(BaseModel):
    building_id:int
    floor_id:int
    block_id:Optional[int]=None
    category_id:int
    room_number:str
    capacity:int=Field(ge=1,le=50)
    gender_policy:str='ANY'


@app.post('/api/admin/rooms')
def add_room(body:NewRoom,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if not row(db,"""SELECT f.id FROM dorm_booking.floors f
          WHERE f.id=:f AND f.building_id=:b AND f.organization_id=:org""",
          f=body.floor_id,b=body.building_id,org=oid):
        fail('Этаж не принадлежит выбранному общежитию',404)
    if not row(db,"""SELECT id FROM dorm_booking.comfort_categories
        WHERE id=:c AND organization_id=:org""",c=body.category_id,org=oid):
        fail('Категория комфортности не найдена',404)
    # Explain inherited gender policy BEFORE PostgreSQL constraints fire.
    inherited = row(db, '''SELECT bu.gender_policy AS building_gender,
        f.gender_policy AS floor_gender, bu.name AS building_name,
        f.floor_number FROM dorm_booking.floors f
        JOIN dorm_booking.buildings bu ON bu.id=f.building_id
        WHERE f.id=:floor AND bu.id=:building AND bu.organization_id=:org''',
        floor=body.floor_id, building=body.building_id, org=oid)
    inherited=inherited or {}
    building_gender=inherited.get('building_gender')
    floor_gender=inherited.get('floor_gender')
    names={'FEMALE':'только женщины','MALE':'только мужчины'}
    gender=body.gender_policy
    if gender not in ('ANY','MALE','FEMALE'):
        fail('Укажите пол комнаты: мужчины, женщины или без отдельного ограничения.', 422)
    for source, parent_gender in [('общежития', building_gender),
                                  ('этажа', floor_gender)]:
        if parent_gender in names and gender in names and parent_gender != gender:
            fail(f'Нельзя создать комнату «{body.room_number}»: правило {source} — «{names[parent_gender]}», '
                 f'а для комнаты выбрано «{names[gender]}». Выберите совместимое ограничение.', 409)
    if building_gender in names and floor_gender in names and building_gender != floor_gender:
        fail('Этаж имеет ограничение по полу, противоречащее правилу общежития. Исправьте параметры этажа.', 409)
    if body.block_id is not None:
        block_gender = row(db, 'SELECT gender_policy FROM dorm_booking.blocks WHERE id=:id AND floor_id=:floor AND organization_id=:org',
                           id=body.block_id, floor=body.floor_id, org=oid)
        if block_gender and block_gender.get('gender_policy') in names:
            rule=block_gender['gender_policy']
            if gender in names and gender != rule:
                fail(f'Нельзя создать комнату «{body.room_number}»: блок предназначен для '
                     f'«{names[rule]}», а комната — для «{names[gender]}».',409)
            if any(g in names and g != rule for g in (building_gender, floor_gender)):
                fail('Ограничение блока противоречит ограничению этажа или здания. Исправьте настройки блока.', 409)
        building=row(db,'SELECT building_layout FROM dorm_booking.buildings WHERE id=:id AND organization_id=:org',id=body.building_id,org=oid)
        if building and building.get('building_layout')=='CORRIDOR':
            fail('Коридорное общежитие не поддерживает блоки',409)
        block=row(db,"""SELECT bl.id,(SELECT count(*) FROM dorm_booking.rooms r
             WHERE r.block_id=bl.id)::INTEGER AS room_count
             FROM dorm_booking.blocks bl WHERE bl.id=:bl AND bl.organization_id=:org
             AND bl.building_id=:b AND bl.floor_id=:f""",
             bl=body.block_id,org=oid,b=body.building_id,f=body.floor_id)
        if not block:
            fail('Блок не относится к выбранному этажу',404)
        if block['room_count']>=MAX_ROOMS_PER_BLOCK:
            fail(f'В блоке не может быть более {MAX_ROOMS_PER_BLOCK} комнат',409)
    try:
        rid=scalar(db,"""INSERT INTO dorm_booking.rooms(organization_id,building_id,floor_id,block_id,
           comfort_category_id,room_number,capacity,gender_policy)
           VALUES (:o,:b,:f,:bl,:c,:num,:cap,:g) RETURNING id""",
           o=oid,b=body.building_id,f=body.floor_id,bl=body.block_id,c=body.category_id,
           num=body.room_number,cap=body.capacity,g=body.gender_policy)
        log(db,u,'ROOM_CREATED','rooms',rid)
        db.commit()
        return {'id':rid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


@app.put('/api/admin/rooms/{room_id}/activate')
def admin_room_activate(
    room_id: int,
    db: Session = Depends(db_session),
    u: dict = Depends(current_user),
):
    """Activate an organization-owned room without publishing its beds."""
    oid = org_admin(u)

    try:
        room = row(db, """
            SELECT
                r.id,
                r.room_number,
                r.status,
                r.comfort_category_id,
                cc.is_active AS category_active
            FROM dorm_booking.rooms r
            LEFT JOIN dorm_booking.comfort_categories cc
              ON cc.id = r.comfort_category_id
             AND cc.organization_id = r.organization_id
            WHERE r.id = :room_id
              AND r.organization_id = :org_id
            FOR UPDATE OF r
        """, room_id=room_id, org_id=oid)

        if room is None:
            fail('Комната не найдена', 404)

        if room['status'] == 'ACTIVE':
            return {
                'ok': True,
                'room_id': room_id,
                'status': 'ACTIVE',
            }

        if room['comfort_category_id'] is None:
            fail('Перед активацией назначьте комнате категорию комфортности', 409)

        if not room['category_active']:
            fail('Категория комфортности комнаты неактивна', 409)

        db.execute(text("""
            UPDATE dorm_booking.rooms
            SET status = 'ACTIVE'
            WHERE id = :room_id
              AND organization_id = :org_id
        """), {'room_id': room_id, 'org_id': oid})

        log(
            db,
            u,
            'ROOM_ACTIVATED',
            'rooms',
            room_id,
            f"Активирована комната {room['room_number']}",
        )
        db.commit()
        return {
            'ok': True,
            'room_id': room_id,
            'status': 'ACTIVE',
        }
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class BlockStatusIn(BaseModel):
    status: str


@app.put('/api/admin/blocks/{block_id}/status')
def update_block_status(block_id:int,body:BlockStatusIn,
                        db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.status not in ('ACTIVE','INACTIVE'):
        fail('Недопустимый статус блока')
    block=row(db,"""SELECT bl.id,bl.status,
          (SELECT count(*) FROM dorm_booking.rooms r WHERE r.block_id=bl.id)::INTEGER AS rooms_count
          FROM dorm_booking.blocks bl WHERE bl.id=:id AND bl.organization_id=:o""",
          id=block_id,o=oid)
    if not block:
        fail('Блок не найден',404)
    if body.status=='ACTIVE' and not (2<=block['rooms_count']<=MAX_ROOMS_PER_BLOCK):
        fail(f'Для активации блок должен включать 2–{MAX_ROOMS_PER_BLOCK} комнаты',409)
    try:
        db.execute(text('UPDATE dorm_booking.blocks SET status=:status WHERE id=:id'),
            {'status':body.status,'id':block_id})
        log(db,u,'BLOCK_STATUS_CHANGED','blocks',block_id,body.status)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class FloorUpdate(BaseModel):
    name: Optional[str] = Field(default=None, max_length=100)
    gender_policy: Optional[str] = None
    status: str = 'ACTIVE'


@app.put('/api/admin/floors/{floor_id}')
def admin_update_floor(floor_id:int,body:FloorUpdate,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.gender_policy not in (None,'ANY','MALE','FEMALE') or body.status not in ('ACTIVE','INACTIVE'):
        fail('Недопустимый пол или статус этажа',422)
    if not row(db,'SELECT id FROM dorm_booking.floors WHERE id=:id AND organization_id=:org',id=floor_id,org=oid):
        fail('Этаж не найден',404)
    try:
        db.execute(text('''UPDATE dorm_booking.floors SET name=:name,gender_policy=:gender,status=:status
                   WHERE id=:id AND organization_id=:org'''),{'name':body.name,'gender':body.gender_policy,
                   'status':body.status,'id':floor_id,'org':oid})
        log(db,u,'FLOOR_UPDATED','floors',floor_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class BlockUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=150)
    gender_policy: Optional[str] = None


@app.put('/api/admin/blocks/{block_id}')
def admin_update_block(block_id:int,body:BlockUpdate,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.gender_policy not in (None,'ANY','MALE','FEMALE'):
        fail('Недопустимый пол блока',422)
    if not row(db,'SELECT id FROM dorm_booking.blocks WHERE id=:id AND organization_id=:org',id=block_id,org=oid):
        fail('Блок не найден',404)
    try:
        db.execute(text('UPDATE dorm_booking.blocks SET name=:name,gender_policy=:gender WHERE id=:id'),
                   {'name':body.name.strip(),'gender':body.gender_policy,'id':block_id})
        log(db,u,'BLOCK_UPDATED','blocks',block_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class RoomUpdate(BaseModel):
    room_number: str = Field(min_length=1, max_length=64)
    capacity: int = Field(ge=1, le=50)
    comfort_category_id: int
    gender_policy: Optional[str] = None
    block_id: Optional[int] = None


@app.put('/api/admin/rooms/{room_id}')
def admin_update_room(room_id:int,body:RoomUpdate,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    existing=row(db,'SELECT id,building_id,floor_id,block_id FROM dorm_booking.rooms WHERE id=:id AND organization_id=:org',id=room_id,org=oid)
    if not existing:
        fail('Комната не найдена',404)
    if body.gender_policy not in (None,'ANY','MALE','FEMALE'):
        fail('Недопустимое правило размещения',422)
    if not row(db,'SELECT id FROM dorm_booking.comfort_categories WHERE id=:id AND organization_id=:org AND is_active',id=body.comfort_category_id,org=oid):
        fail('Выберите действующую категорию комфортности',422)
    if body.block_id is not None:
        block=row(db,"""SELECT bl.id,(SELECT count(*) FROM dorm_booking.rooms r WHERE r.block_id=bl.id AND r.id<>:room) AS rooms_count
          FROM dorm_booking.blocks bl WHERE bl.id=:id AND bl.building_id=:b AND bl.floor_id=:f AND bl.organization_id=:org""",
          id=body.block_id,b=existing['building_id'],f=existing['floor_id'],org=oid,room=room_id)
        if not block or block['rooms_count']>=MAX_ROOMS_PER_BLOCK:
            fail(f'Неверный блок или в нём уже {MAX_ROOMS_PER_BLOCK} комнаты',409)
    try:
        db.execute(text('''UPDATE dorm_booking.rooms SET room_number=:number,capacity=:cap,
          comfort_category_id=:category,gender_policy=:gender,block_id=:block
          WHERE id=:id AND organization_id=:org'''),
          {'number':body.room_number.strip(),'cap':body.capacity,'category':body.comfort_category_id,
           'gender':body.gender_policy,'block':body.block_id,'id':room_id,'org':oid})
        log(db,u,'ROOM_UPDATED','rooms',room_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class NewBed(BaseModel):
    room_id:int
    bed_number:Optional[str]=None


@app.post('/api/admin/beds')
def add_bed(body:NewBed,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if not row(db,'SELECT 1 FROM dorm_booking.rooms WHERE id=:id AND organization_id=:o',id=body.room_id,o=oid):
        fail('Комната не найдена',404)
    number=(body.bed_number or '').strip()
    if len(number)>64:
        fail('Номер койко-места не может быть длиннее 64 символов',422)
    try:
        bid=scalar(db,"""INSERT INTO dorm_booking.beds(organization_id,room_id,bed_number)
           VALUES(:o,:r,:number) RETURNING id""",o=oid,r=body.room_id,number=number or None)
        log(db,u,'BED_CREATED','beds',bid)
        db.commit()
        return {'id':bid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


class BedEdit(BaseModel):
    bed_number: str = Field(min_length=1, max_length=64)
    gender_policy: Optional[str] = None
    tariff_id: Optional[int] = Field(default=None, gt=0)
    tariff_ids: Optional[list[int]] = Field(default=None, max_length=100)


@app.put('/api/admin/beds/{bed_id}')
def admin_edit_bed(bed_id: int, body: BedEdit,
                   db: Session = Depends(db_session),
                   u: dict = Depends(current_user)):
    org_id = org_admin(u)
    if body.gender_policy not in (None,'ANY','MALE','FEMALE'):
        fail('Недопустимое ограничение по полу', 422)
    bed = row(db, '''SELECT id,room_id,bed_number,status,tariff_id FROM dorm_booking.beds
        WHERE id=:id AND organization_id=:org''', id=bed_id, org=org_id)
    if not bed:
        fail('Место не найдено', 404)
    room_data = row(db, """SELECT building_id,comfort_category_id,capacity
        FROM dorm_booking.rooms WHERE id=:id""",id=bed['room_id'])
    ids = body.tariff_ids if body.tariff_ids is not None else ([body.tariff_id] if body.tariff_id else [])
    # Allow clearing the selection to return the bed to a non-bookable state.
    if ids:
        ids=validate_assigned_tariffs(db,org_id,room_data['building_id'],
             room_data['comfort_category_id'],room_data['capacity'],ids)
    try:
        db.execute(text('''UPDATE dorm_booking.beds SET bed_number=:number,
             gender_policy=:gender, tariff_id=:tariff WHERE id=:id AND organization_id=:org'''),
            {'number':body.bed_number.strip(),'gender':body.gender_policy,
             'tariff':ids[0] if ids else None,'id':bed_id,'org':org_id})
        replace_inventory_tariffs(db,'bed',bed_id,org_id,ids)
        log(db,u,'BED_UPDATED','beds',bed_id,details={'tariff_ids':ids})
        db.commit()
        return {'ok':True,'id':bed_id}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


# Column names presented to administrators; English names remain accepted for older templates.
STUDENT_IMPORT_HEADERS = {
    'Логин':'login', 'Пароль':'password', 'Фамилия':'last_name',
    'Имя':'first_name', 'Отчество':'middle_name', 'Пол':'gender',
    'Гражданство':'citizenship_category', 'Основа обучения':'education_basis',
    'Статус обучения':'study_status', 'Дата окончания обучения':'study_end_date',
    'Дата начала обучения':'study_start_date',
}
BED_IMPORT_HEADERS = {
    'Общежитие':'building', 'Этаж':'floor', 'Блок':'block',
    'Номер комнаты':'room_number', 'Комната':'room_number',
    'Вместимость':'capacity', 'Категория комфортности':'category',
    'Номер койко-места':'bed_number', 'Номер места':'bed_number',
    'Пол':'gender_policy', 'ID тарифа':'tariff_id',
    'ID комнаты':'room_id',
}
IMPORT_GENDER_VALUES = {
    'МУЖСКОЙ':'MALE', 'МУЖЧИНЫ':'MALE', 'М':'MALE', 'MALE':'MALE',
    'ЖЕНСКИЙ':'FEMALE', 'ЖЕНЩИНЫ':'FEMALE', 'Ж':'FEMALE', 'FEMALE':'FEMALE',
    'ЛЮБОЙ':'ANY', 'ЛЮБОЙ ПОЛ':'ANY', 'БЕЗ ОГРАНИЧЕНИЙ':'ANY', 'ANY':'ANY',
}


def _excel_choice(value, mapping):
    if value is None or str(value).strip() == '':
        return ''
    return mapping.get(str(value).strip().upper(), str(value).strip().upper())


def _excel_int(value, title, required=False):
    if value is None or str(value).strip() == '':
        if required:
            raise ValueError(f'{title}: заполните обязательное поле')
        return None
    try:
        number = int(value)
        if number < 1 or isinstance(value, bool) or str(value).strip() not in {str(number), f'{number}.0'}:
            raise ValueError()
        return number
    except (ValueError, TypeError):
        raise ValueError(f'{title}: «{value}» — требуется положительное целое число') from None


def _excel_bed_number(value):
    result = str(value or '').strip()
    if result.endswith('.0') and result[:-2].isdigit():
        result = result[:-2]
    if not result or len(result) > 64:
        raise ValueError('Номер койко-места: заполните номер длиной не более 64 символов')
    return result


@app.post('/api/admin/beds/import')
async def import_beds_excel(
    file: UploadFile = File(...),
    db: Session = Depends(db_session),
    u: dict = Depends(current_user),
):
    """Atomic import with automatic creation of missing blocks and rooms.

    Building/floor/category must exist. Newly created blocks and rooms remain
    INACTIVE and beds DRAFT; publication remains an explicit operation.
    """
    oid = org_admin(u)
    if not file.filename or not file.filename.lower().endswith('.xlsx'):
        fail('Загрузите Excel-файл .xlsx')
    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        fail('Файл превышает ограничение загрузки')
    from openpyxl import load_workbook
    try:
        book = load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        try:
            sheet = book['Места'] if 'Места' in book.sheetnames else book.active
            iterator = sheet.values
            columns = _excel_headers(next(iterator, ()), {'bed_number'}, BED_IMPORT_HEADERS)
            if 'room_id' not in columns and not {'building','floor','room_number'}.issubset(columns):
                raise ValueError('Укажите столбцы «Общежитие», «Этаж», «Номер комнаты» (или старый room_id)')
            raw_rows = [(i,dict(zip(columns,vals))) for i,vals in enumerate(iterator,2)
                        if any(v is not None and str(v).strip() for v in vals)]
        finally:
            book.close()
    except ValueError as exc:
        return {'ok':False,'imported':0,'total_errors':1,'errors':[{'row':1,'error':str(exc)}]}
    except Exception:
        fail('Некорректный XLSX. Проверьте файл и заголовки столбцов')
    if len(raw_rows)>MAX_IMPORT_ROWS:
        fail(f'Превышен лимит строк: {MAX_IMPORT_ROWS}')
    if not raw_rows:
        return {'ok':False,'imported':0,'total_errors':1,'errors':[{'row':2,'error':'Нет строк для импорта'}]}

    buildings = rows(db,"""SELECT id,code,name,building_layout FROM dorm_booking.buildings
        WHERE organization_id=:o""",o=oid)
    floors = rows(db,"""SELECT id,building_id,floor_number,status FROM dorm_booking.floors
        WHERE organization_id=:o""",o=oid)
    blocks = rows(db,"""SELECT id,building_id,floor_id,name,code,status FROM dorm_booking.blocks
        WHERE organization_id=:o""",o=oid)
    categories = rows(db,"""SELECT id,name,is_active FROM dorm_booking.comfort_categories
        WHERE organization_id=:o""",o=oid)
    rooms = rows(db,"""SELECT id,building_id,floor_id,block_id,room_number,capacity,
        comfort_category_id FROM dorm_booking.rooms WHERE organization_id=:o""",o=oid)
    existing_rooms = {int(room['id']):room for room in rooms}
    tariffs = {int(t['id']):t for t in rows(db,"""SELECT id,building_id,comfort_category_id,room_capacity
        FROM dorm_booking.tariffs WHERE organization_id=:o AND status='ACTIVE'""",o=oid)}
    existing_beds=rows(db,"""SELECT room_id,bed_number,status FROM dorm_booking.beds
        WHERE organization_id=:o""",o=oid)
    taken = {(int(b['room_id']),str(b['bed_number']).strip().casefold()) for b in existing_beds}
    counts = {}
    for bed in existing_beds:
        if bed['status']!='ARCHIVED':
            counts[int(bed['room_id'])]=counts.get(int(bed['room_id']),0)+1
    errors, prepared, new_rooms, new_blocks, seen = [], [], {}, {}, set()

    def one(items, what, predicate):
        found=[item for item in items if predicate(item)]
        if len(found)!=1:
            raise ValueError(f'{what}: '+('не найдено' if not found else 'найдено несколько совпадений; уточните значение'))
        return found[0]

    for index,record in raw_rows:
        try:
            bed_number=_excel_bed_number(record.get('bed_number'))
            # beds.gender_policy is NOT NULL; blank Excel means no restriction.
            gender=_excel_choice(record.get('gender_policy'),IMPORT_GENDER_VALUES) or 'ANY'
            if gender not in ('ANY','MALE','FEMALE'):
                raise ValueError(f'Пол: «{record.get("gender_policy")}» — мужчины, женщины, без ограничений или пусто')
            raw_id=record.get('room_id')
            if raw_id is not None and str(raw_id).strip():
                room_id=_excel_int(raw_id,'ID комнаты',True)
                room=existing_rooms.get(room_id)
                if not room: raise ValueError(f'ID комнаты: {room_id} — комната организации не найдена')
                key=('existing',room_id)
                capacity=int(room['capacity'])
                category=int(room['comfort_category_id']) if room.get('comfort_category_id') is not None else None
                building_id=int(room['building_id']) if room.get('building_id') is not None else None
            else:
                building_name=str(record.get('building') or '').strip()
                if not building_name: raise ValueError('Общежитие: укажите название общежития')
                building=one(buildings,'Общежитие',lambda b:building_name.casefold() in
                             {str(b['name']).strip().casefold(),str(b['code']).strip().casefold(),str(b['id'])})
                building_id=int(building['id'])
                floor_no=_excel_int(record.get('floor'),'Этаж',True)
                floor=one(floors,'Этаж',lambda f:int(f['building_id'])==building_id and int(f['floor_number'])==floor_no)
                floor_id=int(floor['id'])
                block_name=str(record.get('block') or '').strip()
                block_ref=None  # Existing block ID, or a key for a not-yet-created block.
                if block_name:
                    if len(block_name)>150:
                        raise ValueError('Блок: название не может быть длиннее 150 символов')
                    matches=[b for b in blocks if int(b['building_id'])==building_id
                             and int(b['floor_id'])==floor_id and block_name.casefold() in
                             {str(b['name']).strip().casefold(),str(b['code'] or '').strip().casefold(),str(b['id'])}]
                    if len(matches)>1:
                        raise ValueError('Блок: найдено несколько совпадений; уточните значение')
                    if matches:
                        block_ref=int(matches[0]['id'])
                    else:
                        if building['building_layout']=='CORRIDOR':
                            raise ValueError('Блок: для коридорного общежития нельзя создавать блоки; '
                                             'проверьте планировку общежития')
                        block_ref=('new_block',building_id,floor_id,block_name.casefold())
                        new_blocks[block_ref]=(building_id,floor_id,block_name)
                room_number=str(record.get('room_number') or '').strip()
                if not room_number or len(room_number)>64:
                    raise ValueError('Номер комнаты: заполните номер длиной не более 64 символов')
                same_number=[r for r in rooms if int(r['floor_id'])==floor_id
                       and str(r['room_number']).strip().casefold()==room_number.casefold()]
                found=[r for r in same_number if r['block_id']==block_ref]
                if block_ref is None and not found and any(r['block_id'] is not None for r in same_number):
                    raise ValueError(f'Комната «{room_number}» существует в блоке: заполните столбец «Блок», чтобы не создавать дубликат')
                if len(found)>1: raise ValueError(f'Комната «{room_number}»: найдено несколько совпадений')
                category_raw=str(record.get('category') or '').strip()
                cap_value=_excel_int(record.get('capacity'),'Вместимость')
                cat= None
                if category_raw:
                    cat=one(categories,'Категория комфортности',lambda c:category_raw.casefold() in
                            {str(c['name']).strip().casefold(),str(c['id'])})
                if found:
                    room=found[0]
                    room_id=int(room['id'])
                    key=('existing',room_id)
                    capacity=int(room['capacity'])
                    category=int(room['comfort_category_id']) if room.get('comfort_category_id') is not None else None
                    if cap_value is not None and cap_value!=capacity:
                        raise ValueError(f'Комната «{room_number}»: вместимость {cap_value} расходится с БД ({capacity})')
                    if cat and int(cat['id'])!=category:
                        raise ValueError(f'Комната «{room_number}»: категория комфортности не соответствует БД')
                else:
                    if cap_value is None or not 1<=cap_value<=50:
                        raise ValueError(f'Комната «{room_number}» отсутствует: укажите вместимость от 1 до 50')
                    if not cat or not cat['is_active']:
                        raise ValueError(f'Комната «{room_number}» отсутствует: укажите действующую категорию комфортности')
                    key=('new',building_id,floor_id,block_ref,room_number.casefold())
                    capacity=cap_value
                    category=int(cat['id'])
                    room_id=None
                    details=(building_id,floor_id,block_ref,room_number,capacity,category)
                    if key in new_rooms and new_rooms[key]!=details:
                        raise ValueError(f'Комната «{room_number}»: конфликт вместимости или категории в строках файла')
                    new_rooms[key]=details
                    if block_ref is not None:
                        # Count distinct rooms, not the number of bed rows for each room.
                        siblings={r['id'] for r in rooms if r['block_id']==block_ref}
                        added=sum(1 for k in new_rooms if k[0]=='new' and k[3]==block_ref)
                        if len(siblings)+added>MAX_ROOMS_PER_BLOCK:
                            raise ValueError(f'Блок «{block_name}»: нельзя создать более '
                                             f'{MAX_ROOMS_PER_BLOCK} комнат')
            raw_tariff=record.get('tariff_id')
            tariff_id=_excel_int(raw_tariff,'ID тарифа')
            if tariff_id is not None:
                tariff=tariffs.get(tariff_id)
                if not tariff or int(tariff['building_id'])!=building_id or int(tariff['comfort_category_id'])!=category or int(tariff['room_capacity'])!=capacity:
                    raise ValueError(f'ID тарифа (tariff_id={tariff_id}): тариф неактивен либо не подходит по общежитию, категории или вместимости')
            unique=(key,bed_number.casefold())
            if unique in seen or (room_id is not None and (room_id,bed_number.casefold()) in taken):
                raise ValueError(f'Номер койко-места «{bed_number}»: уже существует в комнате или файле')
            seen.add(unique)
            counts[key]=counts.get(key,counts.get(room_id,0) if room_id else 0)+1
            if counts[key]>capacity:
                raise ValueError(f'Комната: превышена вместимость {capacity} мест')
            prepared.append((key,bed_number,gender,tariff_id))
        except ValueError as exc:
            errors.append({'row':index,'error':str(exc)})
    if errors:
        return {'ok':False,'imported':0,'created_rooms':0,'created_blocks':0,
                'total_errors':len(errors),'errors':errors[:100]}

    try:
        created={}
        created_blocks={}
        inserted_block_ids=[]
        # Serialize imports within the floors, so concurrent Excel imports cannot
        # independently create the same named block on the same floor.
        involved_floors={details[1] for details in new_rooms.values()}
        involved_floors.update(details[1] for details in new_blocks.values())
        for floor_id in sorted(involved_floors):
            db.execute(text("SELECT id FROM dorm_booking.floors WHERE id=:id AND organization_id=:org FOR UPDATE"),
                       {'id':floor_id,'org':oid})

        for block_key,(building_id,floor_id,name) in new_blocks.items():
            # Recheck after the floor lock in case another import has committed
            # the same block since our initial validation snapshot.
            matches=rows(db,"""SELECT id FROM dorm_booking.blocks
                WHERE organization_id=:org AND building_id=:b AND floor_id=:f
                  AND lower(btrim(name))=:name ORDER BY id FOR UPDATE""",
                org=oid,b=building_id,f=floor_id,name=name.casefold())
            if len(matches)>1:
                fail(f'Блок «{name}»: в БД уже есть несколько блоков с таким названием',409)
            if matches:
                created_blocks[block_key]=int(matches[0]['id'])
                continue
            bid=scalar(db,"""INSERT INTO dorm_booking.blocks
                (organization_id,building_id,floor_id,name,gender_policy,status)
                VALUES (:org,:b,:f,:name,'ANY','ACTIVE') RETURNING id""",
                org=oid,b=building_id,f=floor_id,name=name)
            code=f'SYS_BLOCK_{bid}'
            if scalar(db,"""SELECT 1 FROM dorm_booking.blocks
                    WHERE building_id=:building AND code=:code AND id<>:id""",
                    building=building_id,code=code,id=bid):
                code='SYS_BLOCK_'+secrets.token_hex(10).upper()
            db.execute(text("""UPDATE dorm_booking.blocks SET code=:code
                WHERE id=:id AND organization_id=:org"""),
                {'code':code,'id':bid,'org':oid})
            created_blocks[block_key]=int(bid)
            inserted_block_ids.append(int(bid))

        for key,(building_id,floor_id,block_ref,number,capacity,category) in new_rooms.items():
            block_id=created_blocks[block_ref] if isinstance(block_ref,tuple) else block_ref
            rid=scalar(db,"""INSERT INTO dorm_booking.rooms
                  (organization_id,building_id,floor_id,block_id,comfort_category_id,room_number,capacity,status)
                  VALUES (:o,:b,:f,:block,:category,:num,:capacity,'ACTIVE') RETURNING id""",
                  o=oid,b=building_id,f=floor_id,block=block_id,category=category,num=number,capacity=capacity)
            created[key]=rid
        for key,bed_number,gender,tariff_id in prepared:
            rid=key[1] if key[0]=='existing' else created[key]
            db.execute(text("""INSERT INTO dorm_booking.beds
                (organization_id,room_id,bed_number,gender_policy,tariff_id,status)
                VALUES (:org,:room,:number,:gender,:tariff,'DRAFT')"""),
                {'org':oid,'room':rid,'number':bed_number,'gender':gender,'tariff':tariff_id})
        log(db,u,'IMPORT_BEDS','beds',None,'Импорт конкретных мест из Excel',
            {'count':len(prepared),'created_rooms':len(created),
             'created_blocks':len(inserted_block_ids),'new_block_ids':inserted_block_ids[:100]})
        db.commit()
        return {'ok':True,'imported':len(prepared),'created_rooms':len(created),
                'created_blocks':len(inserted_block_ids),
                'assigned_tariffs':sum(tariff_id is not None for _,_,_,tariff_id in prepared),'errors':[]}
    except HTTPException:
        db.rollback()
        raise
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class AssignIn(BaseModel):
    bed_id:int


@app.post('/api/admin/bookings/{bid}/assign')
def admin_assign(bid:int,body:AssignIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    bk=row(db,'''SELECT bk.id,bk.organization_id,bk.status,bk.bed_period_id,sl.pool_id,
        qp.status AS period_status,qp.check_in_date,qp.check_out_date,uu.gender AS person_gender
        FROM dorm_booking.bookings bk
       JOIN dorm_booking.users uu ON uu.id=bk.person_user_id
       JOIN dorm_booking.quota_slot_periods qp ON qp.id=bk.quota_slot_period_id
       JOIN dorm_booking.quota_slots sl ON sl.id=qp.slot_id
       WHERE bk.id=:id AND bk.booking_mode='QUOTA' ''',id=bid)
    if not bk or bk['organization_id']!=oid:
        fail('Заявка не найдена',404)
    mode = scalar(db,'SELECT inventory_mode FROM dorm_booking.quota_pools WHERE id=:pool',pool=bk['pool_id'])
    if mode != 'PHYSICAL':
        fail('В виртуальном количественном пуле назначение конкретной койки не требуется',409)
    if not row(db, 'SELECT bed_id FROM dorm_booking.quota_pool_beds WHERE pool_id=:pool AND bed_id=:bed',
               pool=bk['pool_id'], bed=body.bed_id):
        fail('Место не закреплено за этим количественным пулом',409)
    if bk['status'] not in ('PENDING_ADMIN','CONFIRMED'):
        fail('Назначение невозможно: заявку уже отменили, завершили или отметили неявку.',409)
    if bk['bed_period_id'] is not None:
        fail('Для этой заявки физическое койко-место уже назначено.',409)
    if bk['period_status']!='ACTIVE':
        fail('Резерв пула отменён или завершён. Назначение места невозможно.',409)
    candidate=row(db,'''SELECT b.status AS bed_status,b.organization_id,
            r.status AS room_status,f.status AS floor_status,
            bu.status AS building_status,bl.status AS block_status,
            dorm_booking.bed_allowed_for_gender(b.id,:gender) AS gender_compatible,
            EXISTS (SELECT 1 FROM dorm_booking.bed_periods bp WHERE bp.bed_id=b.id
               AND bp.status='ACTIVE' AND daterange(bp.check_in_date,bp.check_out_date,'[)')
                && daterange(CAST(:start AS date),CAST(:finish AS date),'[)')) AS occupied
        FROM dorm_booking.beds b JOIN dorm_booking.rooms r ON r.id=b.room_id
        JOIN dorm_booking.floors f ON f.id=r.floor_id
        JOIN dorm_booking.buildings bu ON bu.id=r.building_id
        LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
        WHERE b.id=:bed''',gender=bk['person_gender'],start=bk['check_in_date'],
        finish=bk['check_out_date'],bed=body.bed_id)
    if not candidate:
        fail('Койка не найдена в базе данных.',404)
    if candidate['bed_status'] not in ('DRAFT','INACTIVE'):
        fail('Койка уже опубликована отдельно или архивирована и не может быть назначена из пула.',409)
    if candidate['room_status']!='ACTIVE' or candidate['floor_status']!='ACTIVE' or candidate['building_status']!='ACTIVE' or candidate['block_status'] not in ('ACTIVE',None):
        fail('Не удалось назначить койку: комната, этаж, блок или общежитие не активны.',409)
    if not candidate['gender_compatible']:
        fail('Нельзя назначить койку: она не подходит по ограничению пола проживающего.',409)
    if candidate['occupied']:
        fail('Койка занята на даты этой заявки. Выберите другое свободное место.',409)
    try:
        pid=scalar(db,'SELECT dorm_booking.assign_quota_booking_bed(:bid,:bed,:actor)',bid=bid,bed=body.bed_id,actor=u['id'])
        log(db,u,'BED_ASSIGNED','bookings',bid,'Назначена кровать',{'bed_id':body.bed_id})
        db.commit()
        return {'ok':True,'period_id':pid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


@app.get('/api/admin/assignable-beds/{booking_id}')
def assignable_beds(booking_id:int,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    bk=row(db,"""SELECT bk.id,t.building_id,t.comfort_category_id,t.room_capacity,
        qp.check_in_date,qp.check_out_date,sl.pool_id FROM dorm_booking.bookings bk
        JOIN dorm_booking.tariffs t ON t.id=bk.tariff_id
        JOIN dorm_booking.quota_slot_periods qp ON qp.id=bk.quota_slot_period_id
        JOIN dorm_booking.quota_slots sl ON sl.id=qp.slot_id
        WHERE bk.id=:id AND bk.organization_id=:org AND bk.booking_mode='QUOTA'""",id=booking_id,org=oid)
    if not bk:
        fail('Заявка для распределения не найдена',404)
    if scalar(db,'SELECT inventory_mode FROM dorm_booking.quota_pools WHERE id=:pool',pool=bk['pool_id'])!='PHYSICAL':
        return []
    return rows(db,"""SELECT b.id,b.bed_number,r.room_number,f.floor_number,bl.name AS block_name
       FROM dorm_booking.beds b JOIN dorm_booking.rooms r ON r.id=b.room_id
       JOIN dorm_booking.floors f ON f.id=r.floor_id
       LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
       JOIN dorm_booking.buildings bu ON bu.id=r.building_id
       JOIN dorm_booking.bookings bk ON bk.id=:booking
       JOIN dorm_booking.users person ON person.id=bk.person_user_id
       WHERE b.organization_id=:org AND b.status IN ('DRAFT','INACTIVE')
         AND r.status='ACTIVE' AND f.status='ACTIVE' AND bu.status='ACTIVE'
         AND (bl.id IS NULL OR bl.status='ACTIVE')
         AND dorm_booking.bed_allowed_for_gender(b.id,person.gender)
        AND EXISTS (SELECT 1 FROM dorm_booking.quota_pool_beds
                    WHERE pool_id=:pool AND bed_id=b.id)
        AND NOT EXISTS (SELECT 1 FROM dorm_booking.quota_pool_beds WHERE bed_id=b.id AND pool_id<>:pool)
        AND r.building_id=:building AND r.comfort_category_id=:cat AND r.capacity=:cap
        AND NOT EXISTS(SELECT 1 FROM dorm_booking.bed_periods bp WHERE bp.bed_id=b.id AND bp.status='ACTIVE'
           AND daterange(bp.check_in_date,bp.check_out_date,'[)') && daterange(:start,:finish,'[)'))
       ORDER BY f.floor_number,r.room_number,b.bed_number""",
       booking=booking_id,org=oid,pool=bk['pool_id'],building=bk['building_id'],cat=bk['comfort_category_id'],cap=bk['room_capacity'],
       start=bk['check_in_date'],finish=bk['check_out_date'])


class AdminBookingAction(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=1000)


@app.post('/api/admin/bookings/{bid}/{action}')
def admin_booking_action(bid: int, action: str,
                         body: AdminBookingAction,
                         db: Session = Depends(db_session),
                         u: dict = Depends(current_user)):
    oid=org_admin(u)
    bk=row(db,'SELECT id,organization_id FROM dorm_booking.bookings WHERE id=:id',id=bid)
    if not bk or bk['organization_id']!=oid:
        fail('Заявка не найдена',404)
    names={'confirm':'confirm_booking_admin','cancel':'cancel_booking_by_admin','no_show':'mark_booking_no_show'}
    if action not in names:
        fail('Неизвестное действие')
    reason = (body.reason or '').strip()
    if action == 'cancel' and not reason:
        fail('Укажите причину отмены', 422)
    try:
        scalar(db, f'SELECT dorm_booking.{names[action]}(:id,:actor)', id=bid, actor=u['id'])
        if action == 'cancel':
            # Existing DDL function sets CANCELLED and releases the reserved bed.
            # The author and reason distinguish an administrative cancellation.
            db.execute(text('''UPDATE dorm_booking.bookings
                SET cancel_reason=:reason, cancelled_by_user_id=:actor
                WHERE id=:id AND organization_id=:org AND status='CANCELLED' '''),
                {'reason': reason, 'actor': u['id'], 'id': bid, 'org': oid})
        log(db,u,'BOOKING_'+action.upper(),'bookings',bid,reason or action)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)




class NewCategory(BaseModel):
    name:str


@app.post('/api/admin/categories')
def add_category(body:NewCategory,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if not body.name.strip(): fail('Укажите название категории',422)
    try:
        cid=scalar(db,"""INSERT INTO dorm_booking.comfort_categories(organization_id,name)
          VALUES(:o,:name) RETURNING id""",o=oid,name=body.name.strip())
        log(db,u,'CATEGORY_CREATED','comfort_categories',cid)
        db.commit()
        return {'id':cid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


@app.get('/api/admin/categories')
def admin_categories(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    return rows(db, """SELECT id,name,is_active,created_at FROM dorm_booking.comfort_categories
                    WHERE organization_id=:org ORDER BY is_active DESC,name,id""", org=oid)


class CategoryEdit(BaseModel):
    name: str=Field(min_length=1,max_length=150)
    is_active: bool=True


@app.put('/api/admin/categories/{category_id}')
def admin_category_update(category_id:int,body:CategoryEdit,
                          db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    current=row(db, """SELECT id,name,is_active FROM dorm_booking.comfort_categories
                      WHERE id=:id AND organization_id=:org FOR UPDATE""",id=category_id,org=oid)
    if not current:
        fail('Категория не найдена',404)
    if current['is_active'] and not body.is_active:
        pending=scalar(db, """SELECT 1 FROM dorm_booking.bookings bk
          JOIN dorm_booking.tariffs t ON t.id=bk.tariff_id
          WHERE t.comfort_category_id=:id AND t.organization_id=:org
            AND bk.status IN ('PENDING_ADMIN','CONFIRMED') LIMIT 1""",id=category_id,org=oid)
        if pending:
            fail('Категорию нельзя отключить при действующих бронированиях',409)
    try:
        db.execute(text("""UPDATE dorm_booking.comfort_categories
          SET name=:name,is_active=:active WHERE id=:id AND organization_id=:org"""),
          {'name':body.name.strip(),'active':body.is_active,'id':category_id,'org':oid})
        log(db,u,'CATEGORY_UPDATED','comfort_categories',category_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.get('/api/admin/amenities')
def amenity_list(q:str='',include_inactive:bool=False,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    return rows(db,"""SELECT id,code,name,amenity_group,organization_id,is_active FROM dorm_booking.amenity_definitions
      WHERE (is_active OR (organization_id=:o AND :include_inactive))
        AND (organization_id IS NULL OR organization_id=:o)
       AND (lower(name) LIKE :q OR lower(code) LIKE :q OR similarity(lower(name),:needle)>0.2)
      ORDER BY organization_id NULLS FIRST,name LIMIT 100""",o=oid,include_inactive=include_inactive,q='%'+q.lower()+'%',needle=q.lower())


class AmenityIn(BaseModel):
    code:Optional[str]=None
    name:str
    amenity_group:str='OTHER'


@app.post('/api/admin/amenities')
def add_amenity(body:AmenityIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    similar=rows(db,"""SELECT id,name,code FROM dorm_booking.amenity_definitions WHERE is_active
      AND (organization_id IS NULL OR organization_id=:org)
      AND lower(name)=lower(:name)""",
       org=oid,name=body.name.strip())
    if similar:
        fail('Такая характеристика уже существует: '+similar[0]['name'],409)
    if body.amenity_group not in ('FURNITURE','PERSONAL_KIT','HALLWAY','KITCHEN','BATHROOM','INFRASTRUCTURE','OTHER'):
        fail('Неизвестная группа оснащения')
    try:
        code = 'SYS_AMENITY_' + secrets.token_hex(12).upper()
        aid=scalar(db,"""INSERT INTO dorm_booking.amenity_definitions(organization_id,code,name,amenity_group)
          VALUES(:o,:code,:name,:grp) RETURNING id""",o=oid,code=code,name=body.name.strip(),grp=body.amenity_group)
        generated_code=f'SYS_AMENITY_{aid}'
        if scalar(db, '''SELECT 1 FROM dorm_booking.amenity_definitions
                        WHERE organization_id=:org AND lower(code)=lower(:code)''',
                  org=oid, code=generated_code):
            generated_code='SYS_AMENITY_'+secrets.token_hex(12).upper()
        db.execute(text('UPDATE dorm_booking.amenity_definitions SET code=:code WHERE id=:id'),
                   {'code':generated_code, 'id':aid})
        log(db,u,'AMENITY_CREATED','amenity_definitions',aid,body.name)
        db.commit()
        return {'id':aid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


class AmenityEdit(BaseModel):
    name: str=Field(min_length=1,max_length=200)
    amenity_group: str='OTHER'
    is_active: bool=True


@app.put('/api/admin/amenities/{amenity_id}')
def admin_edit_amenity(amenity_id:int,body:AmenityEdit,
                       db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.amenity_group not in ('FURNITURE','PERSONAL_KIT','HALLWAY','KITCHEN',
                                  'BATHROOM','INFRASTRUCTURE','OTHER'):
        fail('Неизвестная группа оснащения',422)
    if not body.name.strip():
        fail('Укажите название характеристики',422)
    if not row(db,'''SELECT id FROM dorm_booking.amenity_definitions
                   WHERE id=:id AND organization_id=:org''',id=amenity_id,org=oid):
        fail('Характеристика не найдена или является общеплатформенной',404)
    try:
        db.execute(text('''UPDATE dorm_booking.amenity_definitions
          SET name=:name,amenity_group=:grp,is_active=:active
          WHERE id=:id AND organization_id=:org'''),
          {'name':body.name.strip(),'grp':body.amenity_group,'active':body.is_active,
           'id':amenity_id,'org':oid})
        log(db,u,'AMENITY_UPDATED','amenity_definitions',amenity_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class AmenityAssignmentIn(BaseModel):
    amenity_id: int
    target_type: str
    target_id: int
    quantity: Optional[float] = Field(default=None, ge=0)


ASSIGNABLE_AMENITY_TARGETS={'building':('buildings','building_id'),
    'category':('comfort_categories','comfort_category_id'),
    'floor':('floors','floor_id'),'block':('blocks','block_id'),
    'room':('rooms','room_id'),'bed':('beds','bed_id')}


@app.get('/api/admin/amenity-targets')
def admin_amenity_targets(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    return {
      'building':rows(db,"SELECT id,name FROM dorm_booking.buildings WHERE organization_id=:org ORDER BY name",org=oid),
      'category':rows(db,"SELECT id,name FROM dorm_booking.comfort_categories WHERE organization_id=:org ORDER BY name",org=oid),
      'floor':rows(db,"""SELECT f.id,concat(b.name,' · этаж ',f.floor_number) AS name
        FROM dorm_booking.floors f JOIN dorm_booking.buildings b ON b.id=f.building_id
        WHERE f.organization_id=:org ORDER BY b.name,f.floor_number""",org=oid),
      'block':rows(db,"""SELECT bl.id,concat(b.name,' · ',bl.name) AS name
        FROM dorm_booking.blocks bl JOIN dorm_booking.buildings b ON b.id=bl.building_id
        WHERE bl.organization_id=:org ORDER BY b.name,bl.name""",org=oid),
      'room':rows(db,"""SELECT r.id,concat(b.name,' · комната ',r.room_number) AS name
        FROM dorm_booking.rooms r JOIN dorm_booking.buildings b ON b.id=r.building_id
        WHERE r.organization_id=:org ORDER BY b.name,r.room_number""",org=oid),
      'bed':rows(db,"""SELECT bed.id,concat(b.name,' · комната ',r.room_number,' · место ',bed.bed_number) AS name
        FROM dorm_booking.beds bed JOIN dorm_booking.rooms r ON r.id=bed.room_id
        JOIN dorm_booking.buildings b ON b.id=r.building_id
        WHERE bed.organization_id=:org ORDER BY b.name,r.room_number,bed.bed_number""",org=oid)
    }


@app.get('/api/admin/amenity-values')
def admin_amenity_values(target_type:str,target_id:int,
                         db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if target_type not in ASSIGNABLE_AMENITY_TARGETS:
        fail('Неизвестный тип объекта')
    table,col=ASSIGNABLE_AMENITY_TARGETS[target_type]
    if not row(db,f'SELECT id FROM dorm_booking.{table} WHERE id=:id AND organization_id=:o',id=target_id,o=oid):
        fail('Объект не найден',404)
    return rows(db,f"""SELECT av.id,av.amenity_id,av.quantity,a.name,a.code
       FROM dorm_booking.amenity_values av
       JOIN dorm_booking.amenity_definitions a ON a.id=av.amenity_id
       WHERE av.organization_id=:o AND av.{col}=:id AND av.is_present ORDER BY a.name""",
       o=oid,id=target_id)


@app.post('/api/admin/amenity-values')
def admin_assign_amenity(body:AmenityAssignmentIn,
                         db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.target_type not in ASSIGNABLE_AMENITY_TARGETS:
        fail('Неизвестный тип объекта')
    table,col=ASSIGNABLE_AMENITY_TARGETS[body.target_type]
    if not row(db,f'SELECT id FROM dorm_booking.{table} WHERE id=:id AND organization_id=:o',
               id=body.target_id,o=oid):fail('Объект не найден',404)
    if not row(db,"""SELECT id FROM dorm_booking.amenity_definitions WHERE id=:id AND is_active
                     AND (organization_id IS NULL OR organization_id=:o)""",
               id=body.amenity_id,o=oid):fail('Характеристика не найдена',404)
    try:
        existing=row(db,f"""SELECT id FROM dorm_booking.amenity_values
          WHERE organization_id=:o AND amenity_id=:a AND {col}=:id""",
          o=oid,a=body.amenity_id,id=body.target_id)
        if existing:
            db.execute(text("""UPDATE dorm_booking.amenity_values
                  SET is_present=true,quantity=:qty WHERE id=:id"""),
                  {'qty':body.quantity,'id':existing['id']})
            value_id=existing['id']
        else:
            value_id=scalar(db,f"""INSERT INTO dorm_booking.amenity_values
              (organization_id,amenity_id,{col},is_present,quantity)
              VALUES(:o,:a,:id,true,:qty) RETURNING id""",
              o=oid,a=body.amenity_id,id=body.target_id,qty=body.quantity)
        log(db,u,'AMENITY_ASSIGNED','amenity_values',value_id,
            details={'target_type':body.target_type,'target_id':body.target_id})
        db.commit()
        return {'id':value_id}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.delete('/api/admin/amenity-values/{assignment_id}')
def admin_remove_amenity(assignment_id:int,
                         db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    existing=row(db,'SELECT id FROM dorm_booking.amenity_values WHERE id=:id AND organization_id=:o',
                 id=assignment_id,o=oid)
    if not existing:
        fail('Назначение не найдено',404)
    try:
        db.execute(text('UPDATE dorm_booking.amenity_values SET is_present=false WHERE id=:id'),
                   {'id':assignment_id})
        log(db,u,'AMENITY_UNASSIGNED','amenity_values',assignment_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.get('/api/admin/users')
def admin_users(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    return rows(db,"""SELECT id,login,concat_ws(' ',last_name,first_name,middle_name) AS full_name,email,gender,role,education_basis,citizenship_category,
      study_end_date,study_status,status FROM dorm_booking.users WHERE organization_id=:o ORDER BY id DESC""",o=org_admin(u))


def budget_study_end_required(db: Session, organization_id: int) -> bool:
    """Single authoritative organization policy for manual entry and Excel import.

    An organization without configured rules has the default (False) setting;
    availability and publication continue to require configured booking rules.
    """
    rules = rows(db, """SELECT budget_requires_study_end
                    FROM dorm_booking.booking_rules WHERE organization_id=:o""",
                 o=organization_id)
    return bool(rules and rules[0]['budget_requires_study_end'])


class NewStudentIn(BaseModel):
    login: str = Field(min_length=2, max_length=160)
    last_name: str = Field(min_length=1, max_length=150)
    first_name: str = Field(min_length=1, max_length=150)
    middle_name: Optional[str] = None
    password: str = Field(min_length=1)
    gender: str
    citizenship_category: str
    role: str = 'STUDENT'
    education_basis: Optional[str] = None
    study_end_date: Optional[date] = None
    faculty: Optional[str] = None


@app.post('/api/admin/users')
def admin_create_student(body: NewStudentIn, db: Session=Depends(db_session),
                         u: dict=Depends(current_user)):
    """Create a student or another administrator scoped to the same university."""
    oid = org_admin(u)
    if body.role not in ('STUDENT','ORG_ADMIN'):
        fail('В этой форме допускаются роли «Студент» и «Администратор организации».',422)
    if body.gender not in ('MALE','FEMALE') or body.citizenship_category not in ('RUSSIAN','FOREIGN'):
        fail('Неверный пол или гражданство')
    if body.role == 'STUDENT' and body.education_basis not in ('BUDGET','PAID'):
        fail('Неверная основа обучения')
    if (body.role == 'STUDENT' and body.education_basis == 'BUDGET'
            and not body.study_end_date and budget_study_end_required(db, oid)):
        fail('Для бюджетников этой организации дата окончания обучения обязательна: '
             'в настройках включено «Для бюджетников требовать размещение до окончания обучения».', 422)
    try:
        uid = scalar(db, """INSERT INTO dorm_booking.users
           (organization_id,login,last_name,first_name,middle_name,gender,citizenship_category,
            role,password_hash,study_status,education_basis,study_end_date,faculty,status)
           VALUES(:org,:login,:last,:first,:middle,:gender,:cit,:role,:hash,
                  :study_status,:basis,:study_end,:faculty,:status) RETURNING id""",
           org=oid,login=body.login.strip(),last=body.last_name.strip(),first=body.first_name.strip(),
           middle=body.middle_name.strip() if body.middle_name else None,
           gender=body.gender,cit=body.citizenship_category,hash=make_hash(body.password),
           role=body.role,study_status='STUDYING' if body.role=='STUDENT' else None,
           basis=body.education_basis if body.role=='STUDENT' else None,
           study_end=body.study_end_date if body.role=='STUDENT' else None,
           faculty=body.faculty if body.role=='STUDENT' else None,
           status='ACTIVE' if body.role=='ORG_ADMIN' else 'PENDING')
        log(db,u,'ADMIN_CREATED' if body.role=='ORG_ADMIN' else 'STUDENT_CREATED','users',uid)
        db.commit()
        return {'id':uid}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class StudentStatusIn(BaseModel):
    status: str


@app.put('/api/admin/users/{user_id}/status')
def admin_student_status(user_id:int,body:StudentStatusIn,
                         db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.status not in ('ACTIVE','BLOCKED'):
        fail('Допускается только активация или блокировка студента')
    target=row(db,"""SELECT id,role,status FROM dorm_booking.users
        WHERE id=:id AND organization_id=:org""",id=user_id,org=oid)
    if not target or target['role']!='STUDENT':
        fail('Студент не найден',404)
    try:
        db.execute(text('UPDATE dorm_booking.users SET status=:status WHERE id=:id'),
            {'status':body.status,'id':user_id})
        log(db,u,'STUDENT_STATUS_CHANGED','users',user_id,body.status)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


def _excel_date_value(value):
    """Excel numeric serial/date/string -> ISO date; never parse enum fields as dates."""
    from datetime import datetime as _datetime, date as _date
    from openpyxl.utils.datetime import from_excel
    if value is None or str(value).strip() == '':
        return None
    if isinstance(value, _datetime):
        return value.date()
    if isinstance(value, _date):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value != int(value) or value < 1:
            raise ValueError('должна быть календарной датой')
        result = from_excel(value)
        if not isinstance(result, (_datetime, _date)):
            raise ValueError('числовая дата Excel некорректна')
        return result.date() if isinstance(result, _datetime) else result
    try:
        return _date.fromisoformat(str(value).strip()[:10])
    except (ValueError, TypeError) as exc:
        raise ValueError('укажите дату YYYY-MM-DD или ячейку Excel с датой') from exc


def _excel_headers(values, required, translations=None):
    translations = {k.casefold(): v for k,v in (translations or {}).items()}
    names = [translations.get(str(v or '').strip().casefold(), str(v or '').strip().lower()) for v in values]
    if len(names) != len(set(x for x in names if x)) + names.count(''):
        raise ValueError('Повторяются названия столбцов')
    missing = sorted(required.difference(names))
    if missing:
        raise ValueError('Не хватает обязательных столбцов: ' + ', '.join(missing))
    return names


@app.post('/api/admin/users/import')
async def import_users(file: UploadFile=File(...),db:Session=Depends(db_session),u:dict=Depends(current_user)):
    """Atomic student import, strict categories and human-readable column errors."""
    oid=org_admin(u)
    if not file.filename or not file.filename.lower().endswith('.xlsx'):
        fail('Загрузите файл Excel (.xlsx)')
    raw=await file.read()
    if len(raw)>MAX_UPLOAD_BYTES:
        fail('Файл превышает ограничение загрузки')
    from openpyxl import load_workbook
    required={'login','password','last_name','first_name','gender',
              'citizenship_category','education_basis','study_status'}
    try:
        book=load_workbook(io.BytesIO(raw),read_only=True,data_only=True)
        try:
            sheet=book['Студенты'] if 'Студенты' in book.sheetnames else book.active
            it=sheet.values
            headers=_excel_headers(next(it,()),required, STUDENT_IMPORT_HEADERS)
            records=[(n,dict(zip(headers,values))) for n,values in enumerate(it,2)
                     if any(value is not None and str(value).strip() for value in values)]
        finally:
            book.close()
    except ValueError as exc:
        return {'ok':False,'imported':0,'total':0,'total_errors':1,
                'errors':[{'row':1,'error':str(exc)}]}
    except Exception:
        fail('Не удалось прочитать Excel. Проверьте формат XLSX и названия столбцов')
    if len(records)>MAX_IMPORT_ROWS:
        fail(f'Превышен лимит строк: {MAX_IMPORT_ROWS}')
    if not records:
        return {'ok':False,'imported':0,'total':0,'total_errors':1,
                'errors':[{'row':2,'error':'Файл не содержит строк студентов'}]}
    valid={
      'gender': ('MALE','FEMALE'),
      'citizenship_category': ('RUSSIAN','FOREIGN'),
      'education_basis': ('BUDGET','PAID'),
      'study_status': ('STUDYING','GRADUATED','EXPELLED','INACTIVE'),
    }
    labels={
      'gender':IMPORT_GENDER_VALUES,
      'citizenship_category':{'РОССИЯ':'RUSSIAN','РФ':'RUSSIAN','РОССИЙСКОЕ':'RUSSIAN',
                             'ИНОСТРАННОЕ':'FOREIGN','ИНОСТРАННОЕ ГРАЖДАНСТВО':'FOREIGN',
                             'RUSSIAN':'RUSSIAN','FOREIGN':'FOREIGN'},
      'education_basis':{'БЮДЖЕТ':'BUDGET','БЮДЖЕТНАЯ':'BUDGET','ПЛАТНОЕ':'PAID',
                         'ПЛАТНАЯ':'PAID','ПЛАТНАЯ ОСНОВА':'PAID','BUDGET':'BUDGET','PAID':'PAID'},
      'study_status':{'ОБУЧАЕТСЯ':'STUDYING','УЧИТСЯ':'STUDYING',
                      'ЗАВЕРШИЛ ОБУЧЕНИЕ':'GRADUATED','ВЫПУСКНИК':'GRADUATED',
                      'ОТЧИСЛЕН':'EXPELLED','НЕ ОБУЧАЕТСЯ':'INACTIVE',
                      'STUDYING':'STUDYING','GRADUATED':'GRADUATED',
                      'EXPELLED':'EXPELLED','INACTIVE':'INACTIVE'},
    }
    russian_options={'gender':'Мужской, Женский',
      'citizenship_category':'Россия, Иностранное',
      'education_basis':'Бюджет, Платное',
      'study_status':'Обучается, Завершил обучение, Отчислен, Не обучается'}
    errors=[];prepared=[];seen=set()
    require_budget_end=budget_study_end_required(db, oid)
    existing={x['login'] for x in rows(db,'SELECT lower(login) AS login FROM dorm_booking.users WHERE organization_id=:o',o=oid)}
    for number,rec in records:
        values={key: str(v).strip() if v is not None else '' for key,v in rec.items()}
        problems=[]
        for field in ('login','password','last_name','first_name'):
            if not values.get(field):
                problems.append(f'{field}: обязательное поле не заполнено')
        for field, options in valid.items():
            value=_excel_choice(values.get(field,''),labels[field])
            if value not in options:
                problems.append(f'{field}: значение «{values.get(field, "")}» недопустимо; варианты: {russian_options[field]} ({", ".join(options)})')
            values[field]=value
        login=values.get('login','')
        if login and login.casefold() in seen:
            problems.append(f'login: «{login}» повторяется в загруженном файле')
        if login and login.casefold() in existing:
            problems.append(f'login: «{login}» уже существует в организации')
        seen.add(login.casefold())
        try:
            date_end=_excel_date_value(rec.get('study_end_date'))
        except (ValueError,OverflowError) as exc:
            date_end=None
            problems.append(f'study_end_date: {exc}')
        try:
            date_start=_excel_date_value(rec.get('study_start_date'))
        except (ValueError,OverflowError) as exc:
            date_start=None
            problems.append(f'study_start_date: {exc}')
        if date_start and date_end and date_end<date_start:
            problems.append('Дата окончания обучения раньше даты начала (study_end_date / study_start_date)')
        if require_budget_end and values.get('education_basis')=='BUDGET' and date_end is None:
            problems.append('study_end_date: обязательна дата окончания обучения для бюджетников '
                            'в соответствии с правилами бронирования организации')
        if problems:
            errors.append({'row':number,'error':'; '.join(problems)})
            continue
        prepared.append((number,login,values['password'],values['last_name'],values['first_name'],
                         values.get('middle_name') or None,values['gender'],
                         values['citizenship_category'],values['education_basis'],
                         values['study_status'],date_start,date_end))
    if errors:
        return {'ok':False,'errors':errors[:100], 'total_errors':len(errors),
                'total':len(records),'imported':0}
    try:
        for _,login,pwd,last,first,middle,gender,cit,basis,study,date_start,date_end in prepared:
            db.execute(text("""INSERT INTO dorm_booking.users
              (organization_id,login,password_hash,last_name,first_name,middle_name,gender,
               citizenship_category,role,status,education_basis,study_status,study_start_date,study_end_date)
              VALUES (:org,:login,:pwd,:last,:first,:middle,:gender,:cit,'STUDENT','PENDING',
                      :basis,:study,:study_start,:study_end)"""),dict(org=oid,login=login,pwd=make_hash(pwd),
                        last=last,first=first,middle=middle,gender=gender,cit=cit,
                        basis=basis,study=study,study_start=date_start,study_end=date_end))
        log(db,u,'IMPORT_USERS','users',None,'Импортированы студенты',{'rows_total':len(records),'rows_success':len(prepared)})
        db.commit()
        return {'ok':True,'imported':len(prepared),'total':len(records),'errors':[]}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


@app.get('/api/admin/seasons')
def admin_seasons(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    return rows(db, """SELECT s.id,s.code,s.name,s.is_active,
      COALESCE(array_agg(sm.month_number ORDER BY sm.month_number)
        FILTER(WHERE sm.month_number IS NOT NULL),'{}'::smallint[]) AS months
      FROM dorm_booking.tariff_seasons s
      LEFT JOIN dorm_booking.tariff_season_months sm ON sm.season_id=s.id
      WHERE s.organization_id=:org GROUP BY s.id ORDER BY s.name""",org=oid)


class SeasonInput(BaseModel):
    name: str=Field(min_length=1,max_length=150)
    months: list[int]
    is_active: bool=True


def validate_season_months(months:list[int]):
    if not months or len(set(months)) != len(months) or any(m<1 or m>12 for m in months):
        fail('Выберите уникальные месяцы сезона от 1 до 12',422)


@app.post('/api/admin/seasons')
def admin_create_season(body:SeasonInput,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    validate_season_months(body.months)
    try:
        sid=scalar(db, """INSERT INTO dorm_booking.tariff_seasons(organization_id,code,name,is_active)
              VALUES(:org,:code,:name,:active) RETURNING id""",
              org=oid,code='SYS_NEW_'+secrets.token_hex(12).upper(),name=body.name.strip(),active=body.is_active)
        code=f'SYS_SEASON_{sid}'
        if scalar(db,'SELECT 1 FROM dorm_booking.tariff_seasons WHERE organization_id=:org AND code=:code',org=oid,code=code):
            code='SYS_SEASON_'+secrets.token_hex(10).upper()
        db.execute(text('UPDATE dorm_booking.tariff_seasons SET code=:code WHERE id=:id'),{'code':code,'id':sid})
        for month in sorted(body.months):
            db.execute(text("INSERT INTO dorm_booking.tariff_season_months(season_id,organization_id,month_number) VALUES(:id,:org,:month)"),{'id':sid,'org':oid,'month':month})
        log(db,u,'SEASON_CREATED','tariff_seasons',sid)
        db.commit()
        return {'id':sid}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.put('/api/admin/seasons/{season_id}')
def admin_update_season(season_id:int,body:SeasonInput,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    validate_season_months(body.months)
    if not row(db,'SELECT id FROM dorm_booking.tariff_seasons WHERE id=:id AND organization_id=:org FOR UPDATE',id=season_id,org=oid):
        fail('Сезон не найден',404)
    try:
        db.execute(text('UPDATE dorm_booking.tariff_seasons SET name=:name,is_active=:active WHERE id=:id'),
                   {'name':body.name.strip(),'active':body.is_active,'id':season_id})
        db.execute(text('DELETE FROM dorm_booking.tariff_season_months WHERE season_id=:id'),{'id':season_id})
        for month in sorted(body.months):
            db.execute(text('INSERT INTO dorm_booking.tariff_season_months(season_id,organization_id,month_number) VALUES(:id,:org,:month)'),
                      {'id':season_id,'org':oid,'month':month})
        log(db,u,'SEASON_UPDATED','tariff_seasons',season_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.get('/api/admin/tariffs')
def admin_tariffs(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if not scalar(db,"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='dorm_booking' AND table_name='tariffs' AND column_name='tariff_number')"):
        fail('Не выполнена миграция v6.21: отсутствует номер тарифа внутри организации.',503)
    return {'tariffs':rows(db,"""SELECT t.*,bu.name AS building_name,c.name AS category_name,
       t.amount AS monthly_amount,s.name AS season_name FROM dorm_booking.tariffs t
       JOIN dorm_booking.buildings bu ON bu.id=t.building_id
       JOIN dorm_booking.comfort_categories c ON c.id=t.comfort_category_id
       LEFT JOIN dorm_booking.tariff_seasons s ON s.id=t.season_id
       WHERE t.organization_id=:o ORDER BY bu.name ASC,t.amount DESC,t.id ASC""",o=oid),
       'policy':row(db,"SELECT * FROM dorm_booking.booking_fee_policies WHERE organization_id=:o AND occupant_category='ANY' AND citizenship_category='ANY'",o=oid),
       'seasons':rows(db,'SELECT * FROM dorm_booking.tariff_seasons WHERE organization_id=:o',o=oid)}


class TariffStatusIn(BaseModel):
    """ACTIVE is available for new bookings; INACTIVE is the tariff archive."""
    status: str


@app.put('/api/admin/tariffs/{tariff_id}/status')
def admin_tariff_change_status(
    tariff_id: int,
    body: TariffStatusIn,
    db: Session = Depends(db_session),
    u: dict = Depends(current_user),
):
    """Archive/restore only this organization's tariff; retain historical bookings."""
    oid = org_admin(u)
    if body.status not in ('ACTIVE', 'INACTIVE'):
        fail('Допустимые статусы тарифа: ACTIVE и INACTIVE')

    try:
        tariff = row(db, """
            SELECT id, status, building_id, comfort_category_id, room_capacity,
                   occupant_category, citizenship_category, season_id
            FROM dorm_booking.tariffs
            WHERE id = :tariff_id AND organization_id = :org_id
            FOR UPDATE
        """, tariff_id=tariff_id, org_id=oid)

        if tariff is None:
            fail('Тариф не найден в вашей организации', 404)

        if tariff['status'] == body.status:
            return {'ok': True, 'id': tariff_id, 'status': body.status, 'unchanged': True}

        if body.status == 'ACTIVE':
            # Matches the two unique indexes on ACTIVE tariffs, including NULL seasons.
            existing_id = scalar(db, """
                SELECT id
                FROM dorm_booking.tariffs
                WHERE organization_id = :org_id
                  AND building_id = :building_id
                  AND comfort_category_id = :category_id
                  AND room_capacity = :room_capacity
                  AND occupant_category = :occupant_category
                  AND citizenship_category = :citizenship_category
                  AND season_id IS NOT DISTINCT FROM :season_id
                  AND status = 'ACTIVE'
                  AND id <> :tariff_id
                LIMIT 1
            """, org_id=oid, building_id=tariff['building_id'],
                 category_id=tariff['comfort_category_id'],
                 room_capacity=tariff['room_capacity'],
                 occupant_category=tariff['occupant_category'],
                 citizenship_category=tariff['citizenship_category'],
                 season_id=tariff['season_id'], tariff_id=tariff_id)
            if existing_id is not None:
                fail('Нельзя восстановить тариф: уже есть активный тариф с такими же параметрами', 409)

        db.execute(text("""
            UPDATE dorm_booking.tariffs
            SET status = :status
            WHERE id = :tariff_id AND organization_id = :org_id
        """), {'status': body.status, 'tariff_id': tariff_id, 'org_id': oid})

        log(db, u, 'TARIFF_ACTIVATED' if body.status == 'ACTIVE' else 'TARIFF_ARCHIVED',
            'tariffs', tariff_id,
            'Тариф восстановлен из архива' if body.status == 'ACTIVE' else 'Тариф перемещён в архив',
            details={'old_status': tariff['status'], 'new_status': body.status})
        db.commit()
        return {'ok': True, 'id': tariff_id, 'status': body.status}

    except IntegrityError as exc:
        db.rollback()
        constraint = getattr(getattr(exc.orig, 'diag', None), 'constraint_name', None)
        if body.status == 'ACTIVE' and constraint in (
            'uq_tariff_active_year_round', 'uq_tariff_active_season'
        ):
            fail('Нельзя восстановить тариф: уже есть активный тариф с такими же параметрами', 409)
        raise wrap_db_error(exc)
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class TariffBulkStatusIn(BaseModel):
    tariff_ids: list[int]
    status: str


@app.post('/api/admin/tariffs/bulk-status')
def admin_tariffs_bulk_status(
    body: TariffBulkStatusIn,
    db: Session = Depends(db_session),
    u: dict = Depends(current_user),
):
    """Apply archive/restore atomically to explicitly selected tariff IDs."""
    oid = org_admin(u)
    if body.status not in ('ACTIVE', 'INACTIVE'):
        fail('Допустимые статусы тарифа: ACTIVE и INACTIVE')
    if not body.tariff_ids or len(body.tariff_ids) > 5000:
        fail('Выберите тарифы (не более 5000)')

    ids = sorted(set(body.tariff_ids))
    try:
        selected = rows(db, """
            SELECT id, status
            FROM dorm_booking.tariffs
            WHERE organization_id = :org AND id = ANY(:ids)
            ORDER BY id
            FOR UPDATE
        """, org=oid, ids=ids)
        if len(selected) != len(ids):
            fail('Некоторые тарифы не принадлежат вашей организации', 403)

        to_change = [tariff['id'] for tariff in selected if tariff['status'] != body.status]
        if body.status == 'ACTIVE' and to_change:
            # Check conflicts with both already-active rates and other selected
            # archived rates. The unique indexes remain the final race guard.
            conflict = row(db, """
                SELECT a.id AS restoring_id, b.id AS conflicting_id
                FROM dorm_booking.tariffs a
                JOIN dorm_booking.tariffs b
                  ON b.organization_id = a.organization_id
                 AND b.building_id = a.building_id
                 AND b.comfort_category_id = a.comfort_category_id
                 AND b.room_capacity = a.room_capacity
                 AND b.occupant_category = a.occupant_category
                 AND b.citizenship_category = a.citizenship_category
                 AND b.season_id IS NOT DISTINCT FROM a.season_id
                 AND b.id <> a.id
                 AND (b.status = 'ACTIVE' OR b.id = ANY(:ids))
                WHERE a.organization_id = :org
                  AND a.id = ANY(:ids)
                  AND a.status = 'INACTIVE'
                LIMIT 1
            """, org=oid, ids=to_change)
            if conflict:
                fail('Нельзя восстановить выбранные тарифы: существует активный или повторяющийся тариф с такими же параметрами', 409)

        if to_change:
            db.execute(text("""
                UPDATE dorm_booking.tariffs
                SET status = :status
                WHERE organization_id = :org AND id = ANY(:ids)
            """), {'status': body.status, 'org': oid, 'ids': to_change})

        log(db, u,
            'TARIFF_BULK_ACTIVATED' if body.status == 'ACTIVE' else 'TARIFF_BULK_ARCHIVED',
            'tariffs', None,
            'Массовое восстановление тарифов' if body.status == 'ACTIVE' else 'Массовая архивация тарифов',
            details={'ids': to_change, 'status': body.status, 'unchanged': len(ids) - len(to_change)})
        db.commit()
        return {'ok': True, 'count': len(to_change), 'unchanged': len(ids) - len(to_change),
                'status': body.status}

    except HTTPException:
        db.rollback()
        raise
    except IntegrityError as exc:
        db.rollback()
        constraint = getattr(getattr(exc.orig, 'diag', None), 'constraint_name', None)
        if body.status == 'ACTIVE' and constraint in ('uq_tariff_active_year_round', 'uq_tariff_active_season'):
            fail('Восстановление невозможно: среди тарифов уже есть активный дубликат', 409)
        raise wrap_db_error(exc)
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class TariffPriceEdit(BaseModel):
    amount: Decimal = Field(ge=0, decimal_places=2, max_digits=12)
    billing_unit: str = 'MONTH'


@app.put('/api/admin/tariffs/{tariff_id}/price')
def admin_edit_tariff_price(tariff_id: int, body: TariffPriceEdit,
                            db: Session = Depends(db_session), u: dict = Depends(current_user)):
    org_id = org_admin(u)
    if body.billing_unit not in ('MONTH','DAY'):
        fail('Единица оплаты: MONTH (месяц) или DAY (сутки)', 422)
    target = row(db, """SELECT id,status FROM dorm_booking.tariffs
        WHERE id=:id AND organization_id=:org FOR UPDATE""",id=tariff_id,org=org_id)
    if not target:
        fail('Тариф не найден',404)
    if target['status']!='ACTIVE':
        fail('Архивный тариф нельзя редактировать. Сначала восстановите его.',409)
    try:
        db.execute(text("""UPDATE dorm_booking.tariffs SET amount=:amount,billing_unit=:unit
            WHERE id=:id AND organization_id=:org"""),
            {'amount':body.amount,'unit':body.billing_unit,'id':tariff_id,'org':org_id})
        affected=scalar(db, 'SELECT count(*) FROM dorm_booking.bed_tariff_links WHERE tariff_id=:id AND organization_id=:org',id=tariff_id,org=org_id)
        affected_pools=scalar(db, 'SELECT count(*) FROM dorm_booking.quota_pool_tariff_links WHERE tariff_id=:id AND organization_id=:org',id=tariff_id,org=org_id)
        log(db,u,'TARIFF_PRICE_UPDATED','tariffs',tariff_id,details={'affected_beds':affected,'affected_pools':affected_pools})
        db.commit()
        return {'ok':True,'id':tariff_id,'affected_beds':affected,'affected_pools':affected_pools}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class TariffIn(BaseModel):
    billing_unit:str='MONTH'
    citizenship_category:str='ANY'
    building_id:int
    category_id:int
    room_capacity:int=Field(ge=1)
    occupant_category:str='ANY'
    season_id:Optional[int]=None
    monthly_amount:float=Field(ge=0)


@app.post('/api/admin/tariffs')
def create_tariff(body:TariffIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.occupant_category not in ('ANY','BUDGET','PAID','GUEST'):
        fail('Неверная категория жильца')
    if body.citizenship_category not in ('ANY','RUSSIAN','FOREIGN'):
        fail('Неверная категория гражданства')
    if body.billing_unit not in ('MONTH','DAY'):
        fail('Неверная единица тарифа')
    try:
        tid=scalar(db,"""INSERT INTO dorm_booking.tariffs(organization_id,building_id,comfort_category_id,
             room_capacity,occupant_category,citizenship_category,season_id,amount,billing_unit)
             VALUES (:o,:b,:c,:cap,:occupant,:cit,:season,:amount,:unit) RETURNING id""",
             o=oid,b=body.building_id,c=body.category_id,cap=body.room_capacity,
             occupant=body.occupant_category,cit=body.citizenship_category,season=body.season_id,amount=body.monthly_amount,unit=body.billing_unit)
        log(db,u,'TARIFF_CREATED','tariffs',tid)
        db.commit()
        return {'id':tid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


@app.put('/api/admin/tariffs/{tariff_id}')
def edit_tariff(tariff_id:int,body:TariffIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.occupant_category not in ('ANY','BUDGET','PAID','GUEST') or body.citizenship_category not in ('ANY','RUSSIAN','FOREIGN'):
        fail('Некорректная категория проживающего или гражданство',422)
    if body.billing_unit not in ('MONTH','DAY'):
        fail('Укажите корректную единицу тарифа',422)
    current=row(db,"""SELECT * FROM dorm_booking.tariffs WHERE id=:id AND organization_id=:org FOR UPDATE""",id=tariff_id,org=oid)
    if not current:
        fail('Тариф не найден',404)
    if current['status']!='ACTIVE':
        fail('Архивный тариф сначала необходимо восстановить',409)
    structure_changed=(int(current['building_id'])!=body.building_id or
          int(current['comfort_category_id'])!=body.category_id or
          int(current['room_capacity'])!=body.room_capacity)
    attached=int(scalar(db,"SELECT count(*) FROM dorm_booking.bed_tariff_links WHERE tariff_id=:id AND organization_id=:org",id=tariff_id,org=oid) or 0)
    pool_attached=int(scalar(db,"SELECT count(*) FROM dorm_booking.quota_pool_tariff_links WHERE tariff_id=:id AND organization_id=:org",id=tariff_id,org=oid) or 0)
    if structure_changed and (attached or pool_attached):
        fail(f'Тариф назначен объектам размещения: койко-местам — {attached}, пулам — {pool_attached}. Сначала переназначьте его, затем измените общежитие, категорию или вместимость.',409)
    if not scalar(db,"SELECT 1 FROM dorm_booking.buildings WHERE id=:id AND organization_id=:org",id=body.building_id,org=oid):
        fail('Общежитие не относится к организации',422)
    if not scalar(db,"SELECT 1 FROM dorm_booking.comfort_categories WHERE id=:id AND organization_id=:org",id=body.category_id,org=oid):
        fail('Категория комфортности не относится к организации',422)
    if body.season_id is not None and not scalar(db,"SELECT 1 FROM dorm_booking.tariff_seasons WHERE id=:id AND organization_id=:org",id=body.season_id,org=oid):
        fail('Тарифный сезон не относится к организации',422)
    try:
        db.execute(text("""UPDATE dorm_booking.tariffs SET building_id=:building,
            comfort_category_id=:category,room_capacity=:capacity,
            occupant_category=:occupant,citizenship_category=:citizenship,
            season_id=:season,amount=:amount,billing_unit=:unit
            WHERE id=:id AND organization_id=:org"""),
            {'building':body.building_id,'category':body.category_id,'capacity':body.room_capacity,
             'occupant':body.occupant_category,'citizenship':body.citizenship_category,
             'season':body.season_id,'amount':Decimal(str(body.monthly_amount)),
             'unit':body.billing_unit,'id':tariff_id,'org':oid})
        log(db,u,'TARIFF_UPDATED','tariffs',tariff_id,details={'affected_beds':attached})
        db.commit()
        return {'ok':True,'id':tariff_id,'affected_beds':attached}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


class FeeIn(BaseModel):
    occupant_category:str='ANY'
    citizenship_category:str='ANY'
    mode:str
    fixed_amount:Optional[float]=None
    multiplier:Optional[float]=None


@app.put('/api/admin/fee-policy')
def update_fee(body:FeeIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.mode not in ('FIXED','TARIFF_MULTIPLIER','TARIFF_TIERS'):
        fail('Неизвестная модель оплаты')
    if body.multiplier is not None and body.multiplier<0:
        fail('Коэффициент не может быть отрицательным')
    try:
        db.execute(text("""INSERT INTO dorm_booking.booking_fee_policies(organization_id,occupant_category,citizenship_category,mode,fixed_amount,multiplier)
         VALUES(:o,:cat,:cit,:mode,:fixed,:k) ON CONFLICT(organization_id,occupant_category,citizenship_category) DO UPDATE SET
         mode=excluded.mode,fixed_amount=excluded.fixed_amount,multiplier=excluded.multiplier"""),
         dict(o=oid,cat=body.occupant_category,cit=body.citizenship_category,mode=body.mode,fixed=body.fixed_amount,k=body.multiplier))
        log(db,u,'FEE_POLICY_UPDATED','booking_fee_policies',None)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


@app.get('/api/admin/logs')
def org_logs(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    return rows(db,"""SELECT l.*,concat_ws(' ',u.last_name,u.first_name,u.middle_name) AS actor_name FROM dorm_booking.activity_logs l
      LEFT JOIN dorm_booking.users u ON u.id=l.actor_user_id
      WHERE l.organization_id=:o ORDER BY l.created_at DESC LIMIT 100""",o=org_admin(u))


@app.get('/api/platform/organizations')
def platform_orgs(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    return rows(db,"""SELECT o.*, (SELECT count(*) FROM dorm_booking.buildings b WHERE b.organization_id=o.id)::INTEGER AS buildings,
      (SELECT count(*) FROM dorm_booking.users a WHERE a.organization_id=o.id AND a.role='ORG_ADMIN')::INTEGER AS admins,
      (SELECT count(*) FROM dorm_booking.beds p WHERE p.organization_id=o.id AND p.status='ACTIVE')::INTEGER AS published
      FROM dorm_booking.organizations o ORDER BY o.id""")


class OrgIn(BaseModel):
    name: str = Field(min_length=2, max_length=255)
    short_name: str = Field(min_length=1, max_length=255)
    organization_type: str = 'UNIVERSITY'
    city: Optional[str] = Field(default=None, max_length=120)
    inn: Optional[str] = None
    kpp: Optional[str] = None
    ogrn: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None
    website_url: Optional[str] = None


def clean_org_requisites(body):
    """Validate persisted organization requisites, even when entered manually."""
    result = {}
    for field, pattern, max_length in (
        ('inn', r'(?:[0-9]{10}|[0-9]{12})', 12),
        ('kpp', r'[0-9]{9}', 9),
        ('ogrn', r'(?:[0-9]{13}|[0-9]{15})', 15),
        ('contact_phone', None, 32),
        ('contact_email', None, 320),
        ('website_url', None, 2048),
    ):
        v = getattr(body, field, None)
        v = str(v).strip() if v is not None else ''
        if len(v) > max_length or (pattern and v and not re.fullmatch(pattern, v)):
            fail(f'Некорректное значение поля {field}')
        result[field] = v or None
    if result['contact_email'] and not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', result['contact_email']):
        fail('Некорректный контактный email')
    if result['website_url'] and not re.fullmatch(r'https?://[^\s]+', result['website_url'], re.IGNORECASE):
        fail('Сайт должен начинаться с https:// или http://')
    return result


@app.get('/api/organizations/lookup')
def api_organization_lookup(inn: str, u: dict=Depends(current_user)):
    """OfData key never goes to client; both admin roles can request an INN."""
    roles(u, 'PLATFORM_ADMIN', 'ORG_ADMIN')
    if not OFDATA_API_KEY:
        fail('Поиск по ИНН не настроен: заполните OFDATA_API_KEY в backend/.env', 503)
    from ofdata_lookup import lookup_company_by_inn, OfDataFailure
    try:
        return lookup_company_by_inn(inn, OFDATA_API_KEY, OFDATA_TIMEOUT_SECONDS)
    except ValueError as e:
        fail(str(e), 422)
    except OfDataFailure as e:
        fail(str(e), e.status)


def ensure_unique_organization_inn(db: Session, inn: Optional[str], exclude_id: Optional[int] = None):
    """Check INN without an untyped NULL bind parameter.

    PostgreSQL can raise SQLSTATE 42P08 / 42P18 for ``:exclude IS NULL``
    when the driver sends an untyped None. The create and update paths have
    separate statements with explicit bind casts; both are parameterized.
    The unique database index remains the final concurrent-write safeguard.
    """
    cleaned_inn = (inn or '').strip()
    if not cleaned_inn:
        return
    sql = ("SELECT id, short_name FROM dorm_booking.organizations "
           "WHERE btrim(inn) = CAST(:inn AS text)")
    params = {'inn': cleaned_inn}
    if exclude_id is not None:
        sql += " AND id <> CAST(:exclude_id AS bigint)"
        params['exclude_id'] = int(exclude_id)
    match = row(db, sql + " LIMIT 1", **params)
    if match:
        fail(f'Организация с ИНН {cleaned_inn} уже существует: '
             f'{match["short_name"]} (№{match["id"]}).', 409)


@app.post('/api/platform/organizations')
def platform_org_create(body:OrgIn, request: Request,
                        db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    if body.organization_type not in ('UNIVERSITY','APARTMENT_OPERATOR'):
        fail('Неверный тип организации')
    stage = 'сохранение организации'
    try:
        fields = clean_org_requisites(body)
        stage = 'проверка уникальности ИНН'
        ensure_unique_organization_inn(db, fields['inn'])
        stage = 'сохранение организации'
        oid=scalar(db,"""INSERT INTO dorm_booking.organizations
          (name,short_name,organization_type,city,inn,kpp,ogrn,contact_phone,contact_email,website_url)
          VALUES(:name,:short,:type,:city,:inn,:kpp,:ogrn,:contact_phone,:contact_email,:website_url)
          RETURNING id""",name=body.name.strip(),short=body.short_name.strip(),type=body.organization_type,
          city=(body.city or '').strip() or None,**fields)
        stage = 'запись события в журнал действий'
        log(db,u,'ORGANIZATION_CREATED','organizations',oid,body.name,org_id=oid)
        stage = 'подтверждение транзакции'
        db.commit()
        return {'id':oid}
    except SQLAlchemyError as e:
        db.rollback()
        source = getattr(e, 'orig', e)
        diag = getattr(source, 'diag', None)
        request.state.db_error = {
            'stage': stage,
            'sqlstate': getattr(source, 'sqlstate', None) or getattr(source, 'pgcode', None),
            'constraint': getattr(diag, 'constraint_name', None),
            'column': getattr(diag, 'column_name', None),
        }
        raise organization_creation_db_error(e, stage)


@app.put('/api/platform/organizations/{organization_id}')
def platform_org_update(organization_id: int, body: OrgIn,
                        db: Session = Depends(db_session),u:dict=Depends(current_user)):
    roles(u, 'PLATFORM_ADMIN')
    if body.organization_type not in ('UNIVERSITY', 'APARTMENT_OPERATOR'):
        fail('Неверный тип организации')
    if not row(db, 'SELECT id FROM dorm_booking.organizations WHERE id=:id', id=organization_id):
        fail('Организация не найдена', 404)
    fields = clean_org_requisites(body)
    ensure_unique_organization_inn(db, fields['inn'], organization_id)
    try:
        db.execute(text('''UPDATE dorm_booking.organizations SET
            name=:name, short_name=:short, organization_type=:type, city=:city,
            inn=:inn,kpp=:kpp,ogrn=:ogrn,contact_phone=:contact_phone,
            contact_email=:contact_email,website_url=:website_url WHERE id=:id'''),
            dict(id=organization_id,name=body.name.strip(),short=body.short_name.strip(),
                 type=body.organization_type,city=(body.city or '').strip() or None,**fields))
        log(db,u,'ORGANIZATION_UPDATED','organizations',organization_id,
            'Редактирование данных организации')
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


# Table names in these errors are shown as readable object categories, never SQL.
ORG_DEPENDENCY_LABELS = {
    'users':'пользователи', 'buildings':'общежития', 'bookings':'бронирования',
    'beds':'койко-места', 'floors':'этажи', 'rooms':'комнаты',
    'blocks':'блоки', 'tariffs':'тарифы', 'booking_rules':'правила бронирования',
    'booking_fee_policies':'настройки комиссии', 'booking_fee_tiers':'ступени комиссии',
    'guest_accesses':'гостевые разрешения', 'quota_pools':'количественные пулы',
    'quota_slots':'слоты пулов', 'media_files':'загруженные файлы',
    'notifications':'уведомления', 'comfort_categories':'категории комфортности',
    'amenity_definitions':'характеристики', 'amenity_values':'значения характеристик',
    'tariff_seasons':'тарифные сезоны', 'tariff_season_months':'месяцы сезонов',
}


@app.delete('/api/platform/organizations/{organization_id}')
def platform_org_delete(organization_id: int,
                        db:Session=Depends(db_session),u:dict=Depends(current_user)):
    # Delete only an empty organization, preserve audit history, no cascade.
    roles(u, 'PLATFORM_ADMIN')
    current = row(db, '''SELECT id,short_name FROM dorm_booking.organizations
        WHERE id=:id FOR UPDATE''', id=organization_id)
    if not current:
        fail('Организация не найдена', 404)
    dependent_fks = rows(db, '''SELECT n.nspname AS schema_name,c.relname AS table_name,
        a.attname AS column_name FROM pg_constraint fk
        JOIN pg_class c ON c.oid=fk.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_attribute a ON a.attrelid=fk.conrelid AND a.attnum=fk.conkey[1]
        WHERE fk.contype='f' AND fk.confrelid='dorm_booking.organizations'::regclass
          AND array_length(fk.conkey,1)=1 AND array_length(fk.confkey,1)=1
          AND n.nspname='dorm_booking' AND a.attname<>'id' ''')
    blockers=[]
    for fk in dependent_fks:
        if fk['table_name']=='activity_logs':
            continue
        # Identifiers originate from PostgreSQL's own pg_catalog, not user input.
        safe_ident = lambda x: '"' + x.replace('"','""') + '"'
        table_name = safe_ident(fk['schema_name'])+'.'+safe_ident(fk['table_name'])
        column_name = safe_ident(fk['column_name'])
        exists = scalar(db, f'SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {column_name}=:oid)',oid=organization_id)
        if exists:
            blockers.append(ORG_DEPENDENCY_LABELS.get(fk['table_name'], fk['table_name']))
    if blockers:
        readable=', '.join(sorted(set(blockers))[:6])
        fail('Невозможно удалить организацию, пока с ней связаны данные: '+readable+
             '. Сохраните историю: отключите организацию вместо удаления.',409)
    try:
        # activity_logs.organization_id is nullable; preserve original events.
        db.execute(text('UPDATE dorm_booking.activity_logs SET organization_id=NULL WHERE organization_id=:id'),
                   {'id':organization_id})
        db.execute(text('DELETE FROM dorm_booking.organizations WHERE id=:id'),{'id':organization_id})
        log(db,u,'ORGANIZATION_DELETED','organizations',organization_id,
            'Удалена пустая организация '+current['short_name'])
        db.commit()
        return {'ok':True,'id':organization_id}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.get('/api/platform/admins')
def platform_admin_list(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    return rows(db,"""SELECT a.id,a.organization_id,a.login,a.last_name,a.first_name,
           a.middle_name,a.status,o.short_name AS organization_name,o.name AS full_organization_name
           FROM dorm_booking.users a
           JOIN dorm_booking.organizations o ON o.id=a.organization_id
           WHERE a.role='ORG_ADMIN' ORDER BY o.short_name,a.last_name,a.id""")


class PlatformOrganizationStatus(BaseModel):
    status: str
    reason: Optional[str] = None


@app.put('/api/platform/organizations/{organization_id}/status')
def platform_organization_status(organization_id:int, body:PlatformOrganizationStatus,
                                 db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    if body.status not in ('ACTIVE','INACTIVE'):
        fail('Допустимый статус организации: ACTIVE или INACTIVE')
    existing=row(db,'SELECT id,status FROM dorm_booking.organizations WHERE id=:id',id=organization_id)
    if not existing:
        fail('Организация не найдена',404)
    if body.status=='INACTIVE':
        ongoing=scalar(db,"""SELECT EXISTS (
           SELECT 1 FROM dorm_booking.bookings bk
           WHERE bk.organization_id=:id AND bk.status IN ('PENDING_ADMIN','CONFIRMED'))""",id=organization_id)
        if ongoing:
            fail('Сначала завершите или отмените действующие заявки организации',409)
    try:
        db.execute(text('UPDATE dorm_booking.organizations SET status=:s WHERE id=:id'),
                   {'s':body.status,'id':organization_id})
        log(db,u,'ORGANIZATION_STATUS_CHANGED','organizations',organization_id,
            body.reason or body.status,org_id=organization_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.get('/api/platform/amenities')
def platform_amenities(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    return rows(db,"""SELECT id,code,name,amenity_group,is_active FROM dorm_booking.amenity_definitions
        WHERE organization_id IS NULL ORDER BY is_active DESC,amenity_group,name""")


@app.post('/api/platform/amenities')
def platform_create_amenity(body: AmenityIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    if body.amenity_group not in ('FURNITURE','PERSONAL_KIT','HALLWAY','KITCHEN','BATHROOM','INFRASTRUCTURE','OTHER'):
        fail('Неизвестная группа оснащения')
    code='SYS_NEW_'+secrets.token_hex(12).upper()
    if not body.name.strip():
        fail('Укажите название')
    try:
        aid=scalar(db,"""INSERT INTO dorm_booking.amenity_definitions
              (organization_id,code,name,amenity_group)
              VALUES(NULL,:code,:name,:grp) RETURNING id""",
              code=code,name=body.name.strip(),grp=body.amenity_group)
        generated_code=f'SYS_AMENITY_{aid}'
        if scalar(db, '''SELECT 1 FROM dorm_booking.amenity_definitions
                        WHERE organization_id IS NULL AND lower(code)=lower(:code)''',
                  code=generated_code):
            generated_code='SYS_AMENITY_'+secrets.token_hex(12).upper()
        db.execute(text('UPDATE dorm_booking.amenity_definitions SET code=:code WHERE id=:id'),
                   {'code':generated_code, 'id':aid})
        log(db,u,'GLOBAL_AMENITY_CREATED','amenity_definitions',aid,body.name)
        db.commit()
        return {'id':aid}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.put('/api/platform/amenities/{amenity_id}/status')
def platform_amenity_status(amenity_id:int,body:dict,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    active=body.get('is_active')
    if not isinstance(active,bool):
        fail('Укажите is_active: true или false')
    existing=row(db,"""SELECT id FROM dorm_booking.amenity_definitions
        WHERE id=:id AND organization_id IS NULL""",id=amenity_id)
    if not existing:
        fail('Общая характеристика не найдена',404)
    try:
        db.execute(text('UPDATE dorm_booking.amenity_definitions SET is_active=:a WHERE id=:id'),
            {'a':active,'id':amenity_id})
        log(db,u,'GLOBAL_AMENITY_STATUS_CHANGED','amenity_definitions',amenity_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class AdminIn(BaseModel):
    organization_id:int
    login:str
    full_name:str
    password:str=Field(min_length=10, max_length=128)


@app.post('/api/platform/admins')
def platform_admin_create(body:AdminIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    if body.login.strip().casefold()==body.password.casefold():
        fail('Пароль не должен совпадать с логином')
    if not row(db,'SELECT id FROM dorm_booking.organizations WHERE id=:o',o=body.organization_id):
        fail('Организация не найдена')
    try:
        uid=scalar(db,"""INSERT INTO dorm_booking.users(organization_id,login,last_name,first_name,middle_name,password_hash,role,status)
                VALUES(:o,:login,:last,:first,:middle,:hash,'ORG_ADMIN','ACTIVE') RETURNING id""",
              o=body.organization_id,login=body.login,last=split_full_name(body.full_name)[0],first=split_full_name(body.full_name)[1],middle=split_full_name(body.full_name)[2],hash=make_hash(body.password))
        log(db,u,'ORG_ADMIN_CREATED','users',uid,body.full_name,org_id=body.organization_id)
        db.commit()
        return {'id':uid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


@app.get('/api/platform/logs')
def platform_logs(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    roles(u,'PLATFORM_ADMIN')
    return rows(db,"""SELECT l.*,concat_ws(' ',u.last_name,u.first_name,u.middle_name) AS actor_name FROM dorm_booking.activity_logs l
        LEFT JOIN dorm_booking.users u ON u.id=l.actor_user_id ORDER BY l.created_at DESC LIMIT 100""")


class GuestRequest(BaseModel):
    organization_id:int
    last_name:str=Field(min_length=1,max_length=150)
    first_name:str=Field(min_length=1,max_length=150)
    middle_name:Optional[str]=None
    password:str=Field(min_length=8)
    email:str
    gender:str
    citizenship_category:str
    request_source:str='ADMIN'
    inviter_login:Optional[str]=None
    check_in:date
    check_out:date


@app.post('/api/guest/request')
def guest_request(body:GuestRequest,db:Session=Depends(db_session)):
    # Guest sign-up is request-only. Account cannot log in until final admin approval.
    if body.request_source not in ('STUDENT','ADMIN') or body.check_out<=body.check_in or (body.check_out-body.check_in).days>7:
        fail('Гость может запросить размещение на срок до 7 дней')
    if body.gender not in ('MALE','FEMALE') or body.citizenship_category not in ('RUSSIAN','FOREIGN'):
        fail('Пол и гражданство обязательны')
    email = body.email.strip().lower()
    if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', email):
        fail('Некорректный email гостя')
    guest_login = email.split('@', 1)[0]
    if not guest_login or len(guest_login) > 160:
        fail('Некорректный логин, полученный из email')
    if not body.last_name.strip() or not body.first_name.strip():
        fail('Укажите фамилию и имя гостя')
    if row(db, """SELECT id FROM dorm_booking.users
              WHERE organization_id=:o AND lower(login)=lower(:login)""",
           o=body.organization_id,login=guest_login):
        fail('Логин из указанного email уже занят в организации. Укажите другой email.',409)

    org=row(db,'SELECT id FROM dorm_booking.organizations WHERE id=:id AND status=\'ACTIVE\'',id=body.organization_id)
    if not org:
        fail('Организация не найдена',404)
    if body.request_source == 'STUDENT':
        if not (body.inviter_login or '').strip():
            fail('Для приглашения студентом укажите его логин', 422)
        invite=row(db,"""SELECT id,role,organization_id FROM dorm_booking.users
              WHERE lower(login)=lower(:login) AND role='STUDENT' AND status='ACTIVE'
                AND organization_id=:org LIMIT 1""",
              login=body.inviter_login.strip(),org=body.organization_id)
        if not invite:
            fail('Приглашающий студент не найден в выбранной организации',404)
    else:
        # Schema requires an org-admin inviter ID. When a guest chooses the
        # administration rather than a specific person, use a valid admin for
        # routing; the final approver is captured separately in admin_approved_by.
        invite=row(db,"""SELECT id,role,organization_id FROM dorm_booking.users
            WHERE organization_id=:org AND role='ORG_ADMIN' AND status='ACTIVE'
            ORDER BY id LIMIT 1""", org=body.organization_id)
        if not invite:
            fail('В организации пока нет действующего администратора',409)
    try:
        uid=scalar(db,"""INSERT INTO dorm_booking.users(organization_id,login,last_name,first_name,middle_name,email,
          gender,citizenship_category,password_hash,role,status)
          VALUES(:o,:login,:last,:first,:middle,:email,:gender,:cit,:hash,'GUEST','PENDING') RETURNING id""",
          o=body.organization_id,login=guest_login,last=body.last_name.strip(),first=body.first_name.strip(),middle=(body.middle_name or '').strip() or None,email=email,
          gender=body.gender,cit=body.citizenship_category,hash=make_hash(body.password))
        gid=scalar(db,"""INSERT INTO dorm_booking.guest_accesses(host_organization_id,request_source,
             sponsor_user_id,inviter_admin_user_id,guest_user_id,status,starts_at,expires_at)
          VALUES(:org,:source,:sponsor,:admin,:guest,:status,:start,:finish) RETURNING id""",
          org=body.organization_id,source=body.request_source,
          sponsor=invite['id'] if body.request_source=='STUDENT' else None,
          admin=invite['id'] if body.request_source=='ADMIN' else None,guest=uid,
          status='WAITING_STUDENT' if body.request_source=='STUDENT' else 'WAITING_ADMIN',
          start=datetime.combine(body.check_in,datetime.min.time(),ZoneInfo(require_env('DEFAULT_TIMEZONE'))),
          finish=datetime.combine(body.check_out,datetime.min.time(),ZoneInfo(require_env('DEFAULT_TIMEZONE'))))
        # Notify all active organization administrators; a student invitation still
        # needs sponsor approval before the admin may finalize it.
        db.execute(text("""INSERT INTO dorm_booking.notifications
            (recipient_user_id,organization_id,notification_type,title,body,guest_access_id)
            SELECT adm.id,:org,'GUEST_REQUEST','Новый гостевой запрос',:notice,:guest
            FROM dorm_booking.users adm WHERE adm.organization_id=:org
              AND adm.role='ORG_ADMIN' AND adm.status='ACTIVE'"""),
            {'org':body.organization_id, 'guest':gid,
             'notice': 'Ожидает согласования студента' if body.request_source=='STUDENT'
                 else 'Ожидает согласования администратора'})
        log(db,None,'GUEST_REQUEST','guest_accesses',gid, 'Запрос гостевого размещения',org_id=body.organization_id)
        db.commit()
        return {'id':gid,'status':'WAITING_STUDENT' if body.request_source=='STUDENT' else 'WAITING_ADMIN'}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


GUEST_APPROVALS_QUERY = """SELECT g.id,g.host_organization_id,g.request_source,g.sponsor_user_id,
    g.inviter_admin_user_id,g.guest_user_id,g.status,g.starts_at,g.expires_at,
    g.sponsor_confirmed_at,g.admin_approved_by,g.admin_approved_at,
    g.decision_reason,g.decision_at,g.decision_by,g.created_at,
    concat_ws(' ',ug.last_name,ug.first_name,ug.middle_name) AS guest_name,
    ug.login AS guest_login, ug.gender,ug.citizenship_category,
    org.short_name AS organization_name,
    concat_ws(' ',decision_user.last_name,decision_user.first_name,decision_user.middle_name) AS decision_by_name,
    COALESCE((SELECT json_agg(json_build_object(
           'action',hist.action,'reason',hist.reason,'date',hist.created_at,
           'actor_name',concat_ws(' ',actor.last_name,actor.first_name,actor.middle_name),
           'actor_login',actor.login)
           ORDER BY hist.created_at,hist.id)
        FROM dorm_booking.guest_access_decisions hist
        LEFT JOIN dorm_booking.users actor ON actor.id=hist.actor_user_id
        WHERE hist.guest_access_id=g.id),'[]'::json) AS history
    FROM dorm_booking.guest_accesses g
    JOIN dorm_booking.users ug ON ug.id=g.guest_user_id
    JOIN dorm_booking.organizations org ON org.id=g.host_organization_id
    LEFT JOIN dorm_booking.users decision_user ON decision_user.id=g.decision_by
"""

@app.get('/api/guest/approvals')
def guest_approvals(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    if u['role']=='STUDENT':
        return rows(db,GUEST_APPROVALS_QUERY+" WHERE g.sponsor_user_id=:id ORDER BY g.created_at DESC",id=u['id'])
    if u['role']=='ORG_ADMIN':
        return rows(db,GUEST_APPROVALS_QUERY+" WHERE g.host_organization_id=:org ORDER BY g.created_at DESC",org=u['organization_id'])
    fail('Недостаточно прав',403)


class GuestDecision(BaseModel):
    reason: str = Field(default='',max_length=1000)


@app.post('/api/guest/approvals/{gid}/approve')
def approve_guest(gid:int, body:GuestDecision,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    guest=row(db,'SELECT * FROM dorm_booking.guest_accesses WHERE id=:id FOR UPDATE',id=gid)
    if not guest:
        fail('Заявка гостя не найдена',404)
    if u['role']=='STUDENT' and guest['sponsor_user_id']==u['id'] and guest['status']=='WAITING_STUDENT':
        db.execute(text("""UPDATE dorm_booking.guest_accesses
          SET status='WAITING_ADMIN',sponsor_confirmed_at=now(),
              decision_reason=:reason,decision_by=:actor,decision_at=now() WHERE id=:id"""),
              dict(id=gid,reason=body.reason.strip() or None,actor=u['id']))
        db.execute(text("""INSERT INTO dorm_booking.notifications
            (recipient_user_id,organization_id,notification_type,title,body,guest_access_id)
            SELECT adm.id,:org,'GUEST_STUDENT_APPROVED',
                   'Гость согласован студентом','Требуется решение администратора',:guest
            FROM dorm_booking.users adm WHERE adm.organization_id=:org
              AND adm.role='ORG_ADMIN' AND adm.status='ACTIVE'"""),
            {'org':guest['host_organization_id'], 'guest':gid})
    elif u['role']=='ORG_ADMIN' and guest['host_organization_id']==u['organization_id'] and guest['status']=='WAITING_ADMIN':
        db.execute(text("""UPDATE dorm_booking.guest_accesses
           SET status='ACTIVE',admin_approved_by=:actor, admin_approved_at=now(),
               decision_reason=:reason,decision_by=:actor,decision_at=now() WHERE id=:id"""),
           dict(id=gid,actor=u['id'],reason=body.reason.strip() or None))
        db.execute(text("UPDATE dorm_booking.users SET status='ACTIVE' WHERE id=:uid"),dict(uid=guest['guest_user_id']))
    else:
        fail('Заявка недоступна для согласования',403)
    db.execute(text("""INSERT INTO dorm_booking.guest_access_decisions
        (guest_access_id,actor_user_id,action,reason)
        VALUES (:gid,:actor,:action,:reason)"""),
        {'gid':gid,'actor':u['id'],'action':'STUDENT_APPROVED' if u['role']=='STUDENT' else 'ADMIN_APPROVED',
         'reason':body.reason.strip() or None})
    log(db,u,'GUEST_APPROVED','guest_accesses',gid,details={'note':body.reason.strip()})
    db.commit()
    return {'ok':True}


@app.post('/api/guest/approvals/{gid}/reject')
def reject_guest(gid:int,body:GuestDecision,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    reason=body.reason.strip()
    if not reason:
        fail('Укажите причину отклонения приглашения',422)
    guest=row(db,'SELECT * FROM dorm_booking.guest_accesses WHERE id=:id FOR UPDATE',id=gid)
    if not guest:
        fail('Приглашение не найдено',404)
    student_can=u['role']=='STUDENT' and guest['sponsor_user_id']==u['id'] and guest['status']=='WAITING_STUDENT'
    admin_can=u['role']=='ORG_ADMIN' and guest['host_organization_id']==u['organization_id'] and guest['status']=='WAITING_ADMIN'
    if not (student_can or admin_can):
        fail('Приглашение недоступно для отклонения',403)
    db.execute(text("""UPDATE dorm_booking.guest_accesses SET status='REJECTED',
        decision_reason=:reason,decision_by=:actor,decision_at=now() WHERE id=:id"""),
        {'reason':reason,'actor':u['id'],'id':gid})
    db.execute(text("""INSERT INTO dorm_booking.guest_access_decisions
        (guest_access_id,actor_user_id,action,reason)
        VALUES (:gid,:actor,:action,:reason)"""),
        {'gid':gid,'actor':u['id'],'action':'STUDENT_REJECTED' if student_can else 'ADMIN_REJECTED','reason':reason})
    log(db,u,'GUEST_REJECTED','guest_accesses',gid,details={'reason':reason})
    db.commit()
    return {'ok':True}


@app.get('/api/config')
def public_config():
    # EvaMaps search runs in the browser using a domain-bound public project key.
    return {'maps_provider': 'EvaMaps', 'geocoding_provider': 'EvaMaps'}


class QuoteInput(BaseModel):
    mode: str
    offer_id: int
    check_in: date
    check_out: Optional[date] = None  # Only the guest specifies checkout


@app.post('/api/quote')
def quote(body: QuoteInput, db: Session=Depends(db_session), u:dict=Depends(current_user)):
    roles(u,'STUDENT','GUEST')
    finish, _ = booking_period_for_person(db, u, body.check_in, body.check_out)
    category='GUEST' if u['role']=='GUEST' else u.get('education_basis')
    citizenship=u.get('citizenship_category')
    if not category or not citizenship:
        fail('У пользователя не указана категория проживания или гражданство')
    if body.mode=='EXACT_BED':
        offer=row(db,"""SELECT b.organization_id,r.building_id,r.comfort_category_id,r.capacity,
                  b.tariff_id
          FROM dorm_booking.beds b JOIN dorm_booking.rooms r ON r.id=b.room_id
          WHERE b.id=:id AND b.status='ACTIVE'""",id=body.offer_id)
    elif body.mode=='QUOTA':
        offer=row(db,"""SELECT organization_id,building_id,comfort_category_id,room_capacity AS capacity
          FROM dorm_booking.quota_pools WHERE id=:id AND status='ACTIVE'""",id=body.offer_id)
    else:
        fail('Неизвестный режим бронирования')
    if not offer:
        fail('Предложение не найдено',404)
    if body.mode == 'EXACT_BED':
        if not scalar(db, 'SELECT dorm_booking.bed_allowed_for_gender(:bed, :gender)',
                      bed=body.offer_id, gender=u.get('gender')):
            fail('Это койко-место не предназначено для вашего пола', 409)
        ready = scalar(db, """SELECT EXISTS (
           SELECT 1 FROM dorm_booking.beds b
           JOIN dorm_booking.rooms r ON r.id=b.room_id AND r.status='ACTIVE'
           JOIN dorm_booking.floors f ON f.id=r.floor_id AND f.status='ACTIVE'
           JOIN dorm_booking.buildings bu ON bu.id=r.building_id AND bu.status='ACTIVE'
           JOIN dorm_booking.organizations org ON org.id=bu.organization_id
           LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
           WHERE b.id=:bed AND b.status='ACTIVE'
             AND (bl.id IS NULL OR bl.status='ACTIVE')
             AND org.status='ACTIVE' AND org.platform_blocked_at IS NULL
             AND NOT EXISTS(SELECT 1 FROM dorm_booking.bed_periods bp
                 WHERE bp.bed_id=b.id
                 AND (bp.status='ACTIVE' OR EXISTS(SELECT 1 FROM dorm_booking.bookings bk
                     WHERE bk.bed_period_id=bp.id AND bk.status IN ('PENDING_ADMIN','CONFIRMED')))
                 AND daterange(bp.check_in_date,bp.check_out_date,'[)')
                   && daterange(CAST(:start AS date),CAST(:finish AS date),'[)')))
           """,bed=body.offer_id,start=body.check_in,finish=finish)
        if not ready:
            fail('Это место уже забронировано на выбранные даты или сейчас недоступно',409)
    if body.mode == 'QUOTA':
        allowed = scalar(db, """SELECT (q.gender_policy='ANY' OR q.gender_policy=:gender)
             AND (bu.gender_policy IS NULL OR bu.gender_policy='ANY' OR bu.gender_policy=:gender)
             FROM dorm_booking.quota_pools q JOIN dorm_booking.buildings bu ON bu.id=q.building_id
             WHERE q.id=:pool""", pool=body.offer_id, gender=u.get('gender'))
        if not allowed:
            fail('Этот пул не предназначен для вашего пола', 409)
        free_slot = scalar(db, """SELECT EXISTS(
             SELECT 1 FROM dorm_booking.quota_slots sl
             WHERE sl.pool_id=:pool AND sl.status='ACTIVE'
               AND NOT EXISTS(SELECT 1 FROM dorm_booking.quota_slot_periods qp
                 WHERE qp.slot_id=sl.id AND qp.status='ACTIVE'
                 AND daterange(qp.check_in_date,qp.check_out_date,'[)')
                     && daterange(CAST(:start AS date),CAST(:finish AS date),'[)')))
             """, pool=body.offer_id,start=body.check_in,finish=finish)
        if not free_slot:
            fail('На выбранные даты в пуле уже нет свободных мест', 409)
        physical_available = scalar(db, """SELECT q.inventory_mode <> 'PHYSICAL' OR EXISTS (
              SELECT 1 FROM dorm_booking.quota_pool_beds member
              JOIN dorm_booking.beds b ON b.id=member.bed_id
              JOIN dorm_booking.rooms r ON r.id=b.room_id AND r.status='ACTIVE'
              JOIN dorm_booking.floors f ON f.id=r.floor_id AND f.status='ACTIVE'
              LEFT JOIN dorm_booking.blocks bl ON bl.id=r.block_id
              WHERE member.pool_id=q.id AND b.status IN ('DRAFT','INACTIVE')
                AND (bl.id IS NULL OR bl.status='ACTIVE')
                AND dorm_booking.bed_allowed_for_gender(b.id,:gender)
                AND NOT EXISTS(SELECT 1 FROM dorm_booking.bed_periods bp
                    WHERE bp.bed_id=b.id AND bp.status='ACTIVE'
                      AND daterange(bp.check_in_date,bp.check_out_date,'[)')
                        && daterange(CAST(:start AS date),CAST(:finish AS date),'[)')))
             FROM dorm_booking.quota_pools q WHERE q.id=:pool""",
             pool=body.offer_id,gender=u.get('gender'),start=body.check_in,finish=finish)
        if not physical_available:
            fail('На выбранные даты нет свободных физических мест, подходящих вам по полу', 409)
    require_guest_offer(db, u, body.mode, body.offer_id)
    if offer['organization_id']!=u['organization_id']:
        fail('Предложение принадлежит другой организации',403)
    tariff_id=scalar(db,"""SELECT dorm_booking.assigned_tariff_id(
        :mode,:item,:org,:building,:comfort,CAST(:capacity AS smallint),:occupant,:citizenship,:start)
        """,mode=body.mode,item=body.offer_id,org=offer['organization_id'],
        building=offer['building_id'],comfort=offer['comfort_category_id'],
        capacity=offer['capacity'],occupant=category,citizenship=citizenship,
        start=body.check_in)
    if tariff_id is None:
        fail('Для пользователя не настроен подходящий тариф',409)
    tariff=row(db,'SELECT amount,billing_unit FROM dorm_booking.tariffs WHERE id=:id',id=tariff_id)
    days=(finish-body.check_in).days
    stay=(Decimal(tariff['amount'])*Decimal(days)/(Decimal(30) if tariff['billing_unit']=='MONTH' else Decimal(1))).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP)
    fee=row(db,'''SELECT * FROM dorm_booking.calculate_booking_fee_for_booking(
       :org, :stay, :tariff, :cat, :cit, :unit)''',
       org=offer['organization_id'],stay=stay,tariff=tariff['amount'],
       cat=category,cit=citizenship,unit=tariff['billing_unit'])
    building_photo=row(db,"""SELECT bu.name AS building_name,
         (SELECT CASE WHEN mf.relative_path LIKE '/media/%' THEN mf.relative_path
            ELSE '/media/' || ltrim(mf.relative_path,'/') END
            FROM dorm_booking.media_files mf WHERE mf.building_id=bu.id
            ORDER BY mf.is_primary DESC,mf.sort_order,mf.id LIMIT 1) AS photo_url
         FROM dorm_booking.buildings bu WHERE bu.id=:id""",id=offer['building_id']) or {}
    return {'tariff_id':tariff_id, 'building_name':building_photo.get('building_name'),
            'photo_url':building_photo.get('photo_url'),
            'monthly_amount':float(tariff['amount']),'tariff_unit':tariff['billing_unit'],
            'stay_amount':float(stay),'booking_amount':float(fee['result_amount']),
            'mode':fee['mode'],'coefficient':float(fee['coefficient']) if fee['coefficient'] is not None else None}


class NewBlock(BaseModel):
    building_id: int
    floor_id: int
    name: str = Field(min_length=1,max_length=150)
    gender_policy: str='ANY'


@app.post('/api/admin/blocks')
def add_block(body: NewBlock,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    building = row(db, """SELECT b.building_layout FROM dorm_booking.buildings b
          JOIN dorm_booking.floors f ON f.building_id=b.id AND f.organization_id=b.organization_id
          WHERE b.id=:b AND f.id=:f AND b.organization_id=:o""",
          b=body.building_id,f=body.floor_id,o=oid)
    if not building:
        fail('Выбранный этаж не относится к общежитию',404)
    if building['building_layout']=='CORRIDOR':
        fail('В коридорной планировке блоки не предусмотрены',409)
    if body.gender_policy not in ('ANY','MALE','FEMALE'):
        fail('Недопустимое правило размещения по полу',422)
    try:
        # An empty block starts inactive; it can be activated after rooms are created.
        bid=scalar(db,"""INSERT INTO dorm_booking.blocks(organization_id,building_id,floor_id,name,gender_policy,status)
           VALUES(:o,:b,:f,:name,:gender,'INACTIVE') RETURNING id""",
           o=oid,b=body.building_id,f=body.floor_id,name=body.name.strip(),gender=body.gender_policy)
        code = f'SYS_BLOCK_{bid}'
        if scalar(db,'SELECT 1 FROM dorm_booking.blocks WHERE building_id=:building AND code=:code',
                  building=body.building_id,code=code):
            code='SYS_BLOCK_'+secrets.token_hex(10).upper()
        db.execute(text('UPDATE dorm_booking.blocks SET code=:code WHERE id=:id'),{'code':code,'id':bid})
        log(db,u,'BLOCK_CREATED','blocks',bid)
        db.commit()
        return {'id':bid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


class BookingRulesSettings(BaseModel):
    start_min_days: int = Field(ge=0, le=365)
    start_max_days: int = Field(ge=0, le=365)
    paid_max_months: int = Field(ge=1, le=120)
    guest_max_days: int = Field(ge=1, le=365)
    budget_requires_study_end: bool = False


@app.get('/api/admin/booking-rules')
def admin_booking_rules(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    value=row(db, """SELECT start_min_days,start_max_days,paid_max_months,
        guest_max_days,budget_requires_study_end FROM dorm_booking.booking_rules
        WHERE organization_id=:org""",org=oid)
    return {'configured':value is not None,
            **(value or dict(start_min_days=1,start_max_days=7,paid_max_months=12,
                              guest_max_days=7,budget_requires_study_end=False))}


@app.put('/api/admin/booking-rules')
def admin_update_booking_rules(body:BookingRulesSettings,
                               db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.start_max_days<body.start_min_days:
        fail('Последний день начала проживания не может быть раньше первого',422)
    if body.budget_requires_study_end:
        missing = scalar(db, """SELECT count(*) FROM dorm_booking.users
            WHERE organization_id=:org AND role='STUDENT'
              AND education_basis='BUDGET' AND study_end_date IS NULL""", org=oid) or 0
        if missing:
            fail(f'Нельзя включить обязательную дату окончания обучения: у {missing} '
                 f'бюджетных студентов она не заполнена. Сначала заполните эти даты '
                 f'или оставьте настройку выключенной.', 409)
    try:
        db.execute(text("""INSERT INTO dorm_booking.booking_rules
          (organization_id,start_min_days,start_max_days,paid_max_months,
           guest_max_days,budget_requires_study_end)
          VALUES(:org,:min,:max,:months,:days,:study)
          ON CONFLICT (organization_id) DO UPDATE SET
          start_min_days=EXCLUDED.start_min_days,
          start_max_days=EXCLUDED.start_max_days,
          paid_max_months=EXCLUDED.paid_max_months,
          guest_max_days=EXCLUDED.guest_max_days,
          budget_requires_study_end=EXCLUDED.budget_requires_study_end"""),
          {'org':oid,'min':body.start_min_days,'max':body.start_max_days,
           'months':body.paid_max_months,'days':body.guest_max_days,
           'study':body.budget_requires_study_end})
        log(db,u,'BOOKING_RULES_UPDATED','booking_rules',oid)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.get('/api/admin/organization')
def admin_organization(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    return row(db,'SELECT id,short_name,name,organization_type,city,status,description,inn,kpp,ogrn,contact_phone,contact_email,website_url,brand_logo_url,brand_primary_color,brand_accent_color,brand_background_color,brand_font FROM dorm_booking.organizations WHERE id=:id',id=oid)


class OrgUpdate(BaseModel):
    brand_primary_color: str = '#941e2b'
    brand_accent_color: str = '#fbeef0'
    brand_background_color: str = '#f6f7fb'
    brand_font: str = 'Inter'
    short_name: str = Field(min_length=1, max_length=255)
    name: str = Field(min_length=2, max_length=255)
    description: Optional[str]=None
    city: Optional[str] = Field(default=None, max_length=120)
    status: str='ACTIVE'
    inn: Optional[str] = None
    kpp: Optional[str] = None
    ogrn: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None
    website_url: Optional[str] = None


@app.put('/api/admin/organization')
def update_organization(body:OrgUpdate,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    if body.status not in ('ACTIVE','INACTIVE'):
        fail('Допускаются статусы ACTIVE или INACTIVE')
    fields = clean_org_requisites(body)
    ensure_unique_organization_inn(db, fields['inn'], oid)
    if not re.fullmatch(r'#[0-9a-fA-F]{6}', body.brand_primary_color):
        fail('Основной цвет должен быть в формате #RRGGBB', 422)
    if not re.fullmatch(r'#[0-9a-fA-F]{6}', body.brand_accent_color):
        fail('Дополнительный цвет должен быть в формате #RRGGBB', 422)
    if not re.fullmatch(r'#[0-9a-fA-F]{6}', body.brand_background_color):
        fail('Цвет фона должен быть в формате #RRGGBB', 422)
    if body.brand_font not in ('Inter', 'Arial', 'Georgia', 'Verdana', 'Tahoma'):
        fail('Недоступный шрифт', 422)
    try:
        db.execute(text("""UPDATE dorm_booking.organizations SET name=:name,short_name=:short,
                 description=:desc,city=:city,status=:status,inn=:inn,kpp=:kpp,ogrn=:ogrn,
                 contact_phone=:contact_phone,contact_email=:contact_email,website_url=:website_url,
                 brand_primary_color=:primary_color,brand_accent_color=:accent_color,
                 brand_background_color=:background_color,brand_font=:brand_font
                 WHERE id=:id"""),
                 dict(name=body.name.strip(),short=body.short_name.strip(),desc=body.description,
                      city=(body.city or '').strip() or None,status=body.status,id=oid,
                      primary_color=body.brand_primary_color,accent_color=body.brand_accent_color,
                      background_color=body.brand_background_color,brand_font=body.brand_font,**fields))
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)
    log(db,u,'ORGANIZATION_UPDATED','organizations',oid)
    db.commit()
    return {'ok':True}


class FeeTierIn(BaseModel):
    from_amount: float=Field(ge=0)
    to_amount: Optional[float]=None
    multiplier: float=Field(ge=0,le=1)


@app.get('/api/admin/fee-tiers')
def admin_fee_tiers(db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    return rows(db,"""SELECT t.* FROM dorm_booking.booking_fee_tiers t
      JOIN dorm_booking.booking_fee_policies p ON p.id=t.policy_id WHERE p.organization_id=:o AND p.occupant_category='ANY' AND p.citizenship_category='ANY'
      ORDER BY t.from_amount""",o=oid)


@app.post('/api/admin/fee-tiers')
def create_fee_tier(body:FeeTierIn,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    pid=scalar(db,"SELECT id FROM dorm_booking.booking_fee_policies WHERE organization_id=:org AND occupant_category='ANY' AND citizenship_category='ANY'",org=oid)
    if pid is None:
        fail('Сначала создайте политику стоимости')
    try:
        tid=scalar(db,"""INSERT INTO dorm_booking.booking_fee_tiers(policy_id,from_amount,to_amount,multiplier)
          VALUES(:p,:lo,:hi,:k) RETURNING id""",p=pid,lo=body.from_amount,hi=body.to_amount,k=body.multiplier)
        log(db,u,'FEE_TIER_CREATED','booking_fee_tiers',tid)
        db.commit()
        return {'id':tid}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


@app.delete('/api/admin/fee-tiers/{tier_id}')
def delete_fee_tier(tier_id:int,db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    existing=row(db,"""SELECT t.id FROM dorm_booking.booking_fee_tiers t
      JOIN dorm_booking.booking_fee_policies p ON p.id=t.policy_id
      WHERE t.id=:id AND p.organization_id=:org""",id=tier_id,org=oid)
    if not existing:
        fail('Диапазон не найден',404)
    try:
        db.execute(text('DELETE FROM dorm_booking.booking_fee_tiers WHERE id=:id'),{'id':tier_id})
        log(db,u,'FEE_TIER_DELETED','booking_fee_tiers',tier_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as e:
        db.rollback()
        raise wrap_db_error(e)


def media_location(relative_path: str) -> str:
    return relative_path if relative_path.startswith('/media/') else '/media/' + relative_path.lstrip('/')


@app.get('/api/admin/media')
def admin_media_list(target_type: str, target_id: int, db: Session = Depends(db_session),
                     u: dict = Depends(current_user)):
    org_id = org_admin(u)
    targets = {'building':'building_id','room':'room_id','bed':'bed_id','category':'comfort_category_id'}
    column = targets.get(target_type)
    if not column:
        fail('Неверный тип объекта', 422)
    return [{**item, 'url': media_location(item['relative_path'])} for item in rows(db,
        f'''SELECT id,original_filename,relative_path,is_primary,sort_order FROM dorm_booking.media_files
            WHERE organization_id=:org AND {column}=:target ORDER BY is_primary DESC,sort_order,id''',
        org=org_id,target=target_id)]


@app.put('/api/admin/media/{media_id}/primary')
def admin_media_make_primary(media_id: int, db: Session = Depends(db_session),
                             u: dict = Depends(current_user)):
    org_id = org_admin(u)
    item = row(db, '''SELECT id,building_id,room_id,bed_id,comfort_category_id
                      FROM dorm_booking.media_files WHERE id=:id AND organization_id=:org''',
               id=media_id,org=org_id)
    if not item:
        fail('Фотография не найдена',404)
    column = next((field for field in ('building_id','room_id','bed_id','comfort_category_id') if item[field] is not None),None)
    try:
        db.execute(text(f'''UPDATE dorm_booking.media_files SET is_primary=FALSE
            WHERE organization_id=:org AND {column}=:target AND is_primary'''),
            {'org':org_id,'target':item[column]})
        db.execute(text('UPDATE dorm_booking.media_files SET is_primary=TRUE WHERE id=:id'),{'id':media_id})
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.delete('/api/admin/media/{media_id}')
def admin_media_delete(media_id: int, db: Session = Depends(db_session),
                       u: dict = Depends(current_user)):
    org_id = org_admin(u)
    item = row(db, '''SELECT id,relative_path FROM dorm_booking.media_files
                      WHERE id=:id AND organization_id=:org''',id=media_id,org=org_id)
    if not item:
        fail('Фотография не найдена',404)
    try:
        db.execute(text('DELETE FROM dorm_booking.media_files WHERE id=:id'),{'id':media_id})
        log(db,u,'PHOTO_REMOVED','media_files',media_id)
        db.commit()
        relative = item['relative_path']
        if relative.startswith('uploads/'):
            stored = (MEDIA_DIR / relative).resolve()
            if stored.is_relative_to((MEDIA_DIR/'uploads').resolve()):
                stored.unlink(missing_ok=True)
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.post('/api/admin/media')
async def upload_photo(request: Request, file:UploadFile=File(...), db:Session=Depends(db_session),u:dict=Depends(current_user)):
    oid=org_admin(u)
    form=await request.form()
    target_type=str(form.get('target_type') or 'building')
    target_id=int(form.get('target_id') or 0)
    column={'building':'building_id','room':'room_id','bed':'bed_id','category':'comfort_category_id'}.get(target_type)
    table={'building':'buildings','room':'rooms','bed':'beds','category':'comfort_categories'}.get(target_type)
    if not column or not target_id:
        fail('Неверный тип фотографии')
    if not row(db,f'SELECT id FROM dorm_booking.{table} WHERE id=:id AND organization_id=:o',id=target_id,o=oid):
        fail('Объект не найден',404)
    raw=await file.read()
    if len(raw)>MAX_UPLOAD_BYTES:
        fail('Размер фото не должен превышать 6 МБ')
    from PIL import Image,UnidentifiedImageError
    try:
        picture=Image.open(io.BytesIO(raw))
        picture.verify()
        picture=Image.open(io.BytesIO(raw)).convert('RGB')
        picture.thumbnail((2200,1600))
    except (UnidentifiedImageError,ValueError):
        fail('Файл не является изображением')
    uploads=MEDIA_DIR/'uploads'
    uploads.mkdir(exist_ok=True)
    file_name=secrets.token_hex(16)+'.webp'
    path=uploads/file_name
    picture.save(path,'WEBP',quality=84)
    try:
        mid=scalar(db,f"""INSERT INTO dorm_booking.media_files
          (organization_id,{column},original_filename,relative_path,mime_type,size_bytes,uploaded_by)
          VALUES(:o,:target,:original,:path,'image/webp',:size,:actor) RETURNING id""",
          o=oid,target=target_id,original=file.filename or 'photo',path='uploads/'+file_name,
          size=path.stat().st_size,actor=u['id'])
        log(db,u,'PHOTO_UPLOADED','media_files',mid)
        db.commit()
        return {'id':mid,'url':'/media/uploads/'+file_name}
    except SQLAlchemyError as e:
        path.unlink(missing_ok=True)
        db.rollback()
        raise wrap_db_error(e)



@app.post('/api/admin/organization/logo')
async def admin_upload_brand_logo(file: UploadFile = File(...),
                                  db: Session = Depends(db_session),
                                  u: dict = Depends(current_user)):
    org_id = org_admin(u)
    raw = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(raw) > MAX_UPLOAD_BYTES:
        fail('Файл слишком большой', 413)
    from PIL import Image, UnidentifiedImageError
    try:
        image = Image.open(io.BytesIO(raw))
        image.verify()
        image = Image.open(io.BytesIO(raw)).convert('RGBA')
        image.thumbnail((700, 280))
    except (UnidentifiedImageError, ValueError, OSError):
        fail('Загрузите корректное изображение эмблемы', 422)
    uploads = MEDIA_DIR / 'uploads'
    uploads.mkdir(parents=True, exist_ok=True)
    path = uploads / (secrets.token_hex(16) + '.webp')
    image.save(path, 'WEBP', quality=88)
    url = '/media/uploads/' + path.name
    try:
        db.execute(text('''UPDATE dorm_booking.organizations
            SET brand_logo_url=:logo WHERE id=:id'''),
            {'logo': url, 'id': org_id})
        log(db, u, 'ORGANIZATION_BRAND_LOGO_UPDATED', 'organizations', org_id)
        db.commit()
        return {'ok': True, 'brand_logo_url': url}
    except SQLAlchemyError as exc:
        db.rollback()
        path.unlink(missing_ok=True)
        raise wrap_db_error(exc)


class BuildingEdit(BaseModel):
    name: str = Field(min_length=2, max_length=255)
    address: Optional[str] = None
    district: Optional[str] = None
    description: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    building_layout: Optional[str] = None
    building_type: str = 'DORMITORY'
    total_floors: Optional[int] = Field(default=None,ge=1)
    gender_policy: Optional[str] = None


@app.put('/api/admin/buildings/{building_id}')
def admin_update_building(building_id: int, body: BuildingEdit,
                          db: Session = Depends(db_session),
                          u: dict = Depends(current_user)):
    org_id = org_admin(u)
    if body.building_layout not in (None, 'CORRIDOR','BLOCK','APARTMENT','MIXED'):
        existing_layout = scalar(db, 'SELECT building_layout FROM dorm_booking.buildings WHERE id=:id AND organization_id=:org', id=building_id, org=org_id)
        if body.building_layout not in ('HOTEL','SECTION') or body.building_layout != existing_layout:
            fail('Выберите доступную планировку: коридорная, блочная, квартирная или смешанная', 422)
    if body.gender_policy not in (None, 'ANY','MALE','FEMALE'):
        fail('Недопустимое ограничение по полу', 422)
    if body.building_type not in ('DORMITORY','APARTMENT'):
        fail('Недопустимый тип общежития',422)
    if not row(db, 'SELECT id FROM dorm_booking.buildings WHERE id=:id AND organization_id=:org',
               id=building_id, org=org_id):
        fail('Общежитие не найдено', 404)
    if body.building_layout=='CORRIDOR':
        existing_blocks=scalar(db,'''SELECT 1 FROM dorm_booking.blocks
            WHERE building_id=:building AND organization_id=:org LIMIT 1''',
            building=building_id,org=org_id)
        if existing_blocks:
            fail('Нельзя выбрать коридорную планировку, пока у общежития есть блоки',409)
    values = dict(id=building_id, org=org_id,
                  name=body.name.strip(),
                  address=(body.address or '').strip() or None,
                  district=(body.district or '').strip() or None,
                  description=body.description,
                  lat=six_decimal_coordinate(body.latitude, 90),
                  lon=six_decimal_coordinate(body.longitude, 180),
                  layout=body.building_layout,building_type=body.building_type,
                  total_floors=body.total_floors,gender=body.gender_policy)
    try:
        db.execute(text('''UPDATE dorm_booking.buildings SET
            name=:name,address=:address,district=:district,
            description=:description,latitude=:lat,longitude=:lon,
            building_layout=:layout,building_type=:building_type,total_floors=:total_floors,
            gender_policy=:gender
            WHERE id=:id AND organization_id=:org'''), values)
        log(db,u,'BUILDING_UPDATED','buildings',building_id)
        db.commit()
        return {'ok':True}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class GuestBuildingPolicy(BaseModel):
    allow_guests: bool


@app.put('/api/admin/buildings/{building_id}/guest-access')
def admin_building_guest_access(building_id: int, body: GuestBuildingPolicy,
                                db: Session = Depends(db_session), u: dict = Depends(current_user)):
    oid = org_admin(u)
    building = row(db, """SELECT id,allow_guests FROM dorm_booking.buildings
        WHERE id=:id AND organization_id=:org FOR UPDATE""", id=building_id, org=oid)
    if not building:
        fail('Общежитие не найдено', 404)
    if building['allow_guests'] and not body.allow_guests:
        count = scalar(db, """SELECT count(*) FROM dorm_booking.bookings bk
            JOIN dorm_booking.users guest ON guest.id=bk.person_user_id AND guest.role='GUEST'
            JOIN dorm_booking.tariffs t ON t.id=bk.tariff_id
            WHERE t.building_id=:id AND bk.status IN ('PENDING_ADMIN','CONFIRMED')""",id=building_id)
        if count:
            fail('Нельзя запретить гостей: имеются действующие гостевые бронирования', 409)
    try:
        db.execute(text("""UPDATE dorm_booking.buildings SET allow_guests=:enabled
            WHERE id=:id AND organization_id=:org"""),
            {'enabled':body.allow_guests,'id':building_id,'org':oid})
        log(db,u,'BUILDING_GUEST_POLICY_UPDATED','buildings',building_id,
            'Размещение гостей разрешено' if body.allow_guests else 'Размещение гостей запрещено')
        db.commit()
        return {'ok':True,'allow_guests':body.allow_guests}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


class BuildingStatusIn(BaseModel):
    status: str


@app.put('/api/admin/buildings/{building_id}/status')
def admin_building_status(building_id: int, body: BuildingStatusIn,
                          db: Session = Depends(db_session), u: dict = Depends(current_user)):
    """Only this organization's admin may activate/deactivate a building.

    Existing applications block deactivation to avoid hiding a committed placement.
    """
    oid = org_admin(u)
    if body.status not in ('ACTIVE', 'INACTIVE'):
        fail('Допустимые статусы: ACTIVE и INACTIVE')
    building = row(db, 'SELECT id,status FROM dorm_booking.buildings '
                       'WHERE id=:id AND organization_id=:o', id=building_id, o=oid)
    if not building:
        fail('Здание не найдено', 404)
    if body.status == 'INACTIVE':
        pending = scalar(db, '''SELECT count(*) FROM dorm_booking.bookings bk
            JOIN dorm_booking.tariffs t ON t.id=bk.tariff_id
            WHERE t.building_id=:building AND bk.status IN ('PENDING_ADMIN','CONFIRMED')''',
            building=building_id)
        if pending:
            fail('Нельзя отключить здание с действующими или ожидающими заявками', 409)
    try:
        db.execute(text('UPDATE dorm_booking.buildings SET status=:status WHERE id=:id'),
                   {'status': body.status, 'id': building_id})
        log(db, u, 'BUILDING_STATUS_UPDATED', 'buildings', building_id,
            'Изменён статус здания', {'status': body.status})
        db.commit()
        return {'ok': True, 'status': body.status}
    except SQLAlchemyError as exc:
        db.rollback()
        raise wrap_db_error(exc)


@app.get('/api/guest/accesses/mine')
def guest_accesses_mine(db: Session = Depends(db_session), u: dict = Depends(current_user)):
    roles(u, 'GUEST')
    accesses = rows(db, """SELECT id, host_organization_id, status, starts_at, expires_at,
               (starts_at AT TIME ZONE :tz)::date AS check_in,
               (expires_at AT TIME ZONE :tz)::date AS check_out,
               (admin_approved_at IS NOT NULL) AS approved
        FROM dorm_booking.guest_accesses WHERE guest_user_id=:uid
        ORDER BY starts_at,id""", uid=u['id'], tz=require_env('DEFAULT_TIMEZONE'))
    rules = row(db, """SELECT start_min_days,guest_max_days FROM dorm_booking.booking_rules
        WHERE organization_id=:org""", org=u['organization_id']) or {}
    earliest = clock_date(db) + timedelta(days=int(rules.get('start_min_days', 1)))
    max_days = int(rules.get('guest_max_days') or 7)
    for access in accesses:
        start = max(access['check_in'], earliest)
        finish = min(access['check_out'], start + timedelta(days=max_days))
        if access['status'] == 'ACTIVE' and access['approved'] and finish > start:
            access['bookable_check_in'] = start.isoformat()
            access['bookable_check_out'] = finish.isoformat()
        else:
            access['bookable_check_in'] = None
            access['bookable_check_out'] = None
    return accesses


# SPA fallback MUST come after every /api route.
@app.get('/', include_in_schema=False)
@app.get('/{page:path}', include_in_schema=False)
def spa(page: str = ''):
    if page.startswith(('api/', 'media/', 'assets/')):
        fail('Ресурс не найден', 404)
    entry = FRONTEND_DIST / 'index.html'
    if not entry.is_file():
        return {'message': 'Frontend React не собран. Запустите npm install и npm run dev в frontend/ (порт 5173).',
                'api_docs': '/docs'}
    return FileResponse(entry)


