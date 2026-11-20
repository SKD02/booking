"""Minimal OfData adapter: fixed HTTPS endpoints, no API key in browser or URL.

Docs: https://ofdata.ru/api/company and https://ofdata.ru/api/entrepreneur
"""
from __future__ import annotations

import json
import re
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


class OfDataFailure(Exception):
    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


def extract_city(data: dict) -> str:
    """Extract locality from OfData legal address, never guess from region only."""
    address = data.get('ЮрАдрес') or data.get('Адрес') or {}
    if not isinstance(address, dict):
        address = {'АдресРФ': str(address)}
    # Structured locality has priority; may be missing in older OfData responses.
    for source in (address, data):
        for key in ('Город', 'г', 'НасПункт', 'НаселенныйПункт', 'НаселённыйПункт', 'city'):
            value = source.get(key)
            if isinstance(value, dict):
                value = value.get('Наим') or value.get('Название') or value.get('name')
            if isinstance(value, str) and value.strip():
                cleaned = re.sub(r'^(?:г\.?|город|пгт\.?|пос\.?)\s+', '', value.strip(), flags=re.I)
                return cleaned[:120]
    text = str(address.get('АдресРФ') or address.get('Текст') or '')
    for pattern in (r'(?<![\w])г\.?\s+([^,;]+)', r'(?<![\w])город\s+([^,;]+)'):
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()[:120]
    return ''


def extract_city(data: dict) -> str:
    """Extract locality from OfData legal address, never guess from region only."""
    address = data.get('ЮрАдрес') or data.get('Адрес') or {}
    if not isinstance(address, dict):
        address = {'АдресРФ': str(address)}
    # Structured locality has priority; may be missing in older OfData responses.
    for source in (address, data):
        for key in ('Город', 'г', 'НасПункт', 'НаселенныйПункт', 'НаселённыйПункт', 'city'):
            value = source.get(key)
            if isinstance(value, dict):
                value = value.get('Наим') or value.get('Название') or value.get('name')
            if isinstance(value, str) and value.strip():
                cleaned = re.sub(r'^(?:г\.?|город|пгт\.?|пос\.?)\s+', '', value.strip(), flags=re.I)
                return cleaned[:120]
    text = str(address.get('АдресРФ') or address.get('Текст') or '')
    for pattern in (r'(?<![\w])г\.?\s+([^,;]+)', r'(?<![\w])город\s+([^,;]+)'):
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()[:120]
    return ''


def lookup_company_by_inn(inn: str, api_key: str, timeout: float = 8.0) -> dict:
    inn = (inn or '').strip()
    if not re.fullmatch(r'(?:[0-9]{10}|[0-9]{12})', inn):
        raise ValueError('ИНН должен содержать 10 цифр для юридического лица или 12 цифр для ИП')
    if not api_key:
        raise OfDataFailure('Ключ OfData не настроен', 503)
    kind = 'company' if len(inn) == 10 else 'entrepreneur'
    endpoint = 'https://api.ofdata.ru/v2/' + kind
    body = json.dumps({'key': api_key, 'inn': inn}).encode('utf-8')
    req = Request(endpoint, data=body, headers={
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json', 'User-Agent': 'DormBooking/5.0'}, method='POST')
    try:
        with urlopen(req, timeout=timeout) as response:
            raw = response.read(2_000_001)
    except HTTPError as e:
        if e.code in (401, 403):
            raise OfDataFailure('OfData отклонил API-ключ или доступ запрещён', 502) from None
        if e.code == 429:
            raise OfDataFailure('Превышен лимит запросов OfData', 429) from None
        raise OfDataFailure('Ошибка запроса к OfData: HTTP ' + str(e.code)) from None
    except (URLError, TimeoutError, OSError) as e:
        raise OfDataFailure('Сервис OfData недоступен. Повторите попытку позже') from None
    if len(raw) > 2_000_000:
        raise OfDataFailure('Слишком большой ответ от OfData')
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise OfDataFailure('OfData вернул некорректный JSON') from None
    if not isinstance(payload, dict):
        raise OfDataFailure('OfData вернул неожиданный формат ответа')
    meta = payload.get('meta') or {}
    if not isinstance(meta, dict) or meta.get('status') != 'ok':
        # No untrusted API text exposed (may contain traces, errors, or secrets).
        raise OfDataFailure('OfData не нашёл организацию либо отклонил запрос; проверьте ИНН и API-ключ', 404)
    data = payload.get('data')
    if not isinstance(data, dict):
        raise OfDataFailure('В ответе OfData нет данных организации', 404)
    returned_inn = str(data.get('ИНН') or '')
    if returned_inn != inn:
        raise OfDataFailure('Не найдено данных по указанному ИНН', 404)
    if kind == 'company':
        name = data.get('НаимПолн') or data.get('НаимСокр') or ''
        short = data.get('НаимСокр') or name
        ogrn = data.get('ОГРН') or ''
        kpp = data.get('КПП') or ''
    else:
        fio = data.get('ФИО') or ''
        name = ('Индивидуальный предприниматель ' + fio).strip() if fio else ''
        short = ('ИП ' + fio).strip() if fio else ''
        ogrn = data.get('ОГРНИП') or ''
        kpp = ''
    contacts = data.get('Контакты') or {}
    if not isinstance(contacts, dict):
        contacts = {}
    phone = contacts.get('Тел') or []
    email = contacts.get('Емэйл') or []
    phone = phone[0] if isinstance(phone, list) and phone else (phone if isinstance(phone, str) else '')
    email = email[0] if isinstance(email, list) and email else (email if isinstance(email, str) else '')
    website = str(contacts.get('ВебСайт') or '').strip()
    if website and not re.match(r'^https?://', website, re.I):
        website = 'https://' + website
    status = data.get('Статус') or {}
    if isinstance(status, dict):
        status = status.get('Наим') or ''
    legal_address = data.get('ЮрАдрес') or {}
    if isinstance(legal_address, dict):
        legal_address = legal_address.get('АдресРФ') or ''
    return {
        'inn': inn, 'city': extract_city(data),
        'name': str(name)[:255], 'short_name': str(short)[:255],
        'kpp': str(kpp)[:9], 'ogrn': str(ogrn)[:15],
        'contact_phone': str(phone)[:32], 'contact_email': str(email)[:320],
        'website_url': website[:2048],
        'registry_status': str(status)[:150], 'legal_address': str(legal_address)[:400],
        'entity_kind': kind,
        'source': 'OfData',
    }
