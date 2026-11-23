"""S3-compatible Timeweb storage. Backend-only credentials; no public bucket required."""
from __future__ import annotations

import io
import os
import re
from functools import lru_cache

S3_ROUTE = '/api/media/s3/'


def configured_mode() -> str:
    mode = os.getenv('MEDIA_STORAGE', 'local').strip().lower()
    if mode not in ('local', 's3'):
        raise ValueError('MEDIA_STORAGE должен быть local или s3')
    return mode


def object_url(key: str) -> str:
    if not re.fullmatch(r'organizations/[0-9]+/(?:photos|logos)/[0-9a-f]{32}\.webp', key):
        raise ValueError('Некорректный ключ S3 фотографии')
    return S3_ROUTE + key


def object_key(url: str) -> str | None:
    if not url or not url.startswith(S3_ROUTE):
        return None
    try:
        key = url[len(S3_ROUTE):]
        return key if object_url(key) == url else None
    except ValueError:
        return None


@lru_cache(maxsize=1)
def client():
    # Lazy import so local development and import-only tests do not contact S3.
    try:
        import boto3
        from botocore.config import Config
    except (AttributeError, ImportError) as exc:
        # Common in system Anaconda: old pyOpenSSL and newer cryptography. This
        # fails before any network connection to Timeweb and is not an SSL error
        # from the S3 server.
        raise RuntimeError(
            'Библиотеки S3 несовместимы в текущем Python-окружении '
            '(boto3/botocore/pyOpenSSL/cryptography). Используйте отдельное venv '
            'и установите backend/requirements.txt заново.'
        ) from exc
    access = os.getenv('S3_ACCESS_KEY_ID', '').strip()
    secret = os.getenv('S3_SECRET_ACCESS_KEY', '').strip()
    bucket = os.getenv('S3_BUCKET', '').strip()
    if not (access and secret and bucket):
        raise RuntimeError('Заполните S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY и S3_BUCKET в backend/.env')
    return boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT','https://s3.twcstorage.ru').strip(),
        aws_access_key_id=access, aws_secret_access_key=secret,
        region_name=os.getenv('S3_REGION', 'ru-1').strip(),
        config=Config(signature_version='s3v4', s3={'addressing_style':'path'},
                      connect_timeout=7, read_timeout=20, retries={'max_attempts':2}))


def bucket_name() -> str:
    name = os.getenv('S3_BUCKET', '').strip()
    if not name:
        raise RuntimeError('Не задан S3_BUCKET')
    return name


def put(key: str, data: bytes):
    object_url(key)
    client().put_object(Bucket=bucket_name(), Key=key, Body=data,
                        ContentType='image/webp', CacheControl='public, max-age=86400')
    info = client().head_object(Bucket=bucket_name(), Key=key)
    if int(info.get('ContentLength',-1)) != len(data):
        raise RuntimeError('S3 не подтвердил размер сохранённого файла')


def read(key: str) -> bytes:
    object_url(key)
    return client().get_object(Bucket=bucket_name(), Key=key)['Body'].read()


def delete(key: str) -> None:
    object_url(key)
    client().delete_object(Bucket=bucket_name(), Key=key)
