#!/usr/bin/env python3
"""Этап 2. Загрузка CSV с вакансиями в бакет hh-lake в RustFS.

Запуск:  python upload_to_s3.py путь/к/it_vacancies_2024.csv
"""
import os
import sys
import time

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

import config


def main(path):
    size = os.path.getsize(path)
    s3 = boto3.client(
        "s3",
        endpoint_url=config.ENDPOINT,
        aws_access_key_id=config.ACCESS_KEY,
        aws_secret_access_key=config.SECRET_KEY,
        region_name=config.REGION,
        config=Config(s3={"addressing_style": "path"}),
    )

    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    print(f"Бакеты в хранилище: {buckets}")
    if config.BUCKET not in buckets:
        sys.exit(f"Бакета {config.BUCKET} нет. Создай его в консоли http://localhost:9001")

    print(f"Загружаю {path} ({size / 1024 / 1024:.1f} МБ) -> s3://{config.BUCKET}/{config.CSV_KEY}")
    t0 = time.time()
    s3.upload_file(
        path,
        config.BUCKET,
        config.CSV_KEY,
        Config=TransferConfig(multipart_threshold=64 * 1024 * 1024,
                              multipart_chunksize=64 * 1024 * 1024,
                              max_concurrency=4),
    )
    dt = time.time() - t0

    head = s3.head_object(Bucket=config.BUCKET, Key=config.CSV_KEY)
    remote = head["ContentLength"]
    print(f"Загружено за {dt:.1f} с. Размер в хранилище: {remote} байт, локальный файл: {size} байт")
    print("РАЗМЕРЫ СОВПАДАЮТ" if remote == size else "ВНИМАНИЕ: РАЗМЕРЫ РАЗНЫЕ!")

    print("\nСодержимое бакета:")
    for obj in s3.list_objects_v2(Bucket=config.BUCKET).get("Contents", []):
        print(f"  {obj['Key']}  {obj['Size']} байт")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Использование: python upload_to_s3.py путь/к/it_vacancies_2024.csv")
    main(sys.argv[1])
