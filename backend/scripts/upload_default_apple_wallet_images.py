import asyncio
import os
from pathlib import Path

from common.s3_service.impl.S3Client import S3Client
from common.s3_service.models.S3SettingsModel import S3SettingsModel


class DefaultImagesUploader:
    def __init__(self, bucket_name: str = '5075293c-docs_generated', s3_folder: str = 'photos', static_files_dir: str = '/backend/static_files'):
        self.bucket_name = bucket_name
        self.s3_folder = s3_folder
        self.static_files_dir = Path(static_files_dir)
        self.s3_client = S3Client(S3SettingsModel(
            aws_access_key_id=os.getenv("S3_ACCESS"),
            aws_secret_access_key=os.getenv("S3_SECRET"),
            endpoint_url=os.getenv("S3_URL")
        ))

    async def upload_all(self):
        for file_path in self.static_files_dir.iterdir():
            if file_path.is_file():
                s3_key = f"{self.s3_folder}/{file_path.name}"
                with open(file_path, 'rb') as f:
                    file_bytes = f.read()
                await self.s3_client.upload_file_object(self.bucket_name, s3_key, file_bytes)
