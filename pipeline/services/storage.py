"""Interfaces and concrete classes for reading and writing to storage.
"""

import google.cloud as gcloud
import os
import pandas as pd
from abc import ABC, abstractmethod
from io import StringIO
from pipeline.constants import LOCAL_STORAGE_DIR_PATH
from typing import Union


class IDataReader(ABC):
    """Duck-typed abstract class serving as an
    interface for classes reading data from storage.
    """

    @abstractmethod
    def get_filepath_or_buffer(
        self,
        storage_dir_name: str,
        file_name: str) -> Union[str, StringIO]:
        """Returns a file path or a text stream/buffer that can be
        read immediately into a Pandas DataFrame.

        Args:
            storage_dir_name (`str`): The name of the file's 
                parent directory or bucket.

            file_name (`str`): The file name.
        
        Returns:
            (`str`) or (`io.StringIO`): The file path or text stream.
        """
        raise NotImplementedError

class LocalStorageReader(IDataReader):
    """Class for reading local files. Implements `IDataReader` interface.
    """
    
    def get_filepath_or_buffer(
        self, 
        storage_dir_name: str, 
        file_name: str) -> str:
        """Generates a local path for the given file.

        Args:
            storage_dir_name (`str`): The path for the file's
                immediate parent directory.

            file_name (`str`): The file name (e.g., "project.csv")

        Returns:
            (`str`): The file path.
        """
        filepath = f"{LOCAL_STORAGE_DIR_PATH}/{storage_dir_name}/{file_name}"
        return filepath if os.path.exists(filepath) else None

class CloudStorageReader(IDataReader):
    """Class for downloading blobs from Google Cloud Storage.
    Implements `IDataReader` interface.
    """

    def __init__(self, cloud_project_name:str) -> None:
        """Initializes a new instance of a `CloudStorageReader`.

        Args:
            cloud_project_name (`str`): The Google Cloud 
                project holding storage data.

        Returns:
            `None`
        """
        self.storage_client = gcloud.storage.Client(project=cloud_project_name)

    def get_filepath_or_buffer(
        self,
        storage_dir_name:str,
        file_name:str) -> StringIO:
        """Downloads the blob (file) with the given name, located 
        in the given Google Cloud Storage bucket. Then writes the 
        downloaded blob bytes to an in-memory text stream/buffer.

        Args:
            storage_dir_name (`str`): The name of storage bucket.

            file_name (`str`): The name of the blob to download.

        Returns:
            (`io.StringIO`): The downloaded blob (i.e., file).
        """
        try:
            # Download requested file from Cloud Storage bucket
            bucket = self.storage_client.get_bucket(storage_dir_name)
            blob = bucket.blob(file_name)
            bytes_file = blob.download_as_bytes(timeout=(3, 60))

            # Parse downloaded bytes into in-memory text stream 
            s = str(bytes_file, encoding='utf-8')
            return StringIO(s)
        except gcloud.exceptions.NotFound:
            return None

class IDataWriter(ABC):
    """Duck-typed abstract class serving as an interface for
    classes writing files to data storage.
    """

    @abstractmethod
    def write_records(
        self,
        storage_location: str,
        records: pd.DataFrame) -> None:
        """Writes records in a Pandas DataFrame to storage.

        Args:
            storage_location (`str`): The name of, or 
                path to, the storage location.

            records (`pd.DataFrame`): The records.
        
        Returns:
            `None`
        """
        raise NotImplementedError

class LocalStorageWriter(IDataWriter):
    """Class for writing to local file paths.
    Implements `IDataWriter` interface.
    """

    def write_records(
        self,
        storage_location: str,
        records: pd.DataFrame) -> None:
        """Writes records from a Pandas DataFrame to storage.

        Args:
            storage_location (`str`): The file path where 
                the output file is written.

            file_name (`str`): The name of the file to 
                write (e.g., "project.csv").

            records (`pd.DataFrame`): The records.
        
        Returns:
            `None`
        """
        segments = storage_location.split('/')
        if not segments:
            raise ValueError("Unable to write records. "
                             "Must provide a valid file path.")
        file_name = segments[-1]
        parent_dir = f"{LOCAL_STORAGE_DIR_PATH}/{'/'.join(segments[:-1])}"
        os.makedirs(parent_dir, exist_ok=True)
        records.to_csv(f"{parent_dir}/{file_name}", index=False)

class CloudStorageWriter(IDataWriter):
    """Class for writing to cloud storage. Implements `IDataWriter` interface.
    """

    def __init__(self, cloud_project_name: str) -> None:
        """Initializes a new instance of a `CloudStorageWriter`.

        Args:
            cloud_project_name (`str`): The Google Cloud project
                holding storage data.

        Returns:
            `None`
        """
        self.storage_client = gcloud.storage.Client(project=cloud_project_name)

    def write_records(
        self,
        storage_location:str,
        records:pd.DataFrame) -> None:
        """Uploads a file to Google Cloud Storage.

        Args:
            storage_location (`str`): The bucket to hold the file.

            file_name (`str`): The name of the blob (file) 
                to write (e.g., "project.csv").

            records (`pd.DataFrame`): The records.

        Returns:
            `None`
        """
        # Parse storage location for bucket and blob
        segments = storage_location.split('/')
        bucket_name = segments[0]
        blob_name = '/'.join(segments[1:])

        # Retrieve storage resources
        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Write records
        csv_str = records.to_csv(header=True, encoding='utf-8', index=False)

        # Upload data
        blob.upload_from_string(csv_str, content_type='text/csv')
