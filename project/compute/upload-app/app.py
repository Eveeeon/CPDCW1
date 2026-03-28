import boto3
import time
import logging
import pathlib
from pathlib import Path
from botocore.exceptions import ClientError

# Helpers
def check_file_in_s3(s3_client: boto3.client, bucket: str, name_key: str) -> bool:
    """
    Check if a file exists in the bucket to handle duplicate cases

    Args:
        s3_client (boto3.client): a boto3 s3 client instance
        bucket (str): name of the bucket
        name_key (str): name of the image, used as the key

    Returns:
        boolean: if a file exists in s3 with the same name key
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=name_key)
        return True
    # --- catch boto clienterror
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        # --- if error code is 404 not found, then there is no image with the same name
        if error_code == "404":
            return False
        else:
            # --- raise the exception, likely boto error
            raise


def upload_file(s3_client: boto3.client, bucket: str, file_path: pathlib.Path):
    """
    Upload a file to S3 from the local drive

    Args:
        s3_client (boto3.client): a boto3 s3 client instance
        bucket (str): name of the bucket
        file_path (pathlib.Path): file path of the file, including the file name
    """
    try:
        file_name = file_path.name
        s3_client.upload_file(str(file_path), bucket, file_name)
    except Exception as e:
        # --- raise the exception, likely boto error
        raise


def get_next_file(
    directory: str, type_ext: set[str], max_size_mb: int, min_size_mb: int
) -> pathlib.Path:
    """
    Gets the next file to be uploaded, takes the oldest file in the directory matching any of the given file extensions

    Args:
        directory (str): the directory containing the files
        type_ext (set[str]): set of file extensions e.g. {".png", ".jpg"}
        max_size_mb (int): max valid file size in mb
        min_size_mb (int): min valid file size in mb

    Returns:
        pathlib.Path: the file path of the next file
    """
    file_dir = Path(directory)
    # --- get generator of valid files
    valid_files = (
        file
        for file in file_dir.iterdir()
        if file.is_file() and validate_file(file, type_ext, max_size_mb, min_size_mb)
    )
    oldest_file = min(valid_files, key=lambda file: file.stat().st_mtime, default=None)
    return oldest_file


def validate_file(
    file_path: pathlib.Path,
    type_ext: set[str],
    max_size_mb: int,
    min_size_mb: int,
) -> bool:
    """
    Filter function to determine if the file a file is valid

    Args:
        file_path (pathlib.Path): path of file
        type_ext (set[str]): set of valid types
        max_size_mb (int): max valid file size in mb
        min_size_mb (int): min valid file size in mb

    Returns:
        bool: if the file is valid
    """

    if file_path.suffix.lower() not in type_ext:
        return False

    size_mb = file_path.stat().st_size / (1024 * 1024)

    if size_mb > max_size_mb or size_mb < min_size_mb:
        return False

    return True


def process_file(
    s3_client: boto3.client,
    bucket: str,
    directory: str,
    type_ext: set[str],
    logger: logging.Logger,
    max_size_mb: int,
    min_size_mb: int,    
):
    """
    Gets the next file to be uploaded, performs checks, uploads, and deletes the file

    Args:
        s3_client (boto3.client): a boto3 s3 client instance
        bucket (str): name of the bucket
        directory (str): directory of the files to be uploaded
        type_ext (set[str]): set of file extensions e.g. {".png", ".jpg"}
        logger (logging.Logger): logger
        max_size_mb (int): max valid file size in mb
        min_size_mb (int): min valid file size in mb
    """
    next_file = get_next_file(directory, type_ext, max_size_mb, min_size_mb)
    if not next_file:
        # --- no valid files
        return
    next_file_name_key = next_file.name
    logger.info(f"Processing file: {next_file_name_key}")
    is_file_in_s3 = check_file_in_s3(s3_client, bucket, next_file_name_key)
    if is_file_in_s3:
        # --- don't upload if already in S3
        logger.info(f"File already in S3: {next_file_name_key}, deleting")
        next_file.unlink()
    else:
        try:
            logger.info(f"Uploading file: {next_file_name_key}")
            response = upload_file(s3_client, bucket, next_file)
            logger.info(f"Uploaded file: {next_file_name_key}, deleting")
            next_file.unlink()
            # --- on successfull upload, remove
        except Exception as e:
            logger.warning(f"Failed to upload file: {next_file_name_key}")
            logger.warning(f"{e}")
            # --- likely due to some connection error, not file error, so don't delete file and reprocess next time


# -----------------------------
# Main Loop
# -----------------------------


def main():
    # Config
    # TODO: Extract to config file
    base_dir = Path(__file__).parent
    directory = base_dir / "uploadfiles"
    log_file = base_dir / "uploader.log"
    bucket = "bucket-images-s2264323"
    interval = 30
    valid_extensions = {".jpg", ".jpeg", ".png"}
    max_size_mb, min_size_mb = 1000, 1
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("uploader")
    logger.info("Starting S3 file uploader")

    s3_client = boto3.client("s3")

    # --- create if not exist
    Path(directory).mkdir(parents=True, exist_ok=True)

    # --- keep the app running, processing a file after interval
    while True:

        process_file(
            s3_client,
            bucket,
            directory,
            valid_extensions,
            logger,
            max_size_mb,
            min_size_mb
        )

        time.sleep(interval)


if __name__ == "__main__":
    main()
