import asyncio
import aiofiles
from pathlib import Path
import argparse
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("file_sorter.log"), logging.StreamHandler()]
)


async def read_folder(source: Path, destination: Path):
    """
    Asynchronously reads files from the source folder and sorts them into subfolders
    based on their extensions in the destination folder.
    """
    if not source.exists():
        logging.error(f"Source folder '{source}' does not exist.")
        return

    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)

    for item in source.iterdir():
        if item.is_dir():
            await read_folder(item, destination)
        elif item.is_file():
            await copy_file(item, destination)


async def copy_file(file_path: Path, destination: Path):
    """
    Copies a file to the corresponding subfolder in the destination folder based on its extension.
    """
    try:
        extension = file_path.suffix[1:].lower() or "unknown"  # Handle files without extensions
        target_folder = destination / extension
        target_folder.mkdir(parents=True, exist_ok=True)

        target_path = target_folder / file_path.name
        if not target_path.exists():
            async with aiofiles.open(file_path, "rb") as src, aiofiles.open(target_path, "wb") as dst:
                while chunk := await src.read(1024 * 1024):  # Read in chunks of 1 MB
                    await dst.write(chunk)
            logging.info(f"Copied '{file_path}' to '{target_path}'.")
        else:
            logging.warning(f"File '{target_path}' already exists. Skipping.")
    except Exception as ex:
        logging.error(f"Error copying file '{file_path}': {ex}")


async def main(source: str, destination: str):
    source_path = Path(source)
    destination_path = Path(destination)

    logging.info(f"Starting file sorting from '{source_path}' to '{destination_path}'...")
    await read_folder(source_path, destination_path)
    logging.info("File sorting completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Asynchronous file sorter.")
    parser.add_argument("source", type=str, help="Path to the source folder.")
    parser.add_argument("destination", type=str, help="Path to the destination folder.")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.source, args.destination))
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
