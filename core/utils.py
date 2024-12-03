import argparse
import os
from pathlib import Path
from argparse import ArgumentTypeError


def files_path(s_path: str) -> Path:
    path = Path(s_path)
    if not path.exists():
        raise ArgumentTypeError(f"Could not find '{s_path}' directory")
    if path.is_file():
        raise ArgumentTypeError(
            f"Passed path ('{s_path}' is a file path, not a directory"
        )
    return path


def parse_args() -> dict:
    parser = argparse.ArgumentParser(
        description="Парсинг аргументов из командной строки"
    )

    parser.add_argument(
        "--input_claims",
        required=True,
        type=files_path,
        help="Path to 'claims' input data",
    )
    parser.add_argument(
        "--input_reverts",
        required=True,
        type=files_path,
        help="Path to 'reverts' input data",
    )
    parser.add_argument(
        "--input_pharmacies",
        required=True,
        type=files_path,
        help="Path to 'pharmacies' input data",
    )
    parser.add_argument(
        "--cores",
        required=True,
        type=int,
        default=10,
        help="Number of cores used in multiprocessing",
    )

    args = parser.parse_args()
    return vars(args)


# def memory_info() -> str:
#     process = psutil.Process(os.getpid())
#     mem_info = round(process.memory_info().rss / 1024 **2, 2)
#     return f"PID={process.pid}, RAM={mem_info}MB"