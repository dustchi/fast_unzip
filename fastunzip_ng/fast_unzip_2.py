import argparse
import ctypes
import math
import mmap
import os
import pickle
import sys
from array import array
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Final, Iterable, Optional, Union
from zipfile import ZipFile

from .custom_exceptions import (AllFilesAreEmpty, CannotUseZeroCores,
                                EmptyArchiveError, IncorrectNumberOfCores,
                                UnableToCountCores)

# Threshold to choose mode
THRESHOLD: Final = 0.5

# Aliases
ReadOnlyBuffer = bytes
if sys.version_info >= (3, 8):
    WriteableBuffer = Union[bytearray, memoryview,
                            mmap.mmap, pickle.PickleBuffer]
else:
    WriteableBuffer = Union[
        bytearray,
        memoryview,
        array.array[Any],
        mmap.mmap,
        ctypes._CData,
    ]
ReadableBuffer = Union[ReadOnlyBuffer, WriteableBuffer]


class Unzipper:
    """Unzipper prototype."""

    def __init__(self, zip_archive: str, path: str,
                 threads: Optional[int]) -> None:
        self._path = path  # Where to unpack
        self._zip_archive = zip_archive  # Where zip archive is
        self._cpu = os.cpu_count()  # CPU count for automatic work
        self.cnt = 0
        # Check for all situations, raise errors is smth is wrong
        if threads is None and self._cpu is None:
            raise UnableToCountCores("Enter cores or threads explicitly.")
        elif threads is None and self._cpu is not None:
            self._threads = min(self._cpu + 4, 32)
        elif threads is not None:
            if threads == 0:
                raise CannotUseZeroCores("Enter appropriate number.")
            else:
                self._threads = threads

    # save file to disk
    def _save_file(self, data: ReadableBuffer, filename: str) -> None:
        # create a path
        filepath = os.path.join(self._path, filename)
        print("MIKE: filepath to be create: ", filepath)
        # write to disk
        with open(filepath, "wb") as file:
            file.write(data)


class MultiThreadUnzipper(Unzipper):
    """Unzipper for high compression levels"""

    # unzip files from an archive
    def _unzip_files(self, handle: ZipFile, filenames: Iterable[str]) -> None:
        # unzip multiple files
        for filename in filenames:
            # unzip the file            
            self.cnt += 1
            if filename.endswith("layer.json"):
                print("MIKE unzip file:",self.cnt, self._path, filename)
            handle.extract(filename, self._path)

    # unzip a large number of files
    def unzip(self) -> None:
        # open the zip file
        batchNo = 0
        with ZipFile(self._zip_archive, "r") as handle:
            # list of all files to unzip
            files = handle.namelist()
            if len(files) == 0:
                raise EmptyArchiveError("Archive is empty.")
            # determine chunksize
            chunksize = math.ceil(len(files) / self._threads)
            #chunksize = 10000
            print("MIKE: files count: ", len(files))
            print("MIKE: chunksize: ", chunksize)
            print("MIKE: threads count: ", self._threads)

            # cnt = 0
            # for filename in files:                
            #     if filename.endswith("layer.json"):
            #         print(cnt, filename)
            #     cnt = cnt + 1
            # print("Last one", files[182869])
            # print("done")
            # quit()
            
            # start the thread pool
            # with ThreadPoolExecutor(max_workers=4) as exe:
            with ThreadPoolExecutor(self._threads) as exe:
                tasks = set()
                # split the copy operations into chunks
                for i in range(0, len(files), chunksize):
                    batchNo += 1
                    print("MIKE: batchNo: ", batchNo)
                    # select a chunk of filenames
                    filenames = files[i: (i + chunksize)]
                    # submit the batch copy task
                    # _ = exe.submit(self._unzip_files, handle, filenames)
                    task = exe.submit(self._unzip_files, handle, filenames)
                    tasks.add(task)
                    # exception = future.exception()
                    # print(exception)

                for task in as_completed(tasks):
                    try:
                        result = task.result()
                    except Exception as exc:
                        print('Exception found: %s' % (exc))
                    else:
                        print('Task ',task, ' is ok')


class CombinedUnzipper(Unzipper):
    """Unzipper for low compression levels"""

    def __init__(
        self,
        zip_archive: str,
        path: str,
        processes: Optional[int],
        threads: Optional[int],
    ) -> None:
        # Takes Unzipper constructor
        super().__init__(zip_archive, path, threads)
        # Checking for all types of input, raises errors if smth is wrong
        if processes is None and self._cpu is None:
            raise UnableToCountCores("Enter cores or threads explicitly.")
        elif processes is None and self._cpu is not None:
            self.__processes = self._cpu
        elif processes is not None and self._cpu is None:
            if processes == 0:
                raise CannotUseZeroCores("Enter appropriate number.")
            else:
                self.__processes = processes
            print(
                "Attention!!! Unable to count cores. Please ensure you \
                entered correct number, else it can lead to \
                undefined behaviour."
            )
        elif processes is not None and self._cpu is not None:
            if processes > self._cpu:
                raise IncorrectNumberOfCores(
                    "Entered more cores than there \
                    are on computer."
                )
            elif processes == 0:
                raise CannotUseZeroCores("Enter appropriate number.")
            else:
                self.__processes = processes

    # unzip files from an archive
    def _unzip_files(self, filenames: Iterable[str]) -> None:
        # open the zip file
        with ZipFile(self._zip_archive, "r") as handle:
            # create a thread pool
            with ThreadPoolExecutor(self._threads) as exe:
                # unzip each file
                for filename in filenames:
                    # decompress data
                    data = handle.read(filename)
                    # save to disk
                    print("MIKE: Save filename: ",filename)
                    _ = exe.submit(self._save_file, data, filename)

    # unzip a large number of files
    def unzip(self) -> None:
        # create the target directory
        print("MIKE: using path: ", self._path)
        os.makedirs(self._path, exist_ok=True)
        # open the zip file
        with ZipFile(self._zip_archive, "r") as handle:
            # list of all files to unzip
            files = handle.namelist()
            print("MIKE: number of files: ", len(files))
            if len(files) == 0:
                raise EmptyArchiveError("Archive is empty.")
        # determine chunksize
        chunksize = math.ceil(len(files) / self.__processes)
        print("MIKE: chunksize: ", chunksize)
        # start the thread pool
        with ProcessPoolExecutor(self.__processes) as exe:
            # split the copy operations into chunks
            for i in range(0, len(files), chunksize):
                # select a chunk of filenames
                filenames = files[i: (i + chunksize)]
                print("MIKE: number of files in chunk: ",len(filenames))
                print("filename 1: ",filenames[0])
                # submit the batch copy task
                _ = exe.submit(self._unzip_files, filenames)


class Controller:

    """Decides what subprogram to use using precalculated compression level"""

    def __init__(self, zip_archive: str) -> None:
        self.zip_archive = zip_archive  # Where zip archive is

    def __calculate_compression(self) -> float:
        """Analyzes zip archive returning compression level"""

        compression = []
        with ZipFile(self.zip_archive, "r") as handle:
            if len(handle.namelist()) == 0:
                # No files in archive
                raise EmptyArchiveError("Archive is empty.")
            for file in handle.namelist():
                info = handle.getinfo(file)
                try:
                    # In case file is 0 bytes don't append list
                    compression.append(info.compress_size / info.file_size)
                except ZeroDivisionError:
                    pass
        if len(compression) == 0:
            # In case ZIP archive is empty
            raise AllFilesAreEmpty("All files are 0 bytes.")
        return sum(compression) / len(compression)

    def get_compression(self) -> float:
        """Public function working as interface"""

        return self.__calculate_compression()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="fast_unzip",
        description="unzips ZIP archives",
    )
    parser.add_argument(
        "archive_path",
        metavar="PATH",
        type=Path,
        help="path to ZIP archive",
    )
    parser.add_argument(
        "-p",
        "--n_proc",
        default=None,
        type=int,
        metavar="N",
        help="number of processes",
    )
    parser.add_argument(
        "-t",
        "--n_threads",
        default=None,
        type=int,
        metavar="N",
        help="number of threads for tasks in one process",
    )
    parser.add_argument(
        "-d",
        "--outdir",
        default=Path("./ZIP_unpack"),
        metavar="",
        type=Path,
        help="output directory to put unpacked \
                            data",
    )
    parser.add_argument(
        "-m",
        "--mode",
        default=None,
        metavar="MODE",
        type=str,
        help='mode in which program should work:\
                            "mt" or "cmbd"',
    )
    args = parser.parse_args()

    assert args.archive_path.exists(), f"{args.archive_path}"
    return args


def main() -> None:
    args = parse_args()
    print("MIKE: FORCE mode = mt !!!")
    args.mode = "mt"
    if args.mode is None:
        # If mode isn't specified controller decides what to use
        compression = Controller(args.archive_path).get_compression()
        print("MIKE: Compression: ", compression)
        if compression > THRESHOLD:
            print("MIKE: MultiThread Unzipper Used.")
            MultiThreadUnzipper(args.archive_path,
                                args.outdir, args.n_threads).unzip()
        elif compression <= THRESHOLD:
            print("MIKE: Combined Unzipper Used.")
            CombinedUnzipper(
                args.archive_path, args.outdir, args.n_proc, args.n_threads
            ).unzip()
    # If mode is given no controller is needed
    elif args.mode == "mt":
        print("MIKE: MultiThread Unzipper Used.")
        MultiThreadUnzipper(args.archive_path, args.outdir,
                            args.n_threads).unzip()
    elif args.mode == "cmbd":
        print("MIKE: Combined Unzipper Used.")
        CombinedUnzipper(
            args.archive_path, args.outdir, args.n_proc, args.n_threads
        ).unzip()
