import os
from typing import Iterator, Tuple, List


class BatchSplitter:
    """
    Reads raw NASA log files recursively from data_dir
    and yields (batch_id, lines) tuples of at most
    batch_size records.

    Supports nested folder structures like:

        data/raw/
            NASA_access_log_Aug95/
                access_log_Aug95

            NASA_access_log_Jul95/
                access_log_Jul95
    """

    def __init__(
        self,
        data_dir: str,
        batch_size: int = 500_000
    ):

        self.data_dir = data_dir

        self.batch_size = batch_size

        self._files = []

        # ==========================================
        # Recursively collect ALL real files
        # ==========================================

        for root, _, files in os.walk(data_dir):

            for fname in files:

                fpath = os.path.join(root, fname)

                if os.path.isfile(fpath):

                    self._files.append(fpath)

        self._files.sort()

    # =====================================================
    # ITERATE BATCHES
    # =====================================================

    def iter_batches(
        self
    ) -> Iterator[Tuple[int, List[str]]]:

        batch_id = 1

        buf: List[str] = []

        for fpath in self._files:

            with open(
                fpath,
                "r",
                encoding="latin-1",
                errors="replace"
            ) as f:

                for line in f:

                    line = line.rstrip("\n")

                    if line:

                        buf.append(line)

                    if len(buf) >= self.batch_size:

                        yield batch_id, buf

                        batch_id += 1

                        buf = []

        # ==========================================
        # Final partial batch
        # ==========================================

        if buf:

            yield batch_id, buf

    # =====================================================
    # MATERIALIZE ALL BATCHES
    # =====================================================

    def get_batches(
        self
    ) -> List[Tuple[int, List[str]]]:

        return list(self.iter_batches())

    # =====================================================
    # AVERAGE BATCH SIZE
    # =====================================================

    def avg_batch_size(
        self,
        batches: List[Tuple[int, List[str]]]
    ) -> float:

        total = sum(len(b) for _, b in batches)

        n_batches = len([
            b for _, b in batches if b
        ])

        return (
            total / n_batches
            if n_batches
            else 0.0
        )