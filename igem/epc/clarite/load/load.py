from typing import Optional, Union

import clarite


def from_tsv(
    filename: str,
    index_col: Optional[Union[str, int]] = 0,
    **kwargs
):
    return clarite.load.from_tsv(filename, index_col, **kwargs)


def from_csv(
    filename: str,
    index_col: Optional[Union[str, int]] = 0,
    **kwargs
):
    return clarite.load.from_csv(filename, index_col, **kwargs)
