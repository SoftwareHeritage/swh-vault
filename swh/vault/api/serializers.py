# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Callable, Dict, List, Tuple

from swh.model.swhids import CoreSWHID

ENCODERS: List[Tuple[type, str, Callable]] = [
    (CoreSWHID, "core_swhid", str),
]


DECODERS: Dict[str, Callable] = {
    "core_swhid": CoreSWHID.from_string,
}
