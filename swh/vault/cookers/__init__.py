# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.vault.cookers.directory import DirectoryCooker
from swh.vault.cookers.revision_flat import RevisionFlatCooker
from swh.vault.cookers.revision_gitfast import RevisionGitfastCooker

COOKER_TYPES = {
    'directory': DirectoryCooker,
    'revision_flat': RevisionFlatCooker,
    'revision_gitfast': RevisionGitfastCooker,
}
