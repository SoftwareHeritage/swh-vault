# Copyright (C) 2016-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import current_app as app

from swh.model.swhids import CoreSWHID
from swh.vault.cookers import get_cooker


@app.task(name=__name__ + ".SWHCookingTask")
def cook_vault_bundle(bundle_type, swhid):
    """Main task to cook a bundle."""
    get_cooker(bundle_type, CoreSWHID.from_string(swhid)).cook()


# TODO: remove once the scheduler handles priority tasks
@app.task(name=__name__ + ".SWHBatchCookingTask")
def cook_vault_bundle_batch(bundle_type, swhid):
    """Temporary task for the batch queue."""
    get_cooker(bundle_type, CoreSWHID.from_string(swhid)).cook()
