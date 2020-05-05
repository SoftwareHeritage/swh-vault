# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import current_app as app

from swh.vault.cookers import get_cooker


@app.task(name=__name__ + ".SWHCookingTask")
def cook_bundle(obj_type, obj_id):
    """Main task to cook a bundle."""
    get_cooker(obj_type, obj_id).cook()


# TODO: remove once the scheduler handles priority tasks
@app.task(name=__name__ + ".SWHBatchCookingTask")
def batch_cook_bundle(obj_type, obj_id):
    """Temporary task for the batch queue."""
    get_cooker(obj_type, obj_id).cook()
