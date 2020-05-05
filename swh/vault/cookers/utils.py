# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage.algos.revisions_walker import get_revisions_walker


def revision_log(storage, rev_id, per_page=1000):
    """Retrieve a revision log in a paginated way in order to avoid storage
    timeouts when the total number of revisions to fetch is large.

    Args:
        storage (swh.storage.storage.Storage): instance of swh storage
            (either local or remote)
        rev_id (bytes): a revision identifier
        per_page (Optional[int]): the maximum number of revisions to return
            in each page

    Yields:
        dict: Revision information as a dictionary
    """
    rw_state = {}
    nb_revs = 0
    max_revs = per_page
    while True:
        # Get an iterator returning the commits log from rev_id.
        # At most max_revs visited revisions from rev_id in the commits graph
        # will be returned.
        revs_walker = get_revisions_walker(
            "bfs", storage, rev_id, max_revs=max_revs, state=rw_state
        )
        # Iterate on at most per_page revisions in the commits log.
        for rev in revs_walker:
            nb_revs += 1
            yield rev
        # If the total number of iterated revisions is lesser than the
        # maximum requested one, it means that we hit the initial revision
        # in the log.
        if nb_revs < max_revs:
            break
        # Backup iterator state to continue the revisions iteration
        # from where we left it.
        rw_state = revs_walker.export_state()
        # Increment the maximum of revisions to iterate from rev_id
        # to get next revisions in the log.
        max_revs += per_page
