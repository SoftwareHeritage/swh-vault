# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import os
import time
import zlib

from fastimport.commands import (
    BlobCommand,
    CommitCommand,
    FileDeleteCommand,
    FileModifyCommand,
    ResetCommand,
)

from swh.model import hashutil
from swh.model.from_disk import DentryPerms, mode_to_perms
from swh.model.toposort import toposort
from swh.vault.cookers.base import BaseVaultCooker
from swh.vault.cookers.utils import revision_log
from swh.vault.to_disk import get_filtered_files_content


class RevisionGitfastCooker(BaseVaultCooker):
    """Cooker to create a git fast-import bundle """

    CACHE_TYPE_KEY = "revision_gitfast"

    def check_exists(self):
        return not list(self.storage.revision_missing([self.obj_id]))

    def prepare_bundle(self):
        self.log = list(toposort(revision_log(self.storage, self.obj_id)))
        self.gzobj = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
        self.fastexport()
        self.write(self.gzobj.flush())

    def write_cmd(self, cmd):
        chunk = bytes(cmd) + b"\n"
        super().write(self.gzobj.compress(chunk))

    def fastexport(self):
        """Generate all the git fast-import commands from a given log.
        """
        self.rev_by_id = {r["id"]: r for r in self.log}
        self.obj_done = set()
        self.obj_to_mark = {}
        self.next_available_mark = 1

        last_progress_report = None

        for i, rev in enumerate(self.log, 1):
            # Update progress if needed
            ct = time.time()
            if last_progress_report is None or last_progress_report + 2 <= ct:
                last_progress_report = ct
                pg = "Computing revision {}/{}".format(i, len(self.log))
                self.backend.set_progress(self.obj_type, self.obj_id, pg)

            # Compute the current commit
            self._compute_commit_command(rev)

    def mark(self, obj_id):
        """Get the mark ID as bytes of a git object.

        If the object has not yet been marked, assign a new ID and add it to
        the mark dictionary.
        """
        if obj_id not in self.obj_to_mark:
            self.obj_to_mark[obj_id] = self.next_available_mark
            self.next_available_mark += 1
        return str(self.obj_to_mark[obj_id]).encode()

    def _compute_blob_command_content(self, file_data):
        """Compute the blob command of a file entry if it has not been
        computed yet.
        """
        obj_id = file_data["sha1"]
        if obj_id in self.obj_done:
            return
        contents = list(get_filtered_files_content(self.storage, [file_data]))
        content = contents[0]["content"]
        self.write_cmd(BlobCommand(mark=self.mark(obj_id), data=content))
        self.obj_done.add(obj_id)

    def _author_tuple_format(self, author, date):
        # We never want to have None values here so we replace null entries
        # by ''.
        if author is not None:
            author_tuple = (author.get("name") or b"", author.get("email") or b"")
        else:
            author_tuple = (b"", b"")
        if date is not None:
            date_tuple = (
                date.get("timestamp", {}).get("seconds") or 0,
                (date.get("offset") or 0) * 60,
            )
        else:
            date_tuple = (0, 0)
        return author_tuple + date_tuple

    def _compute_commit_command(self, rev):
        """Compute a commit command from a specific revision.
        """
        if "parents" in rev and rev["parents"]:
            from_ = b":" + self.mark(rev["parents"][0])
            merges = [b":" + self.mark(r) for r in rev["parents"][1:]]
            parent = self.rev_by_id[rev["parents"][0]]
        else:
            # We issue a reset command before all the new roots so that they
            # are not automatically added as children of the current branch.
            self.write_cmd(ResetCommand(b"refs/heads/master", None))
            from_ = None
            merges = None
            parent = None

        # Retrieve the file commands while yielding new blob commands if
        # needed.
        files = list(self._compute_file_commands(rev, parent))

        # Construct and write the commit command
        author = self._author_tuple_format(rev["author"], rev["date"])
        committer = self._author_tuple_format(rev["committer"], rev["committer_date"])
        self.write_cmd(
            CommitCommand(
                ref=b"refs/heads/master",
                mark=self.mark(rev["id"]),
                author=author,
                committer=committer,
                message=rev["message"] or b"",
                from_=from_,
                merges=merges,
                file_iter=files,
            )
        )

    @functools.lru_cache(maxsize=4096)
    def _get_dir_ents(self, dir_id=None):
        """Get the entities of a directory as a dictionary (name -> entity).

        This function has a cache to avoid doing multiple requests to retrieve
        the same entities, as doing a directory_ls() is expensive.
        """
        data = self.storage.directory_ls(dir_id) if dir_id is not None else []
        return {f["name"]: f for f in data}

    def _compute_file_commands(self, rev, parent=None):
        """Compute all the file commands of a revision.

        Generate a diff of the files between the revision and its main parent
        to find the necessary file commands to apply.
        """
        # Initialize the stack with the root of the tree.
        cur_dir = rev["directory"]
        parent_dir = parent["directory"] if parent else None
        stack = [(b"", cur_dir, parent_dir)]

        while stack:
            # Retrieve the current directory and the directory of the parent
            # commit in order to compute the diff of the trees.
            root, cur_dir_id, prev_dir_id = stack.pop()
            cur_dir = self._get_dir_ents(cur_dir_id)
            prev_dir = self._get_dir_ents(prev_dir_id)

            # Find subtrees to delete:
            #  - Subtrees that are not in the new tree (file or directory
            #    deleted).
            #  - Subtrees that do not have the same type in the new tree
            #    (file -> directory or directory -> file)
            # After this step, every node remaining in the previous directory
            # has the same type than the one in the current directory.
            for fname, f in prev_dir.items():
                if fname not in cur_dir or f["type"] != cur_dir[fname]["type"]:
                    yield FileDeleteCommand(path=os.path.join(root, fname))

            # Find subtrees to modify:
            #  - Leaves (files) will be added or modified using `filemodify`
            #  - Other subtrees (directories) will be added to the stack and
            #    processed in the next iteration.
            for fname, f in cur_dir.items():
                # A file is added or modified if it was not in the tree, if its
                # permissions changed or if its content changed.
                if f["type"] == "file" and (
                    fname not in prev_dir
                    or f["sha1"] != prev_dir[fname]["sha1"]
                    or f["perms"] != prev_dir[fname]["perms"]
                ):
                    # Issue a blob command for the new blobs if needed.
                    self._compute_blob_command_content(f)
                    yield FileModifyCommand(
                        path=os.path.join(root, fname),
                        mode=mode_to_perms(f["perms"]).value,
                        dataref=(b":" + self.mark(f["sha1"])),
                        data=None,
                    )
                # A revision is added or modified if it was not in the tree or
                # if its target changed
                elif f["type"] == "rev" and (
                    fname not in prev_dir or f["target"] != prev_dir[fname]["target"]
                ):
                    yield FileModifyCommand(
                        path=os.path.join(root, fname),
                        mode=DentryPerms.revision,
                        dataref=hashutil.hash_to_hex(f["target"]).encode(),
                        data=None,
                    )
                # A directory is added or modified if it was not in the tree or
                # if its target changed.
                elif f["type"] == "dir":
                    f_prev_target = None
                    if fname in prev_dir and prev_dir[fname]["type"] == "dir":
                        f_prev_target = prev_dir[fname]["target"]
                    if f_prev_target is None or f["target"] != f_prev_target:
                        stack.append(
                            (os.path.join(root, fname), f["target"], f_prev_target)
                        )
