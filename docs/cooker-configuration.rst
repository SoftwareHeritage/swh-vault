.. _vault-cooker-configuration:

Cooker configuration
====================

Keys read from ``vault/cooker`` and passed to every cooker.

``max_bundle_size``
   Bytes a bundle may reach before cooking is abandoned. Counted on what is
   written out, so it cannot bound a tree of empty files.

``max_directory_entries``
   Entries (aka inodes) a directory tree may expand to before the vault
   refuses to write it out. ``None`` disables the limit.

``max_directory_size``
   Bytes of content a directory tree may expand to, counted once per path. A
   tree under the entry limit can still be enormous. ``None`` disables it.

``max_cooking_time``
   Seconds one tree may be walked for. ``None`` disables it.

``thread_pool_size``
   Threads used to fetch file contents while writing a tree to disk.
