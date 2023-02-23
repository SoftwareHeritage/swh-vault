Software Heritage - Vault
=========================

User-facing service that allows to retrieve parts of the archive as
self-contained bundles (e.g., individual releases, entire repository snapshots,
etc.)
The creation of a bundle is called "cooking" a bundle.

Architecture
------------

The vault is made of two main parts:

1. a stateful RPC server called the **backend**
2. Celery tasks, called **cookers**
