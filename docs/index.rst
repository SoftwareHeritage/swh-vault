.. _swh-vault:

.. include:: README.rst

The Vault backend
~~~~~~~~~~~~~~~~~

The Vault backend is the RPC server other |swh| components (mainly
:ref:`swh-web <swh-web>`) interact with.

It is in charge of receiving cooking requests, scheduling corresponding tasks (via
:ref:`swh-scheduler <swh-scheduler>` and Celery), getting heartbeats and final
results from these, cooking tasks, and finally serving the results.

It uses the same RPC protocol as the other components of the archive, and
its interface is described in :mod:`swh.vault.interface`.

The cookers
~~~~~~~~~~~

Cookers are Python modules/classes, each in charge of cooking a type of bundle.
The main ones are :mod:`swh.vault.cookers.directory` for flat tarballs of directories,
and :mod:`swh.vault.cookers.git_bare` for bare ``.git`` repositories of any
type of git object.
They all derive from :class:`swh.vault.cookers.base.BaseVaultCooker`.

The base cooker first notifies the backend the cooking task is in progress,
then runs the cooker (which does the bundle-specific handling and uploads the result),
then notifies the backend of the final result (success/failure).

Cookers may notify the backend of the progress, so they can be displayed in
swh-web's vault interface, which polls the status from the vault backend.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting-started.rst
   api.rst


Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   cli

.. only:: standalone_package_doc

   Indices and tables
   ------------------

   * :ref:`genindex`
   * :ref:`modindex`
   * :ref:`search`
