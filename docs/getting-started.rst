.. _vault-primer:

Getting started
===============

The Vault is a service in charge of reconstructing parts of the archive
as self-contained bundles, that can then be imported locally, for
instance in a Git repository. This is basically where you can do a
``git clone`` of a repository stored in Software Heritage.

The Vault is asynchronous : you first need to do a request to prepare
the bundle you need, and then a second request to fetch the bundle once
the Vault has finished to reconstitute the bundle.

Example: retrieving a directory
-------------------------------

First, ask the Vault to prepare your bundle:

.. code:: shell

    curl -X POST https://archive.softwareheritage.org/api/1/vault/flat/:swhid/

where ``:swhid`` is a :ref:`persistent-identifiers`. This initial request and all
subsequent requests to this endpoint will return some JSON data containing
information about the progress of bundle creation:

.. code:: json

    {
        "id": 42,
        "fetch_url": "/api/1/vault/flat/:swhid/raw/",
        "swhid": ":swhid",
        "progress_message": "Creating tarball...",
        "status": "pending"
    }

Once the status is ``done``, you can fetch the bundle at the address
given in the ``fetch_url`` field.

.. code:: shell

    curl -o bundle.tar.gz https://archive.softwareheritage.org/api/1/vault/flat/:swhid/raw
    tar xaf bundle.tar.gz

E-mail notifications
--------------------

You can also ask to be notified by e-mail once the bundle you requested is
ready, by giving an ``email`` POST parameter:

.. code:: shell

    curl -X POST -d 'email=example@example.com' \
        https://archive.softwareheritage.org/api/1/vault/directory/:dir_id/

API reference
~~~~~~~~~~~~~

For a more exhaustive overview of the Vault API, see the :ref:`vault-api-ref`.
