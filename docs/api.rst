.. _vault-api-ref:

Vault API Reference
===================

Software source code **objects**---e.g., individual files, directories,
commits, tagged releases, etc.---are stored in the Software Heritage (SWH)
Archive in fully deduplicated form. That allows direct access to individual
artifacts, but require some preparation ("cooking") when fast access to a large
set of related objects (e.g., an entire repository) is required.

The **Software Heritage Vault** takes care of that preparation by
asynchronously assembling **bundles** of related source code objects, caching,
and garbage collecting them as needed.

The Vault is accessible via a RPC API documented below.

All endpoints are mounted at API root, which is currently at :swh_web:`api/1/`.

Unless otherwise stated, API endpoints respond to HTTP GET method.


Object identification
---------------------

The vault stores bundles corresponding to different kinds of objects (see
:ref:`data-model`).

The URL fragment ``:bundletype/:swhid`` is used throughout the vault API to
identify vault objects. See :ref:`persistent-identifiers` for details on
the syntax and meaning of ``:swhid``.


Bundle types
------------


Flat
~~~~

Flat bundles are simple tarballs that can be read without any specialized software.

When cooking directories, they are (very close to) the original directories that
were ingested.
When cooking other types of objects, they have multiple root directories,
each corresponding to an original object (revision, ...)

This is typically only useful to cook directories; cooking other types of objects
(revisions, releases, snapshots) are usually done with ``git-bare`` as it is
more efficient and closer to the original repository.

You can extract the resulting bundle using:

.. code:: shell

    tar xaf bundle.tar.gz


gitfast
~~~~~~~

A gzip-compressed `git fast-export
<https://git-scm.com/docs/git-fast-export>`_. You can extract the resulting
bundle using:

.. code:: shell

    git init
    zcat bundle.gitfast.gz | git fast-import
    git checkout HEAD


git-bare
~~~~~~~~

A tarball that can be decompressed to get a real git repository.
It is without a checkout, so it is the equivalent of what one would get
with ``git clone --bare``.

This is the most flexible bundle type, as it allow to perfectly recreate
original git repositories, including branches.

You can extract the resulting bundle using:

.. code:: shell

    tar xaf bundle.tar.gz

Then explore its content like a normal ("non-bare") git repository by cloning it:

.. code:: shell

   git clone path/to/extracted/:swhid


Cooking and status checking
---------------------------

Vault bundles might be ready for retrieval or not. When they are not, they will
need to be **cooked** before they can be retrieved. A cooked bundle will remain
around until it expires; after expiration, it will need to be cooked again
before it can be retrieved. Cooking is idempotent, and a no-op in between a
previous cooking operation and expiration.

.. http:post:: /vault/:bundletype/:swhid
.. http:get:: /vault/:bundletype/:swhid

    **Request body**: optionally, an ``email`` POST parameter containing an
    e-mail to notify when the bundle cooking has ended.

    **Allowed HTTP Methods:**

    - :http:method:`post` to **request** a bundle cooking
    - :http:method:`get` to check the progress and status of the cooking
    - :http:method:`head`
    - :http:method:`options`

    **Response:**

    :statuscode 200: bundle available for cooking, status of the cooking
    :statuscode 400: malformed SWHID
    :statuscode 404: unavailable bundle or object not found

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "id": 42,
            "fetch_url": "/api/1/vault/flat/:swhid/raw/",
            "swhid": ":swhid",
            "progress_message": "Creating tarball...",
            "status": "pending"
        }

    After a cooking request has been started, all subsequent GET and POST
    requests to the cooking URL return some JSON data containing information
    about the progress of the bundle creation. The JSON contains the
    following keys:

    - ``id``: the ID of the cooking request

    - ``fetch_url``: the URL that can be used for the retrieval of the bundle

    - ``swhid``: the identifier of the requested bundle

    - ``progress_message``: a string describing the current progress of the
      cooking. If the cooking failed, ``progress_message`` will contain the
      reason of the failure.

    - ``status``: one of the following values:

      - ``new``: the bundle request was created
      - ``pending``: the bundle is being cooked
      - ``done``: the bundle has been cooked and is ready for retrieval
      - ``failed``: the bundle cooking failed and can be retried

Retrieval
---------

Retrieve a specific bundle from the vault with:

.. http:get:: /vault/:bundletype/:swhid/raw

    **Allowed HTTP Methods:** :http:method:`get`, :http:method:`head`,
    :http:method:`options`

    **Response**:

    :statuscode 200: bundle available; response body is the bundle.
    :statuscode 404: unavailable bundle; client should request its cooking.
