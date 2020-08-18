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

The Vault is accessible via a RESTful API documented below.

All endpoints are mounted at API root, which is currently at
https://archive.softwareheritage.org/api/1/.

Unless otherwise stated, API endpoints respond to HTTP GET method.


Object identification
---------------------

The vault stores bundles corresponding to different kinds of objects (see
:ref:`data-model`). The following object kinds are currently supported by the
Vault:

-  directories
-  revisions
-  snapshots

The URL fragment ``:objectkind/:objectid`` is used throughout the vault API to
identify vault objects. The syntax and meaning of ``:objectid`` for the
different object kinds is detailed below.

In the case of revisions, a third parameter, ``:format``, must be used to
specify the format of the resulting bundle. The URL fragment then becomes
``:objectkind/:objectid/:format``.


Directories
~~~~~~~~~~~

-  object kind: ``directory``
-  URL fragment: ``directory/:dir_id``

where ``:dir_id`` is a :py:func:`directory identifier
<swh.model.identifiers.directory_identifier>`.

The only format available for a directory export is a gzip-compressed
tarball. You can extract the resulting bundle using:

.. code:: shell

    tar xaf bundle.tar.gz


Revisions
~~~~~~~~~

-  object kind: ``revision``
-  URL fragment: ``revision/:rev_id/:format``

where ``:rev_id`` is a :py:func:`revision identifier
<swh.model.identifiers.revision_identifier>` and ``:format`` is the export
format.

The only format available for a revision export is ``gitfast``: a
gzip-compressed `git fast-export
<https://git-scm.com/docs/git-fast-export>`_. You can extract the resulting
bundle using:

.. code:: shell

    git init
    zcat bundle.gitfast.gz | git fast-import
    git checkout HEAD


Repository snapshots
~~~~~~~~~~~~~~~~~~~~

.. TODO

**(NOT AVAILABLE YET)**

-  object kind: ``snapshot``
-  URL fragment: ``snapshot/:snp_id``

where ``:snp_id`` is a :py:func:`snapshot identifier
<swh.model.identifiers.snapshot_identifier>`.


Cooking and status checking
---------------------------

Vault bundles might be ready for retrieval or not. When they are not, they will
need to be **cooked** before they can be retrieved. A cooked bundle will remain
around until it expires; after expiration, it will need to be cooked again
before it can be retrieved. Cooking is idempotent, and a no-op in between a
previous cooking operation and expiration.

.. http:post:: /vault/:objectkind/:objectid[/:format]
.. http:get:: /vault/:objectkind/:objectid[/:format]

    **Request body**: optionally, an ``email`` POST parameter containing an
    e-mail to notify when the bundle cooking has ended.

    **Allowed HTTP Methods:**

    - :http:method:`post` to **request** a bundle cooking
    - :http:method:`get` to check the progress and status of the cooking
    - :http:method:`head`
    - :http:method:`options`

    **Response:**

    :statuscode 200: bundle available for cooking, status of the cooking
    :statuscode 400: malformed identifier hash or format
    :statuscode 404: unavailable bundle or object not found

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "id": 42,
            "fetch_url": "/api/1/vault/directory/:dir_id/raw/",
            "obj_id": ":dir_id",
            "obj_type": "directory",
            "progress_message": "Creating tarball...",
            "status": "pending"
        }

    After a cooking request has been started, all subsequent GET and POST
    requests to the cooking URL return some JSON data containing information
    about the progress of the bundle creation. The JSON contains the
    following keys:

    - ``id``: the ID of the cooking request

    - ``fetch_url``: the URL that can be used for the retrieval of the bundle

    - ``obj_type``: an internal identifier uniquely representing the object
      kind and the format of the required bundle.

    - ``obj_id``: the identifier of the requested bundle

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

.. http:get:: /vault/:objectkind/:objectid[/:format]/raw

   Where ``:format`` is optional, depending on the object kind.

    **Allowed HTTP Methods:** :http:method:`get`, :http:method:`head`,
    :http:method:`options`

    **Response**:

    :statuscode 200: bundle available; response body is the bundle.
    :statuscode 404: unavailable bundle; client should request its cooking.
