Software Heritage Vault API Reference
=====================================

Software source code **objects**---e.g., individual source code files,
tarballs, commits, tagged releases, etc.---are stored in the Software
Heritage (SWH) Archive in fully deduplicated form. That allows direct
access to individual artifacts but require some preparation, usually in
the form of collecting and assembling multiple artifacts in a single
**bundle**, when fast access to a set of related artifacts (e.g., the
snapshot of a VCS repository, the archive corresponding to a Git commit,
or a specific software release as a zip archive) is required.

The **Software Heritage Vault** is a cache of pre-built source code
bundles which are assembled opportunistically retrieving objects from
the Software Heritage Archive, can be accessed efficiently, and might be
garbage collected after a long period of non-use.

API
---

All URLs below are meant to be mounted at API root, which is currently
at https://archive.softwareheritage.org/api/1/. Unless otherwise stated,
all API endpoints respond on HTTP GET method.

Object identification
---------------------

The vault stores bundles corresponding to different kinds of objects.
The following object kinds are supported:

-  directories
-  revisions
-  repository snapshots (not available yet)

The URL fragment ``:objectkind/:objectid`` is used throughout the vault
API to identify vault objects. The syntax and meaning of ``:objectid`` for
the different object kinds is detailed below.

Optionally, a third parameter, ``:format``, can sometimes be used to
specify the format of the resulting bundle when needed. The URL fragment
becomes ``:objectkind/:objectid/:format``.

Directories
~~~~~~~~~~~

-  object kind: ``directory``
-  URL fragment: ``directory/:sha1git``

where ``:sha1git`` is the directory ID in the SWH data model.

Currently, the only format available for a directory export is a
gzip-compressed tarball. You can extract the resulting bundle using:

.. code:: shell

    tar xvf bundle.tar.gz

Revisions
~~~~~~~~~

-  object kind: ``revision``
-  URL fragment: ``revision/:sha1git/:format``

where ``:sha1git`` is the revision ID in the SWH data model, and
``:format`` is the export format.

Currently, the only format available for a revision export is
``gitfast``: a gzip-compressed git fast-export, according to the format
documented in ``git-fast-import(1)``. You can extract the resulting
bundle using:

.. code:: shell

    git init
    zcat bundle.gitfast.gz | git fast-import
    git checkout HEAD

Repository snapshots
~~~~~~~~~~~~~~~~~~~~

**[NOT YET AVAILABLE]**

-  object kind: ``snapshot``
-  URL fragment: ``snapshot/:sha1git``

where ``:sha1git`` is the snapshot ID in the SWH data model. (**TODO**
repository snapshots don't exist yet as first-class citizens in the SWH
data model; see References below.)

Cooking and status checking
---------------------------

Bundles in the vault might be ready for retrieval or not. When they are
not, they will need to be **cooked** before they can be retrieved. A
cooked bundle will remain around until it expires; at that point it will
need to be cooked again before it can be retrieved. Cooking is
idempotent, and a no-op in between a previous cooking operation and
expiration.

.. http:post:: /vault/:objectkind/:objectid/:format
.. http:get:: /vault/:objectkind/:objectid/:format

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
            "fetch_url": "/api/1/vault/directory/<sha1_git:dir_id>/raw/",
            "obj_id": "<sha1_git:dir_id>",
            "obj_type": "directory",
            "progress_message": "Creating tarball...",
            "status": "pending"
        }

    After a cooking request has been started, all subsequent GET and POST
    requests to the cooking URL return some JSON data containing information
    about the progress of the bundle creation. The JSON contains the
    following keys:

    -  ``id``: the ID of the cooking request

    -  ``fetch_url``: the URL that can be used for the retrieval of the
       bundle

    -  ``obj_type``: an internal identifier uniquely representing the object
       kind and the format of the required bundle.

    -  ``obj_id``: the identifier of the requested bundle

    -  ``status``: one of the following values:

    -  ``new``: the bundle request was created
    -  ``pending``: the bundle is being cooked
    -  ``done``: the bundle has been cooked and is ready for retrieval
    -  ``failed``: the bundle cooking failed and can be retried

    -  ``progress_message``: a string describing the current progress of the
       cooking. If the cooking failed, ``progress_message`` will contain the
       reason of the failure.

Retrieval
---------

Retrieve a specific bundle from the vault with:

.. http:get:: /vault/:objectkind/:objectid/:format/raw

    **Allowed HTTP Methods:** :http:method:`get`, :http:method:`head`,
    :http:method:`options`

    **Response**:

    :statuscode 200: bundle available; response body is the bundle.
    :statuscode 404: unavailable bundle; client should request its cooking.
