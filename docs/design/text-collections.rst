Text collections
================

This document describes how Ambuda organizes texts.


Overview
--------

Users browsing the library typically have one of two intents: finding a specific text and
exploring what texts are available. To help serve both of these needs, we organize texts into
*collections* that share a common theme.

Collections have two important properties:

1. A text may belong to multiple collections. For example, a text may be both a stotra and a
   Vedanta text. Or, a text may be both a muktika upanishad and a Shaiva upanishad.

2. Collections can be hierarchical. For example, a text may be both a nataka and a kavya.


The collection is the successor model to our existing `Genre` and `TextTag` models.


Data model
----------

A Collection has the following fields:

- id (unique): primary key
- parent_id (nullable): the parent collection
- slug (unique): the URL-friendly name of this collection
- order: a numeric order with respect to the parent collection. This is for the sake of sorting
  collections for display to the user. (Even if parent_id is null, `order` is still meaningful,
  since it determines the toplever ordering.)
- title: the human-readable name of this collection
- description (nullable): a short description of this collection, tentatively in Markdown.
- created_at: a datetime of when the collection was created.

Relationships:
- Collection is in a many-many relationship with Text. A text belongs to zero or more collections.
- Collection is a many-many relationship with PublishConfig.
  - the collections on PublishConfig have a two-way sync with the collections on Text. That is:
    - When we publish with a config, the text's collections are set to the collections on the
      publish config.
    - When we update the collections on a text with a publish config, the config's collections are
      set to the text's collections.


Key files
---------

- ambuda/models/texts.py -- defines Text and Collection
- ambuda/seed/text_collections.py -- a simple seed script for collections


Seed scripts
------------

A seed script ambuda/seed/text_collections.py exists that is run along with our other seed scripts.
This script checks if any collections exist, and if not, it creates a simple default set.


Editing collections
-------------------

Collections are exposed for editing in two UIs.

The first UI is available only to users with the `admin` role. This UI lets admins do the following:

- create collections
- edit collections
- reorder collections with a parent
- drag and drop a collection under a new parent. If this is done, all of the collection's
  descendants are similarly rearranged.

The second UI is available only to users with the `p2` permission and above. This UI, which is
accessible through the proofed texts list at /proofing/texts Within the proofing UI, lets users do
the following:

- select texts via a checkbox
- perform an "edit collections" batch action on the selected texts. On submit, users may add and
  remove collections for the selected texts.


Collection routes
-----------------

- `GET /collections` lists all collections in order
- `GET /collections/<slug>` lists all texts associated with the given collection and its descendants.
