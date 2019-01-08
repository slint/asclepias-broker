# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 CERN.
#
# Asclepias Broker is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Search tools."""

import json
from datetime import datetime
from pathlib import Path

from invenio_search import current_search
from invenio_search import current_search_client as es_client


def rolling_reindex(alias_name: str):
    """."""
    # NOTE: Covers very basic cases of ONLY ONE index/mapping per alias

    # Get original index name mapping file
    alias_definition = current_search.active_aliases[alias_name]
    assert len(alias_definition) == 1
    index_name, mapping_path = list(alias_definition.items())[0]

    # Determine "active" index
    existing_indices = es_client.indices.get(f'{index_name}*')
    assert len(existing_indices) <= 2
    old_active_index_name = sorted(existing_indices.keys())[0]

    # Generate new active index name with datetime suffix
    now = datetime.utcnow().strftime('%Y-%m-%d_%H%M')
    new_active_index_name = f'{index_name}-{now}'

    alias = es_client.indices.get_alias('relationships')

    # Create the new empty index
    es_client.indices.create(
        index=new_active_index_name,
        body=json.loads(Path(mapping_path).read_text()),
    )

    # TODO: Send reindexing task to newly created index
    from .tasks import reindex_all_relationships
    reindex_all_relationships.delay(index=new_active_index_name)

    # NOTE: This swap should happen only after reindexing has finished
    # TODO: Figure out a way to know when reindexing has finished...
    # Could be done with a Celery task group + chain
    # Swap indices via alias update
    es_client.indices.update_aliases({
        'actions': [
            {'add': {'index': new_active_index_name, 'alias': alias}},
            {'remove': {'index': old_active_index_name, 'alias': alias}},
        ]
    })
