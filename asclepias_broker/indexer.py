# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# Asclepias Broker is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
"""Elasticsearch indexing module."""

from collections import defaultdict
from copy import deepcopy
from itertools import chain

import sqlalchemy as sa

from .mappings.dsl import DB_RELATION_TO_ES, ObjectDoc, ObjectRelationshipsDoc
from .models import Group, GroupRelationship, GroupType


def _build_group_metadata(group: Group) -> dict:
    if group.type == GroupType.Version:
        # Identifiers of the first identity group from all versions
        id_group = group.groups[0]
        ids = id_group.identifiers
        doc = deepcopy((id_group.data and id_group.data.json) or {})
    else:
        doc = deepcopy((group.data and group.data.json) or {})
        ids = group.identifiers

    doc['Identifier'] = [{'ID': i.value, 'IDScheme': i.scheme} for i in ids]
    doc['GroupType'] = group.type.name
    return doc


def _build_relationship_history(rel: GroupRelationship) -> dict:
    if rel.type == GroupType.Version:
        # History of the first group relationship from all versions
        # TODO: Maybe concatenate all histories?
        id_rel = rel.relationships[0]
        return deepcopy((id_rel.data and id_rel.data.json) or {})
    else:
        return deepcopy((rel.data and rel.data.json) or {})


def _build_relationship_metadata(group: Group) -> dict:
    relationships = GroupRelationship.query.filter(
        GroupRelationship.type == group.type,
        sa.or_(
            GroupRelationship.source_id == group.id,
            GroupRelationship.target_id == group.id),
    )
    doc = defaultdict(list)
    for rel in relationships:
        es_rel, es_inv_rel = DB_RELATION_TO_ES[rel.relation]
        is_reverse = str(group.id) == str(rel.target_id)
        rel_key = es_inv_rel if is_reverse else es_rel
        target_id = rel.source_id if is_reverse else rel.target_id
        doc[rel_key].append({
            'TargetID': str(target_id),
            'History': _build_relationship_history(rel),
        })
    return doc


def delete_group(group, with_relationships=True):
    """Delete a group and its relationships document from the indices."""
    obj_doc = ObjectDoc.get(str(group.id), ignore=404)
    if obj_doc:
        obj_doc.delete(ignore=404)

    if with_relationships:
        obj_rel_doc = ObjectRelationshipsDoc.get(str(group.id), ignore=404)
        if obj_rel_doc:
            obj_rel_doc.delete(ignore=404)
    return obj_doc, obj_rel_doc


def index_group(group: Group) -> ObjectDoc:
    """Index a group."""
    doc = _build_group_metadata(group)
    obj_doc = ObjectDoc(meta={'id': str(group.id)}, **doc)
    obj_doc.save()
    return obj_doc


def index_relationships(group: Group) -> ObjectRelationshipsDoc:
    """Index the relationships of a group."""
    doc = _build_relationship_metadata(group)
    rel_doc = ObjectRelationshipsDoc(meta={'id': str(group.id)}, **doc)
    rel_doc.save()
    return rel_doc


def update_indices(src_group: Group, trg_group: Group,
                   merged_group: Group=None):
    """Updates Elasticsearch indices with the updated groups."""
    # `src_group` and `trg_group` were merged into `merged_group`.
    if merged_group:
        # Delete Source and Target groups
        delete_group(src_group)
        delete_group(trg_group)

        # Index the merged object and its relationships
        obj_doc = index_group(merged_group)
        obj_rel_doc = index_relationships(merged_group)

        # Update all group relationships of the merged group
        # TODO: This can be optimized to avoid fetching a lot of the same
        # GroupMetadata, by keeping a temporary cache of them...
        relationships = chain.from_iterable(obj_rel_doc.to_dict().values())
        target_ids = Group.query.filter(
            Group.id.in_(r.get('TargetID') for r in relationships))
        for i in target_ids:
            index_relationships(i)
        return (obj_doc, obj_rel_doc), (obj_doc, obj_rel_doc)

    # No groups were merged, this is a simple relationship
    # Index Source and Target objects and their relationships
    src_doc = index_group(src_group)
    trg_doc = index_group(trg_group)
    src_rel_doc = index_relationships(src_group)
    trg_rel_doc = index_relationships(trg_group)
    return (src_doc, src_rel_doc), (trg_doc, trg_rel_doc)
