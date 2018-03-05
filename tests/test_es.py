"""Test ElasticSearch indexing."""

from typing import List, Tuple

from helpers import create_objects_from_relations
import arrow

from asclepias_broker.datastore import Group, GroupMetadata, Identifier, \
    Relation
from asclepias_broker.es import ObjectDoc, RelationshipDoc
from asclepias_broker.indexer import index_identity_group
from asclepias_broker.tasks import get_or_create_groups, merge_identity_groups


def dates_equal(a, b):
    return arrow.get(a) == arrow.get(b)


def _create_identity_groups(session, identifier_groups) -> List[Tuple[Group, GroupMetadata]]:
    all_groups = []
    for ids, metadata in identifier_groups:
        temp_ids = []
        temp_groups = []
        for i in ids:
            id_ = Identifier(value=i, scheme='doi')
            session.add(id_)
            temp_ids.append(id_)
            temp_groups.append(get_or_create_groups(session, id_)[0])
        base_id = temp_ids.pop()
        id_group, _ = get_or_create_groups(session, base_id)
        while len(temp_groups) > 0:
            merge_identity_groups(session, id_group, temp_groups.pop())
            session.commit()
            id_group, _ = get_or_create_groups(session, base_id)

        group_metadata = id_group.data or GroupMetadata(group=id_group)
        group_metadata.update(metadata)
        session.commit()
        all_groups.append((id_group, group_metadata))
    return all_groups


def _gen_metadata(id_):
    return {
        'Title': 'Title for {}'.format(id_),
        'Creator': [{'Name': 'Creator for {}'.format(id_)}],
        'Type': {'Name': 'literature'},
        'PublicationDate': '2018-01-01',
    }


def test_init(es):
    assert es.indices.exists(index='objects')
    assert es.indices.exists(index='relationships')


def test_simple_groups(broker, es):
    s = broker.session

    ids = {'A': ('A1', 'A2', 'A3'), 'B': ('B1', 'B2'), 'C': ('C1',)}
    (group, group_metadata), = _create_identity_groups(s, [
        (ids['A'], _gen_metadata('A')),
    ])

    assert len(ObjectDoc.all()) == 0

    index_identity_group(s, group)
    es.indices.refresh()
    all_object_docs = ObjectDoc.all()
    assert len(all_object_docs) == 1

    obj_doc = all_object_docs[0]
    assert obj_doc._id == str(group.id)
    assert obj_doc.Title == group_metadata.json['Title']
    assert obj_doc.Creator == group_metadata.json['Creator']
    assert dates_equal(obj_doc.PublicationDate, group_metadata.json['PublicationDate'])
    assert obj_doc.Identifier == [{'ID': i, 'IDScheme': 'doi'} for i in ids['A']]
    assert obj_doc.Relationships == {}

    groups = _create_identity_groups(s, [
        (ids['B'], _gen_metadata('B')),
        (ids['C'], _gen_metadata('C')),
    ])

    assert len(ObjectDoc.all()) == 1

    for group, _ in groups:
        index_identity_group(s, group)
    es.indices.refresh()
    assert len(ObjectDoc.all()) == 3
