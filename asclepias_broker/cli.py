# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# Asclepias Broker is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
"""CLI commands."""

import click
from flask.cli import with_appcontext
from .mappings.dsl import ObjectDoc, ObjectRelationshipsDoc
import json


@click.group('utils')
def cli():
    pass


@cli.command()
@with_appcontext
def dump_mappings():
    """Dump Elasticsearch mappings."""
    click.secho(json.dumps(
        {'mappings': ObjectDoc._doc_type.mapping.to_dict()},
        indent=2, separators=(', ', ': '), sort_keys=True), fg='blue')
    click.secho(json.dumps(
        {'mappings': ObjectRelationshipsDoc._doc_type.mapping.to_dict()},
        indent=2, separators=(', ', ': '), sort_keys=True), fg='blue')
