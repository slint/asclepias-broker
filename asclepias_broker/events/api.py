# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# Asclepias Broker is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
"""Events API."""

import jsonschema
from flask import current_app
from invenio_db import db
from marshmallow.exceptions import \
    ValidationError as MarshmallowValidationError

from ..graph.tasks import process_event
from ..jsonschemas import EVENT_SCHEMA
from ..schemas.loaders import RelationshipSchema
from .models import Event, EventStatus


class EventAPI:
    """Event API."""

    @classmethod
    def handle_event(cls, event: dict, no_index: bool = False,
                     user_id: int = None, eager: bool = False) -> Event:
        """Handle an event payload."""
        # Raises JSONSchema ValidationError
        jsonschema.validate(event, EVENT_SCHEMA)

        # Validate the entries in the payload
        for payload in event:
            errors = RelationshipSchema(check_existing=True).validate(payload)
            if errors:
                raise MarshmallowValidationError(errors)

        event_obj = Event(payload=event, status=EventStatus.New,
                          user_id=user_id)
        db.session.add(event_obj)
        db.session.commit()
        event_uuid = str(event_obj.id)
        idx_enabled = current_app.config['ASCLEPIAS_SEARCH_INDEXING_ENABLED'] \
            and (not no_index)
        task = process_event.s(
            event_uuid=event_uuid, indexing_enabled=idx_enabled)
        if eager:
            task.apply(throw=True)
        else:
            task.apply_async()
        return event_obj
