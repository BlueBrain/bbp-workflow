# SPDX-License-Identifier: Apache-2.0

"""Create entities in nexus, with an attached file, but only once.

ex:
    contents = json.dumps({'an': 'amazing', 'configution': 'format'})
    resource = create_or_find(properties={'some': 'pro', 'per': 'ties'},
                              contents=contents,
                              filename='amazing-conf.json',
                              content_type='application/json')
"""

import hashlib
import io
from urllib.error import HTTPError

from entity_management.core import DataDownload, WorkflowExecution
from entity_management.nexus import sparql_query
from entity_management.settings import WORKFLOW
from entity_management.util import get_entity

from bbp_workflow.settings import L


def _create_entity(
    entity_cls,
    properties: dict,
    serialized_content: bytes,
    filename: str,
    content_type: str,
):
    buffer = io.BytesIO(serialized_content)

    distribution = DataDownload.from_file(
        file_like=buffer,
        name=filename,
        content_type=content_type,
    )
    workflow = get_entity(WORKFLOW, cls=WorkflowExecution)

    entity_content = properties | {"distribution": distribution, "wasGeneratedBy": workflow}

    # To soothe some definitions that derive from Entity which has a mandatory 'name' attr
    if "name" not in entity_content:
        entity_content["name"] = entity_cls.__name__

    entity = entity_cls(**entity_content)
    return entity.publish()


def _find_owning_resource_by_file_digest(digest, entity_cls):
    """Find entity that has distribution with the correct file digest.

    When a file is stored in Nexus, the relationship is:
    Entity with a `distribution` property, that points to a file
    The file is registered with SHA-256 of the content, and that is how
    this function looks it up; by digest.
    """
    entity_type = f"bmo:{entity_cls.__name__}"
    assert WORKFLOW
    query = f"""
        PREFIX sch: <http://schema.org/>
        PREFIX nsh: <https://neuroshapes.org/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX bmo: <https://bbp.epfl.ch/ontologies/core/bmo/>
        PREFIX nxv: <https://bluebrain.github.io/nexus/vocabulary/>

        SELECT DISTINCT ?id
        WHERE {{
            ?id a {entity_type} ;
                nxv:deprecated   false ;
                prov:wasGeneratedBy <{WORKFLOW}> ;
                sch:distribution / nsh:digest / sch:value '{digest}'
        }}
        LIMIT 1
    """
    try:
        json_payload = sparql_query(query)
    except HTTPError as e:
        raise RuntimeError(f"Query:\n{query}") from e
    for r in json_payload["results"]["bindings"]:
        yield get_entity(resource_id=r["id"]["value"], cls=entity_cls)


def _match_resource(resource, properties: dict) -> bool:
    """Check to see if all the expected properties match, as well as the type."""
    for k, v in properties.items():
        if getattr(resource, k) != v:
            return False
    return True


def create_or_find(
    entity_cls,
    properties: dict,
    serialized_content: str | bytes,
    filename: str,
    content_type: str = "application/json",
):
    """Create or find a nexus entity with a file, aka distribution.

    Args:
        forge: KGForge instance
        config_type(str): name of the entity
        properties(dict): properties of the entity keys/values must be str -> (int | float | str)
        contents(str): contents of the config file
        filename(str): filename to use when uploading the contents
        content_type(str): set the content type
    """
    if isinstance(serialized_content, str):
        serialized_content = serialized_content.encode()

    digest = hashlib.sha256(serialized_content).hexdigest()
    L.debug("Resource %s content digest: %s", entity_cls, digest)

    for entity in _find_owning_resource_by_file_digest(digest, entity_cls):
        if _match_resource(entity, properties):
            L.debug("Matched resource: %s", entity.get_id())
            return entity

    entity = _create_entity(entity_cls, properties, serialized_content, filename, content_type)
    L.debug("No existing resources found. Created: %s", entity.get_id())
    return entity
