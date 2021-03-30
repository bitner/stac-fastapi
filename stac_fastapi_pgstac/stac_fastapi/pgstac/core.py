"""Item crud client."""
import json
import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Type, Union
from urllib.parse import urlencode, urljoin

import attr
from stac_pydantic import Collection
from stac_pydantic.api import ConformanceClasses, LandingPage
from stac_pydantic.api.extensions.paging import PaginationLink
from stac_pydantic.shared import Link, MimeTypes, Relations

from stac_fastapi.extensions.core import ContextExtension, FieldsExtension
from stac_fastapi.types.config import Settings
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.errors import NotFoundError
from stac_fastapi.types.search import STACSearch

logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CoreCrudClient(BaseCoreClient):
    """Client for core endpoints defined by stac."""

    def landing_page(self, **kwargs) -> LandingPage:
        """Landing page.

        Called with `GET /`.

        Returns:
            API landing page, serving as an entry point to the API.
        """
        return None


    def post_search(self, search_request, **kwargs) -> Dict[str, Any]:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        return None

    def get_search(
        self,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Cross catalog search (GET).

        Called with `GET /search`.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        return None

    def get_item(self, id: str, **kwargs):
        """Get item by id.

        Called with `GET /collections/{collectionId}/items/{itemId}`.

        Args:
            id: Id of the item.

        Returns:
            Item.
        """
        return None



    def get_collection(self, id: str, **kwargs):
        """Get collection by id.

        Called with `GET /collections/{collectionId}`.

        Args:
            id: Id of the collection.

        Returns:
            Collection.
        """
        return None

    def item_collection(
        self, id: str, limit: int = 10, token: str = None, **kwargs
    ):
        """Get all items from a specific collection.

        Called with `GET /collections/{collectionId}/items`

        Args:
            id: id of the collection.
            limit: number of items to return.
            token: pagination token.

        Returns:
            An ItemCollection.
        """
        return None

    def conformance(self, **kwargs) -> ConformanceClasses:
        """Conformance classes."""
        return ConformanceClasses(
            conformsTo=[
                "https://stacspec.org/STAC-api.html",
                "http://docs.opengeospatial.org/is/17-069r3/17-069r3.html#ats_geojson",
            ]
        )

    async def all_collections(self, **kwargs):
        """Read all collections from the database."""
        pool = kwargs["request"].app.state.readpool
        response = []
        async with pool.acquire() as conn:
            collections = await conn.fetch(
                """
                SELECT * FROM get_collections();
                """
            )

        return collections
