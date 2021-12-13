"""stac_fastapi.types.search module."""

from typing import Dict, Optional

from pydantic import root_validator, validator

from stac_fastapi.types.search import BaseSearchPostRequest


class PgstacSearch(BaseSearchPostRequest):
    """Search model.

    Overrides the validation for datetime from the base request model.
    """

    datetime: Optional[str] = None
    conf: Optional[Dict] = None

    @validator("datetime")
    def validate_datetime(cls, v):
        """Pgstac does not require the base validator for datetime."""
        return v

    @root_validator
    def validate_query_uses_cql(cls, values):
        """If using query syntax, forces cql-json."""
        if "query" in values:
            values["filter-lang"] = "cql-json"
