"""stac_fastapi.types.search module."""

from typing import Dict, Optional

from pydantic import Field, validator

from stac_fastapi.types.search import BaseSearchPostRequest


class PgstacSearch(BaseSearchPostRequest):
    """Search model.

    Overrides the validation for datetime from the base request model.
    """

    datetime: Optional[str] = None
    filter_lang: Optional[str] = Field(alias="filter-lang")
    conf: Optional[Dict] = None

    @validator("datetime")
    def validate_datetime(cls, v):
        """Pgstac does not require the base validator for datetime."""
        return v
