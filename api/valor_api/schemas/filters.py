from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


from valor_api.schemas.validators import (
    validate_type_bool,
    validate_type_box,
    validate_type_date,
    validate_type_datetime,
    validate_type_duration,
    validate_type_float,
    validate_type_integer,
    validate_type_linestring,
    validate_type_multilinestring,
    validate_type_multipoint,
    validate_type_multipolygon,
    validate_type_point,
    validate_type_polygon,
    validate_type_string,
    validate_type_time,
)

def validate_type_symbol(x):
    if not isinstance(x, Symbol):
        raise TypeError

filterable_types_to_validator = {
    "symbol": validate_type_symbol,
    "bool": validate_type_bool,
    "string": validate_type_string,
    "integer": validate_type_integer,
    "float": validate_type_float,
    "datetime": validate_type_datetime,
    "date": validate_type_date,
    "time": validate_type_time,
    "duration": validate_type_duration,
    "point": validate_type_point,
    "multipoint": validate_type_multipoint,
    "linestring": validate_type_linestring,
    "multilinestring": validate_type_multilinestring,
    "polygon": validate_type_polygon,
    "box": validate_type_box,
    "multipolygon": validate_type_multipolygon,
    "tasktypeenum": validate_type_string,
    "label": None,
    "embedding": None,
    "raster": None,
}


class Symbol(BaseModel):
    name: str
    key: str | None = None
    attribute: str | None = None
    dtype: str


class Value(BaseModel):
    type: str
    value: bool | int | float | str | Symbol | list | dict
    model_config = ConfigDict(extra="forbid")

    # @model_validator(mode="after")
    # def _validate_value(self):
    #     self.type = self.type.lower()
    #     if self.type not in filterable_types_to_validator:
    #         raise TypeError(f"Value of type '{self.type}' are not supported.")
    #     validator = filterable_types_to_validator[self.type]
    #     if validator is None:
    #         raise NotImplementedError(
    #             f"A validator for type '{self.type}' has not been implemented."
    #         )
    #     validator(self.value)


class OneArgFunc(BaseModel):
    op: str
    arg: "OneArgFunc | TwoArgFunc | NArgFunc | Value"
    model_config = ConfigDict(extra="forbid")

    @field_validator("op")
    @classmethod
    def _validate_operator(cls, op: str) -> str:
        """Validate the operator."""
        valid_functions = {
            "not",
            "isnull",
            "isnotnull",
        }
        op = op.lower()
        if op not in valid_functions:
            raise NotImplementedError(f"Operator '{op}' is not supported.")
        return op


class TwoArgFunc(BaseModel):
    op: str
    lhs: "OneArgFunc | TwoArgFunc | NArgFunc | Value"
    rhs: "OneArgFunc | TwoArgFunc | NArgFunc | Value"
    model_config = ConfigDict(extra="forbid")

    @field_validator("op")
    @classmethod
    def _validate_operator(cls, op: str) -> str:
        """Validate the operator."""
        valid_functions = {
            "eq",
            "ne",
            "gt",
            "ge",
            "lt",
            "le",
            "intersects",
            "inside",
            "outside",
            "contains",
        }
        op = op.lower()
        if op not in valid_functions:
            raise NotImplementedError(f"Operator '{op}' is not supported.")
        return op


class NArgFunc(BaseModel):
    op: str
    args: list["OneArgFunc | TwoArgFunc | NArgFunc | Value"]
    model_config = ConfigDict(extra="forbid")

    @field_validator("op")
    @classmethod
    def _validate_operator(cls, op: str) -> str:
        """Validate the operator."""
        valid_functions = {
            "and",
            "or",
            "xor",
        }
        op = op.lower()
        if op not in valid_functions:
            raise NotImplementedError(f"Operator '{op}' is not supported.")
        return op


FilterType = OneArgFunc | TwoArgFunc | NArgFunc
