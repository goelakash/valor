import json

from pydantic import (
    BaseModel,
    ConfigDict,
    create_model,
    field_validator,
    model_validator,
)

from valor_api.enums import TaskType
from valor_api.schemas.geometry import GeoJSON
from valor_api.schemas.timestamp import Date, DateTime, Duration, Time

category_to_supported_functions = {
    "equatable": {"eq", "ne"},
    "quantifiable": {"eq", "ne", "gt", "ge", "lt", "le"},
    "spatial": {"intersects", "inside", "outside"},
}


filterable_types_to_function_category = {
    "bool": {"equatable"},
    "string": {"equatable"},
    "integer": {"equatable", "quantifiable"},
    "float": {"equatable", "quantifiable"},
    "datetime": {"equatable", "quantifiable"},
    "date": {"equatable", "quantifiable"},
    "time": {"equatable", "quantifiable"},
    "duration": {"equatable", "quantifiable"},
    "point": {"equatable", "spatial"},
    "multipoint": {"spatial"},
    "linestring": {"spatial"},
    "multilinestring": {"spatial"},
    "polygon": {"spatial"},
    "box": {"spatial"},
    "multipolygon": {"spatial"},
    "tasktypeenum": {"equatable"},
    "raster": {"spatial"},
    "label": {"equatable"},
    "embedding": {},
}


class Value(BaseModel):
    type: str
    value: bool | int | float | str | list | dict
    model_config = ConfigDict(extra="forbid")

    @field_validator("type")
    @classmethod
    def _validate_type_string(cls, type_: str) -> str:
        """Validate the type string."""
        if type_ != "bool":
            raise ValueError(
                f"Recieved value with type '{type(bool)}' with a declared typing of '{type_}'."
            )
        return type_


class OneArgFunc(BaseModel):
    op: str
    arg: "Value | OneArgFunc | TwoArgFunc | NArgFunc"
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
    lhs: "Value | OneArgFunc | TwoArgFunc | NArgFunc"
    rhs: "Value | OneArgFunc | TwoArgFunc | NArgFunc"
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
    args: list["Value | OneArgFunc | TwoArgFunc | NArgFunc"]
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
