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
    type: str | None = None


class Value(BaseModel):
    type: str
    value: bool | int | float | str | list | dict
    model_config = ConfigDict(extra="forbid")


class Operands(BaseModel):
    lhs: Symbol
    rhs: Value
    model_config = ConfigDict(extra="forbid")


class And(BaseModel):
    logical_and: list["FilterType"]
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def args(self):
        return self.logical_and


class Or(BaseModel):
    logical_or: list["FilterType"]
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def args(self):
        return self.logical_or


class Not(BaseModel):
    logical_not: "FilterType"
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def arg(self):
        return self.logical_not


class IsNull(BaseModel):
    isnull: Symbol
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def arg(self):
        return self.isnull


class IsNotNull(BaseModel):
    isnotnull: Symbol
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def arg(self):
        return self.isnotnull


class Equal(BaseModel):
    eq: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.eq.lhs

    @property
    def rhs(self):
        return self.eq.rhs


class NotEqual(BaseModel):
    ne: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.ne.lhs

    @property
    def rhs(self):
        return self.ne.rhs


class GreaterThan(BaseModel):
    gt: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.gt.lhs

    @property
    def rhs(self):
        return self.gt.rhs


class GreaterThanEqual(BaseModel):
    ge: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.ge.lhs

    @property
    def rhs(self):
        return self.ge.rhs


class LessThan(BaseModel):
    lt: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.lt.lhs

    @property
    def rhs(self):
        return self.lt.rhs


class Intersects(BaseModel):
    intersects: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.intersects.lhs

    @property
    def rhs(self):
        return self.intersects.rhs


class Inside(BaseModel):
    inside: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.inside.lhs

    @property
    def rhs(self):
        return self.inside.rhs


class Outside(BaseModel):
    outside: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.outside.lhs

    @property
    def rhs(self):
        return self.outside.rhs


class Contains(BaseModel):
    contains: Operands
    model_config = ConfigDict(extra="forbid")

    @property
    def op(self) -> str:
        return type(self).__name__.lower()

    @property
    def lhs(self):
        return self.contains.lhs

    @property
    def rhs(self):
        return self.contains.rhs


LogicalType = And | Or | Not
FunctionType = (
    IsNull
    | IsNotNull
    | Equal
    | NotEqual
    | GreaterThan
    | GreaterThanEqual
    | LessThan
    | Intersects
    | Inside
    | Outside
    | Contains
)
FilterType = LogicalType | FunctionType


NArgFunction = And | Or
OneArgFunction = Not | IsNull | IsNotNull
TwoArgFunction = (
    Equal
    | NotEqual
    | GreaterThan
    | GreaterThanEqual
    | LessThan
    | Intersects
    | Inside
    | Outside
    | Contains
)
