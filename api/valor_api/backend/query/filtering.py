import operator
from typing import Callable

from geoalchemy2.functions import ST_Area, ST_Count, ST_GeomFromGeoJSON
from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Float,
    Integer,
    and_,
    cast,
    func,
    not_,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import INTERVAL, TEXT
from sqlalchemy.sql.elements import BinaryExpression, ColumnElement

from valor_api import enums
from valor_api.backend.models import (
    Annotation,
    Dataset,
    Datum,
    Embedding,
    Label,
    Model,
    Prediction,
)
from valor_api.backend.query.types import TableTypeAlias
from valor_api.schemas import Duration, Time
from valor_api.schemas.filters import (
    NArgFunc,
    OneArgFunc,
    Symbol,
    TwoArgFunc,
    Value,
)

filterable_types_to_function_category = {
    "bool": {"eq", "ne"},
    "string": {"eq", "ne"},
    "integer": {"eq", "ne", "gt", "ge", "lt", "le"},
    "float": {"eq", "ne", "gt", "ge", "lt", "le"},
    "datetime": {"eq", "ne", "gt", "ge", "lt", "le"},
    "date": {"eq", "ne", "gt", "ge", "lt", "le"},
    "time": {"eq", "ne", "gt", "ge", "lt", "le"},
    "duration": {"eq", "ne", "gt", "ge", "lt", "le"},
    "point": {"eq", "ne", "intersects", "inside", "outside"},
    "multipoint": {"intersects", "inside", "outside"},
    "linestring": {"intersects", "inside", "outside"},
    "multilinestring": {"intersects", "inside", "outside"},
    "polygon": {"intersects", "inside", "outside"},
    "box": {"intersects", "inside", "outside"},
    "multipolygon": {"intersects", "inside", "outside"},
    "raster": {"intersects", "inside", "outside"},
    "tasktypeenum": {"eq", "ne"},
    "label": {"eq", "ne"},
    "embedding": {},
}


opstr_to_operator = {
    "eq": operator.eq,
    "ne": operator.ne,
    "gt": operator.gt,
    "ge": operator.ge,
    "lt": operator.lt,
    "le": operator.le,
    "intersect": lambda lhs, rhs: func.ST_Intersects(lhs, rhs),
    "inside": lambda lhs, rhs: func.ST_Covers(rhs, lhs),
    "outside": lambda lhs, rhs: not_(func.ST_Covers(rhs, lhs)),
    "contains": None,
    "isnull": lambda arg : arg.is_(None),
    "isnotnull": lambda arg : arg.isnot(None),
}


symbol_name_to_table = {
    "dataset.name": Dataset,
    "dataset.metadata": Dataset,
    "model.name": Model,
    "model.metadata": Model,
    "datum.uid": Datum,
    "datum.metadata": Datum,
    "annotation.box": Annotation,
    "annotation.polygon": Annotation,
    "annotation.raster": Annotation,
    "annotation.embedding": Embedding,
    "annotation.metadata": Annotation,
    "annotation.labels": Label,
    "label.key": Label,
    "label.value": Label,
}


symbol_to_value_column = {
    "dataset.name": Dataset.name,
    "dataset.metadata": Dataset.meta,
    "model.name": Dataset.name,
    "model.metadata": Dataset.meta,
    "datum.uid": Datum.uid,
    "datum.metadata": Datum.meta,
    "annotation.box": Annotation.box,
    "annotation.polygon": Annotation.polygon,
    "annotation.raster": Annotation.raster,
    "annotation.embedding": Embedding.value,
    "annotation.labels": Label,
    "annotation.metadata": Annotation.meta,
    "label.key": Label.key,
    "label.value": Label.value,
}

symbol_supports_attribute = {
    "area": {
        "annotation.box": lambda x : ST_Area(x),
        "annotation.polygon": lambda x : ST_Area(x),
        "annotation.raster": lambda x : ST_Count(x),
        "dataset.metadata": lambda x : ST_Area(x),
        "model.metadata": lambda x : ST_Area(x),
        "datum.metadata": lambda x : ST_Area(x),
        "annotation.metadata": lambda x : ST_Area(x),
    }
}

symbol_supports_key = {
    "dataset.metadata",
    "model.metadata",
    "datum.metadata",
    "annotation.metadata",
}

symbol_dtype_to_casting = {
    "bool": lambda x : x.astext.cast(Boolean),
    "integer": lambda x : x.astext.cast(Integer),
    "float": lambda x : x.astext.cast(Float),
    "string": lambda x : x.astext,
    "datetime": lambda x : cast(x, TIMESTAMP(timezone=True)),
    "date": lambda x : cast(x, TIMESTAMP(timezone=True)),
    "time": lambda x : cast(x, INTERVAL),
    "duration": lambda x : cast(x, INTERVAL),
    "point": lambda x : ST_GeomFromGeoJSON(x),
    "multipoint": lambda x : ST_GeomFromGeoJSON(x),
    "linestring": lambda x : ST_GeomFromGeoJSON(x),
    "multilinestring": lambda x : ST_GeomFromGeoJSON(x),
    "polygon": lambda x : ST_GeomFromGeoJSON(x),
    "box": lambda x : ST_GeomFromGeoJSON(x),
    "multipolygon": lambda x : ST_GeomFromGeoJSON(x),
    "raster": lambda x : ST_GeomFromGeoJSON(x),
}

value_dtype_to_casting = {
    "bool": lambda x : x,
    "integer": lambda x : x,
    "float": lambda x : x,
    "string": lambda x : x,
    "datetime": lambda x : cast(x, TIMESTAMP(timezone=True)),
    "date": lambda x : cast(x, TIMESTAMP(timezone=True)),
    "time": lambda x : cast(x, INTERVAL),
    "duration": lambda x : cast(cast(x, TEXT), INTERVAL),
    "point": lambda x : ST_GeomFromGeoJSON(x),
    "multipoint": lambda x : ST_GeomFromGeoJSON(x),
    "linestring": lambda x : ST_GeomFromGeoJSON(x),
    "multilinestring": lambda x : ST_GeomFromGeoJSON(x),
    "polygon": lambda x : ST_GeomFromGeoJSON(x),
    "box": lambda x : ST_GeomFromGeoJSON(x),
    "multipolygon": lambda x : ST_GeomFromGeoJSON(x),
    "raster": lambda x : ST_GeomFromGeoJSON(x),
}


def convert_symbol_to_sql(symbol: Symbol):
    if not isinstance(symbol, Symbol):
        raise TypeError

    # get table column
    if symbol.name not in symbol_to_value_column:
        raise ValueError
    col = symbol_to_value_column[symbol.name]

    if symbol.key:
        # add keying
        if symbol.name not in symbol_supports_key:
            raise ValueError
        col = col[symbol.key]

        # type cast
        if symbol.dtype not in symbol_dtype_to_casting:
            raise ValueError
        col = symbol_dtype_to_casting[symbol.dtype](col)

    # add attribute modifier
    if symbol.attribute:
        if symbol.attribute not in symbol_supports_attribute:
            raise ValueError
        if symbol.name not in symbol_supports_attribute[symbol.attribute]:
            raise ValueError
        col = symbol_supports_attribute[symbol.attribute][symbol.name](col)

    return col


def convert_value_to_sql(value: Value):
    if not isinstance(value, Value):
        raise TypeError
    elif isinstance(value.value, Symbol):
        return convert_symbol_to_sql(value.value)




# def create_cte_from_one_arg_func(op: str, arg: Value):
#     if op not in filterable_types_to_function_category[arg.type]:
#         raise NotImplementedError(f"Operator '{op}' not implemented for type '{arg.type}'.")





#     if key:
#         if name not in symbol_supports_key:
#             raise ValueError
#         col = col[key]
#     if attr:
#         if attr not in symbol_supports_attribute:
#             raise ValueError
#         elif name not in symbol_supports_attribute[attr]
#             raise ValueError
#         col =



#     row_id = symbol_name_to_id_column[name]
#     row_value = symbol_name_to_value_column[name]

#     if name in symbol_represents_jsonb:
#         if not key:
#             raise ValueError
#         row_value = row_value[key]

#     elif key:
#         raise ValueError


#     elif


#     return select(id).where(opstr_to_operator[op](value)).cte()


def _recursive_search_logic_tree(func: OneArgFunc | TwoArgFunc | NArgFunc):
    if isinstance(func, OneArgFunc):
        arg = func.arg
        if isinstance(arg, Value):
            return select().where().cte()
            arg = _recursive_search_logic_tree(arg)
        return arg
    elif isinstance(func, TwoArgFunc):
        lhs = func.lhs
        rhs = func.rhs
        if isinstance(lhs, Value):
            lhs = _recursive_search_logic_tree(lhs)
        if not isinstance(rhs, Value):
            rhs = _recursive_search_logic_tree(rhs)
        return (lhs, rhs)
    elif isinstance(func, NArgFunc):
        args = [
            arg
            if isinstance(arg, Value)
            else _recursive_search_logic_tree(arg)
            for arg in func.args
        ]
        return args


def create_logic_tree(filter_):
    pass

