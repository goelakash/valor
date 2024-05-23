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
    case,
    alias,
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
    GroundTruth,
)
from valor_api.backend.query.types import TableTypeAlias
from valor_api.schemas import Duration, Time
from valor_api.schemas.filters import (
    NArgFunc,
    OneArgFunc,
    Symbol,
    TwoArgFunc,
    Value,
    FilterType,
)
from valor_api.schemas.geometry import (
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    Box,
    MultiPolygon,
)


category_to_supported_operations = {
    "nullable": {"isnull", "isnotnull"},
    "equatable": {"eq", "ne"},
    "quantifiable": {"gt", "ge", "lt", "le"},
    "spatial": {"intersects", "inside", "outside"},
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
    "isnull": lambda lhs, _: lhs.is_(None),
    "isnotnull": lambda lhs, _: lhs.isnot(None),
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
    "raster": {"spatial"},
    "tasktypeenum": {"equatable"},
    "label": {"equatable"},
    "embedding": {},
}


symbol_name_to_categories = {
    "dataset.name": {"equatable"},
    "dataset.metadata": {},
    "model.name": {"equatable"},
    "model.metadata": {},
    "datum.uid": {"equatable"},
    "datum.metadata": {},
    "annotation.box": {"spatial", "nullable"},
    "annotation.polygon": {"spatial", "nullable"},
    "annotation.raster": {"spatial", "nullable"},
    "annotation.embedding": {},
    "annotation.metadata": {},
    "annotation.labels": {"equatable"},
    "label.key": {"equatable"},
    "label.value": {"equatable"},
}


symbol_attribute_to_categories = {
    "area": {"equatable", "quantifiable"},
}

symbol_name_to_row_id_value = {
    "dataset.name": (Dataset.id, Dataset.name),
    "dataset.metadata": (Dataset.id, Dataset.meta),
    "model.name": (Dataset.id, Dataset.name),
    "model.metadata": (Dataset.id, Dataset.meta),
    "datum.uid": (Datum.id, Datum.uid),
    "datum.metadata": (Datum.id, Datum.meta),
    "annotation.box": (Annotation.id, Annotation.box),
    "annotation.polygon": (Annotation.id, Annotation.polygon),
    "annotation.raster": (Annotation.id, Annotation.raster),
    "annotation.embedding": (Embedding.id, Embedding.value),
    "annotation.labels": (Label.id, Label),
    "annotation.metadata": (Annotation.id, Annotation.meta),
    "label.key": (Label.id, Label.key),
    "label.value": (Label.id, Label.value),
}

symbol_supports_attribute = {
    "area": {
        "annotation.box": lambda x: ST_Area(x),
        "annotation.polygon": lambda x: ST_Area(x),
        "annotation.raster": lambda x: ST_Count(x),
        "dataset.metadata": lambda x: ST_Area(x),
        "model.metadata": lambda x: ST_Area(x),
        "datum.metadata": lambda x: ST_Area(x),
        "annotation.metadata": lambda x: ST_Area(x),
    }
}

symbol_supports_key = {
    "dataset.metadata",
    "model.metadata",
    "datum.metadata",
    "annotation.metadata",
}

metadata_symbol_type_casting = {
    "bool": lambda x: x.astext.cast(Boolean),
    "integer": lambda x: x.astext.cast(Integer),
    "float": lambda x: x.astext.cast(Float),
    "string": lambda x: x.astext,
    "datetime": lambda x: cast(x, TIMESTAMP(timezone=True)),
    "date": lambda x: cast(x, TIMESTAMP(timezone=True)),
    "time": lambda x: cast(x, INTERVAL),
    "duration": lambda x: cast(x, INTERVAL),
    "point": lambda x: ST_GeomFromGeoJSON(Point),
    "multipoint": lambda x: ST_GeomFromGeoJSON(x),
    "linestring": lambda x: ST_GeomFromGeoJSON(x),
    "multilinestring": lambda x: ST_GeomFromGeoJSON(x),
    "polygon": lambda x: ST_GeomFromGeoJSON(x),
    "box": lambda x: ST_GeomFromGeoJSON(x),
    "multipolygon": lambda x: ST_GeomFromGeoJSON(x),
}

value_dtype_to_casting = {
    "bool": lambda x: x,
    "integer": lambda x: x,
    "float": lambda x: x,
    "string": lambda x: x,
    "datetime": lambda x: cast(x, TIMESTAMP(timezone=True)),
    "date": lambda x: cast(x, TIMESTAMP(timezone=True)),
    "time": lambda x: cast(x, INTERVAL),
    "duration": lambda x: cast(cast(x, TEXT), INTERVAL),
    "point": lambda x: ST_GeomFromGeoJSON(Point(value=x).to_dict()),
    "multipoint": lambda x: ST_GeomFromGeoJSON(MultiPoint(value=x).to_dict()),
    "linestring": lambda x: ST_GeomFromGeoJSON(LineString(value=x).to_dict()),
    "multilinestring": lambda x: ST_GeomFromGeoJSON(MultiLineString(value=x).to_dict()),
    "polygon": lambda x: ST_GeomFromGeoJSON(Polygon(value=x).to_dict()),
    "box": lambda x: ST_GeomFromGeoJSON(Box(value=x).to_dict()),
    "multipolygon": lambda x: ST_GeomFromGeoJSON(MultiPolygon(value=x).to_dict()),
}


# @TODO - Need to implement safeguards
def create_cte(opstr: str, symbol: Symbol, value: Value | None = None):
    if not isinstance(symbol, Symbol):
        raise ValueError
    elif not isinstance(value, Value) and value is not None:
        raise ValueError
    elif value and symbol.dtype != value.type:
        raise ValueError
        
    op = opstr_to_operator[opstr]
    row_id, lhs = symbol_name_to_row_id_value[symbol.name]
    rhs = value_dtype_to_casting[value.type](value.value) if value else None

    if symbol.key:
        lhs = lhs[symbol.key]  # add keying
        lhs = metadata_symbol_type_casting[symbol.dtype](lhs)  # add type cast

    if symbol.attribute:
        modifier = symbol_supports_attribute[symbol.attribute][symbol.name]
        lhs = modifier(lhs)  # add attribute modifier
        rhs = modifier(rhs)  # add attribute modifier

    return (row_id, select(row_id).where(op(lhs, rhs)).cte())


def _recursive_search_logic_tree(func: OneArgFunc | TwoArgFunc | NArgFunc, cte_list: list | None = None):

    if not isinstance(func, OneArgFunc | TwoArgFunc | NArgFunc):
        raise ValueError
    if cte_list is None:
        cte_list = list()
    logical_tree = dict()
    
    if isinstance(func, OneArgFunc):
        if isinstance(func.arg, Value):
            if not isinstance(func.arg.value, Symbol):
                raise ValueError  # one arg functions that are not symbolic dont make sense as filters
            cte_list.append(create_cte(opstr=func.op, symbol=func.arg.value))
            return (len(cte_list)-1, cte_list)
        else:
            branch, cte_list = _recursive_search_logic_tree(func.arg, cte_list)
            logical_tree[func.op] = branch
            return (logical_tree, cte_list)

    elif isinstance(func, TwoArgFunc):
        if isinstance(func.lhs, Value) and isinstance(func.rhs, Value):
            if not isinstance(func.lhs.value, Symbol):
                raise ValueError
            if isinstance(func.rhs.value, Symbol):
                raise ValueError
            lhs = func.lhs.value
            rhs = func.rhs
            cte_list.append(create_cte(opstr=func.op, symbol=lhs, value=rhs))
            return (len(cte_list)-1, cte_list)
        elif not isinstance(func.lhs, Value) and not isinstance(func.rhs, Value):
            lhs_branch, cte_list = _recursive_search_logic_tree(func.lhs, cte_list)
            rhs_branch, cte_list = _recursive_search_logic_tree(func.rhs, cte_list)
            logical_tree[func.op] = {
                "lhs": lhs_branch,
                "rhs": rhs_branch,
            }
            return (logical_tree, cte_list)
        else:
            raise ValueError
    
    elif isinstance(func, NArgFunc):
        branches = list()
        for arg in func.args:
            if isinstance(arg, Value):
                raise ValueError # should only contain expressions
            branch, cte_list = _recursive_search_logic_tree(arg, cte_list)
            branches.append(branch)
        logical_tree[func.op] = branches
        return (logical_tree, cte_list)


logical_operators = {
    "and": and_,
    "or": or_,
    "not": not_,
}

def generate_logic(root, tree: int | dict[str, int | dict | list]):
    if isinstance(tree, int):
        return getattr(root.c, f"cte{tree}") == 1
    if not isinstance(tree, dict) or len(tree.keys()) != 1:
        raise ValueError
    
    op = list(tree.keys())[0]
    if op == "and" or op == "or":
        args = tree[op]
        if not isinstance(args, list):
            raise ValueError
        return logical_operators[op](
            *[
                (getattr(root.c, f"cte{arg}") == 1)
                if isinstance(arg, int)
                else generate_logic(root, arg)
                for arg in args
            ]
        )
    elif op == "not":
        arg = tree["not"]
        if isinstance(arg, list):
            raise ValueError
        return (
            (getattr(root.c, f"cte{arg}") == 1)
            if isinstance(arg, int)
            else generate_logic(root, arg)
        )
    else:
        raise ValueError
