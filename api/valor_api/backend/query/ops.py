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
from sqlalchemy.sql.elements import BinaryExpression, ColumnElement

from valor_api.backend.models import (
    Annotation,
    Dataset,
    Datum,
    GroundTruth,
    Label,
    Model,
    Prediction,
)
from valor_api.backend.query.filtering import _recursive_search_logic_tree, generate_logic
from valor_api.backend.query.mapping import map_arguments_to_tables
from valor_api.backend.query.solvers import solve_graph
from valor_api.backend.query.types import TableTypeAlias
from valor_api.schemas.filters import FilterType


class Query:
    """
    Query generator object.

    Attributes
    ----------
    *args : TableTypeAlias | InstrumentedAttribute
        args is a list of models or model attributes. (e.g. Label or Label.key)

    Examples
    ----------
    Querying
    >>> f = schemas.Filter(...)
    >>> q = Query(Label).filter(f).any()

    Querying model attributes.
    >>> f = schemas.Filter(...)
    >>> q = Query(Label.key).filter(f).any()
    """

    def __init__(self, *args):
        self._args = args
        self._selected: set[TableTypeAlias] = map_arguments_to_tables(args)
        self._filtered = set()
        self._expressions: dict[TableTypeAlias, list[ColumnElement[bool]]] = {}

    def select_from(self, *args):
        self._selected = map_arguments_to_tables(args)
        return self

    def filter(self, conditions: FilterType, pivot = Annotation):
        tree, ctes = _recursive_search_logic_tree(conditions)

        if not ctes or not tree:
            raise ValueError

        agg = (
            select(
                pivot.id.label("pivot_id"),
                *[
                    case(
                        (row_id == cte.c.id, 1),
                        else_=0
                    ).label(f"cte{idx}")
                    for idx, (row_id, cte) in enumerate(ctes)
                ],
            )
            .select_from(Annotation)
            .join(Datum, Datum.id == Annotation.datum_id)
            .join(Dataset, Dataset.id == Datum.dataset_id)
        )
        for row_id, cte in ctes:
            if row_id == Label.id:
                gt = alias(GroundTruth)
                agg = agg.join(gt, gt.c.annotation_id == Annotation.id)
                agg = agg.join(cte, cte.c.id == gt.c.label_id, isouter=True)
            else:
                agg = agg.join(cte, cte.c.id == row_id, isouter=True)
        agg = agg.cte()

        q = (
            select(*self._args)
            .select_from(pivot)
        )
        if pivot is Annotation:
            q = q.join(Datum, Datum.id == Annotation.datum_id)
            q = q.join(Dataset, Dataset.id == Datum.dataset_id)
            q = q.join(GroundTruth, GroundTruth.annotation_id == Annotation.id)
            q = q.join(Label, Label.id == GroundTruth.label_id)
            
        elif pivot is Datum:
            q = q.join(Annotation, Annotation.datum_id == Datum.id)
            q = q.join(Dataset, Dataset.id == Datum.dataset_id)
            q = q.join(GroundTruth, GroundTruth.annotation_id == Annotation.id)
            q = q.join(Label, Label.id == GroundTruth.label_id)
    
        q = q.join(agg, agg.c.pivot_id == pivot.id)
        return q.where(generate_logic(agg, tree))
    
    def filter_annotations(self, conditions: FilterType):
        return self.filter(conditions, pivot=Annotation)
    
    def filter_datums(self, conditions: FilterType):
        return self.filter(conditions, pivot=Datum)
