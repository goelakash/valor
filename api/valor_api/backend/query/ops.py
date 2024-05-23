from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Float,
    Integer,
    alias,
    and_,
    case,
    cast,
    func,
    not_,
    or_,
    select,
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
from valor_api.backend.query.filtering import (
    _recursive_search_logic_tree,
    generate_logic,
)
from valor_api.backend.query.mapping import map_arguments_to_tables
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

    def filter(
        self,
        conditions: FilterType | None,
        pivot=Annotation,
        is_groundtruth: bool = True,
    ):
        gt_or_pd = GroundTruth if is_groundtruth else Prediction
        tree, ctes = _recursive_search_logic_tree(conditions)
        if not ctes or not tree:
            raise ValueError
        agg = (
            select(
                pivot.id.label("pivot_id"),
                *[
                    case((row_id == cte.c.id, 1), else_=0).label(f"cte{idx}")
                    for idx, (row_id, cte) in enumerate(ctes)
                ],
            )
            .select_from(Annotation)
            .join(Datum, Datum.id == Annotation.datum_id)
            .join(Dataset, Dataset.id == Datum.dataset_id)
        )
        agg = self.filter(conditions, pivot)

        for row_id, cte in ctes:
            if row_id == Label.id:
                label_linker = alias(gt_or_pd)
                agg = agg.join(
                    label_linker, label_linker.c.annotation_id == Annotation.id
                )
                agg = agg.join(
                    cte, cte.c.id == label_linker.c.label_id, isouter=True
                )
            else:
                agg = agg.join(cte, cte.c.id == row_id, isouter=True)
        agg = agg.cte()

        q = select(*self._args).select_from(pivot)
        if pivot is Annotation:
            q = q.join(Datum, Datum.id == Annotation.datum_id)
            q = q.join(Dataset, Dataset.id == Datum.dataset_id)
            q = q.join(gt_or_pd, gt_or_pd.annotation_id == Annotation.id)
            q = q.join(Label, Label.id == gt_or_pd.label_id)

        elif pivot is Datum:
            q = q.join(Annotation, Annotation.datum_id == Datum.id)
            q = q.join(Dataset, Dataset.id == Datum.dataset_id)
            q = q.join(gt_or_pd, gt_or_pd.annotation_id == Annotation.id)
            q = q.join(Label, Label.id == gt_or_pd.label_id)

        q = q.join(agg, agg.c.pivot_id == pivot.id)
        return q.where(generate_logic(agg, tree))

    def filter_groundtruths(
        self, conditions: FilterType | None, pivot=Annotation
    ):
        return self.filter(
            conditions=conditions, pivot=pivot, is_groundtruth=False
        )

    def filter_predictions(
        self, conditions: FilterType | None, pivot=Annotation
    ):
        return self.filter(
            conditions=conditions, pivot=pivot, is_groundtruth=False
        )
