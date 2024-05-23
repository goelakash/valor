import operator

import pytest
from sqlalchemy import and_, func, not_

from valor_api.backend import Query, models
from valor_api.backend.query.filtering import (
    _recursive_search_logic_tree,
    create_cte,
)
from valor_api.schemas.filters import (
    And,
    Equal,
    IsNull,
    Operands,
    Or,
    Symbol,
    Value,
)


def test_create_cte():
    cte = create_cte(
        opstr="equal",
        symbol=Symbol(
            name="dataset.metadata",
            key="key1",
            attribute="area",
            type="polygon",
        ),
        value=Value(
            type="polygon",
            value=[
                [
                    [0, 0],
                    [0, 1],
                    [2, 0],
                    [0, 0],
                ]
            ],
        ),
    )
    # print(cte)


def test__recursive_search_logic_tree():
    f = And(
        logical_and=[
            IsNull(
                isnull=Symbol(
                    name="annotation.polygon",
                    type="polygon",
                )
            ),
            Equal(
                eq=Operands(
                    lhs=Symbol(
                        name="dataset.metadata",
                        key="some_string",
                        attribute=None,
                        type="string",
                    ),
                    rhs=Value(
                        type="string",
                        value="hello world",
                    ),
                )
            ),
            Or(
                logical_or=[
                    IsNull(
                        isnull=Symbol(
                            name="annotation.polygon",
                            type="polygon",
                        )
                    ),
                    Equal(
                        eq=Operands(
                            lhs=Symbol(
                                name="dataset.metadata",
                                key="some_string",
                                attribute=None,
                                type="string",
                            ),
                            rhs=Value(
                                type="string",
                                value="hello world",
                            ),
                        )
                    ),
                ]
            ),
        ]
    )

    import json

    print()
    print(json.dumps(f.model_dump(), indent=2))
    print()

    from sqlalchemy import distinct

    q = Query(distinct(models.Label.id)).filter(f, pivot=models.Datum)
    print(q)
