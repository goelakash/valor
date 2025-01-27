from collections import defaultdict

from geoalchemy2.functions import ST_Count, ST_MapAlgebra
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import Select, func, select

from valor_api import enums, schemas
from valor_api.backend import core, models
from valor_api.backend.metrics.metric_utils import (
    create_grouper_mappings,
    create_metric_mappings,
    get_or_create_row,
    log_evaluation_duration,
    log_evaluation_item_counts,
    validate_computation,
)
from valor_api.backend.query import Query
from valor_api.schemas.metrics import IOUMetric, mIOUMetric


def _generate_groundtruth_query(groundtruth_filter: schemas.Filter) -> Select:
    """Generate a sqlalchemy query to fetch a ground truth."""
    return (
        Query(
            models.Annotation.id.label("annotation_id"),
            models.Annotation.datum_id.label("datum_id"),
        )
        .filter(groundtruth_filter)
        .groundtruths("gt")
    )  # type: ignore - SQLAlchemy type issue


def _generate_prediction_query(prediction_filter: schemas.Filter) -> Select:
    """Generate a sqlalchemy query to fetch a prediction."""

    return (
        Query(
            models.Annotation.id.label("annotation_id"),
            models.Annotation.datum_id.label("datum_id"),
        )
        .filter(prediction_filter)
        .predictions("pd")
    )  # type: ignore - SQLAlchemy type issue


def _count_true_positives(
    db: Session,
    groundtruth_subquery: Select,
    prediction_subquery: Select,
) -> int:
    """Computes the pixelwise true positives for the given dataset, model, and label"""
    gt_annotation = aliased(models.Annotation)
    pd_annotation = aliased(models.Annotation)
    ret = db.scalar(
        select(
            func.sum(
                ST_Count(
                    ST_MapAlgebra(
                        gt_annotation.raster,
                        pd_annotation.raster,
                        "[rast1]*[rast2]",  # https://postgis.net/docs/RT_ST_MapAlgebra_expr.html
                    )
                )
            )
        )
        .select_from(groundtruth_subquery)  # type: ignore - SQLAlchemy type issue
        .join(
            prediction_subquery,  # type: ignore - SQLAlchemy type issue
            prediction_subquery.c.datum_id == groundtruth_subquery.c.datum_id,
        )
        .join(
            gt_annotation,
            gt_annotation.id == groundtruth_subquery.c.annotation_id,
        )
        .join(
            pd_annotation,
            pd_annotation.id == prediction_subquery.c.annotation_id,
        )
    )
    if ret is None:
        return 0
    return int(ret)


def _count_groundtruths(db: Session, groundtruth_subquery: Select) -> int:
    """Total number of ground truth pixels for the given dataset and label"""
    ret = db.scalar(
        select(func.sum(ST_Count(models.Annotation.raster)))
        .select_from(groundtruth_subquery)  # type: ignore - SQLAlchemy type issue
        .join(
            models.Annotation,
            models.Annotation.id == groundtruth_subquery.c.annotation_id,
        )
    )
    if ret is None:
        return 0
    return int(ret)


def _count_predictions(
    db: Session,
    prediction_subquery: Select,
) -> int:
    """Total number of predicted pixels for the given dataset, model, and label"""
    ret = db.scalar(
        select(func.sum(ST_Count(models.Annotation.raster)))
        .select_from(prediction_subquery)  # type: ignore - SQLAlchemy type issue
        .join(
            models.Annotation,
            models.Annotation.id == prediction_subquery.c.annotation_id,
        )
    )
    if ret is None:
        return 0
    return int(ret)


def _compute_iou(
    db: Session,
    groundtruth_filter: schemas.Filter,
    prediction_filter: schemas.Filter,
) -> float | None:
    """Computes the pixelwise intersection over union for the given dataset, model, and label"""

    groundtruth_subquery = _generate_groundtruth_query(groundtruth_filter)
    prediction_subquery = _generate_prediction_query(prediction_filter)

    tp_count = _count_true_positives(
        db, groundtruth_subquery, prediction_subquery
    )
    gt_count = _count_groundtruths(
        db,
        groundtruth_subquery=groundtruth_subquery,
    )
    pd_count = _count_predictions(db, prediction_subquery)

    if gt_count == 0:
        return None
    elif pd_count == 0:
        return 0
    return tp_count / (gt_count + pd_count - tp_count)


def _compute_segmentation_metrics(
    db: Session,
    parameters: schemas.EvaluationParameters,
    prediction_filter: schemas.Filter,
    groundtruth_filter: schemas.Filter,
) -> list[IOUMetric | mIOUMetric]:
    """
    Computes segmentation metrics.

    Parameters
    ----------
    db : Session
        The database Session to query against.
    parameters : schemas.EvaluationParameters
        Any user-defined parameters.
    prediction_filter : schemas.Filter
        The filter to be used to query predictions.
    groundtruth_filter : schemas.Filter
        The filter to be used to query groundtruths.

    Returns
    ----------
    List[schemas.IOUMetric | mIOUMetric]
        A list containing one `IOUMetric` for each label in ground truth and one `mIOUMetric` for the mean _compute_IOU over all labels.
    """

    labels = core.fetch_union_of_labels(
        db=db,
        rhs=prediction_filter,
        lhs=groundtruth_filter,
    )

    grouper_mappings = create_grouper_mappings(
        labels=labels,
        label_map=parameters.label_map,
        evaluation_type=enums.TaskType.SEMANTIC_SEGMENTATION,
    )

    ret = []
    ious_per_grouper_key = defaultdict(list)
    for grouper_id, label_ids in grouper_mappings[
        "grouper_id_to_label_ids_mapping"
    ].items():
        # set filter
        groundtruth_filter.label_ids = [label_id for label_id in label_ids]
        prediction_filter.label_ids = [label_id for label_id in label_ids]

        computed_iou_score = _compute_iou(
            db,
            groundtruth_filter,
            prediction_filter,
        )

        # only add an IOUMetric if the label ids associated with the grouper id have at least one gt raster
        if computed_iou_score is None:
            continue

        grouper_label = grouper_mappings[
            "grouper_id_to_grouper_label_mapping"
        ][grouper_id]

        ret.append(
            IOUMetric(
                label=grouper_label,
                value=computed_iou_score,
            )
        )

        ious_per_grouper_key[grouper_label.key].append(computed_iou_score)

    # aggregate IOUs by key
    ret += [
        mIOUMetric(
            value=(
                sum(iou_values) / len(iou_values)
                if len(iou_values) != 0
                else -1
            ),
            label_key=grouper_key,
        )
        for grouper_key, iou_values in ious_per_grouper_key.items()
    ]

    return ret


@validate_computation
def compute_semantic_segmentation_metrics(
    *,
    db: Session,
    evaluation_id: int,
) -> int:
    """
    Create semantic segmentation metrics. This function is intended to be run using FastAPI's `BackgroundTasks`.

    Parameters
    ----------
    db : Session
        The database Session to query against.
    evaluation_id : int
        The job ID to create metrics for.
    """

    # fetch evaluation
    evaluation = core.fetch_evaluation_from_id(db, evaluation_id)

    # unpack filters and params
    groundtruth_filter = schemas.Filter(**evaluation.datum_filter)
    prediction_filter = groundtruth_filter.model_copy()
    prediction_filter.model_names = [evaluation.model_name]
    parameters = schemas.EvaluationParameters(**evaluation.parameters)

    # load task type into filters
    groundtruth_filter.task_types = [parameters.task_type]
    prediction_filter.task_types = [parameters.task_type]

    log_evaluation_item_counts(
        db=db,
        evaluation=evaluation,
        prediction_filter=prediction_filter,
        groundtruth_filter=groundtruth_filter,
    )

    metrics = _compute_segmentation_metrics(
        db=db,
        parameters=parameters,
        prediction_filter=prediction_filter,
        groundtruth_filter=groundtruth_filter,
    )
    metric_mappings = create_metric_mappings(db, metrics, evaluation_id)
    for mapping in metric_mappings:
        # ignore value since the other columns are unique identifiers
        # and have empirically noticed value can slightly change due to floating
        # point errors
        get_or_create_row(
            db,
            models.Metric,
            mapping,
            columns_to_ignore=["value"],
        )

    log_evaluation_duration(
        evaluation=evaluation,
        db=db,
    )

    return evaluation_id
