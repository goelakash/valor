import pytest
from sqlalchemy.orm import Session

from valor_api import crud, enums, schemas
from valor_api.backend import models
from valor_api.backend.core import create_or_get_evaluations
from valor_api.backend.metrics.ranking import compute_ranking_metrics

# TODO
# @pytest.fixture
# def label_map():
#     return [
#         [["animal", "dog"], ["class", "mammal"]],
#         [["animal", "cat"], ["class", "mammal"]],
#     ]


@pytest.fixture
def ranking_test_data(
    db: Session,
    dataset_name: str,
    model_name: str,
    groundtruth_ranking: list[schemas.GroundTruth],
    prediction_ranking: list[schemas.Prediction],
):
    crud.create_dataset(
        db=db,
        dataset=schemas.Dataset(
            name=dataset_name,
            metadata={"type": "image"},
        ),
    )
    for gt in groundtruth_ranking:
        crud.create_groundtruth(db=db, groundtruth=gt)
    crud.finalize(db=db, dataset_name=dataset_name)

    crud.create_model(
        db=db,
        model=schemas.Model(
            name=model_name,
            metadata={"type": "image"},
        ),
    )
    for pd in prediction_ranking:
        crud.create_prediction(db=db, prediction=pd)
    crud.finalize(db=db, dataset_name=dataset_name, model_name=model_name)

    assert len(db.query(models.Datum).all()) == 1
    assert len(db.query(models.Annotation).all()) == 2
    assert len(db.query(models.Label).all()) == 2
    assert len(db.query(models.GroundTruth).all()) == 1
    assert len(db.query(models.Prediction).all()) == 1


def test_ranking(
    db: Session,
    dataset_name: str,
    model_name: str,
    ranking_test_data,
):
    # default request
    job_request = schemas.EvaluationRequest(
        model_names=[model_name],
        datum_filter=schemas.Filter(dataset_names=[dataset_name]),
        parameters=schemas.EvaluationParameters(
            task_type=enums.TaskType.RANKING,
        ),
        meta={},
    )

    # creates evaluation job
    evaluations = create_or_get_evaluations(db=db, job_request=job_request)
    assert len(evaluations) == 1
    assert evaluations[0].status == enums.EvaluationStatus.PENDING

    # computation, normally run as background task
    _ = compute_ranking_metrics(
        db=db,
        evaluation_id=evaluations[0].id,
    )

    # get evaluations
    evaluations = create_or_get_evaluations(db=db, job_request=job_request)
    assert len(evaluations) == 1
    assert evaluations[0].status in {
        enums.EvaluationStatus.RUNNING,
        enums.EvaluationStatus.DONE,
    }

    metrics = evaluations[0].metrics

    assert metrics
    for metric in metrics:
        if isinstance(metric, schemas.ROCAUCMetric):
            if metric.label_key == "animal":
                assert metric.value == 0.8009259259259259
            elif metric.label_key == "color":
                assert metric.value == 0.43125