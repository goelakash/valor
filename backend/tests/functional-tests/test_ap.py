import pytest

from velour_api import crud
from velour_api.metrics import compute_ap_metrics
from velour_api.models import (
    LabeledGroundTruthDetection,
    LabeledPredictedDetection,
)
from velour_api.schemas import (
    DatasetCreate,
    GroundTruthDetection,
    GroundTruthDetectionsCreate,
    Image,
    Label,
    Model,
    PredictedDetection,
    PredictedDetectionsCreate,
    ScoredLabel,
)


def bounding_box(xmin, ymin, xmax, ymax) -> list[tuple[int, int]]:
    return [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)]


def round_dict_(d: dict, prec: int) -> None:
    """Modifies a dictionary in place by rounding every float in it
    to three decimal places
    """
    for k, v in d.items():
        if isinstance(v, float):
            d[k] = round(v, prec)
        elif isinstance(v, dict):
            round_dict_(v, prec)


@pytest.fixture
def images() -> list[Image]:
    return [Image(uri=f"{i}") for i in range(4)]


@pytest.fixture
def groundtruths(
    db, images: list[Image]
) -> list[list[LabeledGroundTruthDetection]]:
    dataset_name = "test dataset"
    crud.create_dataset(db, dataset=DatasetCreate(name=dataset_name))
    gts_per_img = [
        {"boxes": [[214.1500, 41.2900, 562.4100, 285.0700]], "labels": ["4"]},
        {
            "boxes": [
                [13.00, 22.75, 548.98, 632.42],
                [1.66, 3.32, 270.26, 275.23],
            ],
            "labels": ["2", "2"],
        },
        {
            "boxes": [
                [61.87, 276.25, 358.29, 379.43],
                [2.75, 3.66, 162.15, 316.06],
                [295.55, 93.96, 313.97, 152.79],
                [326.94, 97.05, 340.49, 122.98],
                [356.62, 95.47, 372.33, 147.55],
                [462.08, 105.09, 493.74, 146.99],
                [277.11, 103.84, 292.44, 150.72],
            ],
            "labels": ["4", "1", "0", "0", "0", "0", "0"],
        },
        {
            "boxes": [
                [72.92, 45.96, 91.23, 80.57],
                [50.17, 45.34, 71.28, 79.83],
                [81.28, 47.04, 98.66, 78.50],
                [63.96, 46.17, 84.35, 80.48],
                [75.29, 23.01, 91.85, 50.85],
                [56.39, 21.65, 75.66, 45.54],
                [73.14, 1.10, 98.96, 28.33],
                [62.34, 55.23, 78.14, 79.57],
                [44.17, 45.78, 63.99, 78.48],
                [58.18, 44.80, 66.42, 56.25],
            ],
            "labels": [
                "49",
                "49",
                "49",
                "49",
                "49",
                "49",
                "49",
                "49",
                "49",
                "49",
            ],
        },
    ]
    db_gts_per_img = [
        [
            GroundTruthDetection(
                boundary=bounding_box(*box),
                labels=[Label(key="class", value=class_label)],
                image=image,
            )
            for box, class_label in zip(gts["boxes"], gts["labels"])
        ]
        for gts, image in zip(gts_per_img, images)
    ]

    created_ids = [
        crud.create_groundtruth_detections(
            db,
            GroundTruthDetectionsCreate(
                dataset_name=dataset_name, detections=gts
            ),
        )
        for gts in db_gts_per_img
    ]
    return [
        [db.get(LabeledGroundTruthDetection, det_id) for det_id in ids]
        for ids in created_ids
    ]


@pytest.fixture
def predictions(
    db, images: list[Image]
) -> list[list[LabeledPredictedDetection]]:
    model_name = "test model"
    crud.create_model(db, Model(name=model_name))

    # predictions for four images taken from
    # https://github.com/Lightning-AI/metrics/blob/107dbfd5fb158b7ae6d76281df44bd94c836bfce/tests/unittests/detection/test_map.py#L59
    preds_per_img = [
        {
            "boxes": [[258.15, 41.29, 606.41, 285.07]],
            "scores": [0.236],
            "labels": ["4"],
        },
        {
            "boxes": [
                [61.00, 22.75, 565.00, 632.42],
                [12.66, 3.32, 281.26, 275.23],
            ],
            "scores": [0.318, 0.726],
            "labels": ["3", "2"],
        },
        {
            "boxes": [
                [87.87, 276.25, 384.29, 379.43],
                [0.00, 3.66, 142.15, 316.06],
                [296.55, 93.96, 314.97, 152.79],
                [328.94, 97.05, 342.49, 122.98],
                [356.62, 95.47, 372.33, 147.55],
                [464.08, 105.09, 495.74, 146.99],
                [276.11, 103.84, 291.44, 150.72],
            ],
            "scores": [0.546, 0.3, 0.407, 0.611, 0.335, 0.805, 0.953],
            "labels": ["4", "1", "0", "0", "0", "0", "0"],
        },
        {
            "boxes": [
                [72.92, 45.96, 91.23, 80.57],
                [45.17, 45.34, 66.28, 79.83],
                [82.28, 47.04, 99.66, 78.50],
                [59.96, 46.17, 80.35, 80.48],
                [75.29, 23.01, 91.85, 50.85],
                [71.14, 1.10, 96.96, 28.33],
                [61.34, 55.23, 77.14, 79.57],
                [41.17, 45.78, 60.99, 78.48],
                [56.18, 44.80, 64.42, 56.25],
            ],
            "scores": [
                0.532,
                0.204,
                0.782,
                0.202,
                0.883,
                0.271,
                0.561,
                0.204,
                0.349,
            ],
            "labels": ["49", "49", "49", "49", "49", "49", "49", "49", "49"],
        },
    ]

    db_preds_per_img = [
        [
            PredictedDetection(
                boundary=bounding_box(*box),
                scored_labels=[
                    ScoredLabel(
                        label=Label(key="class", value=class_label),
                        score=score,
                    )
                ],
                image=image,
            )
            for box, class_label, score in zip(
                preds["boxes"], preds["labels"], preds["scores"]
            )
        ]
        for preds, image in zip(preds_per_img, images)
    ]

    created_ids = [
        crud.create_predicted_detections(
            db,
            PredictedDetectionsCreate(model_name=model_name, detections=preds),
        )
        for preds in db_preds_per_img
    ]
    return [
        [db.get(LabeledPredictedDetection, det_id) for det_id in ids]
        for ids in created_ids
    ]


def test_compute_ap_metrics(
    db,
    groundtruths: list[list[GroundTruthDetection]],
    predictions: list[list[PredictedDetection]],
):
    iou_thresholds = [round(0.5 + 0.05 * i, 2) for i in range(10)]
    metrics = compute_ap_metrics(
        db=db,
        predictions=predictions,
        groundtruths=groundtruths,
        iou_thresholds=iou_thresholds,
    )

    for iou_thres in [i for i in iou_thresholds if i not in [0.5, 0.75]]:
        k = f"IoU={iou_thres}"
        metrics["mAP"].pop(k)
        for class_label in metrics["AP"].keys():
            metrics["AP"][class_label].pop(k)

    round_dict_(metrics, 3)

    # cf with torch metrics/pycocotools results listed here:
    # https://github.com/Lightning-AI/metrics/blob/107dbfd5fb158b7ae6d76281df44bd94c836bfce/tests/unittests/detection/test_map.py#L231

    target = {
        "AP": {
            ("class", "2"): {
                "IoU=0.5": 0.505,
                "IoU=0.75": 0.505,
                "IoU=0.5:0.95": 0.454,
            },
            ("class", "49"): {
                "IoU=0.5": 0.79,
                "IoU=0.75": 0.576,
                "IoU=0.5:0.95": 0.555,
            },
            ("class", "3"): {
                "IoU=0.5": -1.0,
                "IoU=0.75": -1.0,
                "IoU=0.5:0.95": -1.0,
            },
            ("class", "0"): {
                "IoU=0.5": 1.0,
                "IoU=0.75": 0.723,
                "IoU=0.5:0.95": 0.725,
            },
            ("class", "1"): {
                "IoU=0.5": 1.0,
                "IoU=0.75": 1.0,
                "IoU=0.5:0.95": 0.8,
            },
            ("class", "4"): {
                "IoU=0.5": 1.0,
                "IoU=0.75": 1.0,
                "IoU=0.5:0.95": 0.65,
            },
        },
        "mAP": {"IoU=0.5": 0.859, "IoU=0.75": 0.761, "IoU=0.5:0.95": 0.637},
    }

    assert metrics == target