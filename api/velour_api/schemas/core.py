import io
import re
from base64 import b64decode

import PIL.Image
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from velour_api import enums
from velour_api.schemas.geojson import GeoJSON
from velour_api.schemas.geometry import (
    BoundingBox,
    MultiPolygon,
    Polygon,
    Raster,
)
from velour_api.schemas.label import Label


def _format_name(name: str):
    allowed_special = ["-", "_"]
    pattern = re.compile(f"[^a-zA-Z0-9{''.join(allowed_special)}]")
    return re.sub(pattern, "", name)


def _format_uid(uid: str):
    allowed_special = ["-", "_", "/", "."]
    pattern = re.compile(f"[^a-zA-Z0-9{''.join(allowed_special)}]")
    return re.sub(pattern, "", uid)


class MetaDatum(BaseModel):
    key: str
    value: float | str | GeoJSON

    @field_validator("key")
    @classmethod
    def check_key(cls, v):
        if not isinstance(v, str):
            raise ValueError
        return v

    @property
    def string_value(self) -> str | None:
        if isinstance(self.value, str):
            return self.value
        return None

    @property
    def numeric_value(self) -> float | None:
        if isinstance(self.value, float):
            return self.value
        return None


class Dataset(BaseModel):
    id: int | None = None
    name: str
    metadata: list[MetaDatum] = []

    @field_validator("name")
    @classmethod
    def check_name_valid(cls, v):
        if v != _format_name(v):
            raise ValueError("name includes illegal characters.")
        if not v:
            raise ValueError("invalid string")
        return v


class Model(BaseModel):
    id: int | None = None
    name: str
    metadata: list[MetaDatum] = []

    @field_validator("name")
    @classmethod
    def check_name_valid(cls, v):
        if v != _format_name(v):
            raise ValueError("name includes illegal characters.")
        if not v:
            raise ValueError("invalid string")
        return v


class Datum(BaseModel):
    uid: str
    dataset: str
    metadata: list[MetaDatum] = []

    @field_validator("uid")
    @classmethod
    def format_uid(cls, v):
        if v != _format_uid(v):
            raise ValueError("uid includes illegal characters.")
        if not v:
            raise ValueError("invalid string")
        return v

    @field_validator("dataset")
    @classmethod
    def check_name_valid(cls, v):
        if v != _format_name(v):
            raise ValueError("name includes illegal characters.")
        if not v:
            raise ValueError("invalid string")
        return v


class Annotation(BaseModel):
    task_type: enums.TaskType
    labels: list[Label]
    metadata: list[MetaDatum] | None = None

    # Geometric types
    bounding_box: BoundingBox | None = None
    polygon: Polygon | None = None
    multipolygon: MultiPolygon | None = None
    raster: Raster | None = None

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("labels")
    @classmethod
    def check_labels_not_empty(cls, v):
        if not v:
            raise ValueError("`labels` cannot be empty.")
        return v


class GroundTruth(BaseModel):
    datum: Datum
    annotations: list[Annotation]

    @field_validator("annotations")
    @classmethod
    def check_annotations(cls, v):
        if not v:
            raise ValueError("annotations is empty")
        return v

    @model_validator(mode="after")
    @classmethod
    def validate_annotation_rasters(cls, values):
        return _validate_rasters(values)


class Prediction(BaseModel):
    model: str
    datum: Datum
    annotations: list[Annotation]

    @field_validator("model")
    @classmethod
    def check_name_valid(cls, v):
        if v != _format_name(v):
            raise ValueError("name includes illegal characters.")
        if not v:
            raise ValueError("invalid string")
        return v

    @field_validator("annotations")
    @classmethod
    def check_annotations(cls, v):
        if not v:
            raise ValueError("annotations is empty")
        return v

    @model_validator(mode="after")
    @classmethod
    def validate_annotation_rasters(cls, values):
        return _validate_rasters(values)

    @field_validator("annotations")
    @classmethod
    def validate_annotation_scores(cls, v: list[Annotation]):
        # check that we have scores for all the labels if
        # the task type requires it
        for annotation in v:
            if annotation.task_type in [
                enums.TaskType.CLASSIFICATION,
                enums.TaskType.DETECTION,
                enums.TaskType.INSTANCE_SEGMENTATION,
            ]:
                for label in annotation.labels:
                    if label.score is None:
                        raise ValueError(
                            f"Missing score for label in {annotation.task_type} task."
                        )

        return v

    @field_validator("annotations")
    @classmethod
    def check_label_scores(cls, v: list[Annotation]):
        # check that for classification tasks, the label scores
        # sum to 1
        for annotation in v:
            if annotation.task_type == enums.TaskType.CLASSIFICATION:
                label_keys_to_sum = {}
                for scored_label in annotation.labels:
                    label_key = scored_label.key
                    if label_key not in label_keys_to_sum:
                        label_keys_to_sum[label_key] = 0.0
                    label_keys_to_sum[label_key] += scored_label.score

                for k, total_score in label_keys_to_sum.items():
                    if abs(total_score - 1) > 1e-5:
                        raise ValueError(
                            "For each label key, prediction scores must sum to 1, but"
                            f" for label key {k} got scores summing to {total_score}."
                        )
        return v


def _validate_rasters(d: GroundTruth | Prediction):
    def _mask_bytes_to_pil(mask_bytes):
        with io.BytesIO(mask_bytes) as f:
            return PIL.Image.open(f)

    for annotation in d.annotations:
        if annotation.raster is not None:
            # unpack datum metadata
            metadata = {
                metadatum.key: metadatum.value
                for metadatum in filter(
                    lambda x: x.key in ["height", "width"], d.datum.metadata
                )
            }
            if "height" not in metadata or "width" not in metadata:
                raise RuntimeError(
                    "Attempted raster validation but image dimensions are missing."
                )

            # validate raster wrt datum metadata
            mask_size = _mask_bytes_to_pil(
                b64decode(annotation.raster.mask)
            ).size
            image_size = (metadata["width"], metadata["height"])
            if mask_size != image_size:
                raise ValueError(
                    f"Expected raster and image to have the same size, but got size {mask_size} for the mask and {image_size} for image."
                )
    return d