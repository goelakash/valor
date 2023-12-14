from sqlalchemy.orm import Session

from velour_api.crud import stateflow


@stateflow.finalize
def finalize(*, db: Session, dataset_name: str, model_name: str = None):
    """
    Finalizes dataset and dataset/model pairings.

    No logic is needed as this exists only for controlling state of the backend through the stateflow decorator.
    """
    pass