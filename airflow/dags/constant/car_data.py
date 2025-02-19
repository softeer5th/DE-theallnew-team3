from airflow.models.param import Param

CARS = {
    "Grandeur": {
        "car_name": "Grandeur",
        "alias": ["그랜저"],
    },
    "Santafe": {
        "car_name": "Santafe",
        "alias": ["싼타페"],
    },
}

CAR_TYPE = list(CARS.keys())

CAR_TYPE_PARAM = Param(
    enum=CAR_TYPE,
    default="Grandeur",
    description="수집할 대상 자동차를 선택해주세요.",
    title="자동차 모델",
)
