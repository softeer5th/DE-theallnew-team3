from airflow.models.param import Param

CARS = {
    "Grandeur": {
        "car_name": "Grandeur",
        "alias": "그랜져,그랜저",
    },
    "Santafe": {
        "car_name": "Santafe",
        "alias": "싼타페,산타페",
    },
    "Casper": {
        "car_name": "Casper",
        "alias": "캐스퍼",
    },
    "Palisade": {
        "car_name": "Palisade",
        "alias": "팰리세이드",
    },
    "Sonata": {
        "car_name": "Sonata",
        "alias": "쏘나타,소나타",
    },
    "Avante": {
        "car_name": "Avante",
        "alias": "아반떼,아반테",
    },
    "Kona": {
        "car_name": "Kona",
        "alias": "코나",
    },
    "Tucson": {
        "car_name": "Tucson",
        "alias": "투싼",
    },
    "Venue": {
        "car_name": "Venue",
        "alias": "베뉴",
    },
}

CAR_TYPE = list(CARS.keys())

CAR_TYPE_PARAM = Param(
    enum=CAR_TYPE,
    default="Grandeur",
    description="수집할 대상 자동차를 선택해주세요.",
    title="자동차 모델",
)
