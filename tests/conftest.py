import pytest
from longeval.spark import spark_resource


@pytest.fixture()
def spark():
    with spark_resource(app_name="pytest", cores=4) as spark:
        yield spark
