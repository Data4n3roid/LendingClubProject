import pytest
from lib.utils import get_spark_session
from lib.dataReader import read_customers
from lib.configReader import get_app_config

@pytest.fixture
def spark():
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.mark.slow()
def test_read_customers_df(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 2260701

def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["customers_raw.file.path"] == "data/customers_raw.csv"