from nx_neptune.na_client import NeptuneAnalyticsClient


def test_base():
    assert NeptuneAnalyticsClient.NAME == "nx_neptune"
