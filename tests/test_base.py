from nx_neptune_analytics.na_client import NeptuneAnalyticsClient


def test_base():
    assert NeptuneAnalyticsClient.NAME == "nx_neptune_analytics"
