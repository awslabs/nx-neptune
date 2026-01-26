from typing import Protocol

from nx_neptune.clients import IamClient, NeptuneAnalyticsClient


class GraphContext(Protocol):

    na_client: NeptuneAnalyticsClient
    iam_client: IamClient
