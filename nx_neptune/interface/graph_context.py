from typing import Protocol

from nx_neptune.clients import NeptuneAnalyticsClient, IamClient


class GraphContext(Protocol):

    na_client: NeptuneAnalyticsClient
    iam_client: IamClient
    role_arn: str