from typing import Protocol

from ..clients import IamClient, NeptuneAnalyticsClient


class GraphContext(Protocol):

    na_client: NeptuneAnalyticsClient
    iam_client: IamClient
