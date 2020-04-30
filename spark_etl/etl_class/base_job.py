from pyspark.sql import DataFrame
from typing import Dict, Any
from types import MethodType


class BaseJob(object):

    def __init__(self,
                 df: DataFrame,
                 job_config: Dict[str, Any],
                 trans_func: Any) -> None:

        self.df = df
        self.job_config = job_config
        self.transformation = MethodType(trans_func, self)

    def get_df(self):
        pass

    def load(self):
        pass
