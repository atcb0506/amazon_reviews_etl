from pyspark.sql import DataFrame
from typing import Dict, Any
from etl_class.base_job import BaseJob


class Cleansing(BaseJob):

    def __init__(self,
                 df: DataFrame,
                 meta_job_config: Dict[str, Any]) -> None:

        super(Cleansing, self).__init__(
            df=df,
            job_config=meta_job_config['job_config'],
            trans_func=meta_job_config['trans_func']
        )

    def get_df(self):

        return self.df
