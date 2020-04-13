from pyspark.sql import DataFrame
from typing import Dict, Any
from etl_class.base_job import BaseJob
from io_job.output import df_to_csv


class Resultset(BaseJob):

    def __init__(self,
                 df: DataFrame,
                 meta_job_config: Dict[str, Any],
                 output_config: Dict[str, Any]) -> None:

        super(Resultset, self).__init__(
            df=df,
            job_config=meta_job_config['job_config'],
            trans_func=meta_job_config['trans_func']
        )

        self.output_path = f'{output_config["output_data_path"]}/{meta_job_config["resultset"]}'

    def load(self):

        df_to_csv(
            dataframe=self.df,
            output_path=self.output_path
        )
