from pyspark.sql import functions as sf


def trans_pattern_cleansing(self):
    for col, pattern in self.job_config.items():
        self.df = self.df.withColumn(col, sf.regexp_replace(col, pattern, ''))