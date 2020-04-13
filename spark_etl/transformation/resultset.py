from pyspark.sql import functions as sf


def trans_dim_product(self):

    # init variables
    key = self.job_config['key']
    attributes = self.job_config['attributes']
    ls_column = list()

    if isinstance(key, list):
        ls_column = ls_column + key
    else:
        ls_column.append(key)

    ls_column = ls_column + attributes

    # create list of key with only one attribute details, for 1-1 mapping
    df_uni = self.df \
        .withColumn('concat_attr', sf.concat(*attributes)) \
        .groupBy(key) \
        .agg(sf.countDistinct('concat_attr').alias('cnt_uni_attr')) \
        .filter('cnt_uni_attr = 1')

    # select only key-attri pair with only one attribute details
    self.df = self.df \
        .join(other=df_uni, on=key, how='inner') \
        .groupBy(ls_column) \
        .count() \
        .select(ls_column)


def trans_fct_review(self):

    # init config
    key = self.job_config['key']
    ls_filter = self.job_config['ls_filter']
    agg_mapping = self.job_config['agg_mapping']

    # create list of aggregation expression
    ls_agg = list()
    for new_col, col_agg_pair in agg_mapping.items():
        (col, agg) = col_agg_pair
        agg_col = agg(self.df[col]).alias(new_col)
        ls_agg.append(agg_col)

    # filtering
    df_filtered = self.df
    for f in ls_filter:
        df_filtered = df_filtered \
            .filter(f)

    # aggregation
    self.df = df_filtered \
        .groupBy(key) \
        .agg(*ls_agg)