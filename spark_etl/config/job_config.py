from pyspark.sql import functions as sf
from transformation.cleansing import trans_pattern_cleansing
from transformation.resultset import trans_dim_product, trans_fct_review


class JobConfig:

    CLEANSING_CONFIG = {
        'job_config': {
            'product_category': '\\\\',
            'product_title': '\\\\'
        },
        'trans_func': trans_pattern_cleansing
    }

    DIM_PRPDUCT_CONFIG = {
        'resultset': 'dim_product',
        'job_config': {
            'key': 'product_id',
            'attributes': ['product_category', 'product_title']
        },
        'trans_func': trans_dim_product
    }

    FCT_REVIEW_CONFIG = {
        'resultset': 'fct_review',
        'job_config': {
            'key': ['customer_id', 'product_id'],
            'ls_filter': ['verified_purchase = \'Y\''],
            'agg_mapping': {'total_votes_sum': ('total_votes', sf.sum),
                            'helpful_votes_sum': ('helpful_votes', sf.sum),
                            'star_rating_sum': ('star_rating', sf.sum),
                            'star_rating_mean': ('star_rating', sf.mean),
                            'ttl_review_count': ('review_id', sf.count)}
        },
        'trans_func': trans_fct_review
    }


class PipelineConfig:

    CLEANSING_PIPELINE = [
        {
            'job': 'Replacing \\ in text',
            'job_config': JobConfig().CLEANSING_CONFIG
        }
    ]

    RESULTSET_PIPELINE = [
        {
            'job': 'Create dim_product',
            'job_config': JobConfig().DIM_PRPDUCT_CONFIG
        },
        {
            'job': 'Create fct_review',
            'job_config': JobConfig().FCT_REVIEW_CONFIG
        }
    ]
