class JobConfig:

    SPARK = {
        'session_loglevel': 'WARN'
    }

    INPUT = {
        'input_data_path': {
            'test': 's3a://amazon-reviews-pds-ap-southeast-1/tsv/amazon_reviews_us_Lawn_and_Garden_v1_00.tsv.gz',
            'full': 's3a://amazon-reviews-pds-ap-southeast-1/tsv'
        }
    }

    OUTPUT = {
        'output_data_path': 's3a://emr-amazon-review-analysis-ap-southeast-1',
        'secret_name': 'test-database-1/emr_user',
        'secret_region': 'ap-southeast-1',
        's3_bucket': 'emr-amazon-review-analysis-ap-southeast-1',
        's3_region': 'ap-southeast-1',
        'db_schema': 'amazon_review'
    }