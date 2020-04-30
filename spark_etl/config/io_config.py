class IOConfig(object):

    def __init__(self,
                 mode: str):

        self.mode = mode
        if mode == 'test':
            input_data_path = 's3a://amazon-reviews-pds-ap-southeast-1/tsv/amazon_reviews_us_Lawn_and_Garden_v1_00.tsv.gz'
        elif mode == 'full':
            input_data_path = 's3a://amazon-reviews-pds-ap-southeast-1/tsv'
        else:
            raise Exception('Unmatched mode of ETL')

        self.DF_CONFIG = {
            'input_data_path': input_data_path,
            'file_format': 'csv',
            'sep': '\t',
            'header': True
        }

        self.OUTPUT_CONFIG = {
            'output_data_path': f's3a://emr-amazon-review-analysis-ap-southeast-1-{mode}-run',
            'secret_name': 'test-database-1/emr_user',
            'secret_region': 'ap-southeast-1',
            's3_bucket': 'emr-amazon-review-analysis-ap-southeast-1',
            's3_region': 'ap-southeast-1',
            'db_schema': 'amazon_review'
        }
