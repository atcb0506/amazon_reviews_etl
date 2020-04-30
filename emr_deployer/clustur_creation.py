import boto3
import json
from deployer_config.config_path import ConfigPath


def cluster_creation(emr_region: str,
                     cluster_name: str,
                     release_label: str,
                     log_uri: str,
                     step_name: str,
                     step_arg: str,
                     job_flow_role: str = 'EMR_EC2_DefaultRole',
                     service_role: str = 'EMR_DefaultRole',
                     scaledown_behavior: str = 'TERMINATE_AT_TASK_COMPLETION'
                     ) -> None:

    """

    :param emr_region:
    :param cluster_name:
    :param release_label:
    :param log_uri:
    :param step_name:
    :param step_arg:
    :param job_flow_role:
    :param service_role:
    :param scaledown_behavior:
    :return:
    """

    # func init
    dict_path = ConfigPath().PATH
    session = boto3.Session(region_name=emr_region)
    emr = session.client(service_name='emr')
    emr_para = {
        'Name': cluster_name,
        'ReleaseLabel': release_label,
        'LogUri': log_uri,
        'JobFlowRole': job_flow_role,
        'ServiceRole': service_role,
        'ScaleDownBehavior': scaledown_behavior
    }

    # load jsons
    for key, path in dict_path.items():
        with open(path) as f:
            config = json.load(f)
        emr_para.update({key: config})

    # add step
    emr_para.update(
        {
            'Steps': [
                {
                    'Name': 'Setup hadoop debugging',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['state-pusher-script']
                    }
                },
                {
                    'Name': step_name,
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': step_arg.split(' ')
                    }
                }
            ]
        }
    )

    response = emr.run_job_flow(**emr_para)
    print(response)


if __name__ == '__main__':

    cluster_creation(
        emr_region='ap-southeast-1',
        cluster_name='emr6_with_docker',
        release_label='emr-6.0.0',
        log_uri='s3://emr-loggingresult-ap-southeast-1/',
        step_name='Amazon review ETL',
        step_arg=f'sudo docker run '
                 f'-e ENV_RUN_MODE=full '
                 f'-e ENV_EXECUTION_TYPE=cluster '
                 f'-v /etc/hadoop/conf:/etc/hadoop/conf '
                 f'-v /etc/spark/conf:/etc/spark/conf '
                 f'-v /usr/lib/spark:/usr/lib/spark '
                 f'-v /usr/share/aws:/usr/share/aws '
                 f'--network=host '
                 f'558467021483.dkr.ecr.ap-southeast-1.amazonaws.com/amazon_reviews_etl:v2.0.0'
    )

