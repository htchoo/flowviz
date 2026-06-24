# -*- coding: utf-8 -*-

import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.param import Param

from common import config, date_utils
from common.utils import common_utils

__author__ = "장정우"
__copyright__ = "Copyright 2024, LG전자 해영본 OneData구축"
__credits__ = ["장정우"]
__version__ = "1.0"
__maintainer__ = "장정우"
__email__ = "jeongwoo1.jang@lge.com"
__status__ = "Development"

"""
[수정이력]
2025-05-02 - 장정우 - 최초 작성
2026-06-01 - 장정우 - dag_run config 수정

"""

"""
(@) 변경 대상
  - 프로그램 정보
  - 스케줄 설정
  - 배치 흐름 설정
"""

"""
(@) 프로그램 정보 
"""
dag_id = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
description = "[ONED] GA NOBS CONSENTED SST L1/L2 적재 일배치"

"""
(@) 스케줄 설정
"""
schedule_interval = "0 22 * * *"  # None  # 매일 @schedule_interval
poke_interval = 60  # trigger된 DAG의 상태 확인 주기 (30초)

"""
(@) 배치 흐름 설정
"""
dag_list_1 = [
    "ONED_GL1_GA_GP1_SST_TIME_GR_01",
]

dag_list_2 = [
    "ONED_GL1_GA_GP1_SST_CONSEN_TIME_GR_01",
]

dag_list_3 = [
    "ONED_GL1_GA_GA4_NOBS_SST_TIME_GR_01",
]

dag_list_4 = [
    "ONED_GL1_GA_GA4_NOBS_SST_CONSEN_TIME_GR_01",
]

dag_list_5 = [
    "ONED_GL2_GA_GA4_NOBS_SST_TIME_GR_01",
]

dag_list_all = [
    dag_list_1,
    dag_list_2,
    dag_list_3,
    dag_list_4,
    dag_list_5,
]

""" DAG 공통 파라미터 """
default_args = {
    "owner": config.oned_env["dag_owner"],
    "retries": config.oned_env["batch_retries"],
    "retry_delay": config.oned_env["retry_delay"],
    "provide_context": True,
}

tags = common_utils.parse_tags(dag_id)


def get_empty_op(trigger_rule, idx):
    task_id = f"join_{trigger_rule}_task_{idx}"
    do = EmptyOperator(
        task_id=task_id,
        trigger_rule=trigger_rule,
    )
    return do


with DAG(
    dag_id=dag_id,
    description=description,
    start_date=config.oned_env["start_date"],
    schedule_interval=schedule_interval,
    default_args=default_args,
    tags=tags,
    catchup=False,
    params={
        "tz_gr_no": Param("1", type="string"),
        "start_date": Param("", type="string", description="미입력 시 스케줄 기준일"),
        "end_date": Param("", type="string", description="미입력 시 스케줄 기준일"),
    }
) as dag:
    """
    trigger dag list
    """
    prev_task = None
    next_task = None
    for idx, dag_id_list in enumerate(dag_list_all):
        next_task = get_empty_op(TriggerRule.ALL_SUCCESS, idx)
        for dag_id in dag_id_list:
            triggered_task = TriggerDagRunOperator(
                task_id=f"trigger_{dag_id}",
                trigger_dag_id=dag_id,
                execution_date="{{ execution_date }}",
                pool=config.oned_env["airflow_batch_pool"],
                conf={
                    'tz_gr_no': "{{ dag_run.conf.get('tz_gr_no', '1') }}",
                    'start_date': "{{ dag_run.conf.get('start_date') or '' }}",
                    'end_date': "{{ dag_run.conf.get('end_date') or '' }}",
                },
                reset_dag_run=True,
                wait_for_completion=True,
                poke_interval=poke_interval,
                deferrable=True,
            )
            if prev_task is not None:
                prev_task >> triggered_task
            triggered_task >> next_task

        prev_task = next_task
