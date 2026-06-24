# -*- coding: utf-8 -*-

import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from common import config
from common.utils import common_utils

__author__ = "임석일"
__copyright__ = "Copyright 2024, LG전자 해영본 OneData구축"
__credits__ = ["임석일"]
__version__ = "1.0"
__maintainer__ = "임석일"
__email__ = "sukil.lim@lge.com"
__status__ = "Development"

"""
[수정이력]
2024.12.11 - 임석일 - 최초 작성
2026.02.09 - 이채연 - ONED_IA 마이그레이션 dag 추가
2026.03.31 - 김휘담 - Tableau Extract 자동화 dag 추가
2026.04.17 - 김휘담 - Tableau Extract 자동화 dag 그룹2 추가
2026.05.12 - 김휘담 - Tableau Flow 자동화 dag 추가

"""

"""
Execute DAG 를 trigger 하는 Batch DAG 템플릿

[적용 방법]
제공된 DAG 템플릿에서
아래 수정 대상 '(@)' 부분만 목적에 맞게 변경한다.

(@) 변경 대상
  - 프로그램 정보
  - 스케줄 설정
  - 배치 흐름 설정
"""

"""
(@) 프로그램 정보 
"""
dag_id = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
description = "[ONED] 공통, OBS L1/L2 적재 일배치"

"""
(@) 스케줄 설정
cron 표현식 (https://crontab.guru/)
형식=(* * * * *)=>(분 시간 일자 월 요일)
ex) 매일 6:00 = '0 6 * * *'
ex) 매월 셋째주 금요일 6:30= '30 6 * * 6#3'
ex) 매시간 0분에 실행 (2시간간격으로 실행) '0 */2 * * *'
"""
schedule_interval = "30 20 * * *"  # None  # 매일 @schedule_interval
poke_interval = 60  # trigger된 DAG의 상태 확인 주기 (30초)

"""
(@) 배치 흐름 설정
dag_list_01 -> join -> dag_list_02 -> join 순으로 흐름
dag_list_nn 에는 병렬로 실행할 dag_id(pgm_id)를 콤마(,) 구분자로 작성
dag_list_all 에는 dag_list_nn을 순서대로 콤마(,) 구분자로 작성

"""
dag_list_1 = [
    "ONED_GL1_COMN_01",
]

dag_list_2 = [
    "ONED_GL1_OBS_01",
]

dag_list_3 = [
    "ONED_GL2_OBS_01",
]

dag_list_4 = [
    "ONED_GL2_OBS_EXTRACT_01",
]

dag_list_5 = [
    "ONED_GOB_IA_MIG_OBS_01"
]

dag_list_6 = [
    "ONED_GL2_OBS_EXTRACT_02"
]

dag_list_7 = [
    "ONED_GPT_OBS_FLOW_01"
]

dag_list_all = [
    dag_list_1,
    dag_list_2,
    dag_list_3,
    dag_list_4,
    dag_list_5,
    dag_list_6,
    dag_list_7,
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
                # conf={'key': 'value'},
                reset_dag_run=True,
                wait_for_completion=True,
                poke_interval=poke_interval,
                deferrable=True,
            )
            if prev_task is not None:
                prev_task >> triggered_task
            triggered_task >> next_task

        prev_task = next_task
