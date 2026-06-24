# -*- coding: utf-8 -*-

import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from common import config
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
2025-05-28 - 장정우 - CUST GR 추가
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
description = "[L1_ONED] SST 동의 데이터 L1 적재 일작업 그룹 DAG"

"""
(@) 스케줄 설정
cron 표현식 (https://crontab.guru/)
형식=(* * * * *)=>(분 시간 일자 월 요일)
ex) 매일 6:00 = '0 6 * * *'
ex) 매월 셋째주 금요일 6:30= '30 6 * * 6#3'
ex) 매시간 0분에 실행 (2시간간격으로 실행) '0 */2 * * *'
"""
schedule_interval = None
poke_interval = 60  # trigger된 DAG의 상태 확인 주기 (30초)

"""
(@) 배치 흐름 설정
dag_list_01 -> join -> dag_list_02 -> join 순으로 흐름
dag_list_nn 에는 병렬로 실행할 dag_id(pgm_id)를 콤마(,) 구분자로 작성
dag_list_all 에는 dag_list_nn을 순서대로 콤마(,) 구분자로 작성
"""

dag_list_1 = [
    # 전체 테이블 생성
    "ONED_EL1_MRTK_TRMRKT_GA_VSTR_ID_TRFC_MIG_GP1_NOBS_CONSENTED_SST_ALL_V2",
]
dag_list_2 = [
    # GP1 MST 1 동의
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_CMPGN_CHNL_CONSENTED_SST_M_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_CTNTS_EVENT_FUNN_CONSENTED_SST_M_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_VSTR_ID_LOGIN_CONSENTED_SST_H_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_VSTR_ID_RSVD_SESS_CONSENTED_SST_H_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_MAX_VIEW_PAGE_URL_CONSENTED_SST_H_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_SESS_MAX_STDBY_PAGE_CONSENTED_SST_H_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_SESS_JRNY_CONSENTED_SST_H_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_SESS_CHAT_YN_CONSENTED_SST_H_D01_V2",
    # CUST GR
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_CUST_GR_CONSENTED_SST_M_D01",
]
dag_list_3 = [
    # GP1 MST 2 동의
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_CTNTS_EVENT_PAGE_TYPE_CONSENTED_SST_M_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_SESS_PAGE_TXN_CONSENTED_SST_H_D01_V2",
]
dag_list_4 = [
    # GP1 TEMP_DAY_GA 동의
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_VSTR_ID_TRFC_CONSENTED_SST_T_D01_V2",
]
dag_list_5 = [
    # GP1 TEMP_DAY_GA_AGG 동의
    "ONED_EL1_MRTK_TRMRKT_GA_GP1_VSTR_ID_TRFC_AGGR_CONSENTED_SST_T_D01_V2",
]
dag_list_6 = [
    # GP1 CUST GR
    "ONED_EL1_MRTK_TRMRKT_GA_TRFC_GP1_CUST_GR_AGGR_CONSENTED_SST_D_D01",
    # GP1 MAIN GA
    "ONED_EL1_MRTK_TRMRKT_GA_TRFC_GP1_AGGR_CONSENTED_SST_D_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_TRFC_GP1_BU2_AGGR_CONSENTED_SST_D_D01_V2",
    # GP1 CAMPAIGN GA
    "ONED_EL1_MRTK_TRMRKT_GA_CMPGN_TRFC_GP1_AGGR_CONSENTED_SST_D_D01_V2",
    "ONED_EL1_MRTK_TRMRKT_GA_CMPGN_TRFC_GP1_BU2_AGGR_CONSENTED_SST_D_D01_V2",
]

dag_list_all = [
    dag_list_1,
    dag_list_2,
    dag_list_3,
    dag_list_4,
    dag_list_5,
    dag_list_6,
]


""" DAG 공통 파라미터 """
default_args = {
    "owner": config.oned_env["dag_owner"],
    "retries": config.oned_env["group_retries"],
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
                reset_dag_run=True,
                wait_for_completion=True,
                poke_interval=poke_interval,
                deferrable=True,
                conf="{{ dag_run.conf | tojson }}",
            )
            if prev_task is not None:
                prev_task >> triggered_task
            triggered_task >> next_task

        prev_task = next_task
