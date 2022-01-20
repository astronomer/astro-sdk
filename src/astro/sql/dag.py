from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Type, Union

import jinja2
from airflow.configuration import conf
from airflow.models import DAG
from airflow.models.dag import (
    DagStateChangeCallback,
    ScheduleIntervalArg,
    ScheduleIntervalArgNotSet,
)
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.base import Timetable

if TYPE_CHECKING:
    pass
from astro import sql as aql

astro_default_args = {
    "retries": 1,
    "retry_delay": 0,
}


def create_dag(
    dag_id: str,
    models_directory: str,
    default_args=astro_default_args,
    description: Optional[str] = None,
    schedule_interval: ScheduleIntervalArg = ScheduleIntervalArgNotSet,
    timetable: Optional[Timetable] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    full_filepath: Optional[str] = None,
    template_searchpath: Optional[Union[str, Iterable[str]]] = None,
    template_undefined: Type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
    user_defined_macros: Optional[Dict] = None,
    user_defined_filters: Optional[Dict] = None,
    concurrency: Optional[int] = None,
    max_active_tasks: int = conf.getint("core", "max_active_tasks_per_dag"),
    max_active_runs: int = conf.getint("core", "max_active_runs_per_dag"),
    dagrun_timeout: Optional[timedelta] = None,
    sla_miss_callback: Optional[
        Callable[["DAG", str, str, List[str], List[TaskInstance]], None]
    ] = None,
    default_view: str = conf.get("webserver", "dag_default_view").lower(),
    orientation: str = conf.get("webserver", "dag_orientation"),
    catchup: bool = conf.getboolean("scheduler", "catchup_by_default"),
    on_success_callback: Optional[DagStateChangeCallback] = None,
    on_failure_callback: Optional[DagStateChangeCallback] = None,
    doc_md: Optional[str] = None,
    params: Optional[Dict] = None,
    access_control: Optional[Dict] = None,
    is_paused_upon_creation: Optional[bool] = None,
    jinja_environment_kwargs: Optional[Dict] = None,
    render_template_as_native_obj: bool = False,
    tags: Optional[List[str]] = None,
) -> DAG:
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        timetable=timetable,
        start_date=start_date,
        end_date=end_date,
        full_filepath=full_filepath,
        template_searchpath=template_searchpath,
        template_undefined=template_undefined,
        user_defined_macros=user_defined_macros,
        user_defined_filters=user_defined_filters,
        concurrency=concurrency,
        max_active_tasks=max_active_tasks,
        max_active_runs=max_active_runs,
        dagrun_timeout=dagrun_timeout,
        sla_miss_callback=sla_miss_callback,
        default_view=default_view,
        orientation=orientation,
        catchup=catchup,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        doc_md=doc_md,
        params=params,
        access_control=access_control,
        is_paused_upon_creation=is_paused_upon_creation,
        jinja_environment_kwargs=jinja_environment_kwargs,
        render_template_as_native_obj=render_template_as_native_obj,
        tags=tags,
    )
    with dag:
        aql.render(models_directory)

    return dag
