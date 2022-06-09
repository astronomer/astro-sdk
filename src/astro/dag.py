from airflow.models.dag import DAG as UpstreamDAG

from astro.sql.operators.cleanup import CleanupOperator


class DAG(UpstreamDAG):
    cleanup_task = None

    def create_cleanup_task(self):
        if not self.cleanup_task:
            self.cleanup_task = CleanupOperator()

    def add_task(self, task):
        if task.task_id == "_cleanup":
            super().add_task(task)
        else:
            self.create_cleanup_task()
            super().add_task(task)
            task.set_downstream(self.cleanup_task)

    def add_tasks(self, tasks):
        self.create_cleanup_task()
        super().add_tasks(tasks)
        for task in tasks:
            task.set_downstream(self.cleanup_task)
