from celery._state import get_current_task
from colorlog import ColoredFormatter


class CeleryFormatter(ColoredFormatter):
    def format(self, record):
        task = get_current_task()
        if task and task.request:
            record.__dict__.update(task_id=task.request.id, task_name=task.name)
        else:
            record.__dict__.setdefault("task_name", "???")
            record.__dict__.setdefault("task_id", "???")
        return ColoredFormatter.format(self, record)
