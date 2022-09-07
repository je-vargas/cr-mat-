import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Union, List
from freezegun import freeze_time


@dataclass #* decorator basically adds a constructor with declared class attributes == __init__
class HourlyTask:
    """A task to be done every hour, and backfilled if not up to date."""

    #: From when should this task start occurring?
    start_from: datetime

    #: Until when should this task occur?
    repeat_until: Union[datetime, None] = None

    #: What, if any, is the first time that has been done for this task?
    earliest_done: Union[datetime, None] = None

    #: What if any is the last (most recent) time that has been done for this
    #: task?
    latest_done: Union[datetime, None] = None

    @property
    def next_to_do(self) -> Union[datetime, None]:
        """Return the next datetime that needs doing."""
        next_time = self.start_from + timedelta(hours=1)
        return next_time

    def schedule(self, when: datetime) -> None:
        """Schedule this task at the 'when' time, update local time markers."""
        #! when -> time set back a complete hour
        previous_hour = self.start_from - timedelta(hours=1)
        self.latest_done = previous_hour
        
        # repeat until and earliest done also set here i think! 



class Scheduler:
    """Schedule some work."""

    def __init__(self):
        """Initialise Scheduler."""
        self.task_store = []

    def register_task(self, task: HourlyTask) -> None:
        """Add a task to the local store of tasks known about."""
        self.task_store.append(task)

    def register_tasks(self, task_list: List[HourlyTask]) -> None:
        """Add several tasks to the local store of tasks."""
        [self.register_task(task) for task in task_list]

    def get_tasks_to_do(self) -> List[HourlyTask]:
        """Get the list of tasks that need doing."""
        return self.task_store

    def schedule_tasks(self) -> None:
        """Schedule the tasks.

        Tasks should be prioritised so that tasks with a recent "to do" date
        are done before any that need backfilling.
        """
        tasks = self.get_tasks_to_do()
        now = datetime.utcnow()
        now_hour_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        last_hour_start = now_hour_start - timedelta(hours=1)
        [task.schedule(last_hour_start) for task in tasks]


@dataclass
class Controller:
    """Use a Scheduler to repeatedly check and schedule tasks."""

    #: The scheduler that we are controlling
    scheduler: Scheduler

    #: How long to wait between each schedule check
    throttle_wait: timedelta

    #: Daemon mode?
    run_forever: bool = True

    #: Run this many times (if not in Daemon mode)
    run_iterations: int = 0

    def run(self):
        """Run scheduler"""
        while self.run_iterations or self.run_forever:
            before = datetime.utcnow()
            self.scheduler.schedule_tasks()
            self.run_iterations -= 1
            after = datetime.utcnow()
            elapsed = after - before
            wait = self.throttle_wait.total_seconds() - elapsed.total_seconds()
            time.sleep(max([0, wait]))


# ------------------

# task = HourlyTask(start_from=datetime(2022, 8, 1, 23, 15))
# next_day = task.next_to_do

sch = Scheduler()
with freeze_time(datetime(2022, 8, 1, 8, 15)):
    yesterday = datetime(2022, 7, 31)
    task_too_late = HourlyTask(start_from=datetime.utcnow())
    task_with_todo = HourlyTask(start_from=yesterday)
    task_done = HourlyTask(
        start_from=yesterday,
        latest_done=datetime(2022, 8, 1, 7)
    )
    sch.register_tasks([task_too_late, task_with_todo, task_done])
    todos = sch.get_tasks_to_do()
    todos == [task_with_todo]