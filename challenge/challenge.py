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
        minutes = self.start_from.minute
        next_time = self.start_from + timedelta(hours=1)
        next_time = next_time - timedelta(minutes=minutes)
        return next_time

    def schedule(self, when: datetime) -> None:
        """Schedule this task at the 'when' time, update local time markers."""
        #! when -> time set back a complete hour
        previous_hour = self.start_from - timedelta(hours=1)
        previous_hour
        self.latest_done = previous_hour

    def back_fill(self, backfill:datetime) -> None:
        #not sure yet how the eariest_done is set - but backfilling from what i understand serves to track the progress when going back in time. 
        # not sure if this follows the same rule of starting an hour later from the task time? in which case we take 2 hours.
        self.earliest_done = None



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
        self.task_store.sort(key=lambda x: x.start_from)
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
        [task.schedule(last_hour_start) for task in sorted_tasks]


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

# def task_order(task_list):
#     print(f"{task_list} \n")

# x1 = HourlyTask(datetime(2022, 7, 31, 17))
# x2 = HourlyTask(datetime(2022, 3, 31, 2, 13))
# x3 = HourlyTask(datetime(2022, 10, 31, 23))
# l = [x1, x2, x3]
# x1.start_from
# task_order(l)

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
    
    print(todos == [task_with_todo])





