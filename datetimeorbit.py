import asyncio
import datetime
import inspect
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

ActionHandler = Callable[['Task'], Union[Awaitable[None], None]]

logger = logging.getLogger(__name__)


@dataclass
class User:
    name: str
    quota: int
    executed: int = 0

    def can_execute(self) -> bool:
        return self.executed < self.quota

    def record_execution(self) -> None:
        self.executed += 1


class UserManager:
    def __init__(self, users: Optional[Dict[str, Dict[str, int]]] = None) -> None:
        self._users: Dict[str, User] = {}
        if users:
            for name, data in users.items():
                self.add_user(name, data.get('quota', 0), data.get('executed', 0))

    def add_user(self, name: str, quota: int, executed: int = 0) -> None:
        self._users[name] = User(name=name, quota=quota, executed=executed)
        logger.debug('Added user %s with quota=%s executed=%s', name, quota, executed)

    def get_user(self, name: str) -> Optional[User]:
        return self._users.get(name)

    def can_execute(self, name: str) -> bool:
        user = self.get_user(name)
        allowed = bool(user and user.can_execute())
        logger.debug('User %s can_execute=%s', name, allowed)
        return allowed

    def record_execution(self, name: str) -> None:
        user = self.get_user(name)
        if user:
            user.record_execution()
            logger.info('Recorded execution for %s (%s/%s)', user.name, user.executed, user.quota)


@dataclass
class Task:
    user: str
    time: str
    action: str
    target: str
    params: Dict[str, Any] = field(default_factory=dict)

    def is_due(self, current_time: str) -> bool:
        return self.time == current_time

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        return cls(
            user=data['user'],
            time=data['time'],
            action=data['action'],
            target=data.get('target', ''),
            params=data.get('params', {}),
        )


class TaskExecutor:
    def __init__(self) -> None:
        self._actions: Dict[str, ActionHandler] = {}
        self.register_default_actions()

    def register_action(self, name: str, handler: ActionHandler) -> None:
        self._actions[name] = handler
        logger.debug('Registered action %s', name)

    async def execute(self, task: Task) -> None:
        action = self._actions.get(task.action)
        if not action:
            logger.error('Unknown task action: %s', task.action)
            raise ValueError(f'Unknown task action: {task.action}')

        logger.info(
            'Executing task %s for user %s with params=%s',
            task.action,
            task.user,
            task.params,
        )
        if inspect.iscoroutinefunction(action):
            await action(task)
        else:
            action(task)
        logger.info('Completed task %s for user %s', task.action, task.user)

    def register_default_actions(self) -> None:
        self.register_action('sync', self._sync)
        self.register_action('backup', self._backup)
        self.register_action('delete', self._delete)

    async def _sync(self, task: Task) -> None:
        logger.info('Syncing %s for user=%s params=%s', task.target, task.user, task.params)
        await asyncio.sleep(task.params.get('duration', 0))
        logger.debug('Sync completed for %s', task.target)

    async def _backup(self, task: Task) -> None:
        logger.info('Backing up %s for user=%s params=%s', task.target, task.user, task.params)
        await asyncio.sleep(task.params.get('duration', 0))
        logger.debug('Backup completed for %s', task.target)

    async def _delete(self, task: Task) -> None:
        logger.info('Deleting %s for user=%s params=%s', task.target, task.user, task.params)
        await asyncio.sleep(task.params.get('duration', 0))
        logger.debug('Delete completed for %s', task.target)


class TaskScheduler:
    def __init__(self, user_manager: UserManager, executor: TaskExecutor) -> None:
        self.user_manager = user_manager
        self.executor = executor
        self.tasks: List[Task] = []

    def add_task(self, task: Task) -> None:
        self.tasks.append(task)
        logger.debug('Added task %s for user %s at %s', task.action, task.user, task.time)

    def add_task_from_dict(self, task_data: Dict[str, Any]) -> None:
        self.add_task(Task.from_dict(task_data))

    def due_tasks(self, current_time: Optional[str] = None) -> List[Task]:
        if current_time is None:
            current_time = datetime.datetime.now().strftime('%H:%M')
        due = [task for task in self.tasks if task.is_due(current_time)]
        logger.debug('Found %s due tasks for time %s', len(due), current_time)
        return due

    async def run_task(self, task: Task) -> None:
        logger.info(
            'Pending task %s for user %s scheduled at %s with params=%s',
            task.action,
            task.user,
            task.time,
            task.params,
        )
        if not self.user_manager.can_execute(task.user):
            logger.warning('%s has exceeded quota and will not run task %s', task.user, task.action)
            return
        await self.executor.execute(task)
        self.user_manager.record_execution(task.user)

    async def run_pending(self, current_time: Optional[str] = None) -> None:
        current_time = current_time or datetime.datetime.now().strftime('%H:%M')
        tasks = self.due_tasks(current_time)
        if not tasks:
            logger.info('No pending tasks for %s', current_time)
            return
        logger.info('Running %s pending tasks for %s', len(tasks), current_time)
        await asyncio.gather(*(self.run_task(task) for task in tasks))


def build_sample_scheduler() -> TaskScheduler:
    user_data = {
        'alice': {'quota': 3, 'executed': 0},
        'bob': {'quota': 5, 'executed': 0},
    }

    sample_task_dicts = [
        {
            'user': 'alice',
            'time': '12:00',
            'action': 'sync',
            'target': '/data/x',
            'params': {'duration': 0.1, 'recursive': True},
        },
        {
            'user': 'bob',
            'time': '12:00',
            'action': 'backup',
            'target': '/srv/y',
            'params': {'duration': 0.2, 'compression': 'gzip'},
        },
        {
            'user': 'alice',
            'time': '12:00',
            'action': 'delete',
            'target': '/tmp/z',
            'params': {'force': True},
        },
    ]

    user_manager = UserManager(user_data)
    executor = TaskExecutor()
    scheduler = TaskScheduler(user_manager=user_manager, executor=executor)

    for task_data in sample_task_dicts:
        scheduler.add_task_from_dict(task_data)

    return scheduler


async def main() -> None:
    # Set up crisp log formatting to watch synchronous/asynchronous events
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
    )

    # Initialize Managers
    users = {"alice": 2, "bob": 5}  # Alice only has 2 slots, but 3 tasks queued!
    user_manager = UserManager(users=users)
    executor = TaskExecutor()
    scheduler = TaskScheduler(user_manager, executor)

    # Injecting a completely customized dynamic action handler to show extensibility
    async def custom_report_handler(task: Task) -> None:
        logger.info("📊 Custom Report Generation Engine starting for %s", task.target)
        await asyncio.sleep(task.params.get("delay", 0.0))

    executor.register_action("generate_report", custom_report_handler)

    # Sample Configurable Datasets (Dictionary inputs)
    sample_tasks = [
        {"user": "alice", "time": "12:00", "action": "sync", "target": "/data/photos", "params": {"duration": 0.2}},
        {"user": "bob", "time": "12:00", "action": "backup", "target": "/db/production", "params": {"duration": 0.4}},
        {"user": "alice", "time": "12:00", "action": "delete", "target": "/tmp/cache", "params": {"duration": 0.1}},
        # Alice's 3rd task at 12:00 -- This should hit the quota guardrail
        {"user": "alice", "time": "12:00", "action": "generate_report", "target": "Q2_Financials", "params": {"delay": 0.1}},
    ]

    for raw_task in sample_tasks:
        scheduler.add_task_from_dict(raw_task)

    # Run the engine for time slot "12:00"
    await scheduler.run_pending("12:00")


if __name__ == '__main__':
    asyncio.run(main())