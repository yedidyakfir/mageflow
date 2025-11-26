import abc
import dataclasses
from abc import ABC
from queue import Queue
from typing import Generic, TypeVar, TypeAlias, Any, get_args

from dash import html

from orchestrator.chain.consts import ON_CHAIN_ERROR, ON_CHAIN_END
from orchestrator.chain.model import ChainTaskSignature
from orchestrator.signature.consts import ORCHESTRATOR_TASK_INITIALS
from orchestrator.signature.model import TaskSignature
from orchestrator.swarm.consts import ON_SWARM_ERROR, ON_SWARM_START, ON_SWARM_END
from orchestrator.swarm.model import SwarmTaskSignature, BatchItemTaskSignature

T = TypeVar("T", bound=TaskSignature)
INTERNAL_TASKS = [
    ON_CHAIN_ERROR,
    ON_CHAIN_END,
    ON_SWARM_START,
    ON_SWARM_ERROR,
    ON_SWARM_END,
]
INTERNAL_TASKS += [
    internal_name.lstrip(ORCHESTRATOR_TASK_INITIALS) for internal_name in INTERNAL_TASKS
]


def is_internal_task(task_name: str) -> bool:
    return any(task_name.endswith(internal_task) for internal_task in INTERNAL_TASKS)


@dataclasses.dataclass
class GraphData:
    main_node: dict = dataclasses.field(default_factory=dict)
    nodes: list[dict] = dataclasses.field(default_factory=list)
    edges: list[dict] = dataclasses.field(default_factory=list)
    next_tasks: list[str] = dataclasses.field(default_factory=list)


class Builder(ABC):
    @abc.abstractmethod
    def draw(self) -> GraphData:
        pass

    @property
    @abc.abstractmethod
    def id(self):
        pass

    @abc.abstractmethod
    def present_info(self) -> list:
        pass


class EmptyBuilder(Builder):
    def __init__(
        self,
        task_id: str,
        success_tasks: list[str] = None,
        error_tasks: list[str] = None,
    ):
        self.task_id = task_id
        self.success_tasks = success_tasks or []
        self.error_tasks = error_tasks or []

    @property
    def id(self):
        return self.task_id

    def draw(self) -> GraphData:
        task_node = {"data": {"id": self.task_id, "label": self.task_id}}
        success_edges = [
            {
                "data": {"source": self.task_id, "target": task_id},
                "classes": "success-edge",
            }
            for task_id in self.success_tasks
        ]
        error_edges = [
            {
                "data": {"source": self.task_id, "target": task_id},
                "classes": "error-edge",
            }
            for task_id in self.error_tasks
        ]
        return GraphData(
            main_node=task_node,
            edges=success_edges + error_edges,
        )

    def present_info(self) -> list:
        components = []

        if self.success_tasks:
            components.extend(
                [
                    html.H5("Success Tasks:", className="section-header"),
                    html.Ul(
                        [
                            html.Li(task_id, className="callback-item")
                            for task_id in self.success_tasks
                        ],
                        className="callback-list",
                    ),
                ]
            )

        if self.error_tasks:
            components.extend(
                [
                    html.H5("Error Tasks:", className="section-header"),
                    html.Ul(
                        [
                            html.Li(
                                task_id,
                                className="callback-item",
                            )
                            for task_id in self.error_tasks
                        ],
                        className="callback-list",
                    ),
                ]
            )

        return components


class TaskBuilder(Builder, Generic[T]):
    def __init__(self, task: T):
        self.task = task
        self.ctx: dict[str, "TaskBuilder"] = {}

    @property
    def id(self):
        return self.task.id

    def set_ctx(self, ctx: dict[str, "TaskBuilder"]):
        self.ctx = ctx

    def draw(self) -> GraphData:
        task_node = {"data": {"id": self.task.id, "label": self.task.task_name}}

        success_edges = [
            {
                "data": {"source": self.task.id, "target": task_id},
                "classes": "success-edge",
            }
            for task_id in self.task.success_callbacks
        ]
        error_edges = [
            {
                "data": {"source": self.task.id, "target": task_id},
                "classes": "error-edge",
            }
            for task_id in self.task.error_callbacks
        ]

        return GraphData(
            main_node=task_node,
            edges=success_edges + error_edges,
            next_tasks=self.task.success_callbacks + self.task.error_callbacks,
        )

    def drawn_tasks(self) -> list[str]:
        return [self.task.id]

    def mentioned_tasks(self) -> list[str]:
        return self.task.success_callbacks + self.task.error_callbacks

    @property
    def task_name(self):
        return self.task.task_name

    def remove_success_internal_tasks(self):
        success_task = {
            success: self.ctx.get(success) for success in self.task.success_callbacks
        }
        self.task.success_callbacks = [
            key
            for key, success in success_task.items()
            if success and not is_internal_task(success.task_name)
        ]

    def remove_errors_internal_tasks(self):
        error_tasks = {
            error: self.ctx.get(error) for error in self.task.error_callbacks
        }
        self.task.error_callbacks = [
            key
            for key, error in error_tasks.items()
            if error and not is_internal_task(error.task_name)
        ]

    def present_info(self) -> list:
        components = [
            html.Div(
                [
                    html.Strong("Creation Time: "),
                    html.Span(str(self.task.creation_time)),
                ],
                className="task-info-item",
            ),
            html.Div(
                [
                    html.Strong("Status: "),
                    html.Span(str(self.task.task_status.status)),
                ],
                className="task-info-item",
            ),
            html.H5("Parameters:", className="section-header"),
            html.Pre(
                str(dict(self.task.kwargs)),
                className="code-block",
            ),
            html.H5("Success Callbacks:", className="section-header"),
            html.Ul(
                [
                    html.Li(
                        callback_id,
                        className="callback-item",
                    )
                    for callback_id in self.task.success_callbacks
                ],
                className="callback-list",
            ),
            html.H5("Error Callbacks:", className="section-header"),
            html.Ul(
                [
                    html.Li(
                        callback_id,
                        className="callback-item",
                    )
                    for callback_id in self.task.error_callbacks
                ],
                className="callback-list",
            ),
            html.H5("Task Identifiers:", className="section-header"),
            html.Pre(
                str(dict(self.task.task_identifiers)),
                className="code-block-compact",
            ),
        ]

        return components


class ChainTaskBuilder(TaskBuilder[ChainTaskSignature]):
    def draw(self) -> GraphData:
        base_node = super().draw()

        sub_tasks = [self.ctx.get(task_id) for task_id in self.task.tasks]
        sub_tasks = [
            (
                task
                if task
                else EmptyBuilder(self.task.tasks[i], [self.task.tasks[i + 1]])
            )
            for i, task in enumerate(sub_tasks)
        ]
        draw_tasks = [task_builder.draw() for task_builder in sub_tasks]
        for drawn_task in draw_tasks:
            drawn_task.main_node["data"]["parent"] = base_node.main_node["data"]["id"]
            base_node.nodes.extend(drawn_task.nodes)
            base_node.nodes.append(drawn_task.main_node)
            base_node.edges.extend(drawn_task.edges)
            base_node.next_tasks.extend(drawn_task.next_tasks)

        return base_node

    @property
    def sub_builders(self):
        return [
            self.ctx.get(task_id) for task_id in self.task.tasks if task_id in self.ctx
        ]

    def mentioned_tasks(self) -> list[str]:
        sub_task_mentions = [
            task_id
            for builder in self.sub_builders
            for task_id in builder.mentioned_tasks()
        ]
        return list(
            set(super().mentioned_tasks() + sub_task_mentions + self.task.tasks)
        )

    def drawn_tasks(self):
        sub_tasks_drawn = [
            task_id
            for builder in self.sub_builders
            for task_id in builder.drawn_tasks()
        ]
        return super().drawn_tasks() + self.task.tasks + sub_tasks_drawn


class BatchItemTaskBuilder(TaskBuilder[BatchItemTaskSignature]):
    def draw(self) -> GraphData:
        original_task = self.ctx.get(self.task.original_task_id)
        if original_task:
            return original_task.draw()
        return super().draw()

    def drawn_tasks(self):
        drawn = super().drawn_tasks()
        if self.task.original_task_id in self.ctx:
            drawn += self.ctx.get(self.task.original_task_id).drawn_tasks()

        return drawn

    def mentioned_tasks(self) -> list[str]:
        mentions = super().mentioned_tasks() + [self.task.original_task_id]
        if self.task.original_task_id in self.ctx:
            mentions += self.ctx.get(self.task.original_task_id).mentioned_tasks()
        return mentions

    def present_info(self) -> list:
        if self.task.original_task_id in self.ctx:
            return self.ctx.get(self.task.original_task_id).present_info()
        return super().present_info()


class SwarmTaskBuilder(TaskBuilder[SwarmTaskSignature]):
    def draw(self) -> GraphData:
        base_node = super().draw()

        swarm_tasks = [self.ctx.get(task_id) for task_id in self.task.tasks]
        draw_tasks = [task_builder.draw() for task_builder in swarm_tasks]
        for drawn_task in draw_tasks:
            drawn_task.main_node["data"]["parent"] = base_node.main_node["data"]["id"]
            base_node.nodes.append(drawn_task.main_node)
            base_node.nodes.extend(drawn_task.nodes)
            base_node.next_tasks.extend(drawn_task.next_tasks)
            base_node.edges.extend(drawn_task.edges)

        return base_node

    @property
    def sub_builders(self):
        return [
            self.ctx.get(task_id) for task_id in self.task.tasks if task_id in self.ctx
        ]

    def drawn_tasks(self):
        sub_tasks_drawn = [
            task_id
            for builder in self.sub_builders
            for task_id in builder.drawn_tasks()
        ]
        return super().drawn_tasks() + self.task.tasks + sub_tasks_drawn

    def mentioned_tasks(self) -> list[str]:
        sub_task_mentions = [
            task_id
            for builder in self.sub_builders
            for task_id in builder.mentioned_tasks()
        ]
        all_tasks = (
            super().mentioned_tasks() + list(set(sub_task_mentions)) + self.task.tasks
        )
        return list(set(all_tasks))


task_mapping = {
    TaskSignature: TaskBuilder,
    ChainTaskSignature: ChainTaskBuilder,
    SwarmTaskSignature: SwarmTaskBuilder,
    BatchItemTaskSignature: BatchItemTaskBuilder,
}

CTXType: TypeAlias = dict[str, TaskBuilder]


def find_unmentioned_tasks(ctx: CTXType) -> list[str]:
    mentioned_tasks = set()
    for task in ctx.values():
        mentioned_tasks.update(task.mentioned_tasks())
    real_tasks_keys = {
        task_id
        for task_id, builder in ctx.items()
        if not is_internal_task(builder.task_name)
    }
    return list(real_tasks_keys - mentioned_tasks)


def create_builders(tasks: list[TaskSignature]) -> CTXType:
    ctx = {task.id: task_mapping.get(type(task))(task) for task in tasks}

    # Initialize tasks
    for task_id in ctx.keys():
        task_builder = ctx.get(task_id)
        task_builder.set_ctx(ctx)

        # Remove internal tasks
        task_builder.remove_errors_internal_tasks()
        task_builder.remove_success_internal_tasks()
    return ctx


def build_graph(initial_id: str, ctx: CTXType) -> list[dict]:
    tasks_to_draw = Queue()
    tasks_to_draw.put(initial_id)
    drawn_tasks = []
    elements = []

    while tasks_to_draw.qsize() > 0:
        task_id = tasks_to_draw.get()
        if task_id in drawn_tasks:
            continue

        task_builder = ctx.get(task_id)
        drawn_tasks = task_builder.drawn_tasks()
        drawn_tasks.extend(drawn_tasks)
        if not task_builder:
            continue
        graph_data = task_builder.draw()

        elements.append(graph_data.main_node)
        elements.extend(graph_data.nodes)
        elements.extend(graph_data.edges)

        mentioned_tasks = task_builder.mentioned_tasks()
        undrawn_task = set(mentioned_tasks) - set(drawn_tasks)

        for new_task_id in undrawn_task:
            if new_task_id in ctx:
                tasks_to_draw.put(new_task_id)

    return elements
