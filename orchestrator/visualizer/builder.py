import dataclasses
from queue import Queue
from typing import Generic, TypeVar

from orchestrator.chain.model import ChainTaskSignature
from orchestrator.signature.model import TaskSignature
from orchestrator.swarm.model import SwarmTaskSignature, BatchItemTaskSignature

T = TypeVar("T", bound=TaskSignature)


@dataclasses.dataclass
class GraphData:
    main_node: dict = dataclasses.field(default_factory=dict)
    nodes: list[dict] = dataclasses.field(default_factory=list)
    edges: list[dict] = dataclasses.field(default_factory=list)
    next_tasks: list[str] = dataclasses.field(default_factory=list)


class TaskBuilder(Generic[T]):
    def __init__(self, task: T):
        self.task = task

    def draw(self, ctx: dict[str, "TaskBuilder"]) -> GraphData:
        task_node = {"data": {"id": self.task.id, "label": self.task.task_name}}

        success_edges = [
            {"data": {"source": self.task.id, "target": task_id}}
            for task_id in self.task.success_callbacks
        ]
        error_edges = [
            {"data": {"source": self.task.id, "target": task_id}}
            for task_id in self.task.error_callbacks
        ]

        return GraphData(
            main_node=task_node,
            edges=success_edges + error_edges,
            next_tasks=self.task.success_callbacks + self.task.error_callbacks,
        )

    def mentioned_tasks(self) -> list[str]:
        return self.task.success_callbacks + self.task.error_callbacks


class ChainTaskBuilder(TaskBuilder[ChainTaskSignature]):
    def draw(self, ctx: dict[str, TaskBuilder]) -> GraphData:
        base_node = super().draw(ctx)

        sub_tasks = [ctx.get(task_id) for task_id in self.task.tasks]
        draw_tasks = [task_builder.draw(ctx) for task_builder in sub_tasks]
        for drawn_task in draw_tasks:
            drawn_task.main_node["data"]["parent"] = base_node.main_node["data"]["id"]
            base_node.nodes.extend(drawn_task.nodes)
            base_node.nodes.append(drawn_task.main_node)
            base_node.edges.extend(drawn_task.edges)
            base_node.next_tasks.extend(drawn_task.next_tasks)

        return base_node

    def mentioned_tasks(self) -> list[str]:
        return super().mentioned_tasks() + self.task.tasks


class BatchItemTaskBuilder(TaskBuilder[BatchItemTaskSignature]):
    def draw(self, ctx: dict[str, TaskBuilder]) -> GraphData:
        original_task = ctx.get(self.task.original_task_id)
        return original_task.draw(ctx)

    def mentioned_tasks(self) -> list[str]:
        return super().mentioned_tasks() + [self.task.original_task_id]


class SwarmTaskBuilder(TaskBuilder[SwarmTaskSignature]):
    def draw(self, ctx: dict[str, TaskBuilder]) -> GraphData:
        base_node = super().draw(ctx)

        swarm_tasks = [ctx.get(task_id) for task_id in self.task.tasks]
        draw_tasks = [task_builder.draw(ctx) for task_builder in swarm_tasks]
        for drawn_task in draw_tasks:
            drawn_task.main_node["data"]["parent"] = base_node.main_node["data"]["id"]
            base_node.nodes.append(drawn_task.main_node)
            base_node.nodes.extend(drawn_task.nodes)
            base_node.next_tasks.extend(drawn_task.next_tasks)
            base_node.edges.extend(drawn_task.edges)

        return base_node

    def mentioned_tasks(self) -> list[str]:
        return super().mentioned_tasks() + self.task.tasks


task_mapping = {
    TaskSignature: TaskBuilder,
    ChainTaskSignature: ChainTaskBuilder,
    SwarmTaskSignature: SwarmTaskBuilder,
    BatchItemTaskSignature: BatchItemTaskBuilder,
}


def find_unmentioned_tasks(ctx: dict[str, TaskBuilder]) -> list[str]:
    mentioned_tasks = set()
    for task in ctx.values():
        mentioned_tasks.update(task.mentioned_tasks())
    return list(ctx.keys() - mentioned_tasks)


def build_graph(tasks: list[TaskSignature]) -> list[dict]:
    ctx = {task.id: task_mapping.get(type(task))(task) for task in tasks}
    base_tasks_keys = find_unmentioned_tasks(ctx)
    tasks_to_draw = Queue()
    drawn_tasks = []
    for task_id in base_tasks_keys:
        tasks_to_draw.put(task_id)

    elements = []

    while tasks_to_draw.qsize() > 0:
        task_id = tasks_to_draw.get()
        if task_id in drawn_tasks:
            continue
        drawn_tasks.append(task_id)

        task_builder = ctx.get(task_id)
        if not task_builder:
            continue
        graph_data = task_builder.draw(ctx)

        elements.append(graph_data.main_node)
        elements.extend(graph_data.nodes)
        elements.extend(graph_data.edges)

        for task_id in task_builder.mentioned_tasks():
            if task_id in ctx:
                tasks_to_draw.put(task_id)

    return elements
