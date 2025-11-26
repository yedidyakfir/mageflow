import click
import dash_cytoscape as cyto
import rapyer
from dash import Dash, html, dcc, Input, Output, callback

from orchestrator.visualizer.assets.cytoscape_styles import EDGE_STYLES, GRAPH_STYLES
from orchestrator.visualizer.builder import (
    build_graph,
    create_builders,
    find_unmentioned_tasks,
    CTXType,
)
from orchestrator.visualizer.data import extract_signatures, create
from orchestrator.visualizer.utils import pydantic_validator

# Load extra layouts
cyto.load_extra_layouts()


async def create_app(redis_url: str):
    app = Dash(__name__)
    stylesheet = GRAPH_STYLES + EDGE_STYLES

    app.layout = html.Div(
        [
            html.Div(
                [
                    dcc.Tabs(id="task-tabs", className="tabs-container"),
                    html.Div(
                        [
                            html.H4("Control Panel", className="control-panel-header"),
                            html.Button(
                                "Refresh",
                                id="refresh-button",
                                className="control-button",
                                n_clicks=0,
                            ),
                        ],
                        className="control-panel",
                    ),
                ],
                className="top-bar",
            ),
            html.Div(
                [
                    html.Div(
                        id="tab-content",
                        children=[],
                        className="graph-container",
                    ),
                    html.Div(
                        id="info-window",
                        className="info-window",
                        children=[
                            html.H4("Task Information", className="task-info-header"),
                            html.P(
                                "Click on a task node to see its details here.",
                                className="task-info-placeholder",
                            ),
                        ],
                    ),
                ],
                className="main-layout",
            ),
            dcc.Store(id="start-tasks", data=[]),
            dcc.Store(id="tasks-data", data={}),
        ]
    )

    @callback(
        [Output("tasks-data", "data"), Output("start-tasks", "data")],
        [Input("refresh-button", "n_clicks")],
    )
    @pydantic_validator
    async def refresh_data(n_clicks: int) -> tuple[CTXType, list[str]]:
        await rapyer.init_rapyer(redis_url)
        tasks = await extract_signatures()
        ctx = create_builders(tasks)
        start_tasks = find_unmentioned_tasks(ctx)
        return ctx, start_tasks

    @callback(
        Output("task-tabs", "children"),
        [Input("start-tasks", "data"), Input("tasks-data", "data")],
    )
    @pydantic_validator
    def update_tabs(start_tasks: list[str], ctx: CTXType):
        tabs = [
            dcc.Tab(
                label=ctx.get(task_id).task_name,
                value=task_id,
                id=f"tab-{task_id}",
                className="tab-style",
                selected_className="tab-selected",
            )
            for task_id in start_tasks
        ]
        return tabs

    @callback(
        Output("tab-content", "children"),
        [Input("task-tabs", "value"), Input("tasks-data", "data")],
    )
    @pydantic_validator
    def render_content(active_tab: str, ctx: CTXType):
        if active_tab and active_tab in ctx:
            elements = build_graph(active_tab, ctx)
            return cyto.Cytoscape(
                id="cytoscape-graph",
                elements=elements,
                className="cytoscape-container",
                layout={
                    "name": "klay",
                    "klay": {
                        "direction": "RIGHT",
                        "spacing": 40,
                        "borderSpacing": 30,
                        "edgeRouting": "ORTHOGONAL",
                    },
                },
                stylesheet=stylesheet,
            )
        return html.Div("No task selected")

    @callback(
        Output("info-window", "children"),
        [Input("cytoscape-graph", "tapNodeData"), Input("tasks-data", "data")],
        prevent_initial_call=True,
    )
    @pydantic_validator
    def display_task_info(node_data: dict, ctx: CTXType):
        if node_data is None:
            return [
                html.H4("Task Information", className="task-info-header"),
                html.P(
                    "Click on a task node to see its details here.",
                    className="task-info-placeholder",
                ),
            ]

        task_id = node_data["id"]
        task_builder = ctx.get(task_id)

        if not task_builder:
            return [
                html.H4("Task Information", className="task-info-header"),
                html.P(
                    f"No information available for task: {task_id}",
                    className="task-info-placeholder",
                ),
            ]

        components = [
            html.H4("Task Information", className="task-info-header-detailed"),
            html.Div(
                [
                    html.Strong("Task ID: "),
                    html.Span(
                        task_builder.id,
                        className="task-id-text",
                    ),
                ],
                className="task-info-item",
            ),
            html.Div(
                [
                    html.Strong("Task Name: "),
                    html.Span(task_builder.task_name),
                ],
                className="task-info-item",
            ),
        ]

        components.extend(task_builder.present_info())
        return components

    app.run(debug=True)


DEFAULT_REDIS_URL = "redis://localhost:6379/12"


@click.group()
def cli():
    pass


@cli.command("create")
@click.option("--redis-url", default=DEFAULT_REDIS_URL, help="Redis URL")
def create_db(redis_url: str):
    import asyncio

    asyncio.run(create(redis_url))


@cli.command("app")
@click.option("--redis-url", default=DEFAULT_REDIS_URL, help="Redis URL")
def main(redis_url: str):
    import asyncio

    asyncio.run(create_app(redis_url))


if __name__ == "__main__":
    cli()
