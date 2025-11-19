import dash_cytoscape as cyto
from dash import Dash, html, dcc, Input, Output, callback

from orchestrator.visualizer.builder import (
    build_graph,
    create_builders,
    find_unmentioned_tasks,
)
from orchestrator.visualizer.data import extract_signatures, create

# Load extra layouts
cyto.load_extra_layouts()


async def create_app():
    # await create()
    app = Dash(__name__)
    stylesheet = [
        # Default node style (leaf + parents)
        {
            "selector": "node",
            "style": {
                "shape": "round-rectangle",
                "content": "data(label)",
                "text-valign": "center",
                "text-halign": "center",
                "font-size": "11px",
                "padding": "6px",
                "width": "label",
                "height": "label",
                "background-color": "#E0F2FF",
                "border-width": 1,
                "border-color": "#555",
            },
        },
        # Compound / parent nodes (containers)
        {
            "selector": ":parent",
            "style": {
                "background-color": "#F5F5F5",
                "border-color": "#333",
                "border-width": 2,
                "padding": "20px",  # space around children
                "text-valign": "top",  # label at top of group
                "font-weight": "bold",
            },
        },
        {
            "selector": "edge",
            "style": {
                "curve-style": "bezier",
                "target-arrow-shape": "triangle",
                "arrow-scale": 1.2,
                "line-color": "#888",
                "target-arrow-color": "#888",
                "width": 2,
            },
        },
    ]

    # Get the complex scenario data
    tasks = await extract_signatures()
    ctx = create_builders(tasks)
    start_tasks = find_unmentioned_tasks(ctx)

    # Create tabs for each start task with CSS classes
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

    app.layout = html.Div(
        [
            dcc.Tabs(
                id="task-tabs",
                value=start_tasks[0] if start_tasks else None,
                children=tabs,
                style={"height": "40px", "display": "flex", "flexDirection": "row"},
            ),
            html.Div(
                [
                    html.Div(
                        id="tab-content",
                        style={
                            "width": "70%",
                            "display": "inline-block",
                            "verticalAlign": "top",
                            "height": "600px",
                        },
                    ),
                    html.Div(
                        id="info-window",
                        style={
                            "width": "28%",
                            "display": "inline-block",
                            "verticalAlign": "top",
                            "marginLeft": "2%",
                            "padding": "10px",
                            "border": "1px solid #ddd",
                            "borderRadius": "5px",
                            "backgroundColor": "#f9f9f9",
                            "height": "580px",
                            "overflow": "auto",
                        },
                        children=[
                            html.H4("Task Information", style={"margin": "0 0 10px 0"}),
                            html.P(
                                "Click on a task node to see its details here.",
                                style={"color": "#666"},
                            ),
                        ],
                    ),
                ],
                style={"display": "flex", "width": "100%"},
            ),
        ]
    )

    @callback(Output("tab-content", "children"), [Input("task-tabs", "value")])
    def render_content(active_tab):
        if active_tab:
            elements = build_graph(active_tab, ctx)
            return cyto.Cytoscape(
                id="cytoscape-graph",
                elements=elements,
                style={"width": "100%", "height": "600px"},
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
        [Input("cytoscape-graph", "tapNodeData")],
        prevent_initial_call=True,
    )
    def display_task_info(nodeData):
        if nodeData is None:
            return [
                html.H4("Task Information", style={"margin": "0 0 10px 0"}),
                html.P(
                    "Click on a task node to see its details here.",
                    style={"color": "#666"},
                ),
            ]

        task_id = nodeData["id"]
        task_builder = ctx.get(task_id)

        if not task_builder:
            return [
                html.H4("Task Information", style={"margin": "0 0 10px 0"}),
                html.P(
                    f"No information available for task: {task_id}",
                    style={"color": "#666"},
                ),
            ]

        components = [
            html.H4("Task Information", style={"margin": "0 0 15px 0"}),
            html.Div(
                [
                    html.Strong("Task ID: "),
                    html.Span(
                        task_builder.id,
                        style={"fontFamily": "monospace", "fontSize": "12px"},
                    ),
                ],
                style={"marginBottom": "10px"},
            ),
            html.Div(
                [
                    html.Strong("Task Name: "),
                    html.Span(task_builder.task_name),
                ],
                style={"marginBottom": "10px"},
            ),
        ]

        components.extend(task_builder.present_info())
        return components

    app.run(debug=True)


if __name__ == "__main__":
    import asyncio

    asyncio.run(create_app())
