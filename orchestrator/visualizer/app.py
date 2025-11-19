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
    app = Dash(__name__, assets_folder="assets")
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

    # Create tabs for each start task with inline styles
    tabs = [
        dcc.Tab(
            label=ctx.get(task_id).task_name,
            value=task_id,
            id=f"tab-{task_id}",
            style={
                "height": "40px",
                "width": "120px",
                "lineHeight": "40px",
                "padding": "0 10px",
                "textAlign": "center",
                "overflow": "hidden",
                "textOverflow": "ellipsis",
                "whiteSpace": "nowrap",
            },
            selected_style={
                "height": "40px",
                "width": "auto",
                "minWidth": "120px",
                "maxWidth": "300px",
                "lineHeight": "40px",
                "padding": "0 10px",
                "textAlign": "center",
                "overflow": "visible",
                "whiteSpace": "nowrap",
            },
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
            html.Div(id="tab-content"),
        ]
    )

    @callback(Output("tab-content", "children"), [Input("task-tabs", "value")])
    def render_content(active_tab):
        if active_tab:
            elements = build_graph(active_tab, ctx)
            return cyto.Cytoscape(
                id=f"cytoscape-{active_tab}",
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

    app.run(debug=True)


if __name__ == "__main__":
    import asyncio

    asyncio.run(create_app())
