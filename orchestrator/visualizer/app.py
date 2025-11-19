import dash_cytoscape as cyto
from dash import Dash, html

from orchestrator.visualizer.builder import build_graph
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

    # Build the graph
    elements = build_graph(tasks)

    app.layout = html.Div(
        [
            cyto.Cytoscape(
                id="nested-dag-tasks",
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
        ]
    )

    app.run(debug=True)


if __name__ == "__main__":
    import asyncio

    asyncio.run(create_app())
