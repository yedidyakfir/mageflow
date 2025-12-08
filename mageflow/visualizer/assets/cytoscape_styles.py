# Cytoscape edge styles for task graph visualization

EDGE_STYLES = [
    {
        "selector": ".success-edge",
        "style": {
            "line-color": "#40E0D0",
            "target-arrow-color": "#40E0D0",
        },
    },
    {
        "selector": ".error-edge",
        "style": {
            "line-color": "#FF6B9D",
            "target-arrow-color": "#FF6B9D",
        },
    },
]

GRAPH_STYLES = [
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
            "background-color": "#2D1B69",
            "border-width": 2,
            "border-color": "#9D4EDD",
            "color": "#E0AAFF",
        },
    },
    # Compound / parent nodes (containers)
    {
        "selector": ":parent",
        "style": {
            "background-color": "#10002B",
            "border-color": "#7209B7",
            "border-width": 3,
            "color": "#C77DFF",
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
            "line-color": "#5A189A",
            "target-arrow-color": "#5A189A",
            "width": 2,
        },
    },
]
