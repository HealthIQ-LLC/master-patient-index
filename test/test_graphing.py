from .conftest import (
    CONST_BATCH_ID,
    CONST_PROC_ID
)
from services.web.project.graphing import (
    GraphReCursor,
    GraphCursor,
    MATCH_THRESHOLD,
    NODE_SIZE,
    FONT_SIZE,
    ALPHA,
    WIDTH,
    SEED,
    AX_MARGINS
)


def test_graph_cursor(nodes_and_weights):
    expected_config = {
        "nodes_and_weights": nodes_and_weights,
        "match_threshold": MATCH_THRESHOLD,
        "node_size": NODE_SIZE,
        "font_size": FONT_SIZE,
        "alpha": ALPHA,
        "width": WIDTH,
        "seed": SEED,
        "ax_margins": AX_MARGINS
    }
    my_graph = GraphCursor(nodes_and_weights, CONST_BATCH_ID, CONST_PROC_ID)
    assert my_graph.config == expected_config
    assert my_graph.nodes_and_weights == nodes_and_weights
    assert my_graph.enterprise_id == 12345
    my_graph.graph.add_edge(98765, 12345, weight=0.8)
    assert len(my_graph.graph.nodes.values()) == 5
    assert len(my_graph.graph.edges.values()) == 7


#ToDo: GraphRecursor test
