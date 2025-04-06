__all__ = [
    "match_all_nodes",
    "match_all_edges",
    "insert_query",
    "update_query",
    "delete_query",
    "clear_query",
    "bfs_query",
]


def match_all_nodes():
    """
    :return:
        query string that matches all nodes
    """
    return "MATCH (a) RETURN a"


def match_all_edges():
    """
    :return:
        query string that matches all edges
    """
    return "MATCH (a)-[r]->(b) RETURN r"


def insert_query(create_clause: str, match_clause: str = ""):
    """
    :param create_clause:
    :param match_clause:
    :return:
        query string
    """
    if match_clause:
        return f"MATCH {match_clause} CREATE ({create_clause}) "
    else:
        return f"CREATE ({create_clause}) "


def update_query(match_clause: str, where_clause: str, set_clause: str):
    """
    :param match_clause:
    :param where_clause:
    :param set_clause:
    :return:
        query string
    """
    return f"MATCH {match_clause} WHERE {where_clause} SET {set_clause} "


def delete_query(match_clause: str, delete_clause: str) -> str:
    """
    :return:
        query string
    """
    return f"MATCH ({match_clause}) DELETE {delete_clause} "


def clear_query() -> str:
    """
    :return:
        query string
    """
    return "MATCH (n) DETACH DELETE n"


def bfs_query(source_node_list: str, where_clause: str, parameters):
    """
    :param source_node_list:
    :param where_clause:
    :param parameters:
    :return: query string
    """
    bfs_params = f"{source_node_list}"
    if parameters:
        parameters_list_str = ", ".join(
            ["%s:%s" % (key, value) for (key, value) in parameters.items()]
        )
        bfs_params = f"{bfs_params}, {{{parameters_list_str}}}"

    return (
        f"MATCH ({source_node_list}) where {where_clause} "
        f"CALL neptune.algo.bfs({bfs_params}) YIELD node RETURN node"
    )
