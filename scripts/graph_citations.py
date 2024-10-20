import pandas as pd
import networkx as nx


def is_citation(value1: float, value2: float) -> bool:
    """Indicates if one logarithmic activity cites another

    Args:
        value1 (float): logarithmic activity
        value2 (float): logarithmic activity

    Returns:
        bool: True if one cites another, False otherwise
    """
    return (0 <= abs(value1 - value2) <= 0.02) or (
        2.98 <= abs(value1 - value2) <= 3.02
    )


def is_later(meas1: tuple[str, int, int], meas2: tuple[str, int, int]) -> bool:
    """Indicates if second given activity is published later than first

    Args:
        meas1 (tuple[str, int, int]): activity id, year, month (1)
        meas2 (tuple[str, int, int]): activity id, year, month (2)

    Returns:
        bool: True if second activity published later than first, False otherwise
    """
    id1, year1, month1 = meas1
    id2, year2, month2 = meas2
    if year2 > year1:
        return True
    elif (year2 == year1) and (month2 != month1):
        return month2 > month1
    elif month2 == month1:
        return id2 > id1
    return False


def build_graph(measurements: pd.DataFrame) -> nx.DiGraph:
    """Builds graph for group of activity dataframe

    Args:
        measurements (pd.DataFrame): group of activity dataframe

    Returns:
        nx.DiGraph: resulting graph
    """
    G = nx.DiGraph()
    N = len(measurements)
    for i in range(N):
        G.add_node(i)
    for i in range(N):
        for j in range(N):
            if (
                (i != j)
                and is_later(
                    (
                        measurements.iloc[i, 0],
                        measurements.iloc[i, 5],
                        measurements.iloc[i, 6],
                    ),
                    (
                        measurements.iloc[j, 0],
                        measurements.iloc[j, 5],
                        measurements.iloc[j, 6],
                    ),
                )
                and (
                    is_citation(
                        measurements.iloc[i, 4], measurements.iloc[j, 4]
                    )
                )
            ):
                G.add_edge(j, i)
    return G


def find_originals_and_citations(
    measurements: pd.DataFrame,
) -> tuple[list, dict]:
    """Finds originals and citations for given group of activity dataframe

    Args:
        measurements (pd.DataFrame): Group of activity dataframe

    Returns:
        tuple[list, dict]: list of original activities and mapping between citations and corresponding originals
    """
    measurements = measurements.reset_index(drop=True)
    G = build_graph(measurements)
    wcc = list(nx.weakly_connected_components(G))
    originals = []
    citations_map = {}
    for component in wcc:
        if len(component) == 1:
            originals.append(measurements.iloc[list(component)[0], 0])
        else:
            component_df = measurements.iloc[list(component), :].sort_values(
                by=["year", "month", "activity_chembl_id"]
            )
            original = component_df.iloc[0, 0]
            originals.append(original)
            for node in component_df.iloc[1:, 0]:
                citations_map[node] = original

    return originals, citations_map
