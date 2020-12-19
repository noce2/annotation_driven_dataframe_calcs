"""
    Creates a registrar annotation and a registry for
    storing all functions that utilise the annotation
"""
from types import FunctionType
from typing import Iterable, List
from networkx import DiGraph, draw, topological_sort
from loguru import logger
import matplotlib.pyplot as plt

logger.debug(f"Creating the registry")
_REGISTRY = DiGraph()
logger.debug(f"Registry created")


function_attribute_name_in_node = "function_for_creation"

def register(
    output_series_name: str, depends_on_calculated_input_series: List[str] = []
):
    """
    Registers the decorated function as one that will generate a series
    to be appended to the final dataframe.\n

    Parameters:\n
    output_series_name (str): The name to give to the calculated series\n
    depends_on_calculated_input_series (List[str]): The list of the names of any other
        calculated series that is necessary for the calculation of the annotated
        parameter. MUST NOT include any series already supplied in the original
        dataset.\n
    """
    logger.info(
        f"Registering series {output_series_name} and its dependencies"
        f" {depends_on_calculated_input_series}"
    )

    def registrar(func):
        """
        Registers the supplied function in the store along with any of its
        data dependencies.
        """
        if not(output_series_name in _REGISTRY.nodes):
            _REGISTRY.add_node(output_series_name)
        else:
            error_message = (
                f"Attempted to register a handler, {func},"
                f"for {output_series_name} but that name is aleady in use "
                f"by {calculation_function_for_series(output_series_name)}\n"
            )
            logger.error(error_message)
            raise ValueError("The names of output_registered_series must be unique")

        for each_dependency in depends_on_calculated_input_series:
            _REGISTRY.add_edge(each_dependency, output_series_name)

        _REGISTRY.nodes[output_series_name][function_attribute_name_in_node] = func
        return func  # normally a decorator returns a wrapped function,
        # but here we return func unmodified, after registering it

    return registrar

def sort_calculations_by_dependencies() -> Iterable:
    return topological_sort(_REGISTRY)


def calculation_function_for_series(output_series_name: str) -> FunctionType:
    return _REGISTRY.nodes[output_series_name][function_attribute_name_in_node]

def show_series_dependency_graph():
    draw(_REGISTRY, with_labels=True, font_weight='bold')

def save_task_dependency_graph_to_file(path):
    plt.savefig(path)
