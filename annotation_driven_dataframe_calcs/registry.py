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
__REGISTRY = DiGraph()
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
    logger.debug(
        f"Registering series {output_series_name} and its dependencies"
        f" {depends_on_calculated_input_series}"
    )

    def registrar(func):
        """
        Registers the supplied function in the store along with any of its
        data dependencies.
        """
        __REGISTRY.add_node(output_series_name)

        for each_dependency in depends_on_calculated_input_series:
            __REGISTRY.add_edge(each_dependency, output_series_name)

        __REGISTRY.nodes[output_series_name][function_attribute_name_in_node] = func
        return func  # normally a decorator returns a wrapped function,
        # but here we return func unmodified, after registering it

    return registrar

def sort_calculations_by_dependencies() -> Iterable:
    return topological_sort(__REGISTRY)


def calculation_function_for_series(output_series_name: str) -> FunctionType:
    return __REGISTRY.nodes[output_series_name][function_attribute_name_in_node]
