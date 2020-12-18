"""Main module."""
from pathlib import Path
from typing import Union
from functools import reduce
from loguru import logger
from pandas.core.frame import DataFrame

from annotation_driven_dataframe_calcs.column_names import ACCOUNT_NO, TIMESTEP_NO
from annotation_driven_dataframe_calcs.registry import (
    save_task_dependency_graph_to_file,
    sort_calculations_by_dependencies,
    calculation_function_for_series,
    show_series_dependency_graph,
)

"""
    Although the imported modules are not used, their
    import is necessary to trigger the registration process
"""
## from annotation_driven_dataframe_calcs import first_stage_calcs


def run(input_data, run_config):
    return None


def expand_for_timesteps(input_data, first_time_step: int, last_time_step: int):
    interim_frame = input_data
    interim_frame[TIMESTEP_NO] = [
        list(range(first_time_step, last_time_step + 1))
    ] * len(input_data.index)
    interim_frame_exploded = interim_frame.explode(
        column=TIMESTEP_NO, ignore_index=True
    )
    acct_stmt_indexex_exploded_frame = interim_frame_exploded.set_index(
        [ACCOUNT_NO, TIMESTEP_NO]
    )

    return acct_stmt_indexex_exploded_frame


def join_registered_series_values(acct_stmt_indexex_exploded_frame):
    list_of_series = list(sort_calculations_by_dependencies())
    logger.debug(f"the registered series and handlers are: {list_of_series}")

    logger.info(f"The task graph of the relationships between series shows:\n")
    show_series_dependency_graph()

    dep_grap_path = Path("param_calc_task_graph.png").resolve()
    save_task_dependency_graph_to_file(dep_grap_path)
    logger.info(
        f"The task graph of the relationships has been saved to {dep_grap_path}:\n"
    )

    agregated_inputs_and_outputs = reduce(
        __calculate_series_and_merge_with_df,
        list_of_series,
        acct_stmt_indexex_exploded_frame,
    )
    return agregated_inputs_and_outputs


def __calculate_series_and_merge_with_df(accumulator_df: DataFrame, series_name: str):
    logger.debug(f"calculating the series {series_name}")
    calculated_series = (calculation_function_for_series(series_name))(accumulator_df)
    logger.debug(f"series {series_name} calcuated as:\n {calculated_series}")

    return accumulator_df.join(
        other=calculated_series, on=[ACCOUNT_NO, TIMESTEP_NO], how="left"
    )
