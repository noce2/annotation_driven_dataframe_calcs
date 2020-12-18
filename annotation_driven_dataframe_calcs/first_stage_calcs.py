from typing import Tuple
from loguru import logger
import pandas

from annotation_driven_dataframe_calcs.column_names import (
    PARAM_A,
    PARAM_B,
    PARAM_C,
    SERIES_A_PRIME
)
from annotation_driven_dataframe_calcs.registry import register
from annotation_driven_dataframe_calcs.calculation_helpers import nonrecursive_calculation, calculate_over_window, recursive_calculation

@register(output_series_name='TEST_OUTPUT_C')
@calculate_over_window(
    series_to_run_window_over=PARAM_A,
    window_size=2,
    output_series_name='TEST_OUTPUT_C'
)
@nonrecursive_calculation
def new_core_arithmetric_for_series_b_prime(
    account_no,
    timestep_no,
    current_window_series,
    entire_input_data_set,
) -> float:
    return (
        - entire_input_data_set.loc[(account_no, timestep_no), PARAM_B]
        - entire_input_data_set.loc[(account_no, timestep_no), PARAM_C]
    )

@register(output_series_name=f"{SERIES_A_PRIME}_FROM_DECS")
@calculate_over_window(
    series_to_run_window_over=PARAM_A,
    window_size=2,
    output_series_name=f"{SERIES_A_PRIME}_FROM_DECS"
)
@recursive_calculation(
    number_of_previous_terms_needed=1,
    tuple_of_initial_values=(100,)
)
def series_c_prime(
    account_no,
    timestep_no,
    current_window_series,
    tuple_of_previous_calculated_values,
    entire_input_data_set,
) -> float:
    return (
        tuple_of_previous_calculated_values[0]
        - entire_input_data_set.loc[(account_no, timestep_no), PARAM_B]
        - entire_input_data_set.loc[(account_no, timestep_no), PARAM_C]
    )
