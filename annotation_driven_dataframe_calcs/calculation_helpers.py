from types import FunctionType
from annotation_driven_dataframe_calcs.registry import register
from annotation_driven_dataframe_calcs.caching_tools import LRU
from typing import List, Tuple
from loguru import logger
import pandas
from annotation_driven_dataframe_calcs.column_names import ACCOUNT_NO, TIMESTEP_NO
from functools import wraps

CACHE_BUFFER_FACTOR_FOR_RECURSIVE_CALCS = 4


def calculate_over_window(
    series_to_run_window_over: str,
    window_size: int,
    output_series_name: str
):
    def decorate_calculate_over_window(func):
        @wraps(func)
        def wrapper_calculate_over_window(input_data_set_for_timesteps: pandas.DataFrame):
            series_to_return = (
                input_data_set_for_timesteps.groupby([ACCOUNT_NO, TIMESTEP_NO])[series_to_run_window_over]
                .rolling(window=window_size, min_periods=1)
                .apply(
                    func,
                    kwargs={"entire_input_data_set": input_data_set_for_timesteps},
                )
            )
            return series_to_return.rename(output_series_name)
        return wrapper_calculate_over_window
    return decorate_calculate_over_window

def nonrecursive_calculation(func):
    @wraps(func)
    def wrap_nonrecursive_calculation(
        rolling_window_series: pandas.Series, entire_input_data_set: pandas.DataFrame
    ):
            account_no, timestep_no = rolling_window_series.tail(n=1).index[0]
            logger.debug(
                f"rolling window is currently at {account_no} time_step {timestep_no}"
            )
            return func(
                account_no=account_no,
                timestep_no=timestep_no,
                current_window_series=rolling_window_series,
                entire_input_data_set=entire_input_data_set,
            )
    return wrap_nonrecursive_calculation

def recursive_calculation(
    number_of_previous_terms_needed: int,
    tuple_of_initial_values: Tuple
):
    if not(len(tuple_of_initial_values) == number_of_previous_terms_needed):
        error_message=(
            f"The number of previous terms needed,{number_of_previous_terms_needed},"
            f" must match the number of initial values for the calculation, {tuple_of_initial_values}"
        )
        raise ValueError(error_message)

    def decorate_recursive_calculation(core_arithmetic_for_current_step):
        @wraps(core_arithmetic_for_current_step)
        def wrap_recursive_calculation(
            rolling_window_series: pandas.Series, entire_input_data_set: pandas.DataFrame
        ):
            account_no, timestep_no = rolling_window_series.tail(n=1).index[0]
            logger.debug(
                f"rolling window is currently at {account_no} time_step {timestep_no}"
            )

            if 0 < timestep_no <= len(tuple_of_initial_values):
                wrap_recursive_calculation.cache[f"{account_no}_{timestep_no}"] = tuple_of_initial_values[
                    timestep_no - 1
                ]
            else:
                list_of_previous_computations = []

                for i in range(len(tuple_of_initial_values), 0, -1):
                    list_of_previous_computations.append(
                        wrap_recursive_calculation.cache[f"{account_no}_{timestep_no-i}"]
                    )

                previous_values = tuple(list_of_previous_computations)

                wrap_recursive_calculation.cache[f"{account_no}_{timestep_no}"] = core_arithmetic_for_current_step(
                    account_no=account_no,
                    timestep_no=timestep_no,
                    current_window_series=rolling_window_series,
                    tuple_of_previous_calculated_values=previous_values,
                    entire_input_data_set=entire_input_data_set,
                )

            return wrap_recursive_calculation.cache[f"{account_no}_{timestep_no}"]
        wrap_recursive_calculation.cache = LRU(maxsize=number_of_previous_terms_needed*CACHE_BUFFER_FACTOR_FOR_RECURSIVE_CALCS)
        return wrap_recursive_calculation
    return decorate_recursive_calculation

def integrated_nonrecursive_calculation(
    series_to_run_window_over: str,
    window_size: int,
    output_series_name: str,
    depends_on_calculated_input_series: List[str] = []
):
    def decorate_integrated_nonrecursive_calculation(func):
        function_adapted_for_non_recursion: FunctionType = nonrecursive_calculation(func)
        function_adapted_over_window: FunctionType = (calculate_over_window(
            series_to_run_window_over=series_to_run_window_over,
            window_size=window_size,
            output_series_name=output_series_name
        ))(function_adapted_for_non_recursion)

        registered_function: FunctionType = (register(
            output_series_name=output_series_name,
            depends_on_calculated_input_series=depends_on_calculated_input_series
            ))(function_adapted_over_window)
        return registered_function
    return decorate_integrated_nonrecursive_calculation
