from typing import Tuple
from loguru import logger
import pandas

from annotation_driven_dataframe_calcs.caching_tools import LRU
from annotation_driven_dataframe_calcs.column_names import (
    ACCOUNT_NO,
    PARAM_A,
    PARAM_B,
    PARAM_C,
    SERIES_A_PRIME,
    TIMESTEP_NO,
)
from annotation_driven_dataframe_calcs.registry import register
from annotation_driven_dataframe_calcs.calculation_helpers import adapt_stepwise_nonrecursive_calc_for_window, calculate_over_window


@register(output_series_name=SERIES_A_PRIME)
def generate_series_a_prime(
    input_data_set_for_timesteps: pandas.DataFrame,
) -> pandas.Series:
    series_to_return = (
        input_data_set_for_timesteps.groupby([ACCOUNT_NO, TIMESTEP_NO])[PARAM_A]
        .rolling(window=2, min_periods=1)
        .apply(
            cache_enabled_generate_series_a_prime_mapper_generator(
                cache_size_limit=9, tuple_of_initial_values=(100,)
            ),
            kwargs={"entire_input_data_set": input_data_set_for_timesteps},
        )
    )
    return series_to_return.rename(SERIES_A_PRIME)


def cache_enabled_generate_series_a_prime_mapper_generator(
    cache_size_limit: int, tuple_of_initial_values: Tuple
):
    cache = LRU(maxsize=cache_size_limit)

    if type(tuple_of_initial_values) != tuple:
        raise TypeError(
            f"the argument 'tuple_of_initial_values' should be a tuple"
            f"but a {type(tuple_of_initial_values)} was found instead."
        )

    def closed_generate_series_a_prime_mapper(
        rolling_window_series: pandas.Series, entire_input_data_set: pandas.DataFrame
    ):
        account_no, timestep_no = rolling_window_series.tail(n=1).index[0]
        logger.debug(
            f"rolling window is currently at {account_no} time_step {timestep_no}"
        )

        if 0 < timestep_no <= len(tuple_of_initial_values):
            cache[f"{account_no}_{timestep_no}"] = tuple_of_initial_values[
                timestep_no - 1
            ]
        else:
            list_of_previous_computations = []

            for i in range(len(tuple_of_initial_values), 0, -1):
                list_of_previous_computations.append(
                    cache[f"{account_no}_{timestep_no-i}"]
                )

            previous_values = tuple(list_of_previous_computations)

            cache[f"{account_no}_{timestep_no}"] = core_arithmetic_for_current_step(
                account_no=account_no,
                timestep_no=timestep_no,
                current_window_series=rolling_window_series,
                tuple_of_previous_calculated_values=previous_values,
                entire_input_data_set=entire_input_data_set,
            )

        return cache[f"{account_no}_{timestep_no}"]

    return closed_generate_series_a_prime_mapper


def core_arithmetic_for_current_step(
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

@register(output_series_name='TEST_OUTPUT_C')
@calculate_over_window(
    series_to_run_window_over=PARAM_A,
    window_size=2,
    output_series_name='TEST_OUTPUT_C'
)
@adapt_stepwise_nonrecursive_calc_for_window
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
