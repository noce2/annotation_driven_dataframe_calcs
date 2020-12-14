from annotation_driven_dataframe_calcs.caching_tools import LRU
from annotation_driven_dataframe_calcs.column_names import (
    ACCOUNT_NO, PARAM_A,
    SERIES_A_PRIME,
    TIMESTEP_NO,
)
import pandas


def generate_series_a_prime(
    input_data_set_for_timesteps: pandas.DataFrame,
) -> pandas.Series:
    series_to_return = (
        input_data_set_for_timesteps.groupby([ACCOUNT_NO, TIMESTEP_NO])[PARAM_A]
        .rolling(window=2, min_periods=1)
        .apply(
            cache_enabled_generate_series_a_prime_mapper_generator(9),
            kwargs={"entire_input_data_set": input_data_set_for_timesteps},
        )
    )
    return series_to_return


def cache_enabled_generate_series_a_prime_mapper_generator(cache_size_limit):
    cache = LRU(maxsize=cache_size_limit)

    def closed_generate_series_a_prime_mapper(
        x, entire_input_data_set: pandas.DataFrame
    ):
        return 1

    return closed_generate_series_a_prime_mapper
