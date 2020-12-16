from loguru import logger
import pandas
from annotation_driven_dataframe_calcs.column_names import ACCOUNT_NO, TIMESTEP_NO
from functools import wraps


def calculate_over_window(
    series_to_run_window_over: str,
    window_size: int,
    output_series_name: str
):
    def decorate_calculate_over_window(func):
        @wraps(func)
        def wrapper_calculate_over_window(input_data_set_for_timesteps):
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

def adapt_stepwise_calc_for_window(func):
    @wraps(func)
    def window_adapted_calc(
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
    return window_adapted_calc
