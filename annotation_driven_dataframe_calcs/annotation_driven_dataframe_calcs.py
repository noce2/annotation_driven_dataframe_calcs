"""Main module."""
from typing import Union
import pandas
import dask.dataframe as dd

from annotation_driven_dataframe_calcs.column_names import ACCOUNT_NO, TIMESTEP_NO

def run(
    input_data, run_config
):
    return None

def expand_for_timesteps(
    input_data, first_time_step: int, last_time_step: int
):
    interim_frame = input_data
    interim_frame[TIMESTEP_NO] = [
        list(
            range(first_time_step, last_time_step+1)
        )
    ] * len(input_data.index)
    interim_frame_exploded = interim_frame.explode(column=TIMESTEP_NO, ignore_index=True)
    acct_stmt_indexex_exploded_frame = interim_frame_exploded.set_index([ACCOUNT_NO, TIMESTEP_NO])


    return acct_stmt_indexex_exploded_frame
