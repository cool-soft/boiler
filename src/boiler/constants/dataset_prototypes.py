import pandas as pd

from boiler.constants import column_names

HEATING_OBJ = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP,
    column_names.FORWARD_PIPE_COOLANT_PRESSURE,
    column_names.BACKWARD_PIPE_COOLANT_PRESSURE,
    column_names.FORWARD_PIPE_COOLANT_VOLUME,
    column_names.BACKWARD_PIPE_COOLANT_VOLUME
])

WEATHER = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.WEATHER_TEMP
])

TEMP_GRAPH = pd.DataFrame(columns=[
    column_names.WEATHER_TEMP,
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP
])

TIMEDELTA = pd.DataFrame(columns=[
    column_names.HEATING_OBJ_ID,
    column_names.AVG_TIMEDELTA
])

TEMP_REQUIREMENTS = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP
])

CONTROL_ACTION = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.FORWARD_PIPE_COOLANT_TEMP
])

HEATING_SYSTEM_REACTION = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.HEATING_OBJ_ID,
    column_names.HEATING_OBJ_TYPE,
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP
])
