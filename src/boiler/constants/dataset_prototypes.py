import pandas as pd

from boiler.constants import column_names

HEATING_OBJ = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.FORWARD_TEMP,
    column_names.BACKWARD_TEMP,
    column_names.FORWARD_PRESSURE,
    column_names.BACKWARD_PRESSURE,
    column_names.FORWARD_VOLUME,
    column_names.BACKWARD_VOLUME
])

WEATHER = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.WEATHER_TEMP
])

TEMP_GRAPH = pd.DataFrame(columns=[
    column_names.WEATHER_TEMP,
    column_names.FORWARD_TEMP,
    column_names.BACKWARD_TEMP
])

TIMEDELTA = pd.DataFrame(columns=[
    column_names.HEATING_OBJ_ID,
    column_names.AVG_TIMEDELTA
])

TEMP_REQUIREMENTS = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.FORWARD_TEMP,
    column_names.BACKWARD_TEMP
])

CONTROL_ACTION = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.CIRCUIT_TYPE,
    column_names.FORWARD_TEMP
])

HEATING_SYSTEM_STATE = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.HEATING_OBJ_ID,
    column_names.HEATING_OBJ_TYPE,
    column_names.CIRCUIT_TYPE,
    column_names.FORWARD_TEMP,
    column_names.BACKWARD_TEMP
])
