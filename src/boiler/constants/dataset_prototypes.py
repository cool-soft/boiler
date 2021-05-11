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

# TODO: Заменить моделью связей элементов тепловой сети и свойствами соединения
#       Модель связей:
#           connection_id[int]
#           src_heating_obj_id[str]
#           src_circuit_id[str]
#           src_pipe_type[forward/backward]
#           dst_heating_obj_id[str]
#           dst_circuit_id[str]
#           dst_pipe_type[forward/backward]
#       Свойтсва соединения:
#           connection_id[int]
#           timedelta[pd.Timedelta]
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
    column_names.CIRCUIT_TYPE,
    column_names.FORWARD_PIPE_COOLANT_TEMP
])

HEATING_SYSTEM_STATE = pd.DataFrame(columns=[
    column_names.TIMESTAMP,
    column_names.HEATING_OBJ_ID,
    column_names.HEATING_OBJ_TYPE,
    column_names.CIRCUIT_TYPE,
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP
])
