import math


def arithmetic_round(number: float) -> int:
    number_floor = math.floor(number)
    if number - number_floor < 0.5:
        rounded_number = number_floor
    else:
        rounded_number = number_floor + 1
    return rounded_number
