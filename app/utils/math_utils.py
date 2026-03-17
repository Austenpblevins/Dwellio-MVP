from statistics import median

def safe_divide(a: float, b: float) -> float:
    return 0.0 if b == 0 else a / b

def median_or_zero(values: list[float]) -> float:
    return 0.0 if not values else float(median(values))
