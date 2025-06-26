from datetime import datetime


def apply_range(col, rng: dict, container: list):
    """
    Вспомогательная функция: добавляет выражения >=, <=, = в container.
    """
    if not rng:
        return
    if "gte" in rng:
        container.append(col >= rng["gte"])
    if "lte" in rng:
        container.append(col <= rng["lte"])
    if "eq" in rng:
        container.append(col == rng["eq"])


def apply_date_range(self, col, rng:dict, container: list):
    new_rng = {}
    for k, v in rng.items():
        new_rng[k] = datetime.strptime(v, "%Y-%m-%d").date()
    apply_range(col, new_rng, container)