import sys

sys.path.append("..")


def truncate(num, n):
    integer = int(num * (10 ** n)) / (10 ** n)
    return float(integer)
