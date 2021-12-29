import argparse
import pandas as pd

columns = {"Safra": ["fundo", ...], "Credit": ["n_contrato", ...]}


def main(broker):
    print(broker)
    # df = pd.read_excel(file)
    # df.columns = columns[broker]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Choose broker here", default="Safra")
    args = parser.parse_args()
    df = main(args.broker)
