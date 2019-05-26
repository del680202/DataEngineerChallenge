#!/usr/bin/env python
# encoding: utf-8


import argparse
import pandas as pd
import glob


def read_df(input):
    cols = ["id", "ip", "transition", "during", "start", "end"]
    all_files = glob.glob(input + "/*")
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, names=cols, header=None)
        li.append(df)
    return pd.concat(li)


def handle_average(input):
    df = read_df(input)
    mean_sec = df[["during"]].mean().iloc[0] / 1000
    print("Average session time: %.2f sec." % mean_sec)

def handle_top_k(input, k):
    df = read_df(input)
    print("Top-k sessions:")
    print(df.nlargest(k, columns=["during"]))


def main():
    parser = argparse.ArgumentParser(description='Determine top-k longest session or average session time')
    parser.add_argument("command", help="avg or top-k")
    parser.add_argument("-k", help="size of k", default=1)
    parser.add_argument("--input", help="sessionized logs path", required=True)
    args = parser.parse_args()
    if args.command == "avg":
        handle_average(args.input)
    elif args.command == "top-k":
        handle_top_k(args.input, int(args.k))
    else:
        raise Exception("Unknown command: %s" % args.command)

if __name__ == '__main__':
    main()
