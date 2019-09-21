import os
import sys
import argparse

from distributed import Client, progress


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmip_dir', help="the source CMIP6 directory")
    parser.add_argument('-c', '--cases', help="the list of cases to animate, default is all", default=["all"])
    parser.add_argument('-e', '--ens', help="the ensemble members to animate, default is all", default=["all"])
    parser.add_argument('-l', '--length', help="how many time steps to animate, default is all", default="all")
    parser.add_argument('-v', '--variables', help="which variables to animate, default is all", default=["all"])
    parser.add_argument('-t', '--tablrs', help="a list of tables to animate, default is all", default=["all"])
    parser.add_argument('-n', '--nodes', help="the number of nodes to use to plot in parallel, default = 1", default=1, type=int)

    args_ = parser.parse_args(sys.argv[1:])
    return args_

def plot_var(var_path):
    pass


def main():

    args_ = parse_args()

    if not os.path.exists(args_.cmip_dir):
        raise ValueError("invalid data directory")

    if args_.ens != ['all']:
        variant_ids = ["r{}i1p1f1".format(x) for x in args_.ens]

    cases = os.listdir(args_.cmip_dir)
    for case in cases:
        if case not in args_.tables and args_.tables != ['all']:
            continue

        ens = os.listdir(os.path.join(args_.cmip_dir))
        for e in args_.ens:
            if args_.ens != ['all'] and e not in variant_ids:
                continue

            variables = os.listdir(os.path.join(args_.cmip_dir,  case, ens))
            for var in variables:
                if var not in args_.variables and args_.variables != ['all']:
                    continue
                plot_var(os.listdir(os.path.join(args_.cmip_dir,  case, ens, var)))
            
    return 0


if __name__ == "__main__":
    sys.exit(main())