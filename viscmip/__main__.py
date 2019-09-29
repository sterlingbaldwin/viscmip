import os
import sys
import argparse
import cdms2
import vcs

from tqdm import tqdm
import numpy as np
from distributed import Client, as_completed, LocalCluster


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmip_dir', help="the source CMIP6 directory")
    parser.add_argument('-p', '--plot', action="store_true")
    parser.add_argument(
        '-o', '--output', help="output path, default is ./animations", default="animations")
    parser.add_argument(
        '-c', '--cases', help="the list of cases to animate, default is all", nargs="*", default=["all"])
    parser.add_argument(
        '-e', '--ens', help="the ensemble members to animate, default is all", nargs="*", default=["all"])
    parser.add_argument(
        '-l', '--length', help="how many time steps to animate, default is all", default="all")
    parser.add_argument(
        '-v', '--variables', help="which variables to animate, default is all", nargs="*", default=["all"])
    parser.add_argument(
        '-t', '--tables', help="a list of tables to animate, default is all", nargs="*", default=["all"])
    parser.add_argument(
        '-n', '--nodes', help="the number of nodes to use to plot in parallel, default = 1", default=1, type=int)
    args_ = parser.parse_args(sys.argv[1:])
    return args_


def run_minmax(inpath, varname, i):

    mins = []
    maxs = []
    filedata = cdms2.open(inpath)
    vardata = filedata[varname]
    for step in range(vardata.shape[0]):
        stepdata = vardata[step]
        minmax = vcs.minmax(stepdata)
        mins.append(minmax[0])
        maxs.append(minmax[1])
    return mins, maxs, i


def get_std(mins, maxs, client):
    mins_std_future = client.submit(np.std, mins)
    maxs_std_future = client.submit(np.std, maxs)
    return mins_std_future.result(), maxs_std_future.result()


def get_minmax(varname, varpath, client):

    futures = list()
    numfiles = 0
    for root, _, files in os.walk(varpath):
        if not files:
            continue
        numfiles = len(files)
        pbar = tqdm(files, desc="{}".format(varname))
        for i, f in enumerate(sorted(files)):
            inpath = os.path.join(root, f)
            futures.append(client.submit(run_minmax, inpath, varname, i))

    mins = []
    mins2d = [[] for x in range(numfiles)]
    maxs = []
    maxs2d = [[] for x in range(numfiles)]
    for future, minmax in as_completed(futures, with_results=True):
        pbar.update(1)
        mins2d[minmax[2]] = minmax[0]
        maxs2d[minmax[2]] = minmax[1]

    pbar.close()
    for l in mins2d:
        for i in l:
            mins.append(i)
    for l in maxs2d:
        for i in l:
            maxs.append(i)
    return mins, maxs


def plotminmax(outpath, mins, maxs, varname):
    canvas = vcs.init()

    gm = vcs.create1d()
    mn, mx = vcs.minmax(mins, maxs)
    gm.datawc_y1 = mn
    gm.datawc_y2 = mx

    template = vcs.createtemplate()
    template.scale(.95)
    template.move(.05, 'x')

    template.blank(["max"])
    canvas.plot(mins, gm, template, id=varname)

    template.blank(["min"])
    canvas.plot(maxs, gm, template, id=varname)

    canvas.png(outpath)
    canvas.clear()


def find_minmax_issue(mins, maxs, stdmin, stdmax):
    issues = list()
    for idx, _ in enumerate(mins):
        if (mins[idx] == 0.0 and maxs[idx] == 1.0):
            issues.append(idx)
        elif idx == 0 or idx == len(mins) - 1:
            continue
        elif abs(mins[idx-1] - mins[idx]) >= 3*stdmin and abs(mins[idx+1] - mins[idx]) >= 3*stdmin:
            issues.append(idx)
        elif abs(maxs[idx-1] - maxs[idx]) >= 3*stdmax and abs(maxs[idx+1] - maxs[idx]) >= 3*stdmax:
            issues.append(idx)
    if issues:
        return True, issues
    return False, None


def main():

    args_ = parse_args()
    cluster = LocalCluster(
        n_workers=args_.nodes,
        threads_per_worker=1,
        interface='lo')
    client = Client(address=cluster.scheduler_address)

    if not os.path.exists(args_.cmip_dir):
        raise ValueError("invalid data directory")

    if args_.ens != ['all']:
        variant_ids = ["r{}i1p1f1".format(x) for x in args_.ens]

    if not isinstance(args_.variables, list):
        args_.variables = [args_.variables]

    messages = list()

    cases = os.listdir(args_.cmip_dir)
    for case in cases:
        if case not in args_.cases and args_.cases != ['all']:
            continue

        ens = os.listdir(os.path.join(args_.cmip_dir, case))
        for e in ens:
            if args_.ens != ['all'] and e not in variant_ids:
                continue

            tables = os.listdir(os.path.join(args_.cmip_dir,  case, e))

            for table in tables:
                if args_.tables != ['all'] and table not in args_.tables:
                    continue

                variables = os.listdir(os.path.join(
                    args_.cmip_dir,  case, e, table))
                for var in variables:
                    if var not in args_.variables and args_.variables != ['all']:
                        continue
                    varpath = os.path.join(
                        args_.cmip_dir,  case, e, table, var)

                    mins, maxs = get_minmax(
                        varname=var, varpath=varpath, client=client)

                    stdmin = np.std(mins)
                    stdmax = np.std(maxs)
                    issue, issue_indeces = find_minmax_issue(mins, maxs, stdmin, stdmax)
                    if issue:
                        for idx in issue_indeces:
                            msg = "\tIssue found for {case}-{ens}-{var} at time index {i}".format(
                                case=case, ens=e, var=var, i=idx)
                            print(msg)
                            messages.append(msg)
                    pngpath = os.path.join(
                        args_.output, "{case}-{ens}-{var}-minmax.png".format(case=case, ens=e, var=var))
                    varstring = "{case}-{ens}-{var}".format(
                        case=case, ens=e, var=var)
                    plotminmax(pngpath, mins, maxs, varstring)


    if messages:
        print("---------------------------------------------------")
        print("Error summary:")
        for msg in messages:
            print(msg)
        print("---------------------------------------------------")
    else:
        print("No errors found")

    return 0


if __name__ == "__main__":
    sys.exit(main())
