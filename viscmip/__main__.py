import os
import sys
import argparse
import glob
import cdms2
import vcs

from tqdm import tqdm
from dask_jobqueue import SLURMCluster
from distributed import Client


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmip_dir', help="the source CMIP6 directory")
    parser.add_argument(
        '-o', '--output', help="output path, default is ./animations", default="animations")
    parser.add_argument(
        '-c', '--cases', help="the list of cases to animate, default is all", default=["all"])
    parser.add_argument(
        '-e', '--ens', help="the ensemble members to animate, default is all", default=["all"])
    parser.add_argument(
        '-l', '--length', help="how many time steps to animate, default is all", default="all")
    parser.add_argument(
        '-v', '--variables', help="which variables to animate, default is all", default=["all"])
    parser.add_argument(
        '-t', '--tables', help="a list of tables to animate, default is all", default=["all"])
    parser.add_argument(
        '-n', '--nodes', help="the number of nodes to use to plot in parallel, default = 1", default=1, type=int)
    parser.add_argument(
        '-s', '--serial', help="run on the local node, nothing is submitted to slurm", action="store_true")

    args_ = parser.parse_args(sys.argv[1:])
    return args_


def make_pngs(inpath, outpath, varname, serial=False):

    dataset = cdms2.open(inpath)
    vardata = dataset[varname]
    x = vcs.init(geometry=(1200, 1000))

    pngs_path = os.path.join(outpath, 'pngs')
    if not os.path.exists(pngs_path):
        os.makedirs(pngs_path)

    # assuming that the 0th axis is time
    if serial:
        pbar = tqdm(total=vardata.shape[0], desc="starting: {}".format(varname))
    for step in range(vardata.shape[0]):
        time = vardata.getTime()[step]
        png = os.path.join(pngs_path, '{:06d}.png'.format(time))
        if os.path.exists(png):
            continue
        
        x.plot(vardata[step])
        x.png(png, width=1200, height=1000, units='pixels')
        if serial:
            pbar.set_description("plotting: {} - {}".format(varname, time))
            pbar.update(1)
    if serial:
        pbar.close()

    return png


def plot_var(varname, varpath, outpath, client):

    anim_out_path = os.path.join(outpath, varname)
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    pngs_paths = list()

    for root, _, files in os.walk(varpath):
        if not files:
            continue
        for f in files:
            inpath = os.path.join(root, f)
            out = os.path.join(outpath, varname)

            print('Rendering', inpath, out)
            if client:
                pngs_paths.append(
                    client.submit(make_pngs, inpath, out, varname))
            else:
                pngs_paths.extend(make_pngs(inpath, out, varname, serial=True))

    x = vcs.init(geometry=(1200, 1000))
    if client:
        client.submit(x.ffmpg(anim_out_path, pngs_paths))
    else:
        x.ffmpg(anim_out_path, pngs_paths)


def main():

    args_ = parse_args()


    if not args_.serial:
        print("starting cluster")
        cluster = SLURMCluster(cores=4,
                               memory="1 M",
                               project="e3sm",
                               walltime="01:00:00",
                               queue="slurm")
        cluster.start_workers(args_.nodes)
        client = Client(cluster)
    else:
        client = None

    futures = list()

    if not os.path.exists(args_.cmip_dir):
        raise ValueError("invalid data directory")

    if args_.ens != ['all']:
        variant_ids = ["r{}i1p1f1".format(x) for x in args_.ens]

    if not isinstance(args_.variables, list):
        args_.variables = [args_.variables]

    cases = os.listdir(args_.cmip_dir)
    for case in cases:
        if case not in args_.tables and args_.tables != ['all']:
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
                    plot_var(varname=var, varpath=varpath,
                             outpath=args_.output, client=client)

    return 0


if __name__ == "__main__":
    sys.exit(main())
