import os
import sys
import argparse
import glob
import cdms2
import vcs

from dask_jobqueue import SLURMCluster
from distributed import Client


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmip_dir', help="the source CMIP6 directory")
    parser.add_argument('-o', '--output', help="output path, default is ./animations", default="animations")
    parser.add_argument(
        '-c', '--cases', help="the list of cases to animate, default is all", default=["all"])
    parser.add_argument(
        '-e', '--ens', help="the ensemble members to animate, default is all", default=["all"])
    parser.add_argument(
        '-l', '--length', help="how many time steps to animate, default is all", default="all")
    parser.add_argument(
        '-v', '--variables', help="which variables to animate, default is all", default=["all"])
    parser.add_argument(
        '-t', '--tablrs', help="a list of tables to animate, default is all", default=["all"])
    parser.add_argument(
        '-n', '--nodes', help="the number of nodes to use to plot in parallel, default = 1", default=1, type=int)

    args_ = parser.parse_args(sys.argv[1:])
    return args_


def plot_file(inpath, outpath, varname):
    
    dataset = cdms2.open(inpath)
    vardata = dataset[varname]
    x = vcs.init()

    # assuming that the 0th axis is time
    for step in range(vardata.shape[0]):
        png = os.path.join(outpath, 'pngs', '{:06d}.png'.format(step))
        x.plot(vardata[step])
        x.png(png, width=1200, height=1000, units='pixels')

    anim_name = os.path.join(outpath, "{}.mp4".format(varname))
    pngs = sorted(glob.glob(outpath + "/pngs/*png"))
    x.ffmpeg(anim_name, pngs)


def plot_var(varname, varpath, outpath, client):

    futures = list()

    anim_out_path = os.path.join(outpath, varname)
    if not os.path.exists(anim_out_path):
        os.makedirs(anim_out_path)
    for _, _, files in os.walk(varpath):
        if not files:
            continue
        for f in files:
            inpath = os.path.join(root, f)
            out = os.path.join(outpath, varname)

            print('submitting', varname, inpath, out)
            futures.append(
                client.submit(plot_file, inpath, out, varname))


def main():

    args_ = parse_args()
    import ipdb; ipdb.set_trace()
    cluster = SLURMCluster(cores=4,
                           project="e3sm",
                           walltime="01:00:00",
                           queue="normal")
    cluster.start_workers(args_.nodes)
    client = Client(cluster)
    futures = list()

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
                var_path = os.listdir(os.path.join(
                    args_.cmip_dir,  case, ens, var))
                plot_var(var_path, client)

    return 0


if __name__ == "__main__":
    sys.exit(main())
