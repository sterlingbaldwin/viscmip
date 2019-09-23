import os
import sys
import argparse
import glob
import cdms2

from tqdm import tqdm
from dask_jobqueue import SLURMCluster
from distributed import Client, as_completed, LocalCluster


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


def make_pngs(inpath, outpath, varname, serial=False, res=(800, 600)):
    import vcs
    dataset = cdms2.open(inpath)
    vardata = dataset[varname]
    x = vcs.init(geometry=res)

    pngs_path = os.path.join(outpath, 'pngs')
    if not os.path.exists(pngs_path):
        os.makedirs(pngs_path)

    pngs = list()
    # assuming that the 0th axis is time
    # if serial:
    pbar = tqdm(total=vardata.shape[0], desc="starting: {}".format(varname))
    for step in range(vardata.shape[0]):
        time = int(round(vardata.getTime()[step]))
        png = os.path.join(pngs_path, '{}.png'.format(time))
        if os.path.exists(png):
            print("png {} already exists")
            continue

        x.clear()
        x.plot(vardata[step])
        x.png(png, width=res[0], height=res[1], units='pixels')
        pngs.append(png)
        # if serial:
        pbar.set_description("plotting: {} - {}".format(varname, time))
        pbar.update(1)
    # if serial:
    pbar.close()

    return pngs


def make_mp4(varname, pngs_path, res=(800, 600)):
    import vcs
    canvas = vcs.init(geometry=res)
    canvas.ffmpeg('{}.mp4'.format(varname), sorted(
        glob.glob("{}/*pngs".format(pngs_path))))


def plot_var(varname, varpath, outpath, client, res=(800, 600)):

    if not os.path.exists(outpath):
        os.makedirs(outpath)

    pngs_paths = list()
    if client:
        futures = list()

    for root, _, files in os.walk(varpath):
        if not files:
            continue
        pbar = tqdm(files)
        for f in pbar:
            inpath = os.path.join(root, f)
            out = os.path.join(outpath, varname)

            _, filename = os.path.split(inpath)

            if client:
                futures.append(
                    client.submit(make_pngs, inpath, out, varname))
            else:
                pbar.set_description('Rendering png: {}-{}', varname, filename)
                pngs_paths.extend(make_pngs(inpath, out, varname, serial=True))

    if client:
        for future, png in as_completed(futures, with_results=True):
            pbar.set_description('Rendering pngs: {}-{}', varname, filename)
            pngs_paths.extend(png)

    _, head = os.path.split(pngs_paths[0])
    print("Setting up mpeg4")
    if client:
        f = client.submit(make_mp4, varname, head)
        f.result()

    else:
        make_mp4(varname, head)


def main():

    args_=parse_args()


    if not args_.serial:
        print("starting cluster")
        # cluster=SLURMCluster(cores = 4,
        #                        memory = "1 M",
        #                        project = "e3sm",
        #                        walltime = "02:00:00",
        #                        queue = "slurm")
        cluster = LocalCluster(args_.nodes)
        client=Client(cluster)
    else:
        client=None

    futures=list()

    if not os.path.exists(args_.cmip_dir):
        raise ValueError("invalid data directory")

    if args_.ens != ['all']:
        variant_ids=["r{}i1p1f1".format(x) for x in args_.ens]

    if not isinstance(args_.variables, list):
        args_.variables=[args_.variables]

    cases=os.listdir(args_.cmip_dir)
    for case in cases:
        if case not in args_.tables and args_.tables != ['all']:
            continue

        ens=os.listdir(os.path.join(args_.cmip_dir, case))
        for e in ens:
            if args_.ens != ['all'] and e not in variant_ids:
                continue

            tables=os.listdir(os.path.join(args_.cmip_dir,  case, e))

            for table in tables:
                if args_.tables != ['all'] and table not in args_.tables:
                    continue

                variables=os.listdir(os.path.join(
                    args_.cmip_dir,  case, e, table))
                for var in variables:
                    if var not in args_.variables and args_.variables != ['all']:
                        continue
                    varpath=os.path.join(
                        args_.cmip_dir,  case, e, table, var)
                    plot_var(varname=var, varpath=varpath,
                             outpath=args_.output, client=client)

    return 0


if __name__ == "__main__":
    sys.exit(main())
