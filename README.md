# viscmip
Create plots of a CMIP6 project

Scans through a CMIP6 directory structure and checks for abnormalities, 
plotting the time series of the variables min/max along the way

```
usage: __main__.py [-h] [-p] [-o OUTPUT] [-c [CASES [CASES ...]]]
                   [-e [ENS [ENS ...]]] [-l LENGTH]
                   [-v [VARIABLES [VARIABLES ...]]] [-t [TABLES [TABLES ...]]]
                   [-n NODES]
                   cmip_dir

positional arguments:
  cmip_dir              the source CMIP6 directory

optional arguments:
  -h, --help            show this help message and exit
  -p, --plot
  -o OUTPUT, --output OUTPUT
                        output path, default is ./animations
  -c [CASES [CASES ...]], --cases [CASES [CASES ...]]
                        the list of cases to animate, default is all
  -e [ENS [ENS ...]], --ens [ENS [ENS ...]]
                        the ensemble members to animate, default is all
  -l LENGTH, --length LENGTH
                        how many time steps to animate, default is all
  -v [VARIABLES [VARIABLES ...]], --variables [VARIABLES [VARIABLES ...]]
                        which variables to animate, default is all
  -t [TABLES [TABLES ...]], --tables [TABLES [TABLES ...]]
                        a list of tables to animate, default is all
  -n NODES, --nodes NODES
                        the number of nodes to use to plot in parallel,
                        default = 1
```
