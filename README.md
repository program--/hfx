# hfx

This repository contains a script `hfx.py` that quickly generates a subset of hydrofabric data from the CONUS pre-release GeoPackage.

This **should not** be used in place of [hfsubset](github.com/LynkerIntel/hfsubset), unless you have a specific reason, or a local copy of the CONUS hydrofabric GeoPackage, as this queries directly on feature IDs.

## Usage

Install the requirements with:

```sh
$ pip install -r requirements.txt
```

Then set permissions and run `hfx.py`:

```sh
$ chmod +x hfx.py
$ ./hfx.py -h
#> usage: hfx [-h] [-o OUTPUT] [-c CONUS] [--debug] ID [ID ...]
#> 
#> positional arguments:
#>   ID                    Hydrofabric Identifiers
#>                         (i.e. cat-32, nex-85, wb-3813, cnx-391, ...)
#> 
#> options:
#>   -h, --help            show this help message and exit
#>   -o OUTPUT, --output OUTPUT
#>                         Path to output GeoPackage
#>   -c CONUS, --conus CONUS
#>                         Path to local CONUS hydrofabric GeoPackage. If this
#>                         argument is not specified, then the filtering happens
#>                         via web requests.
#>   --debug
#>                         Output debug messages.
```

> **Note**: it's recommended to download the CONUS GeoPackage
>           and use this script on it instead of the web requests.
>           Otherwise, its performance will be degraded.

