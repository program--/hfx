#!/usr/bin/env python3
import geopandas as gpd
import pandas as pd
import pypika as pika
import pyarrow as pa
import logging

from pyarrow import parquet as pq
from enum import Enum
from typing import TypedDict

# VRT     = 'hydrofabric.vrt'
VRT     = 'https://nextgen-hydrofabric.s3.amazonaws.com/pre-release/conus.gpkg'
NETWORK = 's3://nextgen-hydrofabric/pre-release/conus_net.parquet'

class IDType(Enum):
    UNKNOWN   = 0
    CATCHMENT = 1
    WATERBODY = 2
    NEXUS     = 3

    @staticmethod
    def match(id: str):
        PREFIX = id.split('-', maxsplit=1)[0]
        match PREFIX:
            case 'cat': # Catchment/Divide
                return IDType.CATCHMENT
            case 'wb':  # Waterbody/Flowpath
                return IDType.WATERBODY
            case 'nex': # Normal Nexus
                return IDType.NEXUS
            case 'cnx': # Coastal Nexus
                return IDType.NEXUS
            case 'tnx': # Terminal Nexus
                return IDType.NEXUS
            case _: 
                return IDType.UNKNOWN

class FilterSpec(TypedDict):
    catchments: list[str]
    waterbodies: list[str]
    nexuses: list[str]

    @staticmethod 
    def append(cls, id: str) -> None:
        id_type = IDType.match(id)
        logging.debug(f'Adding {id} as {id_type}')

        match id_type:
            case IDType.CATCHMENT:
                cls['catchments'].append(id)
            case IDType.WATERBODY:
                cls['waterbodies'].append(id)
            case IDType.NEXUS:
                cls['nexuses'].append(id)
            case IDType.UNKNOWN:
                raise ValueError(f'`{id}` is an invalid ID')


def filter_network(spec: FilterSpec) -> FilterSpec:
    """Filter IDs from the hydrofabric network table
       to get corresponding layers.

    Args:
        catchments: Optional list of string catchment/divide IDs
        waterbodies: Optional list of string waterbody/flowpath IDs
        nexuses: Optional list of string nexus IDs

    Returns:
        FilterSpec: Filled FilterSpec containing all associated IDs
    """
    global NETWORK

    id_filter = []

    if len(spec['waterbodies']) > 0:
        id_filter.append([('id', 'in', spec['waterbodies'])])
    if len(spec['nexuses']) > 0:
        id_filter.append([('toid', 'in', spec['nexuses'])])
    if len(spec['catchments']) > 0:
        id_filter.append([('divide_id', 'in', spec['catchments'])])

    logging.info(f'Using ID filter: {id_filter}')
    table: pa.Table = pq.read_table(NETWORK, columns=['id', 'toid', 'divide_id'], filters=id_filter)

    if table.num_rows == 0:
        logging.error(f'Network table query returned 0 rows with filter: {id_filter}')
        exit(1)

    new_spec: FilterSpec = table.rename_columns(['waterbodies', 'nexuses', 'catchments']).to_pydict()
    for k, v in new_spec.items(): # Remove all None values
        new_spec[k] = [x for x in v if x is not None]

    logging.info(f'Query returned spec: {new_spec}')

    return new_spec

def filter_catchments(spec: FilterSpec, output: 'pathlib.Path', hf_path=VRT):
    logging.debug(f'Filtering catchment IDs {spec["catchments"]}')
    table = pika.Table('divides')
    sql = pika.Query.from_(table).where(table.divide_id.isin(spec['catchments'])).select('*')

    logging.debug(f'Catchments using query: {sql}')

    gpd.read_file(
        hf_path,
        sql=str(sql),
        sql_dialect='sqlite',
        use_arrow=True,
        engine='pyogrio'
    ).to_file(
        output,
        driver='GPKG',
        layer='divides',
        engine='pyogrio'
    )

    logging.info(f'Wrote catchments to {output}')

def filter_waterbodies(spec: FilterSpec, output: 'pathlib.Path', hf_path=VRT):
    logging.debug(f'Filtering waterbodies IDs {spec["waterbodies"]}')
    table = pika.Table('flowpaths')
    sql = pika.Query.from_(table).where(table.id.isin(spec['waterbodies'])).select('*')

    logging.debug(f'Waterbodies using query: {sql}')

    gpd.read_file(
        hf_path,
        sql=str(sql),
        sql_dialect='sqlite',
        use_arrow=True,
        engine='pyogrio'
    ).to_file(
        output,
        driver='GPKG',
        layer='flowpaths',
        engine='pyogrio'
    )

    logging.info(f'Wrote waterbodies to {output}')

def filter_nexuses(spec: FilterSpec, output: 'pathlib.Path', hf_path=VRT):
    logging.debug(f'Filtering nexuses IDs {spec["nexuses"]}')
    table = pika.Table('nexus')
    sql = pika.Query.from_(table).where(table.id.isin(spec['nexuses'])).select('*')

    logging.debug(f'Nexuses using query: {sql}')

    gpd.read_file(
        hf_path,
        sql=str(sql),
        sql_dialect='sqlite',
        use_arrow=True,
        engine='pyogrio'
    ).to_file(
        output,
        driver='GPKG',
        layer='nexus',
        engine='pyogrio'
    )

    logging.info(f'Wrote nexuses to {output}')


def filter_hydrofabric(spec: FilterSpec, output: 'pathlib.Path', hf_path=VRT) -> str:
    if len(spec['catchments']) > 0:
        filter_catchments(spec, output, hf_path)
    if len(spec['waterbodies']) > 0:
        filter_waterbodies(spec, output, hf_path)
    if len(spec['nexuses']) > 0:
        filter_nexuses(spec, output, hf_path)
    return str(output)

# =============================================================================

if __name__ == '__main__':
    import argparse
    import pathlib

    def hfx_add_arguments(parser: argparse.ArgumentParser):
        parser.add_argument('identifiers', metavar='ID', type=str, nargs='+',
                            help='Hydrofabric Identifiers (i.e. cat-32, nex-85, wb-3813, cnx-391, ...)')
        
        parser.add_argument('-o', '--output', type=str, default='hydrofabric.gpkg',
                            help='Path to output GeoPackage')
        parser.add_argument('-c', '--conus', type=str,
                            help='Path to local CONUS hydrofabric GeoPackage. If this argument is not specified, then the filtering happens via web requests.')
        parser.add_argument('--debug', action='store_true', default=False,
                            help='Output debug messages.')

    def hfx_logger(debug: bool = False) -> logging.Logger:
        logging.basicConfig(
            level=logging.DEBUG if debug else logging.INFO,
            format='[{asctime}] {levelname} :: {message}',
            datefmt='%Y-%m-%d %H:%M:%S',
            style = '{'
        )
    
        return logging.getLogger()
    # -------------------------------------------------------------------------
    hfx_parser = argparse.ArgumentParser(prog='hfx')
    hfx_add_arguments(hfx_parser)

    args = hfx_parser.parse_args()
    logger = hfx_logger(args.debug)

    identifiers: list[str] = args.identifiers
    logger.debug(f'Identifiers: {identifiers}')

    output = pathlib.Path(args.output)
    logger.debug(f'Output Path: {str(output)}')

    spec: FilterSpec = {'catchments': [], 'waterbodies': [], 'nexuses': []}
    for identifier in identifiers:
        FilterSpec.append(spec, identifier)

    request_spec = filter_network(spec)
    path         = filter_hydrofabric(request_spec, output, hf_path=VRT if args.conus is None else args.conus)
    logging.info(f'Outputted example hydrofabric to {path}')
