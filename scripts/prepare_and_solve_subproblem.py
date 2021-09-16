# SPDX-FileCopyrightText: : 2017-2020 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: GPL-3.0-or-later

# coding: utf-8
"""
...
"""

import pypsa
import logging

import pandas as pd

from _helpers import configure_logging

from solve_network import prepare_network, solve_network
from vresutils.benchmark import memory_logger

def adjust_demand(n_fine, n_coarse, busmap_fine, decompose_c):
    
    country_buses = n_coarse.buses.query("country == @decompose_c").index

    for branch_c in ["lines", "links"]:
        for linequery, bus, p in [("bus0 in @country_buses and not bus1 in @country_buses", "bus0", "p0"),
                                  ("bus1 in @country_buses and not bus0 in @country_buses", "bus1", "p1")]:
    
            #adapt (lines):
            lines_i = getattr(n_coarse, branch_c).query(linequery).index
            buses_i = getattr(n_coarse, branch_c).loc[lines_i][bus]

            flows_b = getattr(n_coarse, branch_c + "_t")[p].groupby(buses_i, axis=1).sum()

            buses_i_fine = [bus.split('dec')[0][:-1] for bus in buses_i.unique()]

            n_fine.loads_t.p_set[buses_i_fine]+=flows_b[buses_i.unique()]

    return n_fine

def drop_components(n, decompose_c):
    # make code more general, eg. specify components to be dropped etc (TODO)
    drop_buses = n.buses.query("country != @decompose_c").index

    query = "bus0 in @drop_buses or bus1 in @drop_buses"
    drop_lines = n.lines.query(query).index
    drop_links = n.links.query(query).index

    query = "bus in @drop_buses"
    drop_genes = n.generators.query(query).index
    drop_stoun = n.storage_units.query(query).index
    drop_store = n.stores.query(query).index
    drop_loads = n.loads.query(query).index
    
    
    n.mremove("Bus", drop_buses)

    n.mremove("Line", drop_lines)
    n.mremove("Link", drop_links)

    n.mremove("Generator", drop_genes)
    n.mremove("StorageUnit", drop_stoun)
    n.mremove("Store", drop_store)
    n.mremove("Load", drop_loads)
    
    return n


if __name__ == "__main__":
    if 'snakemake' not in globals():
        from _helpers import mock_snakemake
        snakemake = mock_snakemake('add_electricity')
    configure_logging(snakemake)

    n_coarse = pypsa.Network(snakemake.input.network_coarse) #solved coarse network for EU
    n_fine = pypsa.Network(snakemake.input.network_fine) #unsolved fine network for a subproblem (read from reference network?)

    busmap_fine = pd.read_csv(snakemake.input.busmap_fine, index_col=0, squeeze=True, dtype=str)
    busmap_fine.index = busmap_fine.index.astype(str)
    
    adjust_demand(n_fine, n_coarse, busmap_fine, snakemake.wildcards.cntry)

    n_fine = drop_components(n_fine, snakemake.wildcards.cntry)

    ### SOLVE NETWORK WITH ADDITIONAL CONSTRAINTS...
    tmpdir = snakemake.config['solving'].get('tmpdir')
    
    if tmpdir is not None:
        Path(tmpdir).mkdir(parents=True, exist_ok=True)
        
    opts = snakemake.wildcards.opts.split('-')
    solve_opts = snakemake.config['solving']['options']

    fn = getattr(snakemake.log, 'memory', None)
    with memory_logger(filename=fn, interval=30.) as mem:
        n_fine = prepare_network(n_fine, solve_opts)
        n_fine = solve_network(n_fine, config=snakemake.config, opts=opts,
                               solver_dir=tmpdir,
                               solver_logfile=snakemake.log.solver1)
        n_fine.export_to_netcdf(snakemake.output.network_dec)

    logger.info("Maximum memory usage: {}".format(mem.mem_usage))

    ### SOLVE SINGLE COUNTRY; FOR REFERENCE...
    n_fine = pypsa.Network(snakemake.input.network_fine) #unsolved fine network for a subproblem (read from reference network?)

    n_fine = drop_components(n_fine, snakemake.wildcards.cntry)

    tmpdir = snakemake.config['solving'].get('tmpdir')
    
    if tmpdir is not None:
        Path(tmpdir).mkdir(parents=True, exist_ok=True)
        
    opts = snakemake.wildcards.opts.split('-')
    solve_opts = snakemake.config['solving']['options']

    fn = getattr(snakemake.log, 'memory', None)
    with memory_logger(filename=fn, interval=30.) as mem:
        n_fine = prepare_network(n_fine, solve_opts)
        n_fine = solve_network(n_fine, config=snakemake.config, opts=opts,
                               solver_dir=tmpdir,
                               solver_logfile=snakemake.log.solver2)
        n_fine.export_to_netcdf(f"results/networks/elec_s_dec:{snakemake.wildcards.cntry}_{snakemake.wildcards.clusters}_ev_lv1.0_{snakemake.wildcards.opts}.nc")

    logger.info("Maximum memory usage: {}".format(mem.mem_usage))
    
