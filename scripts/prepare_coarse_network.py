# SPDX-FileCopyrightText: : 2017-2020 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: GPL-3.0-or-later

# coding: utf-8
"""
...
"""

import pypsa
import logging
import re

import numpy as np
import pandas as pd

from _helpers import configure_logging, update_p_nom_max
from add_electricity import load_costs, add_nice_carrier_names
from cluster_network import busmap_for_n_clusters, clustering_for_n_clusters
from add_extra_components import attach_storageunits, attach_stores, attach_hydrogen_pipelines
from prepare_network import (set_line_s_max_pu, average_every_nhours, add_co2limit,
                             set_transmission_limit, set_line_nom_max)
from solve_network import prepare_network, solve_network
from vresutils.benchmark import memory_logger


def adjust_busmap_for_decomposition(busmap_ref, busmap_fine, decompose_c):
    country_buses = n.buses.query("country == @decompose_c").index
        
    query = "bus0 in @country_buses and not bus1 in @country_buses or bus1 in @country_buses and not bus0 in @country_buses"
    inter_lines = n.lines.query(query)[['bus0', 'bus1']]
    inter_links = n.links.query(query)[['bus0', 'bus1']]

    border_buses_lines = set(inter_lines['bus0']).union(inter_lines['bus1'])##.intersection(country_buses) ???
    border_buses_links = set(inter_links['bus0']).union(inter_links['bus1'])##.intersection(country_buses) ???
    border_buses = list(border_buses_lines.union(border_buses_links))

    border_buses_map = [bus + ' dec' for bus in busmap_fine[border_buses].values]#[n.buses.loc[bus].country + ' ' + bus for bus in border_buses]
    busmap_ref[border_buses] = border_buses_map

    return busmap_ref


if __name__ == "__main__":

    # CLUSTER NETWORK...

    n = pypsa.Network(snakemake.input.network)

    focus_weights = snakemake.config.get('focus_weights', None)

    busmap = pd.read_csv(snakemake.input.busmap, dtype=str, index_col=0, squeeze=True)
    busmap.index = busmap.index.astype(str)

    busmap_fine = pd.read_csv(snakemake.input.busmap_fine, dtype=str, index_col=0, squeeze=True)
    busmap_fine.index = busmap_fine.index.astype(str)

    busmap = adjust_busmap_for_decomposition(busmap, busmap_fine, decompose_c=snakemake.wildcards.cntry)

    Nyears = n.snapshot_weightings.objective.sum()/8760
    costs = (load_costs(Nyears, tech_costs=snakemake.input.tech_costs,
                        config=snakemake.config['costs'],
                        elec_config=snakemake.config['electricity']))

    def consense(x):
        v = x.iat[0]
        assert ((x == v).all() or x.isnull().all()), (
            "The `potential` configuration option must agree for all renewable carriers, for now!"
        )
        return v

    renewable_carriers = pd.Index([tech for tech in n.generators.carrier.unique()
                                   if tech in snakemake.config['renewable']])
    potential_mode = consense(pd.Series([snakemake.config['renewable'][tech]['potential']
                                             for tech in renewable_carriers]))

    aggregate_carriers = None
    
    clustering = clustering_for_n_clusters(n, n_clusters=37, custom_busmap=busmap,
                                           aggregate_carriers=aggregate_carriers,
                                           line_length_factor=snakemake.config['lines']['length_factor'],
                                           potential_mode=potential_mode,
                                           solver_name=snakemake.config['solving']['solver']['name'],
                                           algorithm=snakemake.config['clustering']['algorithm'],
                                           extended_link_costs=costs.at['HVAC overhead', 'capital_cost'],
                                           focus_weights=focus_weights)

    getattr(clustering, "busmap").to_csv(snakemake.output["busmap"])

    # ADD EXTRA COMPONENTS...
    
    n = clustering.network

    update_p_nom_max(n)

    attach_storageunits(n, costs)
    attach_stores(n, costs)
    attach_hydrogen_pipelines(n, costs)

    add_nice_carrier_names(n, config=snakemake.config)

    # PREPARE_NETWORK...

    opts = snakemake.wildcards.opts.split('-')

    set_line_s_max_pu(n, s_max_pu=snakemake.config['lines']['s_max_pu'])

    for o in opts:
        m = re.match(r'^\d+h$', o, re.IGNORECASE)
        if m is not None:
            n = average_every_nhours(n, m.group(0))
            break

    for o in opts:
        m = re.match(r'^\d+seg$', o, re.IGNORECASE)
        if m is not None:
            n = apply_time_segmentation(n, m.group(0)[:-3])
            break

    for o in opts:
        if "Co2L" in o:
            m = re.findall("[0-9]*\.?[0-9]+$", o)
            if len(m) > 0:
                co2limit=float(m[0])*snakemake.config['electricity']['co2base']
                add_co2limit(n, Nyears, co2limit)
            else:
                add_co2limit(n, Nyears, snakemake.config['electricity']['co2limit'])
            break

    for o in opts:
        oo = o.split("+")
        suptechs = map(lambda c: c.split("-", 2)[0], n.carriers.index)
        if oo[0].startswith(tuple(suptechs)):
            carrier = oo[0]
            # handles only p_nom_max as stores and lines have no potentials
            attr_lookup = {"p": "p_nom_max", "c": "capital_cost"}
            attr = attr_lookup[oo[1][0]]
            factor = float(oo[1][1:])
            if carrier == "AC":  # lines do not have carrier
                n.lines[attr] *= factor
            else:
                comps = {"Generator", "Link", "StorageUnit", "Store"}
                for c in n.iterate_components(comps):
                    sel = c.df.carrier.str.contains(carrier)
                    c.df.loc[sel,attr] *= factor

    if 'Ep' in opts:
        add_emission_prices(n)

    ll_type, factor = snakemake.wildcards.ll[0], snakemake.wildcards.ll[1:]
    set_transmission_limit(n, ll_type, factor, costs, Nyears)

    set_line_nom_max(n, s_nom_max_set=snakemake.config["lines"].get("s_nom_max,", np.inf),
                     p_nom_max_set=snakemake.config["links"].get("p_nom_max,", np.inf))
    
    # SOLVE NETWORK
    configure_logging(snakemake)

    tmpdir = snakemake.config['solving'].get('tmpdir')
    if tmpdir is not None:
        Path(tmpdir).mkdir(parents=True, exist_ok=True)
    opts = snakemake.wildcards.opts.split('-')
    solve_opts = snakemake.config['solving']['options']

    fn = getattr(snakemake.log, 'memory', None)
    with memory_logger(filename=fn, interval=30.) as mem:
        n = prepare_network(n, solve_opts)
        n = solve_network(n, config=snakemake.config, opts=opts,
                          solver_dir=tmpdir,
                          solver_logfile=snakemake.log.solver)
        n.export_to_netcdf(snakemake.output.network)

    logger.info("Maximum memory usage: {}".format(mem.mem_usage))
