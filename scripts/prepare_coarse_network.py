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

from _helpers import configure_logging, update_p_nom_max
from add_electricity import load_costs
from cluster_network import busmap_for_n_clusters, clustering_for_n_clusters

def adjust_busmap_for_decomposition(busmap, decompose_c):
    country_buses = n.buses.query("country == @decompose_c").index
        
    query = "bus0 in @country_buses and not bus1 in @country_buses or bus1 in @country_buses and not bus0 in @country_buses"
    inter_lines = n.lines.query(query)[['bus0', 'bus1']]
    inter_links = n.links.query(query)[['bus0', 'bus1']]

    border_buses_lines = set(inter_lines['bus0']).union(inter_lines['bus1'])#.intersection(country_buses)
    border_buses_links = set(inter_links['bus0']).union(inter_links['bus1'])#.intersection(country_buses)
    border_buses = list(border_buses_lines.union(border_buses_links))

    border_buses_map = [n.buses.loc[bus].country + ' ' + bus for bus in border_buses]
    busmap[border_buses] = border_buses_map

    return busmap


if __name__ == "__main__":

    n = pypsa.Network(snakemake.input.network)

    focus_weights = snakemake.config.get('focus_weights', None)

    busmap = busmap_for_n_clusters(n, n_clusters=37, solver_name=snakemake.config['solving']['solver']['name'],
                                   focus_weights=focus_weights,
                                   algorithm=snakemake.config['clustering']['algorithm'])

    busmap = adjust_busmap_for_decomposition(busmap, decompose_c=snakemake.wildcards.cntry)

    Nyears = n.snapshot_weightings.objective.sum()/8760
    hvac_overhead_cost = (load_costs(Nyears,
                                     tech_costs=snakemake.input.tech_costs,
                                     config=snakemake.config['costs'],
                                     elec_config=snakemake.config['electricity'])
                          .at['HVAC overhead', 'capital_cost'])

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
                                           extended_link_costs=hvac_overhead_cost,
                                           focus_weights=focus_weights)

    update_p_nom_max(n)

    #print(martha)
    # now prepare network etc.
    clustering.network.export_to_netcdf(snakemake.output.network)
    
    getattr(clustering, "busmap").to_csv(snakemake.output["busmap"])
