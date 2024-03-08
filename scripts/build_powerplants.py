# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: : 2017-2024 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT

# coding: utf-8
"""
Retrieves conventional powerplant capacities and locations from
`powerplantmatching <https://github.com/FRESNA/powerplantmatching>`_, assigns
these to buses and creates a ``.csv`` file. It is possible to amend the
powerplant database with custom entries provided in
``data/custom_powerplants.csv``.
Lastly, for every substation, powerplants with zero-initial capacity can be added for certain fuel types automatically.

Relevant Settings
-----------------

.. code:: yaml

    electricity:
      powerplants_filter:
      custom_powerplants:
      everywhere_powerplants:

.. seealso::
    Documentation of the configuration file ``config/config.yaml`` at
    :ref:`electricity`

Inputs
------

- ``networks/base.nc``: confer :ref:`base`.
- ``data/custom_powerplants.csv``: custom powerplants in the same format as `powerplantmatching <https://github.com/FRESNA/powerplantmatching>`_ provides

Outputs
-------

- ``resource/powerplants.csv``: A list of conventional power plants (i.e. neither wind nor solar) with fields for name, fuel type, technology, country, capacity in MW, duration, commissioning year, retrofit year, latitude, longitude, and dam information as documented in the `powerplantmatching README <https://github.com/FRESNA/powerplantmatching/blob/master/README.md>`_; additionally it includes information on the closest substation/bus in ``networks/base.nc``.

    .. image:: img/powerplantmatching.png
        :scale: 30 %

    **Source:** `powerplantmatching on GitHub <https://github.com/FRESNA/powerplantmatching>`_

Description
-----------

The configuration options ``electricity: powerplants_filter`` and ``electricity: custom_powerplants`` can be used to control whether data should be retrieved from the original powerplants database or from custom amendmends. These specify `pandas.query <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html>`_ commands.
In addition the configuration option ``electricity: everywhere_powerplants`` can be used to place powerplants with zero-initial capacity of certain fuel types at all substations.

1. Adding all powerplants from custom:

    .. code:: yaml

        powerplants_filter: false
        custom_powerplants: true

2. Replacing powerplants in e.g. Germany by custom data:

    .. code:: yaml

        powerplants_filter: Country not in ['Germany']
        custom_powerplants: true

    or

    .. code:: yaml

        powerplants_filter: Country not in ['Germany']
        custom_powerplants: Country in ['Germany']


3. Adding additional built year constraints:

    .. code:: yaml

        powerplants_filter: Country not in ['Germany'] and YearCommissioned <= 2015
        custom_powerplants: YearCommissioned <= 2015

4. Adding powerplants at all substations for 4 conventional carrier types:

    .. code:: yaml

        everywhere_powerplants: ['Natural Gas', 'Coal', 'nuclear', 'OCGT']
"""

import itertools
import logging

import numpy as np
import pandas as pd
import powerplantmatching as pm
import pypsa
from _helpers import configure_logging, set_scenario_config
from powerplantmatching.export import map_country_bus

logger = logging.getLogger(__name__)


def add_custom_powerplants(ppl, custom_powerplants, custom_ppl_query=False):
    if not custom_ppl_query:
        return ppl
    add_ppls = pd.read_csv(custom_powerplants, dtype={"bus": "str"})
    if isinstance(custom_ppl_query, str):
        add_ppls.query(custom_ppl_query, inplace=True)
    return pd.concat(
        [ppl, add_ppls], sort=False, ignore_index=True, verify_integrity=True
    )


def add_everywhere_powerplants(ppl, substations, everywhere_powerplants):
    # Create a dataframe with "everywhere_powerplants" of stated carriers at the location of all substations
    everywhere_ppl = (
        pd.DataFrame(
            itertools.product(substations.index.values, everywhere_powerplants),
            columns=["substation_index", "Fueltype"],
        ).merge(
            substations[["x", "y", "country"]],
            left_on="substation_index",
            right_index=True,
        )
    ).drop(columns="substation_index")

    # PPL uses different columns names compared to substations dataframe -> rename
    everywhere_ppl = everywhere_ppl.rename(
        columns={"x": "lon", "y": "lat", "country": "Country"}
    )

    # Add default values for the powerplants
    everywhere_ppl["Name"] = (
        "Automatically added everywhere-powerplant " + everywhere_ppl.Fueltype
    )
    everywhere_ppl["Set"] = "PP"
    everywhere_ppl["Technology"] = everywhere_ppl["Fueltype"]
    everywhere_ppl["Capacity"] = 0.0

    # Assign plausible values for the commissioning and decommissioning years
    # required for multi-year models
    everywhere_ppl["DateIn"] = ppl["DateIn"].min()
    everywhere_ppl["DateOut"] = ppl["DateOut"].max()

    # NaN values for efficiency will be replaced by the generic efficiency by attach_conventional_generators(...) in add_electricity.py later
    everywhere_ppl["Efficiency"] = np.nan

    return pd.concat(
        [ppl, everywhere_ppl], sort=False, ignore_index=True, verify_integrity=True
    )


def replace_natural_gas_technology(df):
    mapping = {"Steam Turbine": "CCGT", "Combustion Engine": "OCGT"}
    tech = df.Technology.replace(mapping).fillna("CCGT")
    return df.Technology.mask(df.Fueltype == "Natural Gas", tech)


def replace_natural_gas_fueltype(df):
    return df.Fueltype.mask(
        (df.Technology == "OCGT") | (df.Technology == "CCGT"), "Natural Gas"
    )


if __name__ == "__main__":
    if "snakemake" not in globals():
        from _helpers import mock_snakemake

        snakemake = mock_snakemake("build_powerplants")
    configure_logging(snakemake)
    set_scenario_config(snakemake)

    n = pypsa.Network(snakemake.input.base_network)
    countries = snakemake.params.countries

    ppl = (
        pm.powerplants(from_url=True)
        .powerplant.fill_missing_decommissioning_years()
        .powerplant.convert_country_to_alpha2()
        .query('Fueltype not in ["Solar", "Wind"] and Country in @countries')
        .assign(Technology=replace_natural_gas_technology)
        .assign(Fueltype=replace_natural_gas_fueltype)
    )

    # Correct bioenergy for countries where possible
    opsd = pm.data.OPSD_VRE().powerplant.convert_country_to_alpha2()
    opsd = opsd.query('Country in @countries and Fueltype == "Bioenergy"')
    opsd["Name"] = "Biomass"
    available_countries = opsd.Country.unique()
    ppl = ppl.query('not (Country in @available_countries and Fueltype == "Bioenergy")')
    ppl = pd.concat([ppl, opsd])

    ppl_query = snakemake.params.powerplants_filter
    if isinstance(ppl_query, str):
        ppl.query(ppl_query, inplace=True)

    # add carriers from own powerplant files:
    custom_ppl_query = snakemake.params.custom_powerplants
    ppl = add_custom_powerplants(
        ppl, snakemake.input.custom_powerplants, custom_ppl_query
    )

    if countries_wo_ppl := set(countries) - set(ppl.Country.unique()):
        logging.warning(f"No powerplants known in: {', '.join(countries_wo_ppl)}")

    # Add "everywhere powerplants" to all bus locations
    ppl = add_everywhere_powerplants(
        ppl, n.buses.query("substation_lv"), snakemake.params.everywhere_powerplants
    )

    substations = n.buses.query("substation_lv")
    ppl = ppl.dropna(subset=["lat", "lon"])
    ppl = map_country_bus(ppl, substations)

    bus_null_b = ppl["bus"].isnull()
    if bus_null_b.any():
        logging.warning(
            f"Couldn't find close bus for {bus_null_b.sum()} powerplants. "
            "Removing them from the powerplants list."
        )
        ppl = ppl[~bus_null_b]

    # TODO: This has to fixed in PPM, some powerplants are still duplicated
    cumcount = ppl.groupby(["bus", "Fueltype"]).cumcount() + 1
    ppl.Name = ppl.Name.where(cumcount == 1, ppl.Name + " " + cumcount.astype(str))

    ppl.reset_index(drop=True).to_csv(snakemake.output[0])
