# SPDX-FileCopyrightText: : 2023 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT
rule add_existing_baseyear:
    input:
        overrides="data/override_component_attrs",
        network=RESULTS
        + "prenetworks/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_{planning_horizons}.nc",
        powerplants=RESOURCES + "powerplants.csv",
        busmap_s=RESOURCES + "busmap_elec_s{simpl}.csv",
        busmap=RESOURCES + "busmap_elec_s{simpl}_{clusters}.csv",
        clustered_pop_layout=RESOURCES + "pop_layout_elec_s{simpl}_{clusters}.csv",
        costs="data/costs_{}.csv".format(config["scenario"]["planning_horizons"][0]),
        cop_soil_total=RESOURCES + "cop_soil_total_elec_s{simpl}_{clusters}.nc",
        cop_air_total=RESOURCES + "cop_air_total_elec_s{simpl}_{clusters}.nc",
        existing_heating="data/existing_infrastructure/existing_heating_raw.csv",
        existing_solar="data/existing_infrastructure/solar_capacity_IRENA.csv",
        existing_onwind="data/existing_infrastructure/onwind_capacity_IRENA.csv",
        existing_offwind="data/existing_infrastructure/offwind_capacity_IRENA.csv",
    output:
        RESULTS
        + "prenetworks-brownfield/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_{planning_horizons}.nc",
    wildcard_constraints:
        planning_horizons=config["scenario"]["planning_horizons"][0],  #only applies to baseyear
    threads: 1
    resources:
        mem_mb=2000,
    log:
        LOGS
        + "add_existing_baseyear_elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_{planning_horizons}.log",
    benchmark:
        (
            BENCHMARKS
            + "add_existing_baseyear/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_{planning_horizons}"
        )
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/add_existing_baseyear.py"


rule add_brownfield:
    input:
        overrides="data/override_component_attrs",
        network=RESULTS
        + "prenetworks/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_{planning_horizons}.nc",
        network_p=solved_previous_horizon,  #solved network at previous time step
        costs="data/costs_{planning_horizons}.csv",
        cop_soil_total=RESOURCES + "cop_soil_total_elec_s{simpl}_{clusters}.nc",
        cop_air_total=RESOURCES + "cop_air_total_elec_s{simpl}_{clusters}.nc",
    output:
        RESULTS
        + "prenetworks-brownfield/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_{planning_horizons}.nc",
    threads: 4
    resources:
        mem_mb=10000,
    log:
        LOGS
        + "add_brownfield_elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_{planning_horizons}.log",
    benchmark:
        (
            BENCHMARKS
            + "add_brownfield/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_{planning_horizons}"
        )
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/add_brownfield.py"


rule prepare_perfect_foresight:
    input:
        **{
            f"network_{year}": RESULTS
            + "prenetworks/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_"
            + f"{year}.nc"
            for year in config["scenario"]["planning_horizons"][1:]
        },
        brownfield_network=lambda w: (
            RESULTS
            + "prenetworks-brownfield/"
            + "elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_"
            + "{}.nc".format(str(config["scenario"]["planning_horizons"][0]))
        ),
        overrides="data/override_component_attrs",
    output:
        RESULTS
        + "/prenetworks-brownfield/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_brownfield_all_years.nc",
    threads: 2
    resources:
        mem_mb=10000,
    log:
        LOGS
        + "prepare_perfect_foresight{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}.log",
    benchmark:
        (
            BENCHMARKS
            + "prepare_perfect_foresight{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}"
        )
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/prepare_perfect_foresight.py"


rule solve_sector_network_perfect:
    input:
        overrides="data/override_component_attrs",
        network=RESULTS
        + "prenetworks-brownfield/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_brownfield_all_years.nc",
        costs="data/costs_2030.csv",
        config=RESULTS + "configs/config.yaml",
    output:
        RESULTS
        + "postnetworks/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_brownfield_all_years.nc",
    threads: 4
    resources:
        mem_mb=config["solving"]["mem"],
    shadow:
        "shallow"
    log:
        solver=RESULTS
        + "logs/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_brownfield_all_years_solver.log",
        python=RESULTS
        + "logs/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_brownfield_all_years_python.log",
        memory=RESULTS
        + "logs/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_brownfield_all_years_memory.log",
    benchmark:
        (
            BENCHMARKS
            + "solve_sector_network/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_brownfield_all_years}"
        )
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/solve_network.py"


rule make_summary_perfect:
    input:
        **{
            f"networks_{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}": RESULTS
            + f"/postnetworks/elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_brownfield_all_years.nc"
            for simpl in config["scenario"]["simpl"]
            for clusters in config["scenario"]["clusters"]
            for opts in config["scenario"]["opts"]
            for sector_opts in config["scenario"]["sector_opts"]
            for ll in config["scenario"]["ll"]
        },
        costs="data/costs_2020.csv",
        overrides="data/override_component_attrs",
    output:
        nodal_costs=RESULTS + "csvs/nodal_costs.csv",
        nodal_capacities=RESULTS + "csvs/nodal_capacities.csv",
        nodal_cfs=RESULTS + "csvs/nodal_cfs.csv",
        cfs=RESULTS + "csvs/cfs.csv",
        costs=RESULTS + "csvs/costs.csv",
        capacities=RESULTS + "csvs/capacities.csv",
        curtailment=RESULTS + "csvs/curtailment.csv",
        capital_cost=RESULTS + "csvs/capital_cost.csv",
        energy=RESULTS + "csvs/energy.csv",
        supply=RESULTS + "csvs/supply.csv",
        supply_energy=RESULTS + "csvs/supply_energy.csv",
        prices=RESULTS + "csvs/prices.csv",
        weighted_prices=RESULTS + "csvs/weighted_prices.csv",
        market_values=RESULTS + "csvs/market_values.csv",
        price_statistics=RESULTS + "csvs/price_statistics.csv",
        metrics=RESULTS + "csvs/metrics.csv",
        co2_emissions=RESULTS + "csvs/co2_emissions.csv",
    threads: 2
    resources:
        mem_mb=10000,
    log:
        LOGS + "make_summary_perfect.log",
    benchmark:
        (
            BENCHMARKS
            + "make_summary_perfect"
        )
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/make_summary_perfect.py"


rule plot_summary_perfect:
    input:
        **{
            f"costs_{year}": f"data/costs_{year}.csv"
            for year in config["scenario"]["planning_horizons"]
        },
        costs_csv=RESULTS + "csvs/costs.csv",
        energy=RESULTS + "csvs/energy.csv",
        balances=RESULTS + "csvs/supply_energy.csv",
        eea="data/eea/UNFCCC_v24.csv",
        countries=RESULTS + "csvs/nodal_capacities.csv",
        co2_emissions=RESULTS + "csvs/co2_emissions.csv",
        capacities=RESULTS + "csvs/capacities.csv",
        capital_cost=RESULTS + "csvs/capital_cost.csv",
    output:
        costs1=RESULTS + "graphs/costs.pdf",
        costs2=RESULTS + "graphs/costs2.pdf",
        costs3=RESULTS + "graphs/total_costs_per_year.pdf",
        # energy="results"  + '/' + config['run'] + '/graphs/energy.pdf',
        balances=RESULTS + "graphs/balances-energy.pdf",
        co2_emissions=RESULTS + "graphs/carbon_budget_plot.pdf",
        capacities=RESULTS + "graphs/capacities.pdf",
    threads: 2
    resources:
        mem_mb=10000,
    log:
        LOGS + "plot_summary_perfect.log",
    benchmark:
        (BENCHMARKS + "plot_summary_perfect")
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/plot_summary_perfect.py"


ruleorder: add_existing_baseyear > add_brownfield
