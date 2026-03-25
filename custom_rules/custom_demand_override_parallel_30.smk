# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: PyPSA-Earth and PyPSA-Eur Authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Custom rules for parallel demand override workflow.

This creates a separate workflow branch:
  add_extra_components → override_regional_demand → prepare_network_demand → solve_network_demand

Instead of overriding existing rules (which causes conflicts), this creates
new rules with different names that produce outputs with "_demand" suffix.

To use this workflow, target the demand-specific outputs, e.g.:
  snakemake networks/run/elec_s_9_ec_l1_opts_demand.nc
  snakemake results/run/networks/elec_s_9_ec_l1_opts_demand.nc
"""

# =============================================================================
# STEP 1: Override regional demand after clustering
# =============================================================================

rule override_regional_demand:
    """
    Override regional demand profiles after clustering.
    
    Takes the clustered network with extra components (_ec.nc) and replaces
    the load time series with custom regional demand profiles from CSV.
    
    Creates a parallel output (_ec_demand.nc) that doesn't interfere with
    the standard workflow.
    """
    input:
        network="networks/" + RDIR + "elec_s{simpl}_{clusters}_ec.nc",
        demand_profile="data/ssp2-2.6/2030/era5_2013_custom/regional_demand_30.csv"
    output:
        "networks/" + RDIR + "elec_s{simpl}_{clusters}_ec_demand.nc"
    log:
        "logs/" + RDIR + "override_regional_demand/elec_s{simpl}_{clusters}.log"
    benchmark:
        "benchmarks/" + RDIR + "override_regional_demand/elec_s{simpl}_{clusters}"
    threads: 1
    resources:
        mem_mb=3000
    script:
        "scripts/override_regional_demand.py"


# =============================================================================
# STEP 2: Prepare network (demand variant)
# =============================================================================

rule prepare_network_demand:
    """
    Prepare network for solving (demand override variant).
    
    This is identical to prepare_network but takes the demand-overridden
    network as input and produces output with _demand suffix.
    
    NOTE: This rule uses the SAME script as prepare_network. The script
    doesn't care about the filename - it just reads snakemake.input[0]
    and writes to snakemake.output[0].
    """
    params:
        links=config["links"],
        lines=config["lines"],
        s_max_pu=config["lines"]["s_max_pu"],
        electricity=config["electricity"],
        costs=config["costs"],
    input:
        "networks/" + RDIR + "elec_s{simpl}_{clusters}_ec_demand.nc",
        tech_costs=COSTS,
    output:
        "networks/" + RDIR + "elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_demand.nc",
    log:
        "logs/" + RDIR + "prepare_network_demand/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}.log",
    benchmark:
        (
            "benchmarks/"
            + RDIR
            + "prepare_network_demand/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}"
        )
    threads: 1
    resources:
        mem_mb=4000,
    script:
        "C:/Users/ariel/Documents/TESIS/Git/tesislevi/pypsa-earth-project/pypsa-earth/scripts/prepare_network.py"  # REUSES the existing script!


# =============================================================================
# STEP 3: Solve network (demand variant)
# =============================================================================

rule solve_network_demand:
    """
    Solve network (demand override variant).
    
    This is identical to solve_network but takes the demand-overridden
    prepared network as input.
    
    NOTE: This rule uses the SAME script as solve_network. The script
    reads from snakemake.input.network and writes to snakemake.output[0].
    """
    params:
        solving=config["solving"],
        augmented_line_connection=config["augmented_line_connection"],
        policy_config=config["policy_config"],
    input:
        overrides=BASE_DIR + "/data/override_component_attrs",
        network="networks/" + RDIR + "elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_demand.nc",
    output:
        "results/" + RDIR + "networks/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_demand.nc",
    shadow:
        "copy-minimal" if os.name == "nt" else "shallow"
    log:
        solver="logs/"
        + RDIR
        + "solve_network_demand/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_solver.log",
        python="logs/"
        + RDIR
        + "solve_network_demand/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_python.log",
    benchmark:
        (
            "benchmarks/"
            + RDIR
            + "solve_network_demand/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}"
        )
    threads: 20
    resources:
        mem_mb=4000,
    script:
        "C:/Users/ariel/Documents/TESIS/Git/tesislevi/pypsa-earth-project/pypsa-earth/scripts/solve_network.py"  # REUSES the existing script!


# =============================================================================
# CONVENIENCE RULES
# =============================================================================

rule solve_all_networks_demand:
    """
    Convenience rule to solve all networks with demand override.
    
    Usage:
        snakemake -j1 solve_all_networks_demand
    """
    input:
        expand(
            "results/" + RDIR + "networks/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_demand.nc",
            **config["scenario"],
        )
