# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: PyPSA-Earth and PyPSA-Eur Authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Override regional demand profiles after clustering.

This script reads a clustered network and replaces the load time series
with custom regional demand profiles provided in a CSV file.

Inputs
------
- ``networks/elec_s{simpl}_{clusters}_ec.nc``: Clustered network with extra components
- CSV file with regional demand profiles (semicolon-separated)

Outputs
-------
- ``networks/elec_s{simpl}_{clusters}_ec_demand.nc``: Network with overridden demand profiles
"""

import pandas as pd
import pypsa
import os
import sys

# Make PyPSA-Earth's main scripts/ folder importable (contains _helpers.py)
sys.path.append(os.path.join(os.getcwd(), "scripts"))

from _helpers import configure_logging, create_logger

logger = create_logger(__name__)


def load_regional_demand_csv(csv_path):
    """
    Load regional demand profiles from CSV file.
    
    Expected CSV format (semicolon-separated):
    region_code;time;region_name;Electricity demand
    AR.GBA;2013-01-01 00:00:00;GBA;4343
    AR.GBA;2013-01-01 01:00:00;GBA;4225
    
    Parameters
    ----------
    csv_path : str
        Path to CSV file with regional demand profiles
    
    Returns
    -------
    pd.DataFrame
        DataFrame with datetime index and columns for each region (region_code)
    """
    logger.info(f"Loading regional demand profiles from {csv_path}")
    
    # Read CSV with semicolon separator
    df = pd.read_csv(csv_path, sep=';')
    
    # Parse the time column
    df['time'] = pd.to_datetime(df['time'])
    
    # Pivot to get time series per region
    # Columns will be region_code (e.g., AR.GBA), index will be time
    demand_pivot = df.pivot(index='time', columns='region_code', values='Electricity demand')
    
    logger.info(f"Loaded demand profiles for {len(demand_pivot.columns)} regions")
    logger.info(f"Time range: {demand_pivot.index[0]} to {demand_pivot.index[-1]}")
    logger.info(f"Regions: {list(demand_pivot.columns)}")
    
    return demand_pivot


def assign_regional_demand_to_buses(n, demand_df):
    """
    Assign regional demand profiles to loads in the network.
    
    For each bus, finds the matching region in the demand CSV and assigns
    that region's demand profile to all loads on that bus.
    
    Parameters
    ----------
    n : pypsa.Network
        The clustered network
    demand_df : pd.DataFrame
        DataFrame with regional demand profiles (columns = region codes)
    """
    # Check for NaN values in demand data
    if demand_df.isna().any().any():
        nan_regions = demand_df.columns[demand_df.isna().any()].tolist()
        logger.error(f"NaN values found in demand data for regions: {nan_regions}")
        raise ValueError(f"Demand CSV contains NaN values for regions: {nan_regions}")
    
    # Ensure demand_df index matches network snapshots
    if not demand_df.index.equals(n.snapshots):
        logger.warning("Demand profile timestamps don't match network snapshots exactly.")
        logger.warning(f"Network snapshots: {n.snapshots[0]} to {n.snapshots[-1]} ({len(n.snapshots)} snapshots)")
        logger.warning(f"CSV timestamps: {demand_df.index[0]} to {demand_df.index[-1]} ({len(demand_df)} timestamps)")
        logger.info("Reindexing demand data to match network snapshots...")
        demand_df = demand_df.reindex(n.snapshots, method='nearest')
    
    # Get snapshot weightings for proper energy calculation
    snapshot_weightings = n.snapshot_weightings.objective
    logger.info(f"Using snapshot weightings (sum={snapshot_weightings.sum():.2f} hours)")
    
    # Get all load buses
    load_buses = n.loads.bus.unique()
    logger.info(f"Found {len(load_buses)} buses with loads")
    
    # Group loads by bus
    loads_by_bus = n.loads.groupby('bus').groups
    
    # Track statistics (properly weighted energy in MWh)
    total_assigned_energy = 0
    total_original_energy = 0
    buses_matched = 0
    buses_not_matched = []
    
    for bus in load_buses:
        # Bus name should match region_code in CSV (e.g., "AR.GBA")
        if bus not in demand_df.columns:
            logger.warning(f"Bus '{bus}' not found in demand CSV columns")
            buses_not_matched.append(bus)
            continue
        
        # Get loads on this bus
        load_names = loads_by_bus[bus]
        
        # Get regional demand profile
        regional_demand = demand_df[bus]
        
        # Check for NaN in this specific region's demand
        if regional_demand.isna().any():
            logger.error(f"NaN values found in demand profile for bus {bus}")
            raise ValueError(f"Bus {bus} has NaN values in demand profile")
        
        # Store original demand for comparison (properly weighted)
        original_power = n.loads_t.p_set[load_names].sum(axis=1)
        original_energy = (original_power * snapshot_weightings).sum()
        total_original_energy += original_energy
        
        # If multiple loads on same bus, distribute proportionally based on existing ratios
        if len(load_names) > 1:
            logger.info(f"Bus {bus} has {len(load_names)} loads - distributing demand proportionally")
            
            # Get existing load distribution ratios
            existing_loads = n.loads_t.p_set[load_names]
            total_existing = existing_loads.sum(axis=1)
            
            # Calculate proportions (handle zeros by equal distribution)
            proportions = existing_loads.div(total_existing, axis=0).fillna(1.0 / len(load_names))
            
            # Assign proportionally
            for load_name in load_names:
                n.loads_t.p_set[load_name] = regional_demand * proportions[load_name]
        else:
            # Single load: assign entire regional demand
            load_name = list(load_names)[0]
            n.loads_t.p_set[load_name] = regional_demand
        
        # Track assigned demand (properly weighted)
        assigned_power = n.loads_t.p_set[load_names].sum(axis=1)
        assigned_energy = (assigned_power * snapshot_weightings).sum()
        total_assigned_energy += assigned_energy
        buses_matched += 1
        
        logger.info(f"Bus {bus}: Assigned {assigned_energy:,.2f} MWh (was {original_energy:,.2f} MWh)")
    
    # Report summary
    logger.info("\n" + "="*80)
    logger.info("DEMAND OVERRIDE SUMMARY")
    logger.info("="*80)
    logger.info(f"Buses successfully matched: {buses_matched} / {len(load_buses)}")
    
    if buses_not_matched:
        logger.warning(f"Buses NOT matched: {buses_not_matched}")
        logger.warning("These buses will keep their original demand profiles!")
    
    logger.info(f"\nTotal original energy:  {total_original_energy:,.2f} MWh")
    logger.info(f"Total assigned energy:  {total_assigned_energy:,.2f} MWh")
    
    if total_original_energy > 0:
        change_pct = ((total_assigned_energy - total_original_energy) / total_original_energy) * 100
        logger.info(f"Change: {change_pct:+.2f}%")
        
        if abs(change_pct) > 5:
            logger.warning(f"Large demand change detected (>{5}%)! Verify this is expected.")
    
    # Verify against CSV total (properly weighted)
    csv_energy = (demand_df.sum(axis=1) * snapshot_weightings).sum()
    logger.info(f"\nCSV total energy:       {csv_energy:,.2f} MWh")
    
    if abs(total_assigned_energy - csv_energy) / csv_energy > 0.01:
        match_pct = (total_assigned_energy / csv_energy) * 100
        logger.warning(f"Network energy differs from CSV by {abs(100-match_pct):.2f}%")
        if buses_not_matched:
            logger.warning("This is likely due to unmatched buses keeping original demand")
    else:
        logger.info("✓ Network energy matches CSV total!")
    
    logger.info("="*80 + "\n")


if __name__ == "__main__":
    if "snakemake" not in globals():
        from _helpers import mock_snakemake
        
        snakemake = mock_snakemake(
            "override_regional_demand",
            simpl="",
            clusters="9",
        )
    
    configure_logging(snakemake)
    
    # Load the clustered network
    logger.info(f"Loading network from {snakemake.input.network}")
    n = pypsa.Network(snakemake.input.network)
    
    logger.info(f"Network has {len(n.loads)} loads across {len(n.buses)} buses")
    logger.info(f"Snapshots: {len(n.snapshots)} time steps from {n.snapshots[0]} to {n.snapshots[-1]}")
    
    # Load regional demand profiles
    demand_df = load_regional_demand_csv(snakemake.input.demand_profile)
    
    # Assign demand to buses
    assign_regional_demand_to_buses(n, demand_df)
    
    # Save the modified network
    logger.info(f"Saving modified network to {snakemake.output[0]}")
    n.export_to_netcdf(snakemake.output[0])
    
    logger.info("Done!")
