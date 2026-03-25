# PyPSA-ARG
PyPSA-ARG is a plug-in repository for PyPSA-Earth with custom rules, configs and data tailored to the Argentine power system.
For learning hoy to use PyPSA-Earth, please visit https://pypsa-meets-earth.github.io/
Custom_rules are customized scripts for the Argentine system. They have a script for line restrictions according to CAMMESA, demand profiles and a powerplant database. Since the default demand is overriden, the command snakemake -j 1 solve_all_networks_demand is used.
Custom_data are custom datasets of Argentina, plus auxiliary files that are used in custom notebooks. Custom_notebooks are just post-processing jupyter notebooks. Custom_configs are examples for runs.
