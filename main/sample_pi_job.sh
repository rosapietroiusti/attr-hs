#!/bin/bash

#SBATCH --job-name=sample_pi
#SBATCH --time=2:00:00
#SBATCH --ntasks=1 --cpus-per-task=4
#SBATCH --mem-per-cpu=40G   
#SBATCH --mail-type=ALL
#SBATCH --error=outfiles/slurm-%j.err
#SBATCH --out=outfiles/slurm-%j.out



module --force purge

module load SciPy-bundle/2022.05-foss-2022a # includes numpy, pandas and scipy & Python gets automatically loaded
module load matplotlib/3.5.2-foss-2022a
module load netcdf4-python/1.6.1-foss-2022a
module load xarray/2022.6.0-foss-2022a
module load dask/2022.10.0-foss-2022a 
module load statsmodels/0.13.1-foss-2022a


python -u sample_pi.py "-f"