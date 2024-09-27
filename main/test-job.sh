#!/bin/bash

#SBATCH --job-name=attr-hw
#SBATCH --time=02:00:00
#SBATCH --ntasks=1 --cpus-per-task=4
#SBATCH --mem-per-cpu=50G    
#SBATCH --mail-type=ALL
#SBATCH --error=outfiles/test-shift/slurm-%j.err
#SBATCH --out=outfiles/test-shift/slurm-%j.out
#SBATCH --array=1-6

export OMP_NUM_THREADS=1



module load SciPy-bundle/2022.05-foss-2022a # this bundle includes numpy, pandas and scipy & Python gets automatically loaded with one of these modules
module load matplotlib/3.5.2-foss-2022a
module load netcdf4-python/1.6.1-foss-2022a
module load xarray/2022.6.0-foss-2022a
module load dask/2022.10.0-foss-2022a 
module load statsmodels/0.13.1-foss-2022a

ver="$SLURM_JOB_ID"
arr=$SLURM_ARRAY_TASK_ID



echo "Job ID: $SLURM_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Number of Nodes: $SLURM_JOB_NUM_NODES"
echo "Node List: $SLURM_JOB_NODELIST"
echo "Number of Tasks: $SLURM_NTASKS"
echo "CPUs per Task: $SLURM_CPUS_PER_TASK"
echo "Memory per Node: $SLURM_MEM_PER_NODE"
echo "Memory per CPU: $SLURM_MEM_PER_CPU"


python test-job.py "$ver"$arr


# 9606733 1-3
# --ntasks=2 --cpus-per-task=3
# --mem-per-cpu=50G    
# 3 different chunking types
# 10 years of data 


# 9606743
# 5 years of data 

# 9606746 with client initiated 
# chunking versions 0 and 1 fail on 5 years of data


# try with client initiated on 10 years of data
# 9606756

# ALL FAILED! Try subselecting an area - this worked ! and fast ! 
# look into this better ! which jobs worked fast? 15 min 