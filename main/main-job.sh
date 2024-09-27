#!/bin/bash

#SBATCH --job-name=ah
#SBATCH --time=4:00:00
#SBATCH --ntasks=1 --cpus-per-task=2
#SBATCH --mem-per-cpu=100G   
#SBATCH --mail-type=ALL
#SBATCH --error=outfiles/slurm-%j.err
#SBATCH --out=outfiles/slurm-%j.out
#SBATCH --array=1-12


export OMP_NUM_THREADS=1 # see documentation https://hpc.vub.be/docs/faq/advanced/#how-to-run-python-in-parallel 

#==============================================================================
# LAUNCH ATTR-HW ISIMIP3b models script 
#
# Load all necessary packages and launch script


# JOB SPECS: 
# for empirical percentiles single year (only present): ntasks 1 cpus per task 2, mem 180 G (or less! 100 should be enough), 2h should be enough
# Takes ~1 hour with 6-array
#SBATCH --time=5:00:00
#SBATCH --ntasks=1 --cpus-per-task=2
#SBATCH --mem-per-cpu=150G   

# try with more cores/memory
#SBATCH --time=30:00:00
#SBATCH --partition=broadwell_himem
#SBATCH --ntasks=2 --cpus-per-task=15
#SBATCH --mem-per-cpu=50G   

# for shift fit increase n of cores/nodes and mem - testing see test-job.sh 
#SBATCH --time=25:00:00
#SBATCH --ntasks=1 --cpus-per-task=12 or more???
#SBATCH --mem-per-cpu=20G   or more and go himem??

#SBATCH --time=30:00:00
#SBATCH --ntasks=1 --cpus-per-task=8
#SBATCH --mem-per-cpu=30G  

# Created: July 2023
# By: rosa.pietroiusti@vub.be


#==============================================================================


module --force purge

module load SciPy-bundle/2022.05-foss-2022a # includes numpy, pandas and scipy & Python gets automatically loaded
module load matplotlib/3.5.2-foss-2022a
module load netcdf4-python/1.6.1-foss-2022a
module load xarray/2022.6.0-foss-2022a
module load dask/2022.10.0-foss-2022a 
module load statsmodels/0.13.1-foss-2022a

#==============================================================================
# initialisation
#==============================================================================

# working directory 
echo "working directory is $(pwd)"

# jobid 
ver="$SLURM_JOB_ID"
echo "job name: ${SLURM_JOB_NAME}"

# array 
arr=$SLURM_ARRAY_TASK_ID
echo "array member $arr"

# if you run multiple settings through array
#indx=( 0 1 2 0 1 2 ) # dataset
#indy=( 0 0 0 1 1 1 ) # time period 

indx=( 0 1 2 3 0 1 2 3 0 1 2 3 ) # GCM round 2
indy=( 0 0 0 0 1 1 1 1 2 2 2 2) # variable


START=$(date +%s.%N)
echo ${indx[$arr-1]} ${indy[$arr-1]}
python main.py "$ver" ${indx[$arr-1]} ${indy[$arr-1]}

#python main.py "$ver" $arr


END=$(date +%s.%N)
DIFF=$(echo "($END - $START) / 3600 " | bc)
echo; echo "Elapsed time is" $DIFF "hours."; echo

