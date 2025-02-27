#!/bin/bash

#SBATCH --job-name=shfit-NM-hitol-nan-reanalysis-sigma
#SBATCH --time=50:00:00
#SBATCH --ntasks=2 --cpus-per-task=2
#SBATCH --mem-per-cpu=50G 
#SBATCH --mail-type=END,ARRAY_TASKS 
#SBATCH --error=outfiles/slurm-%A_%a.err
#SBATCH --out=outfiles/slurm-%A_%a.out
#SBATCH --array=0-2


export OMP_NUM_THREADS=1 # see documentation https://hpc.vub.be/docs/faq/advanced/#how-to-run-python-in-parallel 

#==============================================================================
# LAUNCH ATTR-HW jobscript 
#L-BFGS-B-tol1e-5-himem
#SBATCH --partition=broadwell_himem

#SBATCH --partition=broadwell_himem

# Load all necessary packages and launch script


# JOB SPECS: 

# for calc WBGT 

#SBATCH --time=20:00:00
#SBATCH --partition=broadwell_himem
#SBATCH --ntasks=1 --cpus-per-task=8
#SBATCH --mem-per-cpu=50G   

# for calc WBGT 

#SBATCH --job-name=ah-wbgt-ssp370
#SBATCH --time=20:00:00
#SBATCH --partition=broadwell_himem
#SBATCH --ntasks=1 --cpus-per-task=4
#SBATCH --mem-per-cpu=100G   
#SBATCH --mail-type=END,ARRAY_TASKS 
#SBATCH --error=outfiles/slurm-%j.err
#SBATCH --out=outfiles/slurm-%j.out
#SBATCH --array=5-7



# emp pctl also pi
#SBATCH --time=2:00:00
#SBATCH --ntasks=2 --cpus-per-task=2
#SBATCH --mem-per-cpu=50G   

# Test NO
#SBATCH --time=10:00:00
#SBATCH --ntasks=1 --cpus-per-task=2
#SBATCH --mem-per-cpu=100G   

# Test 2 NO
#SBATCH --time=30:00:00
#SBATCH --partition=broadwell_himem
#SBATCH --ntasks=2 --cpus-per-task=8
#SBATCH --mem-per-cpu=50G  


# for empirical percentiles single year (only present): ntasks 1 cpus per task 2, mem 180 G (or less! 100 should be enough), 2h should be enough


# Takes ~1 hour with 6-array
#SBATCH --time=5:00:00
#SBATCH --ntasks=1 --cpus-per-task=2
#SBATCH --mem-per-cpu=100G   


# try with more cores/memory
#SBATCH --time=30:00:00
#SBATCH --partition=broadwell_himem
#SBATCH --ntasks=2 --cpus-per-task=15
#SBATCH --mem-per-cpu=50G   

#SBATCH --time=1:00:00
#SBATCH --ntasks=1 --cpus-per-task=2
#SBATCH --mem-per-cpu=100G  
#SBATCH --mail-type=ALL
#SBATCH --error=outfiles/slurm-%j.err
#SBATCH --out=outfiles/slurm-%j.out
#SBATCH --array=1-30



# for shift fit increase n of cores/nodes and mem - testing see test-job.sh 



# Testing 
#SBATCH --partition=broadwell_himem
#SBATCH --ntasks=2 --cpus-per-task=4
#SBATCH --mem-per-cpu=50G   





#SBATCH --time=25:00:00
#SBATCH --ntasks=1 --cpus-per-task=12 or more???
#SBATCH --mem-per-cpu=20G   or more and go himem??

#SBATCH --time=30:00:00
#SBATCH --ntasks=1 --cpus-per-task=8
#SBATCH --mem-per-cpu=30G  

#SBATCH --time=30:00:00
#SBATCH --ntasks=1 --cpus-per-task=8
#SBATCH --mem-per-cpu=20G   

# Updated: January 2025
# By: rosa.pietroiusti@vub.be


#==============================================================================


module --force purge

module load SciPy-bundle/2022.05-foss-2022a # includes numpy, pandas and scipy & Python gets automatically loaded
module load matplotlib/3.5.2-foss-2022a
module load netcdf4-python/1.6.1-foss-2022a
module load xarray/2022.6.0-foss-2022a
module load dask/2022.10.0-foss-2022a 
module load statsmodels/0.13.1-foss-2022a
module load h5netcdf/1.2.0-foss-2022a

#==============================================================================
# initialisation
#==============================================================================


echo "Job Name: $SLURM_JOB_NAME"
echo "Number of Nodes: $SLURM_JOB_NUM_NODES"
echo "Node List: $SLURM_JOB_NODELIST"
echo "Number of Tasks: $SLURM_NTASKS"
echo "CPUs per Task: $SLURM_CPUS_PER_TASK"
echo "Memory per Node: $SLURM_MEM_PER_NODE"
echo "Memory per CPU: $SLURM_MEM_PER_CPU"


# working directory 
echo "working directory is $(pwd)"

# jobid 
ver="$SLURM_JOB_ID"
echo "job name: ${SLURM_JOB_NAME}"

# array 
arr=$SLURM_ARRAY_TASK_ID
echo "array member $arr"

START=$(date +%s.%N)

# if you run multiple settings through array

# indx=( 0 1 2 3 0 1 2 3 0 1 2 3 ) # GCM round 2
# indy=( 0 0 0 0 1 1 1 1 2 2 2 2) # variable

# empirical pct on all GCMs for 3 vars
#indx=( 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 ) # GCMs all 
#indy=( 0 0 0 0 0 0 0 0 0 0 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 ) # variable

# # shift fit gcms on 2 time periods
# indx=( 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 ) # GCMs all 
# indy=( 0 0 0 0 0 0 0 0 0 0 1 1 1 1 1 1 1 1 1 1 ) # variable

# # array over gcms
# indx=( 0 1 2 3 4 5 6 7 8 9 ) # GCMs all 
# indy=( 0 0 0 0 0 0 0 0 0 0 ) 

# #  array over datasets
indx=( 0 1 2 0 1 2 ) # dataset
indy=( 0 0 0 1 1 1 ) # time period 

echo ${indx[$arr]} ${indy[$arr]}
python -u main.py "$ver" ${indx[$arr]} ${indy[$arr]}  # replace w/ python -u for real-time output in .out file



END=$(date +%s.%N)
DIFF=$(echo "($END - $START) / 3600 " | bc)
echo; echo "Elapsed time is" $DIFF "hours."; echo

