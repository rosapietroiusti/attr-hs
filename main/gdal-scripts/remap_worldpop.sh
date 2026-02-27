#!/bin/bash

#SBATCH --time=4:00:00
#SBATCH --ntasks=1 --cpus-per-task=4
#SBATCH --mail-type=ALL


# remap WorldPop data from 1 km to 0.5 deg 


# tic
START=$(date +%s.%N)

# load gdal
ml load GDAL 


inDIR='/data/brussel/vo/000/bvo00012/data/dataset/WorldPop/unconstrained/global/age-sex/2020'
outDIR='/data/brussel/vo/000/bvo00012/data/dataset/WorldPop/unconstrained/global/age-sex/2020/remapped'

if [ ! -e $outDIR ]
then
	mkdir -p $outDIR
fi

# limit number of parallel jobs
MAX_JOBS=5  
count=0


# loop over files run in parallel (&) 
for FILE in $inDIR/*.tif; do

    # get new filename
    FILENAME=$(basename "${FILE}")
    FILENAME="${FILENAME/_1km/}"           # removes the first occurrence of _1km
    FILENAME="${FILENAME%.*}_05deg.${FILENAME##*.}" 
    echo ${FILENAME}

    gdalwarp -overwrite -t_srs EPSG:4326 -tr 0.5 0.5 -r sum ${FILE} ${outDIR}/${FILENAME} & 

    # limit number of parallel jobs
    ((count++))
    if [ "$count" -ge "$MAX_JOBS" ]; then
    wait
    count=0
    fi
  
    #break  # exit after first file to test

done

wait # wait for background jobs to complete