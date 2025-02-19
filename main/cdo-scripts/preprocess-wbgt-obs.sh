#!/bin/bash

#SBATCH --time=30:00:00
#SBATCH --ntasks=1 --cpus-per-task=4
#SBATCH --mail-type=ALL

# ==================================

# Preprocess ISIMIP3a-derived WBGT to compare it with models 


# Steps
# take 10, 20, 30 year windows ending in 2019 from observational datasets
# output mean, max, min, std, different percentiles
# freq of exceedance of fixed thresholds 28,30,33? to double check 



# alternative: download WBGT and UTCI ERA5 from Copernicus !! To have later years as well!


# possibly do eval on obs data going to 2022 e.g. era5-heat remapped to common grid instead of ISIMIP3a obs data 



# To do:
# do also for TX99
# get matching warming level for models 


#====================================================



# tic
START=$(date +%s.%N)

# load modules
module purge 
module load CDO/2.2.2-gompi-2023a


# set datasets to loop over
datasets=('empty' 'GSWP3-W5E5' '20CRv3-W5E5' '20CRv3-ERA5' )  

# variable
var='WBGT'

period='pre-industrial'

experiment='obsclim'


# set indir / outdir 
inDIRs=${VSC_SCRATCH_VO_USER}/attr-hw/output/output_jan25/WBGT/ISIMIP3a/${experiment}
outDIRs=${VSC_SCRATCH_VO_USER}/attr-hw/output/output_jan25/WBGT/ISIMIP3a/${experiment}/preprocessed



# set 10, 20, 30 year windows - common period for all datasets
if [ $period = 'present' ]
then
    startYEARs=( 1990 2000 2010 )
    endYEAR=2019
elif [ $period = 'pre-industrial' ]
then     
    startYEAR=1901
    endYEARs=( 1910 1920 1930 )
fi



#====================================================

# loop over datasets 
for dataset in "${datasets[@]}"; do   


# Skip the first element
if [ "$dataset" == "${datasets[0]}" ]; then
    continue
fi
echo **processing $dataset


# get input directory 
inDIR=${inDIRs}/${dataset}
echo **indir is $inDIR

# get output directory 
outDIR=${outDIRs}/${dataset}
# make outdir if not exists
if [ ! -e $outDIR ]
then
	mkdir -p $outDIR
fi
echo **outdir is $outDIR


# get basename for saving files 
for FILE in $inDIR/*_${var}_*.nc; do
    FILENAME=$(basename -s .nc ${FILE})
    FILENAME=${FILENAME%_${experiment}_*}_${experiment} # cut everything after experiment name 
    echo ${FILENAME}
    break
done



if [ $period = 'present' ]
then
for startYEAR in "${startYEARs[@]}"; do
    
cdo -O --timestat_date first timmean -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmean.nc

cdo -O --timestat_date first timmin -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmin.nc

cdo -O --timestat_date first timmax -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmax.nc

cdo -O --timestat_date first timstd -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timstd.nc

cdo -O --timestat_date first timmean -gtc,28 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_pctl28.nc

cdo -O --timestat_date first timmean -gtc,30 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_pctl30.nc

cdo -O --timestat_date first timmean -gtc,33 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_pctl33.nc

cdo -O --timestat_date first --percentile inverted_cdf timpctl,99 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmin.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmax.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_99p.nc

cdo -O --timestat_date first selyear,${startYEAR}/${endYEAR} -mergetime "${inDIR}"/*"${var}"_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_mergetime.nc

cdo -O --percentile inverted_cdf timpctl,99 \
    $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_mergetime.nc \
    $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmin.nc \
    $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmax.nc \
    $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_99p.nc

rm $outDIR/${FILENAME}_*_mergetime.nc

done 
fi






if [ $period = 'pre-industrial' ]
then
for endYEAR in "${endYEARs[@]}"; do

cdo -O --timestat_date first timmean -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmean.nc

cdo -O --timestat_date first timmin -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmin.nc

cdo -O --timestat_date first timmax -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmax.nc

cdo -O --timestat_date first timstd -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timstd.nc

cdo -O --timestat_date first timmean -gtc,28 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_pctl28.nc

cdo -O --timestat_date first timmean -gtc,30 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_pctl30.nc

cdo -O --timestat_date first timmean -gtc,33 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_pctl33.nc

cdo -O --timestat_date first selyear,${startYEAR}/${endYEAR} -mergetime "${inDIR}"/*"${var}"_*.nc $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_mergetime.nc

cdo -O --percentile inverted_cdf timpctl,99 \
    $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_mergetime.nc \
    $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmin.nc \
    $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_timmax.nc \
    $outDIR/${FILENAME}_${startYEAR}_${endYEAR}_99p.nc

rm $outDIR/${FILENAME}_mergetime.nc

done
fi




done