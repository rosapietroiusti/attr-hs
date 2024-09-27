#!/bin/bash

#SBATCH --time=30:00:00
#SBATCH --ntasks=1 --cpus-per-task=4
#SBATCH --mail-type=ALL

# ==================================

# Preprocess ISIMIP3b models to compare with ISIMIP3a obs

# for present-day 10yr 20yr and 30yr periods

#====================================================



# tic
START=$(date +%s.%N)

# load modules
module purge 
module load CDO/2.2.2-gompi-2023a

# set indir / outdir 
inDIRs=${VSC_SCRATCH_VO_USER}/attr-hw/output/output_apr24-9110516/WBGT/ISIMIP3b
outDIRs=${VSC_SCRATCH_VO_USER}/attr-hw/output/output_apr24-9110516/WBGT/ISIMIP3b/preprocessed

# set datasets to loop over
datasets=('CanESM5' 'CNRM-CM6-1' 'GFDL-ESM4' 'IPSL-CM6A-LR' 'MIROC6' 'MRI-ESM2-0' ) 

# variable
var='WBGT'

nYEARs=( 10 20 30 ) 


#====================================================

# loop over datasets 
for dataset in "${datasets[@]}"; do   
echo "Processing dataset: $dataset"


if [ $dataset = "${datasets[0]}" ]
then
    endYEARs=( 2004	2008 2009 )
elif [ $dataset = "${datasets[1]}" ]
then     
    endYEARs=( 2021	2021 2020 )
elif [ $dataset = "${datasets[2]}" ]
then     
    endYEARs=( 2029	2029 2029 )
elif [ $dataset = "${datasets[3]}" ]
then     
    endYEARs=( 2009	2010 2011 )
elif [ $dataset = "${datasets[4]}" ]
then     
    endYEARs=( 2034	2034 2033 )
elif [ $dataset = "${datasets[5]}" ]
then     
    endYEARs=( 2021	2022 2024 )
fi




# get input directory 
inDIR=${inDIRs}/${dataset}
echo **indir is $inDIR

# get output directory 
outDIR=${outDIRs}/${dataset}

# make outdir
if [ ! -e $outDIR ]
then
	mkdir -p $outDIR
fi
echo **outdir is $outDIR



# get basename for saving files 
for FILE in $inDIR/*_${var}_*.nc; do
    FILENAME=$(basename -s .nc ${FILE})
    FILENAME=${FILENAME%_WBGT_*} # cut everything after word obsclim 
    echo ${FILENAME}
    break
done



for i in {0..2}; do

nYEAR="${nYEARs[i]}"
endYEAR="${endYEARs[i]}"

startYEAR=$((endYEAR-nYEAR+1))

echo $nYEAR $startYEAR $endYEAR


# min,max,std,mean

cdo -O --timestat_date first timmean -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_timmean.nc

cdo -O --timestat_date first timmin -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_timmin.nc

cdo -O --timestat_date first timmax -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_timmax.nc

cdo -O --timestat_date first timstd -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_timstd.nc


# what is percentile of WBGT28,30,33

cdo -O --timestat_date first timmean -gtc,28 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_pctl28.nc

cdo -O --timestat_date first timmean -gtc,30 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_pctl30.nc

cdo -O --timestat_date first timmean -gtc,33 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_pctl33.nc


# 99th percentile

cdo -O --timestat_date first selyear,${startYEAR}/${endYEAR} -mergetime "${inDIR}"/*"${var}"_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_mergetime.nc

cdo -O --percentile inverted_cdf timpctl,99 \
    $outDIR/${FILENAME}_2019_${nYEAR}yr_mergetime.nc \
    $outDIR/${FILENAME}_2019_${nYEAR}yr_timmin.nc \
    $outDIR/${FILENAME}_2019_${nYEAR}yr_timmax.nc \
    $outDIR/${FILENAME}_2019_${nYEAR}yr_99p.nc

rm $outDIR/${FILENAME}_2019_${nYEAR}yr_mergetime.nc




done




done