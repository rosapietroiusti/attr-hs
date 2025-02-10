#!/bin/bash

#SBATCH --time=5:00:00
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
inDIRs1=${VSC_SCRATCH_VO_USER}/attr-hw/output/output_jan25/WBGT/ISIMIP3b/historical
inDIRs2=${VSC_SCRATCH_VO_USER}/attr-hw/output/output_jan25/WBGT/ISIMIP3b/ssp370
outDIRs=${VSC_SCRATCH_VO_USER}/attr-hw/output/output_jan25/WBGT/ISIMIP3b/preprocessed

# set datasets to loop over
#datasets=('CanESM5' 'CNRM-CM6-1' 'GFDL-ESM4' 'IPSL-CM6A-LR' 'MIROC6' 'MRI-ESM2-0' ) # first round
datasets=( 'EC-Earth3' 'UKESM1-0-LL' 'MPI-ESM1-2-HR' 'CNRM-ESM2-1' ) # second round 

# variable
var='WBGT'

nYEARs=( 10 20 30 ) 


#====================================================

# loop over datasets 
for dataset in "${datasets[@]}"; do   
echo "Processing dataset: $dataset"


# first round 
# if [ $dataset = "${datasets[0]}" ]
# then
#     endYEARs=( 2004	2008 2009 )
# elif [ $dataset = "${datasets[1]}" ]
# then     
#     endYEARs=( 2021	2021 2020 )
# elif [ $dataset = "${datasets[2]}" ]
# then     
#     endYEARs=( 2029	2029 2029 )
# elif [ $dataset = "${datasets[3]}" ]
# then     
#     endYEARs=( 2009	2010 2011 )
# elif [ $dataset = "${datasets[4]}" ]
# then     
#     endYEARs=( 2034	2034 2033 )
# elif [ $dataset = "${datasets[5]}" ]
# then     
#     endYEARs=( 2021	2022 2024 )
# fi


# second round 
if [ $dataset = "${datasets[0]}" ]
then
    endYEARs=( 2013	2016 2020 )
elif [ $dataset = "${datasets[1]}" ]
then     
    endYEARs=( 2019	2021 2023 )
elif [ $dataset = "${datasets[2]}" ]
then     
    endYEARs=( 2018	2019 2021 )
elif [ $dataset = "${datasets[3]}" ]
then     
    endYEARs=( 2026	2026 2027 )
fi



# get input directory 
inDIR1=${inDIRs1}/${dataset}
inDIR2=${inDIRs2}/${dataset}
echo **indir is $inDIR1 and $inDIR2

# get output directory 
outDIR=${outDIRs}/${dataset}

# make outdir
if [ ! -e $outDIR ]
then
	mkdir -p $outDIR
fi
echo **outdir is $outDIR



# get basename for saving files 
for FILE in $inDIR1/*_${var}_*.nc; do
    FILENAME=$(basename -s .nc ${FILE})
    FILENAME=${FILENAME%_WBGT_*} # cut everything after word historical 
    echo ${FILENAME}
    break
done



for i in {0..2}; do

nYEAR="${nYEARs[i]}"
endYEAR="${endYEARs[i]}"

startYEAR=$((endYEAR-nYEAR+1))

echo $nYEAR $startYEAR $endYEAR


# min,max,std,mean

cdo -O --timestat_date first timmean -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR1}/*_${var}_*.nc ${inDIR2}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_timmean.nc

cdo -O --timestat_date first timmin -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR1}/*_${var}_*.nc ${inDIR2}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_timmin.nc

cdo -O --timestat_date first timmax -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR1}/*_${var}_*.nc ${inDIR2}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_timmax.nc

cdo -O --timestat_date first timstd -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR1}/*_${var}_*.nc ${inDIR2}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_timstd.nc


# what is percentile of WBGT28,30,33

cdo -O --timestat_date first timmean -gtc,28 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR1}/*_${var}_*.nc ${inDIR2}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_pctl28.nc

cdo -O --timestat_date first timmean -gtc,30 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR1}/*_${var}_*.nc ${inDIR2}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_pctl30.nc

cdo -O --timestat_date first timmean -gtc,33 -selyear,${startYEAR}/${endYEAR} -mergetime ${inDIR1}/*_${var}_*.nc ${inDIR2}/*_${var}_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_pctl33.nc


# 99th percentile in present-day time period

cdo -O --timestat_date first selyear,${startYEAR}/${endYEAR} -mergetime "${inDIR1}"/*"${var}"_*.nc "${inDIR2}"/*"${var}"_*.nc $outDIR/${FILENAME}_2019_${nYEAR}yr_mergetime.nc

cdo -O --percentile inverted_cdf timpctl,99 \
    $outDIR/${FILENAME}_2019_${nYEAR}yr_mergetime.nc \
    $outDIR/${FILENAME}_2019_${nYEAR}yr_timmin.nc \
    $outDIR/${FILENAME}_2019_${nYEAR}yr_timmax.nc \
    $outDIR/${FILENAME}_2019_${nYEAR}yr_99p.nc

rm $outDIR/${FILENAME}_2019_${nYEAR}yr_mergetime.nc




done




done