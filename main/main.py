"""
Attributable heat stress children v. adults : Main script to extract climate signal 
-------------------------------------------------------------------------------------------

This script:

- Calculates WBGT daily from daily tasmax, pressure and specific humidity
- Calculates return period and return levels for a given percentile or absolute magnitude threshold defined in settings.py 
    - Variables implemented: TX90, TX95, TX99, WBGT28, WBGT30, WBGT33, WBGT99
    - Methods implemented: empirical percentiles (from 50 years in PI and 30 years in present), implemented for ISIMIP3b (models)
                           shift fit with GMST as covariate, implemented for ISIMIP3a (reanalysis)
- Saves outputs as global gridded maps of p0, p1, and int_0, int_1 in pre-industrial and present day


Data:

- ISIMIP3a (reanalysis)
- ISIMIP3b (bias-adjusted CMIP6 GCMs)


Credits:

- WBGT calculation: CDS_heat_stress_indicators (Schwingschackl 2021) 
https://github.com/schwings-clemens/CDS_heat_stress_indicators
- shift fit: dist_cov (Hauser 2017)
https://github.com/mathause/dist_cov/tree/main


Created June 2023, Last upadate May 2024
rosa.pietroiusti@vub.be


To do:
~~~~~~~

Climate: 
- obs code and shift fit wrt GMST: outputting loglike for GOF and CI 
- run SF on tasmax 
- time-series: delete, clean up code 
- use hist-nat (pool+empirical) and/or include all ISIMIP3b hist models - delete, clean up code

Exposure:
- cfr with my exposure calculations wrt my old function
- country level / regional
- pop distribution wrt emissions/vulnerability/GDP etc. 

General:
- clean up code, remove old functions I'm not using
- possibly separate fxn files into different files and rename e.g. attrib_climate.py, dem_exposure.py... ?
- clean up take fxns also from newer nb files 


"""

import numpy as np
import pandas as pd
import os, glob, re 
import math
import xarray as xr
import dask
import netCDF4
import time
from datetime import timedelta, datetime


# My settings and functions
from settings import *
from functions import *

# for wbgt calc and empirical pctls
#dask.config.set(scheduler='processes')
#print('scheduler is processes')


# ======================
# Flags
# for saving and running pieces of this script 
# ======================



flags_run = {}

flags_run['save'] = True

flags_run['hist'] = False # hist+projections

flags_run['hist-nat'] = False    # del this later when i clean up (after review) 

flags_run['calc-wbgt'] = False   # calculate the WBGT and save output in SCRATCH. 

flags_run['run-pi'] = False # if you already have it saved, dont re-run it




# ======================
# Main script
# ======================



if __name__ == '__main__':
    
    from dask.distributed import Client

    # shift fit  
    client = Client() 

    # emp pctl
    # memory_limit = '200GB'  
    #client = Client(n_workers=1, threads_per_worker=1, memory_limit=memory_limit)

    # calculate WBGT
    # memory_limit = '200GB'  
    # client = Client(n_workers=2, threads_per_worker=1, memory_limit=memory_limit)
    
    
    print(datetime.now(), f'client initiated \n')
    print(client)
    
    start_message()
    
    
    
    
    
    # ======================
    # run ISIMIP3b models
    # ======================

    
    if flags['models'] == 'ISIMIP3b':
        print('running analysis for ISIMIP3b models')

        
        # loop over models
        for GCM in [GCMs[idx_models]]:   #  int(sys.argv[2]) replace with idx_models
            print(datetime.now(), GCM)
            tic = time.time()
            
            
            # make/get path for saving output
            if not flags_run['calc-wbgt']: 
                outdir = make_outdir(GCM, makedirs=True)
                     
                
                
            # ======================
            # historical models + ssp370
            # ======================
            
            if flags_run['hist'] == True: 
                print('running analysis for historical (+ssp370)')
                
                
                
                
                
                # ======================
                # use the cmip data as input to calculate the wbgt
                # ======================

                if flags_run['calc-wbgt'] == True: 
    

                    calc_wbgt_newt(GCM, 
                                    scenario1=flags['experiment'], 
                                    scenario2=None,  
                                    variables=VARs,
                                    save=True,
                                    overwrite=False,
                                    outdirname='output_jan25')
                                        

 

                    

                
                
                # ==========================================================
                # calculate p1, p0 and X0, X1 using empirical percentiles
                # ==========================================================

                if flags['method'] == 'empirical_percentile':
                    
                    
                    if flags_run['run-pi']:
                        # open PI data
                        da_pi = open_model_data(GCM, 
                                                period='pre-industrial', 
                                                scenario1='historical', 
                                                scenario2='ssp370')

                        # Calculate what percentile it is in preindustrial
                        data_pi_percentile = compute_quantile(da_pi, percentile) 
                    
                    else:
                        # open pre-calculated PI thresholds
                        filepath = glob.glob(os.path.join(outdirs,'output_empirical',metric, 'ISIMIP3b',GCM, 
                                        f'*{metric}_pre-industrial_returnperiod_{start_pi}_{end_pi}.nc'))[0]  # TODO: CLEANUP this path/fxn/flags outdirs
                        print('filepath pi', filepath)
                        
                        data_pi_percentile = xr.open_dataarray(filepath,  decode_times=False)
                    
                    
                    
                    
                    # run for only one central year and take 30 year interval around that 
                    if flags['time_method'] == 'single-year':

                        # open time-slice with dask 
                        if target_years is not None:
                            da_pres = open_model_data(GCM, 
                                                      period='target-year', 
                                                      scenario1='historical', 
                                                      scenario2='ssp370', 
                                                      target_year=target_years, # target year in obs to match models to
                                                      windowsize=30,
                                                     method=warming_period_method, # ar6 or centered window (30yr period)
                                                     match=warming_period_match) # closest or crossed 
                        elif target_temperature is not None:
                            da_pres = open_model_data(GCM, 
                                                      period='target-year', 
                                                      scenario1='historical', 
                                                      scenario2='ssp370', 
                                                      target_year=None, 
                                                      target_temperature=target_temperature,# target temp to match models to (elsewhere check years)
                                                      windowsize=30,
                                                      method=warming_period_method, 
                                                     match=warming_period_match)                            

                        # calc return level X1 in PD 
                        # i.e. has same return period in present-day as 
                        # reference percentile did in pre-industrial
                        # e.g. to calculate delta I 
                        da_return_level = compute_quantile(da_pres, percentile) 
                        print(datetime.now(), f"computed return level corresponding to \
                        {int(percentile*100)}th percentiles")

                        # calculate present-day percentile 
                        # of what was pi99 in pre-industrial (p1)
                        # e.g. to calculate PR and nAHD
                        # function calcs percentiles - TODO: check error from this
                        da_return_period = calc_percentiles_da(da_pres, data_pi_percentile)
                        
                        print(datetime.now(), f"calculated present-day return period of \
                        pre-industrial {metric}")
                        
                        
                        ext = metric+'_'+flags['time_method']+warming_period_method+warming_period_match
                    
                    
                    
                    
    
                        
                        
                    if flags_run['save'] == True:
                    
                        if flags_run['run-pi']:
                        
                            filesavename = get_filesavename(GCM, 
                                                            'historical',
                                                            'ssp370', 
                                                            ext=metric+'_pre-industrial', 
                                                            data=data_pi_percentile, 
                                                            startyear=start_pi, 
                                                            endyear=end_pi)
                            data_pi_percentile.to_netcdf(os.path.join(outdir, filesavename))
                        
                        filesavename = get_filesavename(GCM, 
                                                        'historical',
                                                        'ssp370', 
                                                        ext+'_returnlevel', 
                                                        data=da_return_level) 
                        da_return_level.to_netcdf(os.path.join(outdir, filesavename))
                        print(datetime.now(), 
                              f"saved return level of {int(percentile*100)}th percentiles")
                        
                        filesavename = get_filesavename(GCM, 
                                                        'historical',
                                                        'ssp370', 
                                                        ext+'_returnperiod', 
                                                        data=da_return_period)
                        da_return_period.to_netcdf(os.path.join(outdir, filesavename))
                        print(datetime.now(), 
                              f"saved return periods of pre-industrial {metric}")


                        
                        
                        
                # ==========================================================       
                # empirical percentiles based on a fixed magnitude threshold
                # ==========================================================

                
                elif flags['method'] == 'fixed_threshold':
                    
                    if flags_run['run-pi']:
                    
                        # open arrays for specified time windows
                        da_pi = open_model_data(GCM, 
                                                period='pre-industrial', 
                                                scenario1='historical', 
                                                scenario2='ssp370')
                        
                        da_return_period_pi = calc_percentiles_da(da_pi, fixed_threshold)
                    
                    if flags['time_method'] == 'single-year':
                        
                        # open time-slice with dask 
                        if target_years is not None:
                            da_pres = open_model_data(GCM, 
                                                      period='target-year', 
                                                      scenario1='historical', 
                                                      scenario2='ssp370', 
                                                      target_year=target_years, # target year in obs to match models to
                                                      windowsize=30,
                                                     method=warming_period_method, # ar6 or centered window (30yr period)
                                                     match=warming_period_match) # closest or crossed 
                        elif target_temperature is not None:
                            da_pres = open_model_data(GCM, 
                                                      period='target-year', 
                                                      scenario1='historical', 
                                                      scenario2='ssp370', 
                                                      target_year=None, 
                                                      target_temperature=target_temperature,# target temp to match models to (elsewhere check years)
                                                      windowsize=30,
                                                     method=warming_period_method, # ar6 or centered window (30yr period)
                                                     match=warming_period_match) # closest or crossed
                            
                        # calculate return period of fixed threshold (for PR/nAHD)
                        da_return_period_pres = calc_percentiles_da(da_pres, fixed_threshold)
  
                        
                        ext = metric+'_'+flags['time_method']+warming_period_method+warming_period_match
                    
                    
                    if flags_run['save'] == True:
                        
                        
                        if flags_run['run-pi']:
                            
                            filesavename = get_filesavename(GCM, 
                                                            'historical',
                                                            'ssp370', 
                                                            ext=metric+'_pre-industrial_returnperiod', 
                                                            data=da_return_period_pi, 
                                                            startyear=start_pi, 
                                                            endyear=end_pi)
                            da_return_period_pi.to_netcdf(os.path.join(outdir, filesavename))
                        
                        filesavename = get_filesavename(GCM, 
                                                        'historical',
                                                        'ssp370', 
                                                        ext+'_returnperiod', 
                                                        data=da_return_period_pres)
                        da_return_period_pres.to_netcdf(os.path.join(outdir, filesavename))
                        
                        
                    
 
                
                
                
                
                elif flags['method'] == 'shift_fit':
                    print('running shift fit, global')
                
                    # make or open output directory
                    outdir = make_outdir(GCM, makedirs=True) 
                    
                    # open data 
                    da = open_model_data(model=GCM, 
                                        period='start-end', 
                                        scenario1=flags['experiment'], 
                                        scenario2=None, 
                                        target_year=None, 
                                        windowsize=None, 
                                        chunk_version=flags['chunk_version'], #
                                        variable=var,
                                        startyear=flags['shift_period'][0],
                                        endyear=flags['shift_period'][1],  
                                       ) 
                    
                    # get lowess smoothed covariate (GMST) from GCM simulation
                    df_gmst_mod = merge_model_gmst(GCM, dir_gmst_models)
                    df_cov_smo = apply_lowess(df_gmst_mod, df_gmst_mod.index, ntime=4)
                    
                    if not flags['shift_loglike']:
                        da_params = norm_shift_fit(da,
                                                   df_cov_smo, 
                                                   shift_sigma=flags['shift_sigma'], 
                                                   by_month=True)
                        ext = metric+'_params_shift_loc_mon' # TODO: fix name of old files that dont actually have loglike 
                    else:
                        # output log-likelihood of model for CI calculation and goodness of fit - TODO FIX or DELETE THIS 
                        da_params = norm_shift_fit_loglike(da,
                                                   df_cov_smo, 
                                                   shift_sigma=flags['shift_sigma'], 
                                                   by_month=True)                
            
                        ext = metric+'_params_shift_loc_mon_loglike'
            
                    if flags_run['save'] == True:
                        
                        filesavename = get_filesavename(GCM, 
                                                        'historical',
                                                        'ssp370', 
                                                        ext=ext, 
                                                        keep_scenario=True,
                                                        startyear=flags['shift_period'][0], 
                                                        endyear=flags['shift_period'][1])  
                        
                        da_params.to_netcdf(os.path.join(outdir, filesavename))
                        
                        print(datetime.now(), f"saved params of shift fit")

        
        
     
        
        
    
        
        
    # ======================    
    # observations ISIMIP3a
    # ======================
    
    
    elif flags['models'] == 'ISIMIP3a': 
        print('running analysis for ISIMIP3a observations')

        # loop over observational datasets
        for dataset in [datasets[idx_models]]:  # int(sys.argv[2]) replaced with idx_models
            print(datetime.now(), dataset)
            tic = time.time()


            # ==============================================================
            # use the daily data as input to calculate the wbgt
            # ==============================================================
            
            if flags_run['calc-wbgt'] == True: 
                


                calc_wbgt_newt(dataset, 
                                    scenario1=flags['experiment'], 
                                    scenario2=None,  
                                    variables=VARs,
                                    save=True,
                                    overwrite=False,
                                    outdirname='output_jan25')
                 






            
            # =========================
            # run shift fit on obs data
            # =========================
            
            
            elif flags['method'] == 'shift_fit':
                
                print('running shift fit, global')
                
                # make or open output directory
                outdir = make_outdir(dataset, makedirs=True) 
                
                # open data 
                da = open_model_data(model=dataset, 
                                    period='start-end', 
                                    scenario1=flags['experiment'], 
                                    scenario2=None, 
                                    target_year=None, 
                                    windowsize=None, 
                                    chunk_version=flags['chunk_version'], #
                                    variable=var,
                                    startyear=flags['shift_period'][0],
                                    endyear=flags['shift_period'][1],  
                                     engine='h5netcdf' # testing this for HDF error thing! 
                                   ) 
                
                # open and smooth covariate (GMST)
                df_cov = pd.read_csv(observed_warming_path_annual
                                    ).rename(columns={'timebound_lower':'year'}
                                    ).set_index('year')[['gmst']]
                df_cov_smo = pd.DataFrame(apply_lowess(df_cov, df_cov.index, ntime=4))
                
   

                print(f'shift fit by month')
                
                # output log-likelihood of model for CI calculation and goodness of fit (developing)
                if not flags['shift_loglike']:
                    da_params = norm_shift_fit(da,
                                               df_cov_smo, 
                                               shift_sigma=flags['shift_sigma'], 
                                               by_month=True)
                    if not flags['shift_sigma']:
                        ext = metric+'_params_shift_loc_mon'
                    else:
                        ext = metric+'_params_shift_loc_sigma_mon' 
                    
                else:
                    da_params = norm_shift_fit_loglike(da,
                                               df_cov_smo, 
                                               shift_sigma=flags['shift_sigma'], 
                                               by_month=True)                
        
                    ext = metric+'_params_shift_loc_mon_loglike' 
        
                if flags_run['save'] == True:
                    
                    filesavename = get_filesavename(dataset, 
                                                    'obsclim',
                                                    None, 
                                                    ext=ext, 
                                                    keep_scenario=True,
                                                    startyear=flags['shift_period'][0], 
                                                    endyear=flags['shift_period'][1])  
                    
                    da_params.to_netcdf(os.path.join(outdir, filesavename))
                    
                    print(datetime.now(), f"saved params of shift fit")
                


        
            
            
        toc = time.time()
        print(f'{datetime.now()}, done, elapsed time: { (toc-tic) / 3600 } hours. \n')                    
                        