"""
Attributable heat stress functions

rosapietroiusti@vub.be
Last update: Jan 2025

to check:
- terminology ecdf object ok? 
- interpolation method inverted_cdf ok? 
- better way of outputting da_out as an xarray data array
"""


import numpy as np
import xarray as xr
import pandas as pd
import datetime as dt
from scipy import interpolate
import os, glob, re, sys
import time
from datetime import datetime
import dask
from dask import delayed, compute
import dask.array as da
import netCDF4

# import my variables and util functions
from settings import *
from utils import * 

# NEWT Tw calculation (Warren, Rogers et al., https://github.com/robwarrenwx/atmos)
sys.path.append('../atmos/')
from atmos import thermo

# dist_cov (M. Hauser et al., https://github.com/mathause/dist_cov)
sys.path.append('../dist_cov/dist_cov/')
import distributions as distributions 
import utils as utils 


    
    
"""
 ---------------------------------------------------------------
 PART 1. Determine present-day warming window in models
 ---------------------------------------------------------------
"""




def merge_model_gmst(GCMs, dir_gmst_models):
    """ Make a single dataframe with all GCMs and their GMST anomaly wrt pre-industrial baseline (1850-1900)
    ---------------------------------------------------------------
    TODO: 
    - Potentially remove loop "for i in GCM_list" to make it coherent with other functions that work on a single GCM? 
    - Make names of variables coherent with other functions 
    - Make flags for non-ISIMIP models
    ---------------------------------------------------------------
    Inputs:
        GCMs: (list or str) list of GCM names or a single GCM name
        dir_gmst_models: (str) directory where gmst warming files are found, as csv
        flags: (dict) dictionary containing flags, e.g., {'models': 'ISIMIP3b'}
        
    Returns:
        df_gmst_mod: (df) dataframe rows are years, columns are models, warming wrt 1850-1900
    """

    # Ensure GCMs is a list, even if it contains a single model
    if isinstance(GCMs, str):
        GCMs = [GCMs]

    for i in GCMs:
        if flags['models'] == 'ISIMIP3b':
            # Concatenate hist to 2014 and ssp3-rcp70 from 2015
            scenarios = ['historical', 'ssp370']   
            gmst_paths = [glob.glob(os.path.join(dir_gmst_models, scenario, i, '*.txt'))[0] for scenario in scenarios]

            gmst_a = pd.read_csv(gmst_paths[0], delim_whitespace=True, comment='#', header=None, names=['year', '{}'.format(i)], index_col=0)
            gmst_b = pd.read_csv(gmst_paths[1], delim_whitespace=True, comment='#', header=None, names=['year', '{}'.format(i)], index_col=0)
            
        else:
            print('Error: Modelled GMST paths not defined for non-ISIMIP models.')
            
        gmst_mod_raw = pd.concat([gmst_a, gmst_b])

        # As anomaly wrt 1850-1900
        baseline = gmst_mod_raw.loc[1850:1900].mean()
        gmst_mod = gmst_mod_raw - baseline

        # Make a single dataframe for easier and nicer plotting and analysis, just make sure not mixing up data! 
        if i == GCMs[0]:
            df_gmst_mod = gmst_mod
        else:
            df_gmst_mod = df_gmst_mod.merge(gmst_mod, left_index=True, right_index=True)
            
    return df_gmst_mod



def calc_warming_periods_models(GCMs, dir_gmst_models, observed_warming_path, target_year=None, target_temperature=None, method='ar6', windowsize=30, match='closest',centered=None):
    """
    calculates year when target warming is reached (central year) and start and end year of 30-year period (standard here) in model 

    if target is warming_ar6_forster window=10, centered=False
    if target is warming_sr15_forster window=30, centered=True # not implemented

    Inputs:
        dir_gmst_models: (str) directory of model gmst time series 
        observed_warming_path: (str) filepath to forster obs warming data
        target_year: (int) what year to calc for
        target_temperature: (float) alternative to target_year, what warming level to calc for 
        method: 'ar6', 'window' 
        match: 'closest', 'crossed' 
    Returns:
        df_out: (df) df model present day warming 
                    columns: model, year, value, target, start_y, end_y
    """
    
    df_gmst_mod = merge_model_gmst(GCMs, dir_gmst_models)
    
    if method == 'ar6':
        window=10 
        centered=False
    elif method=='window':
        window=windowsize
        if centered is None:
            centered=True
    else :
        print('error method and filepath to observed warming not defined')
    
    # models rolling mean 10 years 
    df = df_gmst_mod.rolling(window, min_periods=1, center=centered).mean() 

    # determine temperature to match to 
    if target_temperature is not None:
        val = target_temperature
    elif target_year is not None:
        df_obs = pd.read_csv(observed_warming_path).rename(columns={'timebound_upper':'year'}).set_index('year')[['gmst']]
        val = df_obs.loc[target_year+1].values[0] # the upper timebound is excluded so add +1 - check this
    else:
        print('error define either target year or target temperature')

    # make empty dataframe
    d = {'model': [], 'year': [],  'value': [], 'target': [], 'start_y': [], 'end_y': []}
    df_out = pd.DataFrame(data=d)

    for i, j in zip(df.columns.values, range(len(df.columns.values))):

        if match=='closest':
            df_closest = df[i].iloc[(df[i]-val).abs().argsort()[:1]]
            year = df_closest.index.values[0]
        elif match=='crossed':
            crossing = (df[i] - val).apply(np.sign).diff()
            year = int(crossing[crossing > 0].index.min())
        
        df_mod = pd.Series({'model': i, 'year': year ,
                            'value': np.round(df[i].loc[year],4),
                            'target': val,
                           'start_y': year - int(windowsize / 2) + 1, # e.g. targetyear - 14
                           'end_y': year + int(windowsize /2) }) # e.g. targetyear + 15
        df_out = pd.concat([df_out, df_mod.to_frame().T], ignore_index=True)
    
    df_out = df_out.set_index('model')
    
    
    return df_out 



def get_gmst_smo(ntime=4, observed_warming_path=observed_warming_path_annual):
    
    df_gmst_obs_annual = pd.read_csv(observed_warming_path
                                        ).rename(columns={'timebound_lower':'year'}
                                        ).set_index('year')[['gmst']]
    gmst_smo = pd.DataFrame(apply_lowess(df_gmst_obs_annual, df_gmst_obs_annual.index, ntime=ntime))
    
    return gmst_smo
    


def calc_warming_periods_models_all_years(GCMs, 
                                          dir_gmst_models, 
                                          observed_warming_path,
                                          method='ar6', 
                                          centered=False, # only relevant if method is 'window'
                                          window=10,
                                          min_periods=10,
                                          flatten=False):

    
    """
    Calculates when warming level from observations is reached in models, outputs central year in model timeseries. 
    
    ----
    TODO: check that this kind of GMT remapping is ok, cfr. with Luke's
    clean up this function 
    ----

    Inputs:
        dir_gmst_models: (str) directory of model gmst time series 
        observed_warming_path: (str) filepath to forster obs warming data (important! decadal for ar6, annual for 30-yr)
        GCMs: (list of str) list of model names
        methods:    'ar6' :    end of 10-year average 
                    'window' : end or center of average of number of years you decide
                    'sr15' :  add 14 years to obs data by creating OLS on last 15 years, 
                               and match central year of 30-year average (~SR1.5 method)
    
    Returns:
        df_out: (df) df model warming level years
                    columns: year, temp_obs, year_{model}, temp_{model} for each model in GCMs
                    if flatten==True
                    
        OR
        
        da_out: (da) with coords year (year in obs), model (obs or GCMs), feature (year_mod or temp)
                    if flatten==False:
    """
    
        
    if method=='ar6':
        
        window=10
        centered=False
        
        # obs
        df_obs = pd.read_csv(observed_warming_path).rename(
            columns={'timebound_upper':'year'}).set_index(
            'year')[['gmst']]
        
        df_obs.index = df_obs.index - 1 # 1860-2023 is actually 1859-2022
                                        # the upper timebound is excluded so add -1 to 
                                        # index to get correct value 
                                        # e.g. 2013-2023 (excl) = 2013-2022 (incl) = 1.15 degC (val in paper)

    elif method=='window':
                
        # obs
        df_obs = pd.read_csv(observed_warming_path).rename(
            columns={'timebound_upper':'year'}).set_index(
            'year')[['gmst']].rolling(window, min_periods=min_periods, center=centered).mean().dropna() 
        
        df_obs.index = df_obs.index - 1 # 1860-2023 is actually 1859-2022
                                        # the upper timebound is excluded so add -1 to 
                                        # index to get correct value 
                                        # e.g. 2013-2023 (excl) = 2013-2022 (incl) = 1.15 degC (val in paper)
                    
    
    # for sensitivity test 
    elif method=='sr15':
        # extend obs for 15 last years
        def fit_trend(data, var):
            x=data.index
            y=data[var]
            fit=sm.OLS(y, sm.add_constant(x)).fit()    
            
            return fit, x, y

        window=30
        centered=True
        
        # extend obs for 15 last years
        df_obs = pd.read_csv(observed_warming_path).rename(
            columns={'timebound_upper':'year'}).set_index(
            'year')[['gmst']]
        
        # fit linear regression and predict for next 15 years
        fit, x, y = fit_trend(df_obs[-15:], 'gmst')
        x_fut = np.arange(df_obs.index[-1]+1,df_obs.index[-1]+14)
        x_fut = sm.add_constant(x_fut)
        predictions = fit.predict(x_fut)
        
        # add to previous df
        df_obs =  pd.concat([df_obs, pd.DataFrame({'year':x_fut[:,1].astype(int), 'gmst': predictions}).set_index('year')])
        
        # 30 year rolling mean of obs 
        df_obs = df_obs.rolling(window, min_periods=min_periods, center=centered).mean().dropna() 
        
    else:
        print('error method not defined')
    
    
    
    # open modelled gmst annual 
    df_gmst_mod = merge_model_gmst(GCMs, dir_gmst_models)

    # models rolling mean  
    df = df_gmst_mod.rolling(window, min_periods=min_periods, center=centered).mean().dropna() 

    
    # initiate empty data array 
    # create empty arrays for year, model, and feature
    years = df_obs.index
    models = ['obs'] + df.columns.tolist()  # Add 'obs' to the beginning
    features = ['year_mod', 'temp']

    # Create a MultiIndex for the coordinates
    coords = {
        'year': years,
        'model': models,
        'feature': features
    }

    # Create an empty DataArray with the specified coordinates
    da_out = xr.DataArray(
        data=None,  # Provide your data here
        coords=coords,
        dims=('year', 'model', 'feature')
    )

    # add observational info 
    da_out.loc[{'model': 'obs', 'feature':'year_mod'}] =  df_obs.index
    da_out.loc[{'model': 'obs', 'feature':'temp'}] =  df_obs.gmst.values

    # loop over models
    for i in df.columns.values:

        model_years = []
        model_temps = []

        # loop over years
        for year in years:
            # get target year value
            val = df_obs.loc[year].values[0] # value for single year. 


            # find closest year and temperature
            df_closest = df[i].iloc[(df[i]-val).abs().argsort()[:1]]
            model_y = df_closest.index.values[0]
            model_t = df_closest.values[0]
            # append to list 
            model_years.append(model_y)
            model_temps.append(model_t)

        # assign data to da 
        da_out.loc[{'model': i, 'feature':'year_mod'}] =  np.array(model_years)
        da_out.loc[{'model': i, 'feature':'temp'}] =  np.array(model_temps)
    
    # return a data array
    if flatten == False:
        
        return da_out
    
    # return a dataframe
    elif flatten == True:
        
        df_master = None
        
        # loop over models
        for i in da_out.model.values:
            
            df_add = da_out.sel(model=i).to_pandas().rename(columns={"year_mod": "year"})
            if df_master is None:
                df_master = df_add 
            else:
                df_master = df_master.merge(df_add, suffixes=(f'', f'_{i}'), left_index=True, right_index=True)

        df_master = df_master.drop(columns=["year"]).rename(columns={"temp": "temp_obs"}) 
        
        df_out = df_master
        
        return df_out







def open_model_data(model, 
                    period, 
                    scenario1, 
                    scenario2=None, 
                    target_year=None, 
                    target_temperature=None,
                    method='ar6',
                    match='closest', 
                    windowsize=30, 
                    chunk_version=flags['chunk_version'], #for job submit set to 2! 
                    variable=var,
                    startyear=None,
                    endyear=None,
                    engine='netcdf4'
                   ): 
    """
    Open models or obs data based on window around target year (windowsize default value 30 years) 
    or pre-industrial time-period (50 years 1850-1900) or defined start end years
    
    Inputs 
        model :  str               name of GCM or dataset
        period : str 
                                  'pre-industrial' : takes start and end from start_pi and end_pi (by default 1850-1900)
                                  'target-year' : takes warming level from observational target year and calcs closest 
                                             30-year period in model based on calc_warming_periods_models function. 
                                  'model-year' : 30-year period around a model year 
                                  'start-end' : specify start and end years, can be of any length
        scenario1 : str           'historical' or 'hist-nat' (ISIMIP3b) 'obsclim' or 'counterclim' (ISIMIP3a)
        scenario2 : str           'ssp370' or none
        target_year : int         year to use if period is target-year or model-year
        windowsize : int          length of window if period is target-year or model-year
        chunk version : int       how to chunk with dask (0, 1 or 2) 
        variable : str            if you want to specify what variable to get e.g. to calc WBT or open WBGT files
        startyear,endyear : ints  if period is 'start-end' 
    
    Returns: 
        da : data array for the specified variable and period concatted in time opened with dask 
    """
    
       
    if variable=='tasmax':
        dir1, dir2 = get_dirpaths(model, scenario1, scenario2); # in utils.py
        filepaths = get_filepaths(variable,dir1,dir2);  # in utils.py
        
    elif variable=='wbgt':
        variable=variable.upper() #'WBGT' # NEWT output capital variable name
        dirname='output_jan25' # clean this!!
        # could delete this if not using this function on 3a
        if flags['models']=='ISIMIP3a':
            dir1=os.path.join(scratchdirs, dirname, 'WBGT', flags['models'], scenario1, model )
            filepaths=get_filepaths(variable.upper(),dir1)
        else:
            dir1=os.path.join(scratchdirs, dirname, 'WBGT', flags['models'], 'historical', model ) # if you always change flags metric you can also replace with fxn 
            dir2=os.path.join(scratchdirs, dirname, 'WBGT', flags['models'], 'ssp370', model ) # if you always change flags metric you can also replace with fxn 
            print(dir1,dir2)
            filepaths=get_filepaths(variable.upper(),dir1,dir2) # 'WBGT' not 'wbgt' in filename: possibly change for coherence
    
    print(f'opening data for {variable}')
    
    # preindustrial defined as 1850-1900 in settings.py
    if period == 'pre-industrial':
        startyear = start_pi
        endyear = end_pi
        print('pre-industrial period')
        
    
    # if you give it a target year to match in observations, by default will use ar6 10-year method with minsize=1 and find closest match
    # and calc interval as +/- 15 years wrt central year. Check if you want to change any of these defaults. 
    elif period == 'target-year':
        if target_year is not None:
            warming_periods = calc_warming_periods_models(model, dir_gmst_models, observed_warming_path, target_year=target_year, method=method, match=match, windowsize=windowsize);
        elif target_temperature is not None:
            warming_periods = calc_warming_periods_models(model, dir_gmst_models, observed_warming_path, target_temperature=target_temperature, method=method, match=match, windowsize=windowsize);

        # IMPLEMENT 30-yr window for 1.5 warming level !! 

        startyear = warming_periods.loc[model].start_y
        endyear = warming_periods.loc[model].end_y   



    
    # if you ask for a specific model year
    elif period == 'model-year':
        startyear = target_year-14
        endyear = target_year+15
    
    
    # e.g. for calculating WBGT, give it specific model start-end years
    elif period == 'start-end':
        if startyear is not None:
            startyear=startyear
            endyear=endyear
        else:
            print('error start and endyear not defined')
        
    da = open_arrays_dask(filepaths, variable, startyear, endyear, version=chunk_version, engine=engine); # utils
        
    
    if period == 'target-year' or period == 'model-year':
        if target_year is not None:
            da.attrs['target_year'] = target_year
        
    
    check_length(da, startyear, endyear)
    
    return da







"""
 ---------------------------------------------------------------
 PART 2. Calculate WBGT 
 ---------------------------------------------------------------
"""

def calc_wbgt(GCM, 
                scenario1, 
                scenario2=None,  
                chunk_version=2, 
                variables=None, # VARs
                #startyear=None, # find a way to code this in !! for now does all years 1850-2100
                #endyear=None, # find a way to code this in !!
                save=True,
                overwrite=False,
                outdirname=None): 
    
    dir1, dir2 = get_dirpaths(GCM, scenario1, scenario2); # in utils, gets all the years! 

    print(dir1)
    
    filepaths = [get_filepaths(VAR,dir1,dir2) for VAR in variables]  # in utils

    if scenario2 is None:
        experiment=scenario1
    else:
        experiment=scenario1+'-'+scenario2

    scratchdir =  make_outdir(GCM, 
                              makedirs=True, 
                              scratchdir=True,
                              outdirname=outdirname,
                             experiment=experiment) # utils

    for i in range(len(filepaths[0])):
        print(i)

        # check if file already exists
        if overwrite==False and save==True:

            startyear,endyear = xr.open_dataarray(filepaths[0][i]).time.dt.year[[0,-1]]
            filesavename = get_filesavename(GCM, 
                                            scenario1,
                                            scenario2, 
                                            'WBGT', 
                                            startyear=startyear, 
                                            endyear=endyear, 
                                            keep_scenario=True) 

            if os.path.exists(os.path.join(scratchdir,filesavename)):
                print(f'wbgt {i} exists')
                exists=True
            else:
                exists=False 
            
            

        if overwrite==True or exists==False: 

            print(f'calculating wbgt {i}')

            # extract variables 
            tasmax,huss,ps= [xr.open_dataarray(files[i]) for files in filepaths]
            # calculate relative humidity from specific
            e,RH = hsi.get_humidity(huss, ps, tasmax)
            # calculate daily wet bulb globe temperature
            WBGT = hsi.WBGT(tasmax, RH, ps)

            if save==True:

                filesavename = get_filesavename(GCM, scenario1,scenario2, ext='WBGT', data=WBGT, keep_scenario=True)
                WBGT.rename('wbgt').to_netcdf(os.path.join(scratchdir, filesavename))

                print(f'wbgt {i} calculated and saved')
    
    


""" 
New version with NEWT

"""
    




def calc_wbgt_newt(GCM, 
                scenario1, 
                scenario2=None,  
                variables=None,
                save=True,
                overwrite=False,
                outdirname=None): 

    def calc_wet_bulb(ps_chunk, tasmax_chunk, huss_chunk):
        return thermo.wet_bulb_temperature(ps_chunk, tasmax_chunk, huss_chunk, saturation='pseudo')

    # input directory and input files
    print(f'calculating Tw and WBGT for {GCM} {scenario1}')
    dir1, dir2 = get_dirpaths(GCM, scenario1, scenario2); # utils
    print('input dir:', dir1)
    filepaths = [get_filepaths(VAR,dir1,dir2) for VAR in variables]  # utils

    if scenario2 is None:
        experiment=scenario1
    else:
        experiment=scenario1+'-'+scenario2 # TODO: delete this option and run only one scenario at a time? to make separate folders

    # make output directory in scratch  
    scratchdir =  make_outdir(GCM, 
                              makedirs=True, 
                              scratchdir=True,
                              outdirname=outdirname,
                             experiment=experiment) # utils

    for i in range(len(filepaths[0])):
        print(i)

        # check if output file already exists
        if overwrite==False and save==True:

            startyear,endyear = xr.open_dataarray(filepaths[0][i]).time.dt.year[[0,-1]]
            filesavename = get_filesavename(GCM, 
                                            scenario1,
                                            scenario2, 
                                            ext='WBGT', 
                                            startyear=startyear.values, 
                                            endyear=endyear.values, 
                                            keep_scenario=True,
                                           variable='tasmax') # get basename from input file, not from output WBGT file  

            if os.path.exists(os.path.join(scratchdir,filesavename)):
                print(f'wbgt {i} exists')
                exists=True
            else:
                exists=False 

        if overwrite==True or exists==False:
            print(f'calculating tw and wbgt {i}')
    
            # open variables 
            tasmax,huss,ps= [xr.open_dataarray(files[i], engine="h5netcdf").chunk({"lat": lat_chunk, "lon": lon_chunk }).astype(float) for files in filepaths]

            # "time": 30,
            # .chunk({"lat": lat_chunk, "lon": lon_chunk })
            
            # check units
            if not (tasmax.attrs['units'] == 'K' and 
                    huss.attrs['units'] == 'kg kg-1' and 
                    ps.attrs['units'] == 'Pa'):
                raise ValueError("Units are incorrect")
            
            # calculate wet bulb temperature (K) 
            Tw = da.map_blocks(calc_wet_bulb, ps, tasmax, huss, dtype=float)
            #Tw_compute = Tw.compute()
            Tw_da = xr.DataArray(Tw.compute(), coords=[ ('time', tasmax.time.values), ('lat', tasmax.lat.values), ('lon',tasmax.lon.values)], name="Wet Bulb Temperature") 
    
            # calculate wet bulb globe temperature (deg C) 
            WBGT = (0.7*Tw_da + 0.3*tasmax).compute() - 273.15
            
            if save==True:
                filesavename = get_filesavename(GCM, scenario1,scenario2, ext='Tw', data=Tw_da,variable='tasmax',keep_scenario=True)
                Tw_da.attrs['units'] = 'K'
                Tw_da.rename('Tw').to_netcdf(os.path.join(scratchdir, filesavename))
                print(f'Tw {i} calculated and saved')
            
                filesavename = get_filesavename(GCM, scenario1,scenario2, ext='WBGT', data=WBGT,variable='tasmax', keep_scenario=True)
                WBGT.attrs['units'] = 'C'
                WBGT.rename('WBGT').to_netcdf(os.path.join(scratchdir, filesavename))
                print(f'wbgt {i} calculated and saved')

            del tasmax, huss, ps, Tw, Tw_da, WBGT #Tw_compute,











"""
 ---------------------------------------------------------------
 PART 3. Statistics 

 a) empirical percentiles
 b) non stationary distribution fits

 TODO: clean up use of stat terms and document better and delete unused fxns
 ---------------------------------------------------------------
"""


def compute_quantile(da, quantile):
    """ give it a dataframe and quantile you want (e.g. 0.99) and it will give you a da with values that correspond to that percentile
    """
    
    da_out = da.quantile(
                q=quantile,
                dim='time',
                method='inverted_cdf', # check this is best !!! see differnce if using “closest_observation” which should be equivalent to cdo nrank? 
            )
    
    if 'target_year' in da.attrs:
        da_out['target_year']=int(da.attrs['target_year']) 
        
        
    return da_out




    

class ecdf:
    """ initiate class empirical cumulative distribution function quantiles = return levels, percentiles = 1/return period 
    """
    
    def __init__(self, quantiles, percentiles):
        self.quantiles = quantiles
        self.percentiles = percentiles
        
        
def calc_percentiles_da(data, thresholds):
    """ for values given by thresholds see what percentile they correspond to in data 
     
    takes a data array of data with a time axis, computes the empirical quantiles and percentiles for it
    and outputs the percentiles of the values in data that most closely match the values in thresholds
    no interpolation, only nearest neighbor 

    TODO: 
    - nearest neighbor (check this is best choice)
    - give fxn a double check 
    """
    
    if len(data.shape) == 1:
        data = np.expand_dims(data, axis=0)  # Add a dimension for compatibility with 3D data
    
    # get index of time axis
    ax = [i for i, element in enumerate(data.dims) if element == 'time'] 
    ax = ax[0]
    
    # Sort data along the time axis
    quantiles = np.sort(data, axis=ax)  
    
    # Compute the ECDF values
    percentiles = np.arange(1, quantiles.shape[ax] + 1) / quantiles.shape[ax]  # ecdf value = rank / length
    percentiles_exp = np.full_like(quantiles, percentiles[:, None, None]) # same shape as data
    ecdf_obj = ecdf(quantiles, percentiles_exp)
    
    # get index of value in quantiles that is closest to threshold
    if isinstance(thresholds, int) or isinstance(thresholds, float):
        closest_index = np.argmin(np.abs(ecdf_obj.quantiles - thresholds), axis=ax) # check the dims broadcasting is ok here! or dont make an ecdf object ! 
    else:
        closest_index = np.argmin(np.abs(ecdf_obj.quantiles - thresholds.values), axis=ax) # check the dims broadcasting is ok here! or dont make an ecdf object ! 
    
    # get out the percentile that corresponds to that value
    percentile = ecdf_obj.percentiles[closest_index, np.arange(ecdf_obj.percentiles.shape[ax+1])[:, None], np.arange(ecdf_obj.percentiles.shape[ax+2])] 

    # output it as a data array with same metadata as input data array
    da = data[0,:,:].drop_vars('time').rename('p1') 
    da.values = percentile

    return da






def norm_shift_fit(da, df_cov, shift_sigma=False, by_month=False, round_decimals=4):
    """
    Fit normal distribution with shift fit (varying location or location and scale) as linear functions of a covariate
    Fit a different model for each month of the year 
    Best estimates with max likelihood estimation
    Uses functions from dist_cov (Hauser et al, ETH) 
    
    Input : 
    da : your data as a dataarray
    df_cov : your covariate as an annual dataframe
    shift_sigma :   False: loc = b0 + b1 * cov, scale fixed
                    True: loc = b0 + b1 * cov, scale = sigma_b0 + sigma_b1 * cov 
    by_month : Boolean, if True, fit model for each month separately
    round_decimals : Number of decimals to round the data array before processing. If None, no rounding is applied.
                    
    Returns:
    DataArray with parameter names and values, and month as a coordinate if by_month is True.
    """
    
    def fit_normal_dist(data, covariate, shift_sigma):
        """Fit normal distribution to data with covariate. 
        These functions are adapted from dist_cov (Hauser et al., see GitHub)"""
        if shift_sigma:
            dist = distributions.norm_cov_std(data=data, cov=covariate)
        else:
            dist = distributions.norm_cov(data=data, cov=covariate)
        return dist.fit()
    
    def apply_fit_to_group(da_group):
        """Apply the fitting function to a group of data."""
        if round_decimals is not None:
            # Combine rounding and casting
            da_group = da_group.round(round_decimals).astype("float32")
            
        t = da_group.time.dt.year
        cov = df_cov.loc[t.values].values.squeeze()  # get covariate values for correct years
                
        output_sizes = {'params': 4} if shift_sigma else {'params': 3}
        dask_gufunc_kwargs = {'output_sizes': output_sizes}

        result = xr.apply_ufunc(
            fit_normal_dist,
            da_group,
            cov,
            shift_sigma,
            input_core_dims=[['time'], ['time'], []],
            output_core_dims=[['params']],
            vectorize=True,
            dask='parallelized', 
            output_dtypes=[float],
            dask_gufunc_kwargs=dask_gufunc_kwargs
        )
        
        return result
    
    print('test: dask parallelized')
    
    if by_month:
        results = []
        for month, group in da.groupby("time.month"):
            fit_result = apply_fit_to_group(group)
            fit_result = fit_result.expand_dims("month").assign_coords(month=("month", [month]))
            results.append(fit_result)
        
        result = xr.concat(results, dim="month")
    else:
        if round_decimals is not None:
            da = da.round(round_decimals)  # Round here if processing the entire array
        result = apply_fit_to_group(da)

    # Rename params coordinates
    if shift_sigma:
        result['params'] = ["b0", "b1", "sigma_b0", "sigma_b1"]
    else:
        result['params'] = ["b0", "b1", "sigma"]

    result = result.rename('fit_params')
    
    return result






def norm_shift_fit_loglike(da, df_cov, shift_sigma=False, by_month=False):
    """
    Fit normal distribution with shift fit (varying location or location and scale) as linear functions of a covariate
    Fit a different model for each month of the year 
    Best estimates with max likelihood estimation
    Uses functions from dist_cov (Hauser et al, ETH) 
    Modified to output also log likelihood value and inv_hess computed with L-BFGS-B optimization method (scipy)
    Note that you need to convert float values (.astype(float32)) to get them to properly behave later
    
    Input : 
    da : your data as a dataarray
    df_cov : your covariate as an annual dataframe
    shift_sigma :   False: loc = b0 + b1 * cov, scale fixed
                    True: loc = b0 + b1 * cov, scale = sigma_b0 + sigma_b1 * cov 
    by_month : Boolean, if True, fit model for each month separately
                    
    Returns:
    DataArray with parameter names and values, and month as a coordinate if by_month is True.
    """
    
    def fit_normal_dist(data, covariate, shift_sigma):
        """Fit normal distribution to data with covariate."""
        if shift_sigma:
            dist = distributions.norm_cov_std(data=data, cov=covariate)
        else:
            dist = distributions.norm_cov(data=data, cov=covariate)
        return dist.fit(return_loglike=True)
    
    def apply_fit_to_group(da_group):
        """Apply the fitting function to a group of data."""
        t = da_group.time.dt.year
        cov = df_cov.loc[t.values].values.squeeze()
        
        # Define the output sizes and dimensions
        output_sizes = {'params': 4 + 2} if shift_sigma else {'params': 3 + 2}
        dask_gufunc_kwargs = {'output_sizes': output_sizes}
    
        # Define the apply_ufunc call
        result, loglike, inv_hess = xr.apply_ufunc(
            fit_normal_dist,
            da_group,
            cov,
            shift_sigma,
            input_core_dims=[['time'], ['time'], []],
            output_core_dims=[['params'], [], []],  # Adjust dimensions for each output - FIX THIS !!
            vectorize=True,
            dask='parallelized', 
            output_dtypes=[float, float, object],  # Set dtype for each output
            dask_gufunc_kwargs=dask_gufunc_kwargs
        )

        #xr.concat([da_params[0], da_params[1], da_params[2]], dim='params')
    
        return xr.concat([result, loglike, inv_hess], dim='params')
    
    print('test: dask parallelized')
    
    if by_month:
        results = []
        for month, group in da.groupby("time.month"):
            fit_result = apply_fit_to_group(group)
            fit_result = fit_result.expand_dims("month").assign_coords(month=("month", [month]))
            results.append(fit_result)
        
        result = xr.concat(results, dim="month")
    else:
        result = apply_fit_to_group(da)

    # Rename params coordinates
    if shift_sigma:
        result['params'] = ["b0", "b1", "sigma_b0", "sigma_b1", "loglike","inv_hess"]
    else:
        result['params'] = ["b0", "b1", "sigma", "loglike", "inv_hess"]

    result = result.rename('fit_params')
    
    return result







def norm_shift_fit_boot(da, df_cov, shift_sigma=False, by_month=False, bootsize=3, alpha=0.05, seed=0, incl_mle=True):
    """ Apply normal distribution with shift fit from dist_cov on xarray DataArray with bootstrap.
    Outputs confidence intervals and median if alpha is a float (0-1).
    Outputs full bootstrap sample if alpha = None
    
    Inputs
    
    da :          DataArray
    df_cov :      df, covariate for fit 
    shift_sigma:  Bool, if False loc changes scale is fixed, if True also scale changes
    by_month :    Bool, False one model for full year, True one model per month
    bootsize:     int, number of bootstrap sample
    alpha :       0-1 float, confidence level to calculate CI of parameters
    seed :        for np.random.seed
    incl_mle :    Bool, if alpha is specified will return best estimate from MLE
    
    Returns
    
    result : DataArray with dims 
                            lon, lat
                            params (b0,b1,sigma or sigma_b0,sigma_b1)
                            month (optional)
                            boot (full sample) or quantile (calculated from boot)
    
    """
    
    print('test: boot not parallelized')
    
    def boot(
    da
    ):
        """ Resample with remplacement, note: resamples YEARS.
        """

        years = np.unique(da.time.dt.year)
        years_smp = np.sort(np.random.choice(years, len(years)))
        
        # create new time index based on randomly sampled years
        times = []
        for year in years_smp:
            times.extend(da.time.sel(time=da.time.dt.year == year).values)
        times = np.array(times)
        
        # Reindex da based on the new time index
        da_boot = da.reindex(time=times)
        return da_boot
    
    if incl_mle:
        # provide MLE best estimate
        params_mle = norm_shift_fit(da, df_cov, shift_sigma=shift_sigma, by_month=by_month)
        result = params_mle

    
    if bootsize > 1: 
        # set a seed for reproducibility 
        np.random.seed(seed)
        results = []

        for i in range(bootsize):

            # bootstrap, resample with replacement 
            da_boot = boot(da)
            # apply shift fit function 
            params = norm_shift_fit(da_boot, df_cov, shift_sigma=shift_sigma, by_month=by_month)
            # concat along new dimension 'boot'
            params = params.expand_dims("boot").assign_coords(boot=("boot", [i]))
            results.append(params)

        result = xr.concat(results, dim="boot")

        # if alpha is specified calculate confidence interval, if alpha=None will return full sample
        if alpha:
            result = result.chunk({"boot": -1}).quantile([alpha/2,0.5,1-(alpha/2)], dim='boot')
            
            if incl_mle: 
                result = xr.concat( [ params_mle.expand_dims("quantile").assign_coords(quantile=("quantile", ['mle'])),
                                      result],
                                      dim='quantile')


            
    return result






















"""
 ---------------------------------------------------------------
 PART 3. For exposure analysis: open multi-model data nAHD, deltaI, PR 
 ---------------------------------------------------------------
"""






def open_all_nAHD(GCMs, metric, outdirname,year_pres=None, temp_target=None):
    
    method=None
    
    if '99' in metric:
        p0=0.01
    elif '95' in metric:
        p0=0.05
    elif '90' in metric:
        p0=0.1
    else:
        method='fixed_threshold' # check this ok and i dont add any new percentile values 
        
    if 'CanESM5' in GCMs:
        models='ISIMIP3b'
    elif 'GSWP3-W5E5' in GCMs:
        models='ISIMIP3a'
    
    da_master = None
    
    for GCM in GCMs:
        
        # open p0 
        if method=='fixed_threshold': 
            try:
                filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                            outdirname=outdirname,models=models), 
                                    f'*{metric}_pre-industrial_returnperiod*'))[0]
            except:
                print('p0 file not found')
            
            p0 = 1 - xr.open_dataarray(filepath,  decode_times=False)

        if year_pres:
            filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                            outdirname=outdirname,models=models), 
                                    f'*_{metric}_single-year*returnperiod_{year_pres}*'))[0]
        elif temp_target:
            filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                            outdirname=outdirname,models=models), 
                                    f'*_{metric}_single-year*windowcrossed*returnperiod_{temp_target}.nc'))[0]

        p1 = 1 - xr.open_dataarray(filepath,  decode_times=False)
        
        # calc nAHD 
        da_nAHD = 365 * (p1 - p0)
        da_nAHD.name = 'number of additional days'
        da_nAHD = da_nAHD.assign_coords(model=GCM)
        
        # concat for all GCMs
        if da_master is None:
            da_master = da_nAHD.copy()
        else:
            da_master = xr.concat([da_master, da_nAHD], dim='model')
        
    return da_master


def open_all_deltaI(GCMs, metric, outdirname='output_empirical',models=flags['models'],year_pres=2023): 
    

    da_master = None
    
    for GCM in GCMs:
        filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                        outdirname=outdirname,models=models), 
                                        f'*{metric}_*{start_pi}_{end_pi}.nc'))[0] 
        #print(filepath)
        I0 = xr.open_dataarray(filepath,  decode_times=False)
        
        filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                        outdirname=outdirname,models=models), 
                                        f'*{metric}_*returnlevel_{year_pres}.nc'))[0] 

        I1 = xr.open_dataarray(filepath,  decode_times=False)

        # da_deltaI
        da = I1 - I0 
        da.name = 'change in intensity'
        da = da.assign_coords(model=GCM)

        # concat for all GCMs
        if da_master is None:
            da_master = da.copy()
        else:
            da_master = xr.concat([da_master, da], dim='model')
    
    return da_master


def open_all_PR(GCMs, metric, outdirname, models=flags['models'],year_pres=2022): 
    
    if '99' in metric:
        p0=0.01
    if '95' in metric:
        p0=0.05
    if '90' in metric:
        p0=0.1
    
    da_master = None
    
    for GCM in GCMs:

        filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                            outdirname=outdirname,models=models), 
                                    f'*{metric}*_returnperiod_{year_pres}.nc'))[0]
            
        
        p1 = 1 - xr.open_dataarray(filepath,  decode_times=True) # check time decoding issues 

        da = p1 / p0
        da.name = 'probability ratio'
        da = da.assign_coords(model=GCM)
        try:
            year = xr.open_dataarray(filepath).attrs['target_year']
            da = da.assign_coords(time=year)
        except:
            pass
        
        # concat for all GCMs
        if da_master is None:
            da_master = da.copy()
        else:
            da_master = xr.concat([da_master, da], dim='model')
    
    return da_master 


# could merge all 3 fxns ! 
# eg : def calc_climate_signal_all(GCMs, metric, outdirname, calc='nAHD')
# remove from my main functions the outputs i dont need (e.g. nAHD and PR already computed
# add target year / warming level in attributes so I can track this 


def open_all_TX_preindustrial(GCMs, metric, outdirname, models=flags['models']):
    # TODO: rename this! 
    
    da_master = None
    
    for GCM in GCMs:

        filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                        outdirname=outdirname,models=models), 
                                        f'*{metric}_*pre-industrial_{start_pi}_{end_pi}.nc'))[0] 
                    
        
        I0 = xr.open_dataarray(filepath,  decode_times=False)

        # da_deltaI
        da = I0 
        da.name = f'{metric} pre-industrial'
        da = da.assign_coords(model=GCM)
         
        # concat for all GCMs
        if da_master is None:
            da_master = da.copy()
        else:
            da_master = xr.concat([da_master, da], dim='model')
            
        # add attribute 'metric' 
    
    return da_master




def open_all_p0_p1(GCMs, metric, outdirname,year_pres=None, temp_target=None):
    
    if '99' in metric:
        p0=0.01
    elif '95' in metric:
        p0=0.05
    elif '90' in metric:
        p0=0.1
    else:
        method='fixed_threshold' # check this ok and i dont add any new percentile values 
    
    da_p0 = None
    da_p1 = None
    
    if 'CanESM5' in GCMs:
        models='ISIMIP3b'
    elif 'GSWP3-W5E5' in GCMs:
        models='ISIMIP3a'
    
    for GCM in GCMs:
        
        # open p0 
        if method=='fixed_threshold': 
            try:
                filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                            outdirname=outdirname, models=models), 
                                    f'*{metric}_pre-industrial_returnperiod*'))[0]
            except:
                print('p0 file not found')
            
            p0 = 1 - xr.open_dataarray(filepath,  decode_times=False)
            p0.name = 'p0'
            p0 = p0.assign_coords(model=GCM)
            
            # concat for all GCMs
            if da_p0 is None:
                da_p0 = p0.copy()
            else:
                da_p0 = xr.concat([da_p0, p0], dim='model')
        
        # float value if p0 fixed by def 
        else:
            da_p0 = p0 
        

        if year_pres:
            filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                            outdirname=outdirname,models=models), 
                                    f'*_{metric}_single-year*returnperiod_{year_pres}*'))[0]
        elif temp_target:
            filepath = glob.glob(os.path.join(get_outdir(GCM,metric=metric, 
                                            outdirname=outdirname,models=models), 
                                    f'*_{metric}_single-year*windowcrossed*returnperiod_{temp_target}*'))[0]

        p1 = 1 - xr.open_dataarray(filepath,  decode_times=False)
        p1.name = 'p1'
        p1 = p1.assign_coords(model=GCM)
        
        
        # concat for all GCMs
        if da_p1 is None:
            da_p1 = p1.copy()
        else:
            da_p1 = xr.concat([da_p1, p1], dim='model')
    
    
    return da_p0, da_p1 


def open_all_wbgt_summary(GCMs,
                            metric,
                            open_what,
                            outdirname,
                            experiment=None,
                            period='present',
                            observations=True,
                            nyrs=10):
    
    if '99' in metric:
        p0=0.01
    elif '95' in metric:
        p0=0.05
    elif '90' in metric:
        p0=0.1

        
    da = None
    da_master = None
    
    
    if observations==True:
        if period=='present':
            ext=f'{2019-nyrs+1}_{2019}_{open_what}'
        elif period=='pre-industrial':
            ext=f'{1901}_{1901+nyrs-1}_{open_what}'
            
    if observations==False:
        ext=f'{2019}_{nyrs}yr_{open_what}'
        
    
    for GCM in GCMs:
        
        if experiment is not None:
             filepath=glob.glob(os.path.join('/scratch/brussel/vo/000/bvo00012/vsc10419/attr-hw/output/',
                                        outdirname, f'WBGT/ISIMIP*/{experiment}/preprocessed/',GCM,
                                        f'*_{ext}*'))[0]
        else:
            filepath=glob.glob(os.path.join('/scratch/brussel/vo/000/bvo00012/vsc10419/attr-hw/output/',
                                        outdirname, 'WBGT/ISIMIP*/preprocessed/',GCM,
                                        f'*_{ext}*'))[0]

        try:
            da = xr.open_dataset(filepath)['wbgt']
        except:
            da = xr.open_dataset(filepath)['WBGT']
        da.name = open_what
        da = da.assign_coords(model=GCM).drop_vars('time')
        
        # concat for all GCMs/datasets
        if da_master is None:
            da_master = da.copy()
        else:
            da_master = xr.concat([da_master, da], dim='model')
    
    da_master = da_master #.drop_vars('time')
            
    
    return da_master
        

              
"""
 ---------------------------------------------------------------
 PART 4. Reanalysis 
 ---------------------------------------------------------------
"""                       
                        
def calc_nAHD_shift_fit(da_params, threshold, gmst_smo,year_pres=2023,GWI=1.3):

    from scipy.stats import norm

    if isinstance(gmst_smo, xr.DataArray):
        gmst_pres = gmst_smo.loc[year_pres]
        gmst_pi = gmst_pres - GWI
    else:
        gmst_pres = float(gmst_smo.loc[year_pres].iloc[0])  
        gmst_pi = float(gmst_pres - GWI)
        
    b0 = da_params.sel(params='b0')
    b1 = da_params.sel(params='b1')


    if len(da_params.params) >3:
        sigma_b0 = da_params.sel(params='sigma_b0')
        sigma_b1 = da_params.sel(params='sigma_b1')
        norm_pi, norm_pres = norm(loc=b0+b1*gmst_pi, scale=sigma_b0+sigma_b1*gmst_pi), norm(loc=b0+b1*gmst_pres, scale=sigma_b0+sigma_b1*gmst_pres)
    elif len(da_params.params) ==3:
        sigma_b0 = da_params.sel(params='sigma')
        norm_pi, norm_pres = norm(loc=b0+b1*gmst_pi, scale=sigma_b0), norm(loc=b0+b1*gmst_pres, scale=sigma_b0)

    data = norm_pres.sf(threshold)
    da_p1 = xr.DataArray(
        data=data,
        dims=["dataset", "month", "lat", "lon", ],
        coords=dict(
            lon=(["lon"], da_params.lon.data),
            lat=(["lat"], da_params.lat.data),
            month=da_params.month.data,
            dataset=da_params.dataset.data) )
    
    data = norm_pi.sf(threshold)
    da_p0 = xr.DataArray(
        data=data,
        dims=[ "dataset", "month", "lat", "lon"],
        coords=dict(
            lon=(["lon"], da_params.lon.data),
            lat=(["lat"], da_params.lat.data),
            month=da_params.month.data,
            dataset=da_params.dataset.data) )

    days_in_month = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]) # ignoring leap years

    # add dimension for correct multiplication
    days_in_month_da = xr.DataArray(days_in_month, dims=['month'], coords={'month': da_p1['month']})

    # calc nAHD per month and per year
    da_nAHD_mo = (da_p1 - da_p0) * days_in_month_da
    
    da_nAHD = da_nAHD_mo.sum(dim='month')


    return da_nAHD, da_nAHD_mo, da_p0, da_p1                        
    
                
                        
                        
                        
                        
"""
 ---------------------------------------------------------------
 PART 4. Exposure: Climate + Demographics
 ---------------------------------------------------------------
"""

def calc_number_proportion_people_atleastxdays_10yr(gs_population, 
                                                    GCMs, 
                                                    da_nAHD,  
                                                    age_ranges = np.arange(0, 105, 10), #0-9 ... 90-99
                                                    x_hot_days = [1,5,10,20,50]):
    # calculate number and proportion of people exposed to at least x nAHD
    
    # Group the data by age ranges and calculate the sum
    grouped_data = gs_population.groupby_bins('ages', age_ranges, right=False).sum() # right boundary excluded i.e. [0,9)
    
    # initiate empty dataarray 
    da_master = None
    
    for GCM in GCMs:
        # initiate empty dataframe
        columns = {f'n_atleast_{x}': np.nan for x in x_hot_days}
        columns.update({f'prop_atleast_{x}': np.nan for x in x_hot_days})
        df_out = pd.DataFrame(index=age_ranges[:-1], columns=columns)
        
        # open da_nAHD
        da_nAHD_mod = da_nAHD.sel(model=GCM)
        
        # calculate - loop over each age bin and each threshold 
        for i in range(len(grouped_data.ages_bins)):
            for x in x_hot_days:

                # number of people living through at least x heatwaves 
                n_people_at_least_x = grouped_data.isel(ages_bins=i).where(da_nAHD_mod>=x).sum().values
                df_out.loc[age_ranges[i],f'n_atleast_{x}'] = float(n_people_at_least_x)

                # proportion of people living through at least x heatwaves
                prop_at_least_x = n_people_at_least_x / grouped_data.isel(ages_bins=i).sum().values
                df_out.loc[age_ranges[i],f'prop_atleast_{x}'] = prop_at_least_x
        
        # convert to dataarray
        da = xr.DataArray(df_out, dims = ('age_ranges', 'features')).assign_coords(model=GCM) 
        
        # concat for all GCMs
        if da_master is None:
            da_master = da.copy()
        else:
            da_master = xr.concat([da_master, da], dim='model')
    
    da_n_people = da_master.isel(features=slice(0, len(da_master.features) //2))
    da_n_people.name = 'number of people'
    da_prop_people = da_master.isel(features=slice(len(da_master.features) //2, len(da_master.features))) 
    da_prop_people.name = 'proportion'
        
    return da_n_people, da_prop_people









def calc_number_proportion_people_atleastdeltaI_10yr(gs_population, 
                                                     GCMs, 
                                                     da_deltaI,  
                                                     thresholds = [0,0.5,1,1.5,3]):
    # see previous version in -exposure (deltaI)
    
    # Group the data by age ranges and calculate the sum
    age_ranges = np.arange(0, 105, 10) #0-9 ... 90-99
    grouped_data = gs_population.groupby_bins('ages', age_ranges, right=False).sum() # right boundary excluded i.e. [0,9)
    
    # initiate empty dataarray 
    da_master = None
    
    for GCM in GCMs:
        # initiate empty dataframe
        columns = {f'n_atleast_{x}': np.nan for x in thresholds}
        columns.update({f'prop_atleast_{x}': np.nan for x in thresholds})
        df_out = pd.DataFrame(index=age_ranges[:-1], columns=columns)
        
        # open da_model
        da_deltaI_mod = da_deltaI.sel(model=GCM)
        
        # calculate - loop over each age bin and each threshold 
        for i in range(len(grouped_data.ages_bins)):
            for x in thresholds:

                # number of people living through at least x heatwaves 
                n_people_at_least_x = grouped_data.isel(ages_bins=i).where(da_deltaI_mod>=x).sum().values
                df_out.loc[age_ranges[i],f'n_atleast_{x}'] = float(n_people_at_least_x)

                # proportion of people living through at least x heatwaves
                prop_at_least_x = n_people_at_least_x / grouped_data.isel(ages_bins=i).sum().values
                df_out.loc[age_ranges[i],f'prop_atleast_{x}'] = prop_at_least_x
        
        # convert to dataarray
        da = xr.DataArray(df_out, dims = ('age_ranges', 'features')).assign_coords(model=GCM) # rename features to something else??
        
        # concat for all GCMs
        if da_master is None:
            da_master = da.copy()
        else:
            da_master = xr.concat([da_master, da], dim='model')
    
    da_n_people = da_master.isel(features=slice(0, len(da_master.features) //2))
    da_n_people.name = 'number of people'
    da_prop_people = da_master.isel(features=slice(len(da_master.features) //2, len(da_master.features))) 
    da_prop_people.name = 'proportion'
        
    return da_n_people, da_prop_people


# per year or specified group size e.g. 0-17 years
def calc_number_proportion_people_atleastxdays_1yr(gs_population, 
                                                   GCMs, 
                                                   da_nAHD_all,
                                                   x_hot_days = [1, 5, 10, 20, 50], 
                                                   ages_values=range(0,100),
                                                   grouped = False,
                                                   size_win = 18
                                                   ):
    ''' calculate for each age year if grouped = False. if grouped = True then you have to specify size win (standard 18) and it will group data for 0-17... 
    - make sure to edit indexing if you change columns created in this funciton (e.g. get rid of n whrincr and prop whr incr which i think are kind of useless?)
    '''

    # initiate empty dataarray 
    da_master = None
    
    for GCM in GCMs:
        
        df_out = pd.DataFrame(index=ages_values)

        for x in x_hot_days:
            df_out[f'n_atleast_{x}'] = np.nan
        df_out['n_whrincr'] = np.nan 
        for x in x_hot_days:
            df_out[f'prop_atleast_{x}'] = np.nan
        df_out['prop_whrincr'] = np.nan
        df_out['n_people'] = np.nan
        
        da_nAHD = da_nAHD_all.sel(model=GCM) # select one from the multi-model da_nAHD

        for i in range(len(df_out)):

            for x in x_hot_days:

                # number of people living through at least 1 heatwave 
                n_people_at_least_x = gs_population.isel(ages=i).where(da_nAHD>=x).sum().values
                df_out.loc[i,f'n_atleast_{x}'] = n_people_at_least_x

                # proportion of people living through at least 1 heatwave
                prop_at_least_x = n_people_at_least_x / gs_population.isel(ages=i).sum().values
                df_out.loc[i,f'prop_atleast_{x}'] = prop_at_least_x

            #greater than zero
            n_whereincr = gs_population.isel(ages=i).where(da_nAHD>0).sum().values
            prop_whereincr = n_whereincr / gs_population.isel(ages=i).sum().values
            df_out.loc[i,f'n_whrincr'] = n_whereincr
            df_out.loc[i,f'prop_whrincr'] = prop_whereincr # these are dropped if you group

            # total people of each age 
            n_people = gs_population.isel(ages=i).sum().values
            df_out.loc[i,f'n_people'] = n_people
            
            # convert to dataarray
            da = xr.DataArray(df_out, dims = ('age_ranges', 'features')).assign_coords(model=GCM)
            
        # if you want to group to certain age brackets e.g. 0-17 versus others 
        if grouped == True:

            # group number of people by doing a sum 
            age_ranges = [i * size_win for i in range(df_out.index[-1] // size_win + 1)]
            df_out_grouped_n = df_out.iloc[:,:len(x_hot_days)].groupby(by= ((df_out.index // size_win) + 1)).sum() # sum only good for n not for prop !!
            df_out_grouped_n.index = age_ranges

            # group proportion of people by doing a weighted sum 
            age_ranges = [i * size_win for i in range(df_out.index[-1] // size_win + 1)]
            df_out_weighted = df_out.iloc[:,len(x_hot_days)+1:-2 # proportion of people - check this indexing in case you get rid of additional columns !!
                                         ].multiply(df_out['n_people'], axis = 0) # proportion x number of people of each age group
            sum_proportions_times_people = df_out_weighted.groupby(by= ((df_out_weighted.index // size_win) + 1)).sum()
            sum_people_bracket = df_out['n_people'].groupby(by= ((df_out.index // size_win) + 1)).sum()
            df_out_grouped_prop = sum_proportions_times_people.divide(sum_people_bracket, axis = 0)
            df_out_grouped_prop.index = age_ranges

            df_out_grouped = df_out_grouped_n.join(df_out_grouped_prop)

            # convert to dataarray
            da = xr.DataArray(df_out_grouped, dims = ('age_ranges', 'features')).assign_coords(model=GCM)
            

        # concat for all GCMs
        if da_master is None:
            da_master = da.copy()
        else:
            da_master = xr.concat([da_master, da], dim='model')

    da_n_people = da_master.isel(features=slice(0, len(da_master.features)//2))
    da_n_people.name = 'number of people'
    da_prop_people = da_master.isel(features=slice(len(da_master.features) //2, len(da_master.features))) 
    da_prop_people.name = 'proportion'


    return da_n_people, da_prop_people



def calc_percapita_hotdays_peopledays_1yr(gs_population, 
                                           GCMs, 
                                           da_nAHD_all,
                                           x_hot_days = [1, 5, 10, 20, 50], 
                                           ages_values=range(0,100),
                                           grouped = True,
                                           size_win = 10,
                                          mask_where_decr=False #should be called mask less than zero
                                           ):
    ''' calculate for each age year if grouped = False. if grouped = True then you have to specify size win (standard 18) and it will group data for 0-17... 
    - make sure to edit indexing if you change columns created in this funciton (e.g. get rid of n whrincr and prop whr incr which i think are kind of useless?)
    '''

    # initiate empty dataarray 
    da_master = None
    
    for GCM in GCMs:
        
        df_out = pd.DataFrame(index=ages_values)

        df_out['per_capita_days'] = np.nan
        df_out['people_days'] = np.nan
        
        da_nAHD = da_nAHD_all.sel(model=GCM) # select one from the multi-model da_nAHD

        for i in range(len(df_out)):
            
            #TODO: make flag without masking where da_nAHD>0 - CHECK THIS !!!
            
            if mask_where_decr == True: # only places where there is an increase, not where decrease, test sensitivity to this! 
                # people times days
                people_times_hotdays = (gs_population.isel(ages=i).where(da_nAHD>0) * da_nAHD).sum().values
                df_out.loc[i,f'people_days'] = people_times_hotdays

                # global avg days per capita 
                per_capita_days = people_times_hotdays / gs_population.isel(ages=i).where(da_nAHD>0).sum().values
                df_out.loc[i,f'per_capita_days'] = per_capita_days
            
            elif mask_where_decr == False:
                # people times days
                people_times_hotdays = (gs_population.isel(ages=i) * da_nAHD).sum().values
                df_out.loc[i,f'people_days'] = people_times_hotdays

                # global avg days per capita 
                per_capita_days = people_times_hotdays / gs_population.isel(ages=i).sum().values
                df_out.loc[i,f'per_capita_days'] = per_capita_days
            
            # total people of each age 
            n_people = gs_population.isel(ages=i).sum().values
            df_out.loc[i,f'n_people'] = n_people

    
            # convert to dataarray
            da = xr.DataArray(df_out, dims = ('age_ranges', 'features')).assign_coords(model=GCM)
            
        # if you want to group to certain age brackets e.g. 0-17 versus others 
        if grouped == True:

            # group people x days by doing a sum 
            age_ranges = [i * size_win for i in range(df_out.index[-1] // size_win + 1)]
            df_out_grouped_n = df_out['people_days'].groupby(by= ((df_out.index // size_win) + 1)).sum() 
            df_out_grouped_n.index = age_ranges

            # group per capita days by doing a weighted sum 
            age_ranges = [i * size_win for i in range(df_out.index[-1] // size_win + 1)]
            df_out_weighted = df_out['per_capita_days'].multiply(df_out['n_people'], axis = 0) # weighted average: per capita x number of people of each age group
            sum_percapita_times_people = df_out_weighted.groupby(by= ((df_out_weighted.index // size_win) + 1)).sum()
            sum_people_bracket = df_out['n_people'].groupby(by= ((df_out.index // size_win) + 1)).sum() # could add this as a column 
            df_out_grouped_pc = sum_percapita_times_people.divide(sum_people_bracket, axis = 0)
            df_out_grouped_pc.index = age_ranges
            df_out_grouped_pc.name = 'per_capita_days'

            df_out_grouped = pd.concat([df_out_grouped_n, df_out_grouped_pc], axis=1)

            # convert to dataarray
            da = xr.DataArray(df_out_grouped, dims = ('age_ranges', 'features')).assign_coords(model=GCM)
            
            # could add n_people as a column to grouped df ! 

        # concat for all GCMs
        if da_master is None:
            da_master = da.copy()
        else:
            da_master = xr.concat([da_master, da], dim='model')

    return da_master



def calc_averagedeltaI_peragegroup(gs_population, 
                                   GCMs, 
                                   da_deltaI_all, 
                                   ages_values=range(0,100),
                                   grouped = True,
                                   size_win = 10
                                   ):
    # initiate empty dataarray 
    da_master = None

    for GCM in GCMs:

        da_deltaI = da_deltaI_all.sel(model=GCM)

        # initiate empty data array
        df_out = pd.DataFrame(index=ages_values)
        df_out['avg_deltaI'] = np.nan
        df_out['n_people'] = np.nan

        for i in range(len(df_out)):

            # population weighted average of deltaI, by cohort weight
            pop_weighted_avg = (da_deltaI * gs_population.isel(ages=i) ).sum(
                dim=('lat','lon')) / gs_population.isel(ages=i).sum(dim=('lat','lon'))

            df_out.loc[i,f'avg_deltaI'] = pop_weighted_avg

            # total people of each age 
            n_people = gs_population.isel(ages=i).sum().values
            df_out.loc[i,f'n_people'] = n_people

            # convert to dataarray
            if grouped == False:
                da = xr.DataArray(df_out, dims = ('age_ranges', 'features')).assign_coords(model=GCM)

        if grouped == True:
            # group by doing a weighted sum - maybe there's too much pre-aggregation and I should do this at gridscale not in this fxn on dataframe... 
            age_ranges = [i * size_win for i in range(df_out.index[-1] // size_win + 1)]
            df_out_weighted = df_out['avg_deltaI'].multiply(df_out['n_people'], axis = 0) # weighted average: per capita x number of people of each age group
            sum_percapita_times_people = df_out_weighted.groupby(by= ((df_out_weighted.index // size_win) + 1)).sum()
            sum_people_bracket = df_out['n_people'].groupby(by= ((df_out.index // size_win) + 1)).sum() # could add this as a column !!!
            data_grouped = sum_percapita_times_people.divide(sum_people_bracket, axis = 0).rename('avg_deltaI')
            df_out_grouped = pd.concat([data_grouped, sum_people_bracket], axis=1 ) #.reindex(age_ranges)
            df_out_grouped.index = age_ranges
            
            # convert to dataarray
            da = xr.DataArray(df_out_grouped, dims = ('age_ranges', 'features')).assign_coords(model=GCM)


            # concat for all GCMs
            if da_master is None:
                da_master = da.copy()
            else:
                da_master = xr.concat([da_master, da], dim='model')

    return da_master





