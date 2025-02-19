
import numpy as np
import xarray as xr

import pandas as pd
import datetime as dt
from scipy import interpolate
import os, glob, re, sys
import time
from datetime import datetime
import matplotlib.colors as colors
import netCDF4
import dask
from functools import partial
import statsmodels.api as sm

# import my global variables
from settings import *



# ---------------------------------------------------------------
# Utils - opening files etc
# ---------------------------------------------------------------

def make_outdir(GCM, scratchdir=False, makedirs=True, outdirname=None, experiment=None, ver=ver):
    """ 
    set directory names of output directory when submitting jobs
    ver = JOBID with job submission
    standard name if unspecified will be in format e.g. 'output_jul23-JOBID' or 'output_ju23' if not a job submission
    """
    
    
    if '-f' in ver: # not job submit 
        ver = ''
    else:
        ver = f'-{ver}'
    if outdirname is None:
        outdirname = 'output_' + datetime.now().strftime("%b").lower() +  datetime.now().strftime("%-y") + '{}'.format(ver)

    if scratchdir is False:
        outdir = os.path.join(outdirs, outdirname, flags['metric'], flags['models'], GCM ) # e.g. TX99/ISIMIP3b/CanESM5
    else:
        outdir = os.path.join(scratchdirs, outdirname, flags['metric'], flags['models'], experiment, GCM ) 
        
    
    if not os.path.exists(outdir):
        # If it doesn't exist, create it
        if makedirs == True:
            os.makedirs(outdir)
            print('outdir made:', outdir)
    else:
        print ('outdir exists:', outdir)
    
    return outdir



def get_outdir(GCM,metric,outdirname,models=flags['models']): # repetitive from above, can delete this fxn ??
    if outdirname is None:
        outdirname = 'output_' + datetime.now().strftime("%b").lower() +  datetime.now().strftime("%-y") 

    outdir = os.path.join(outdirs, outdirname, metric, models, GCM ) # e.g. TX99/ISIMIP3b/CanESM5
    
    return outdir 



    
    
def get_dirpaths(GCM, scenario1=None, scenario2=None):
    """ get directory paths for each GCM given one or two scenarios so that later you can join the files together
    ---------------------------------------------------------------
    Inputs:
         GCM: (str) model name
         scenario1: (str) first scenario e.g. historical (directory name)
         scenario2: (str opt) second scenario e.g. ssp370 (directory name)
    ---------------------------------------------------------------
    Returns:
        dir1, dir2: (str) directory paths
    """
    
    if scenario1=='obsclim' or scenario1=='counterclim':
        if scenario1=='obsclim':
            indir=indir_obs
            dir1=os.path.join(indir, GCM)
            dir2=None
        elif scenario1=='counterclim':
            indir=indir_counterclim
            dir1=os.path.join(indir, GCM)
            dir2=None
            
    else: # if scenario1=='historical' or scenario1=='hist-nat':
        if GCM in GCMs_s or scenario1 == 'hist-nat': 
            indir = indir_s
            dir1 = os.path.join(indir, scenario1, GCM) # hist-nat goes all the way to 2100
            if scenario2 is not None:
                dir2 = os.path.join(indir, scenario2, GCM)      # secondary input models, join historical and SSP-RCP
            else:
                dir2 = None
        else: 
            indir = indir_p
            dir1 = os.path.join(indir, scenario1, GCM)
            if scenario2 is not None:
                dir2 = os.path.join(indir, scenario2, GCM)     # primary input models, join historical and ssp-rcp scenario
            else:
                dir2 = None

    return dir1,dir2






                
def get_filepaths(var,dir1,dir2=None):
    """ use globbbing to get ordered list of filepaths in GCM directory 
    
    use globbbing to get ordered list of filepaths in GCM directory of the variable you want for both hist and ssp-rcp (dir1 and dir2) 
    or just hist nat (dir1)
    ---------------------------------------------------------------
    TODO: dirpaths are outputted from get_dirpaths, could merge these in a single function 
    ---------------------------------------------------------------
    Inputs:
        var: (str) variable name
        dir1, dir2 (opt): (str) directory paths
    ---------------------------------------------------------------
    Returns:
        filepaths: globbed filepaths
    """
    
    paths1 = sorted(glob.glob(os.path.join(dir1,'*_{}_*.nc'.format(var))), key=str.casefold)
    
    if dir2 is not None:
        paths2 = sorted(glob.glob(os.path.join(dir2,'*_{}_*.nc'.format(var))), key=str.casefold)
        filepaths = paths1 + paths2
        
    else:
        filepaths = paths1
    
    return filepaths



def open_arrays_dask(filepaths, var, startyear, endyear, version, engine='netcdf4'): 
    """ open multiple ncfiles defined by filepaths as a single data array and slice it to the start and endyear you want 
    
    TODO: add another function or flag-dependent thing in this function that crops only over Europe to make calcs go faster 
    """
    
    print(f'chunking version {version}')
    
    if version == 0:

        with xr.open_mfdataset(filepaths, engine=engine, 
                          chunks = {'lat':lat_chunk, 'lon':lon_chunk, 'time':time_chunk}
                          ) as ds:
            da = ds[str(var)].sel(time=slice('{}-01-01'.format(startyear), '{}-12-31'.format(endyear)))

        da = da.chunk({'time': -1})

        dask.config.set({"array.slicing.split_large_chunks": False}) # False gives less tasks
        
        
        
    elif version == 1:
        with xr.open_mfdataset(filepaths, engine=engine, 
                      chunks = 'auto'
                      ) as ds:
            da = ds[str(var)].sel(time=slice('{}-01-01'.format(startyear), '{}-12-31'.format(endyear)))
            da = da.chunk({'time': -1})
        
        
    elif version == 2:
        with xr.open_mfdataset(filepaths, engine=engine, 
                      chunks = None
                      ) as ds:
            da = ds[str(var)].sel(time=slice('{}-01-01'.format(startyear), '{}-12-31'.format(endyear)))
            da = da.chunk({'time': -1})
        
        
    # preprocess partial func could be useful for lat/lon cropping ! 


    return da



def check_length(da, startyear, endyear):
    """ check length of data array is as expected 
    """
    
    if len(da.time) != len(pd.date_range('{}-01-01'.format(startyear), '{}-12-31'.format(endyear))):
        print("error the number of days in your data array ({}) is not equal to what you would expect between year {} and {} ({})".format(
           len(da.time), startyear, endyear, len(pd.date_range('{}-01-01'.format(startyear), '{}-12-31'.format(endyear)) )))
    else:
        print(f"opening model data between {startyear} and {endyear}")
            

            
            

# ---------------------------------------------------------------
# Utils - saving files 
# ---------------------------------------------------------------



def get_filesavename(GCM, scenario1, scenario2, ext, data=None, filepath=None,startyear=None, endyear=None, keep_scenario=False, variable=None):
    """get filename for saving
    
    NEW VERSION, added scenario2 as required arg, could throw errors in old code! 
    ---------------------------------------------------------------
    Inputs:
        filepath: (str) filepath to get basename from
        ext: (str) extension to add
        data: (DataArray) data you are saving 
    Returns:
        filename: (str)

    TODO: clean this fxn!
    """
    
    if variable is None:
        if var is not None:
            variable=var
        elif VARs is not None:
            variable = VARs[0]
        else:
            print('variable not defined')
            
    if filepath is None:
        if variable == 'wbgt':
            dirname='output_jan25' 
            #if scenario2 is None:
            dir1=os.path.join(scratchdirs, dirname, 'WBGT', flags['models'], scenario1, GCM )
            #else:
            #    dir1=os.path.join(scratchdirs, dirname, 'WBGT', flags['models'], scenario1+'-'+scenario2, GCM ) 
            print(dir1)
            filepath=get_filepaths(variable.upper(),dir1)[0] # 'WBGT' not 'wbgt' in filename
        else:
            dir1, dir2 = get_dirpaths(GCM, scenario1, scenario2)
            filepath = get_filepaths(variable,dir1,dir2)[0]

    
    if startyear==None:
        try:
            data.year.values
            if len(data.year.values) != 1:
                startyear=data.year.values[0]
                endyear= data.year.values[-1]
            else:
                startyear = endyear = data.year.values
        except:
            try:
                data.time.dt.year.values
                if data.time.dt.year.values.size!=1:
                    startyear=data.time.dt.year.values[0]
                    endyear=data.time.dt.year.values[-1]
                elif data.time.dt.year.values.size==1:
                    startyear=endyear=data.time.dt.year.values
            except:
                try: 
                    # TODO: work on this I removed the attrs so this might make issues on single-year calculations now... 
                    startyear = data.attrs['start_date']
                    endyear = data.attrs['end_date'] 
                except:
                    try:
                        startyear=data.attrs['target_year']
                        endyear=None
                    except:
                        try:
                            if target_years is not None:
                                startyear=target_years
                                endyear=None 
                            elif target_temperature is not None:
                                startyear=target_temperature
                                endyear=None
                        except:
                            pass
                            
        
    if keep_scenario == False:
        basename = os.path.basename(filepath)
        basename1 = re.match(r'(.*?)_historical_(.*?)_\d{4}_\d{4}', basename).group(1) # get rid of end years
        basename2 = re.match(r'(.*?)_historical_(.*?)_\d{4}_\d{4}', basename).group(2)
        
        if scenario2 is not None:
            scen = f'_{scenario1}_{scenario2}'
        else:
            scen = f'_{scenario1}'
    
        if endyear is not None:
            exts = '_{}_{}_{}'.format(ext, startyear, endyear) # add extension and start and end years
        else:
            exts = '_{}_{}'.format(ext, startyear)
        filename = str(basename1+scen+exts+'.nc')
        
    elif keep_scenario == True:
        basename = os.path.basename(filepath)
        try:
            basename1 = re.match(f'(.*?)_{variable}_(.*?).nc', basename).group(1)
        except:
            basename1 = re.match(f'(.*?)_{variable.upper()}_(.*?).nc', basename).group(1)
        
        if endyear is not None:
            exts = '_{}_{}_{}'.format(ext, startyear, endyear)
        else:
            exts = '_{}_{}'.format(ext, startyear, endyear)
        filename = str(basename1+exts+'.nc')
    
    return filename




def format_large_numbers(x):
    try:
        x = float(x)
        if abs(x) >= 1e9:
            return f'{x/1e9:.1f}B'
        if abs(x) >= 1e6:
            return f'{x/1e6:.1f}M'
        if abs(x) >= 1e3:
            return f'{x/1e3:.1f}K'
        if abs(x) <= 1e2:
            return f'{x:.1f}'
        return str(x)
    except ValueError:
        return x



def calc_weighted_average(df_values, df_weights, start, stop):
    
    ''' stop is excluded'''
    print(f'note: loc {stop} is excluded')
    
    try:
        weighted_avg = df_values.iloc[start:stop].multiply(df_weights[start:stop], axis = 0).sum(axis=0).divide(df_weights[start:stop].sum(), axis=0)
    except:
        weighted_avg = df_values.iloc[start:stop].multiply(df_weights[start:stop], axis = 0).sum(axis=0) / df_weights[start:stop].sum(axis=0)

        
    return weighted_avg



def apply_lowess(y, x=None, ntime=4):
    ''' y should be an xarray DataArray or Df or np array
        x is used when provided, else falling back to indices
        ntime in units of y provides smoothing window size
    '''
    lowess = sm.nonparametric.lowess
    
    # Squeeze (in case there is a dimension of size 1 that we don't need)
    y = y.squeeze()
    
    # Create a copy, plug in smoothed data later
    y_out = y.copy(deep=True)
    
    if x is None:
        x = np.arange(y.size)
    
    if not isinstance(y, np.ndarray):
        y = y.values

    # Apply LOWESS smoothing
    result = lowess(y, x, frac=ntime/y.size)
    
    x_smooth, y_smooth = result[:, 0], result[:, 1]
    
    # Ensure the smoothed result has the same length as the original data
    y_smooth_full = np.interp(x, x_smooth, y_smooth)

    # Assign the smoothed values back to the xarray DataArray
    y_out[:] = y_smooth_full[:]

    return y_out