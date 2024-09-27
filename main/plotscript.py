
import numpy as np
import pandas as pd
import os, glob, re 
import math
import xarray as xr
import geopandas as gpd
import regionmask as regionmask
import dask
import matplotlib.pyplot as plt
import netCDF4
import cartopy
import cartopy.crs as ccrs
import matplotlib.colors as colors
from scipy.ndimage import gaussian_filter


# ---------------------------------------------------------------
# PART 1. Climate signal
# ---------------------------------------------------------------


def get_mask():
    landmask = xr.open_dataarray('/data/brussel/vo/000/bvo00012/data/dataset/ISIMIP/ISIMIP3b/InputData/geo_conditions/landseamask/landseamask_no-ant.nc', decode_times=False)[0,:,:].drop_vars('time') # move this path to settings?
    return landmask 


# include other fxns ! 





# ---------------------------------------------------------------
# PART 2. Climate + Demographics
# ---------------------------------------------------------------

#update to new versions !!! + add extra stuff

def plot_barplot_n_prop_people_atleastx_modelmean(da, 
                                                  unit, 
                                                  year, 
                                                  ax, 
                                                  proportion=False, 
                                                  x_hot_days = [1,5,10,20,50],
                                                 legend=True,
                                                 errcolor='gray'):
    # plot range 
    if unit == 1e9:
        unit_str = 'billion'
    elif unit == 1e6:
        unit_str='million'
    else:
        unit_str = ''

    upper_error = (da.max(dim='model')-da.mean(dim='model')).values.T /unit
    lower_error = (da.mean(dim='model')-da.min(dim='model')).values.T /unit
    try:
        concatenated =np.concatenate((lower_error,upper_error), axis=1)
        reshaped = np.reshape(concatenated, (len(da.features), 2, len(da.age_ranges)))
    except:
        concatenated =np.concatenate((lower_error,upper_error))
        reshaped = np.reshape(concatenated, (2, len(da.age_ranges)))

    # plot multi-model mean 
    (da.mean(dim='model')/unit).to_pandas().plot.bar(ax=ax,
                                                   yerr=reshaped,
                                                   error_kw=dict(ecolor=errcolor, alpha=1, elinewidth=.5, capsize=1))
                          
    if proportion ==False:
        ax.set_ylabel(f'number of people ({unit_str})')
        ax.set_title(f'number of people experiencing at least n attributable hot days in {year}')
    else:
        ax.set_ylabel(f'proportion of age group')
        ax.set_title(f'proportion of age group experiencing at least n attributable hot days in {year}')
    
    ax.set_xlabel('ages')
    step_ages = int(da.age_ranges[1] -da.age_ranges[0] - 1)
    ax.set_xticks(ticks=ax.get_xticks(), labels=[f'{x}-{x+step_ages}' for x in da.age_ranges],  rotation='horizontal')
    if legend==True:
        ax.legend(labels = [f'at least {x}' for x in x_hot_days])
        
    
    
    
def plot_barplot_n_prop_people_atleastx_modelmean(da, 
                                                  unit, 
                                                  year, 
                                                  ax, 
                                                  proportion=False, 
                                                  x_hot_days = [1,5,10,20,50]):
    # plot range 
    if unit == 1e9:
        unit_str = 'billion'
    elif unit == 1e6:
        unit_str='million'
    else:
        unit_str = ''

    upper_error = (da.max(dim='model')-da.mean(dim='model')).values.T /unit
    lower_error = (da.mean(dim='model')-da.min(dim='model')).values.T /unit
    concatenated =np.concatenate((lower_error,upper_error), axis=1)
    reshaped = np.reshape(concatenated, (len(da.features), 2, len(da.age_ranges)))

    # plot multi-model mean 
    (da.mean(dim='model')/unit).to_pandas().plot.bar(ax=ax,
                                                   yerr=reshaped,
                                                   error_kw=dict(ecolor='gray', alpha=1, elinewidth=.5, capsize=1))
                          
    if proportion ==False:
        ax.set_ylabel(f'number of people ({unit_str})')
    else:
        ax.set_ylabel(f'proportion of age group')
    
    ax.set_xlabel('ages')
    step_ages = int(da.age_ranges[1] -da.age_ranges[0] - 1)
    ax.set_xticks(ticks=ax.get_xticks(), labels=[f'{x}-{x+step_ages}' for x in da.age_ranges],  rotation='horizontal')
    ax.legend(labels = [f'at least {x}' for x in x_hot_days])




