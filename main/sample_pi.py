import numpy as np
import pandas as pd
import os, glob, re 
import xarray as xr
import dask
import netCDF4
import time
import pickle
from scipy.stats import norm

# My settings and functions
from settings_ana import *
from functions_ana import *
from utils_ana import * 

def sample_from_monthly_pi_distributions(da_params,
                                         gmst_smo,
                                         year_pres=2023,
                                         GWI=1.3,
                                         mc_samplesize=1000,
                                            ):


    gmst_pres = float(gmst_smo.loc[year_pres]) # take smoothed or not smoothed covariate ?? 
    gmst_pi = float(gmst_pres - GWI)
    
    b0 = da_params.sel(params='b0')
    b1 = da_params.sel(params='b1')
    
    # Get params
    if len(da_params.params) > 3:
        sigma_b0 = da_params.sel(params='sigma_b0')
        sigma_b1 = da_params.sel(params='sigma_b1')
        mean_pi = b0 + b1 * gmst_pi
        mean_pres = b0 + b1 * gmst_pres
        std_pi = sigma_b0 + sigma_b1 * gmst_pi
        std_pres = sigma_b0 + sigma_b1 * gmst_pres
    else:
        sigma_b0 = da_params.sel(params='sigma')
        mean_pi = b0 + b1 * gmst_pi
        mean_pres = b0 + b1 * gmst_pres
        std_pi = sigma_b0
        std_pres = sigma_b0
    
    # Number of samples for Monte Carlo
    n_samples = mc_samplesize
    
    # Monte Carlo sampling function, reshaped to match the broadcasted dimensions
    def monte_carlo_samples(mean, std_dev, size):
        """Draw Monte Carlo samples from normal distribution."""
        samples_shape = mean.shape + (size,)  
        return norm.rvs(loc=mean, scale=std_dev, size=samples_shape)
    
    # Apply Monte Carlo sampling across all grid cells
    samples_pi = xr.apply_ufunc(
        monte_carlo_samples, mean_pi, std_pi,
        input_core_dims=[[], []],  
        output_core_dims=[['samples']],  # Output will add a `samples` dimension
        vectorize=True, dask='parallelized', kwargs={'size': n_samples}
    )

    return samples_pi

def calc_nAHD_shift_fit_percentile_from_sample(da_params, 
                                             percentile, 
                                             gmst_smo,
                                               samples_pi,
                                             year_pres=2023,
                                             GWI=1.3,
                                            ):

    gmst_pres = float(gmst_smo.loc[year_pres]) # take smoothed or not smoothed covariate ?? 

    b0 = da_params.sel(params='b0')
    b1 = da_params.sel(params='b1')
    
    # Get params
    if len(da_params.params) > 3:
        sigma_b0 = da_params.sel(params='sigma_b0')
        sigma_b1 = da_params.sel(params='sigma_b1')
        mean_pi = b0 + b1 * gmst_pi
        mean_pres = b0 + b1 * gmst_pres
        std_pi = sigma_b0 + sigma_b1 * gmst_pi
        std_pres = sigma_b0 + sigma_b1 * gmst_pres
    else:
        sigma_b0 = da_params.sel(params='sigma')
        mean_pi = b0 + b1 * gmst_pi
        mean_pres = b0 + b1 * gmst_pres
        std_pi = sigma_b0
        std_pres = sigma_b0

    # theoretical distributions per month in present for later 
    norm_pres = norm(loc=mean_pres, scale=std_pres)
    

    # weights 
    coords = dict(month=("month", np.arange(1,13)))
    days_in_month = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]) # ignoring leap years
    weights = xr.DataArray(days_in_month, dims="month", coords=coords)
    
    # weigh the sample by month length and get qth percentile value 
    da_threshold = samples_pi.weighted(weights).quantile(percentile, dim=['samples','month'])

    # expand dims
    da_threshold = da_threshold.expand_dims('month', axis=1)
    # get p of exceedance of pi 90th percentile threshold in each grid cell 
    data = norm_pres.sf(da_threshold)
    da_p1 = xr.DataArray(
        data=data,
        dims=["dataset", "month", "lat", "lon", ],
        coords=dict(
            lon=(["lon"], da_params.lon.data),
            lat=(["lat"], da_params.lat.data),
            month=da_params.month.data,
            dataset=da_params.dataset.data) )
    
    # calc nAHD per month and per year
    da_nAHD_mo = (da_p1 -  (1 - percentile)) *  weights 
    
    da_nAHD = da_nAHD_mo.sum(dim='month')

    return da_nAHD, da_nAHD_mo, da_threshold, da_p1


def open_params_shiftfit(datasets,
                         sigma=False):
    
    # open shift fit parameters 
    da_list = []
    for i in range(len(datasets)):
        dataset = datasets[i]
        if not sigma:
            filepath = glob.glob(os.path.join(outdirs,f'output_shift-fit/forster2024/WBGT/ISIMIP3a/{dataset}/*_obsclim_WBGT_params_shift_loc_mon_loglike_1901_2019.nc'))[0]
        da = xr.open_dataarray(filepath).expand_dims("dataset").assign_coords(dataset=("dataset", [dataset]))
        da_list.append(da)
        da_params = xr.concat(da_list, dim="dataset")

    return da_params


def get_smoothed_gmst(ntime=4):
    df_gmst = pd.read_csv(os.path.join(datadirs,'gmst/gmst-obs/forster2024/annual_averages.csv')).rename(
    columns={'timebound_lower':'year'}).set_index('year')[['gmst']]
    gmst_smo = pd.DataFrame(apply_lowess(df_gmst, df_gmst.index, ntime=ntime))

    return gmst_smo






# Settings 
flags['models']='ISIMIP3a'
dirname = 'output_shift-fit' 
outDIR=os.path.join(outdirs,f'output_shift-fit/forster2024/WBGT/ISIMIP3a/sample_pi/')




# Run sampling and percentile calculation 
if __name__ == '__main__':
    
    # set up dask client
    from dask.distributed import Client
    client = Client() 

    start_message()

    da_params = open_params_shiftfit(datasets)

    gmst_smo = get_smoothed_gmst()

    sample_pi = sample_from_monthly_pi_distributions(da_params,
                                         gmst_smo,
                                         year_pres=2023,
                                         GWI=1.3,
                                         mc_samplesize=1000,
                                            )

    for percentile in [0.9,0.95,0.99]:
        da_nAHD, da_nAHD_mo, da_threshold, da_p1 = calc_nAHD_shift_fit_percentile_from_sample(da_params, 
                                             percentile, 
                                             gmst_smo,
                                             sample_pi,)

        # da_nAHD.to_netcdf(os.path.join(outDIR, f'nAHD_sample_pi_percentile_{str(percentile)}_shiftfit_loc_1901_2019.nc')
        # da_nAHD_mo.to_netcdf(os.path.join(outDIR, f'nAHD_mo_sample_pi_percentile_{str(percentile)}_shiftfit_loc_1901_2019.nc')
        # da_threshold.to_netcdf(os.path.join(outDIR, f'threshold_sample_pi_percentile_{str(percentile)}_shiftfit_loc_1901_2019.nc')
        # da_p1.to_netcdf(os.path.join(outDIR, f'p1_sample_pi_percentile_{str(percentile)}_shiftfit_loc_1901_2019.nc')                                                                   
        
        pickle.dump(da_nAHD, open(os.path.join(outDIR, f'nAHD_sample_pi_percentile_{str(percentile)}_shiftfit_loc_1901_2019.pkl'), 'wb'))
        pickle.dump(da_nAHD_mo, open(os.path.join(outDIR, f'nAHD_mo_sample_pi_percentile_{str(percentile)}_shiftfit_loc_1901_2019.pkl'), 'wb'))
        pickle.dump(da_threshold, open(os.path.join(outDIR, f'threshold_sample_pi_percentile_{str(percentile)}_shiftfit_loc_1901_2019.pkl'), 'wb'))
        pickle.dump(da_p1, open(os.path.join(outDIR, f'p1_sample_pi_percentile_{str(percentile)}_shiftfit_loc_1901_2019.pkl'), 'wb'))
        


