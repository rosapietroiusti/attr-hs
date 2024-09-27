


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





jobid = os.environ['SLURM_JOB_ID']
taskid = os.environ['SLURM_ARRAY_TASK_ID']

if __name__ == '__main__':

    
    # set up dask client
    from dask.distributed import Client
    client = Client(threads_per_worker=1) # test different n of threads per worker 
    
    print('initiate client')

    # test with one dataset
    for dataset in [datasets[0]]:  # array of jobs: for dataset in [datasets[int(sys.argv[2])]]
        print(datetime.now(), dataset)

        tic = time.time()

        ver = sys.argv[1]

        # make/get path for saving output
        outdir = os.path.join(outdirs, 'output_test' )
        #os.makedirs(outdir)

        # open data
        da = open_model_data(model=dataset, 
                            period='start-end', 
                            scenario1='obsclim', 
                            scenario2=None, 
                            target_year=None, 
                            windowsize=None, 
                            chunk_version=[0,1,2][1], #
                            variable='wbgt',
                            startyear=[1901,1911,1921,1931,1941,1951][int(sys.argv[2])-1],
                            endyear=2019,  
                           ).sel(lat=slice(36,0),lon=slice(60,95)) # small area to test speed ! 
        
        chunk_sizes = data_array.chunks
        print(f'The number of chunks in the Dask array is: {chunk_sizes}')
        chunk_size_elements = np.prod([np.mean(dim_chunks) for dim_chunks in chunk_sizes])
        chunk_size_bytes = chunk_size_elements * data_array.dtype.itemsize
        # Convert bytes to gigabytes (1 GB = 1e9 bytes)
        chunk_size_gb = chunk_size_bytes / 1e9
        print(f'The size of each chunk in the xarray DataArray is: {chunk_size_gb:.6f} GB')

        
        
        
        print(datetime.now(), '1) opened data')

        # open and smooth covariate (GMST)
        df_cov = pd.read_csv(observed_warming_path_annual
                            ).rename(columns={'timebound_lower':'year'}
                            ).set_index('year')[['gmst']]
        df_cov_smo = pd.DataFrame(apply_lowess(df_cov, df_cov.index, ntime=4))

        print(datetime.now(), '2) smoothed cov')

        da_params = norm_shift_fit(da,
                               df_cov_smo, 
                               shift_sigma=True, 
                               by_month=True)

        print(datetime.now(), '3) calcd params')

        da_params.to_netcdf(os.path.join(outdir, f'test_shift-params{jobid}_{taskid}.nc'))

        print(datetime.now(), '4) saved file')



        toc = time.time()

        print(f'{datetime.now()}, done, elapsed time: { (toc-tic) / 60 } minutes. \n')                    
