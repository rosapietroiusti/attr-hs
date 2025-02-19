"""
Attributable heat stress research

rosa.pietroiusti@vub.be
Update Sep 2024 

"""

import os, glob, sys
import numpy

# Testing this instead of having two different scripts (_ana and not) 
if sys.argv[1] == '-f':
    idx_models = 0 
    idx_settings = 0
else:
    idx_models = int(sys.argv[2]) 
    idx_settings = int(sys.argv[3])     
                        # sys.argv[1] : version (jobid or '-f' which then gets saved as _moyr)
                        # sys.argv[2]: indexing GCMs/datasets (in main.py) for job array
                        # sys.argv[3]: indexing time window or other settings (here, in settings.py) for job array


# edit here
# --------------------------------------------------
lat_chunk = 90 #'auto' seems not to be faster
lon_chunk = 120 
time_chunk = -1  

# output and datadirectories 
outdirs = os.path.join(os.environ['VSC_DATA_VO_USER'],'attr-hw','output') 
figdirs = os.path.join(os.environ['VSC_DATA_VO_USER'],'attr-hw','figures')
datadirs = os.path.join(os.environ['VSC_DATA_VO_USER'],'attr-hw','data')
scratchdirs = os.path.join(os.environ['VSC_SCRATCH_VO_USER'],'attr-hw','output') 


flags = {}

flags['models'] = 'ISIMIP3a'
                             # 'ISIMIP3b'
                             # 'ISIMIP3a' 
    

flags['experiment'] = 'obsclim' 
                                # obsclim
                                # historical
                                # ssp370, LATER: counterclim, other ssps ssp126, ssp585 (for WBGT calculate)


flags['metric'] = 'WBGT' #['WBGT28', 'WBGT30', 'WBGT33'][idx_settings]  
                            # submit as job array e.g. ['WBGT90', 'WBGT95', 'WBGT99'][int(sys.argv[3])]
                            # 'WBGT' : to calculate it or run shift_fit on full distribution
                            # TX: to run shift fit 
                            # 'TX90', 'TX95', 'TX99' : to get return levels and periods
                            # 'WBGT90', 'WBGT95', 'WBGT99' 
                            # 'WBGT28', 'WBGT30', 'WBGT33'

                            
flags['method'] = 'shift_fit'   
                            # calculate: to calculate WBGT 
                            # empirical_percentile: calculates return levels and periods with empirical percentiles
                            # fixed_threshold: empirical percentiles but based on a fixed magnitude threshold   - TODO:make this the same flag!! have it automatically recognized based on metric 
                            # shift_fit: fits non-stationary distribution per pixel based on dist_cov (Hauser et al)

flags['time_method']=  None
                            # 'single-year' analysis on just one year or temperature level (target year in obs models are matched to!)
                            # None: if running shift fit of WBGT-calc
                
flags['shift_sigma'] = True
                            # True = one model per month, loc and scale both vary
                            # False = one model per month, only loc varies
                            # None: if not running shift fit 

flags['shift_period'] = [(1901, 2019),(1950, 2019)][idx_settings]
                             # 1901, 2019
                             # 1950, 2019 
                             # None for emp percentiles or calc wbgt 

flags['shift_loglike']=None 
                            # True / False : run the shift fit with this on true !!! for CIs !! TODO: del this, not implemented

            
flags['chunk_version']=0     
                            # 0=specified dask chunks - best on Jup Nb (and for shift fit?)
                            # 1=auto dask chunks
                            # 2=no dask chunks - fastest on HPC for empirical percentiles
                            # all of them unchunk time 
                            # test time they take on shift fit! 
            
target_years = None 
                #[2022,2023][int(sys.argv[3])]  
                # int e.g. 2022, 2023 - for empirical percentiles and fixed magnitude
                # None: to calc WBGT or shift fit or target temp

target_temperature = None
                        #1.5, 2 ... other GW levels would also work 

        
warming_period_method = None
                            # window, centered (for 1.5 warming)
                            # ar6 (for matching to obs years)
                            # None if not matching target temperature
warming_period_match = None
                            # 'closest' : for present-day
                            # 'crossed' : for warming levels
                            # None if not matching target temperature
                            
        
        
        
        

# do not edit below this after development 
# --------------------------------------------------

# set paths 
datadir = os.path.join('..', '..', 'data')

# observed warming decadal averages 10 years leading up to final year (AR6 method) or annual average (Forster et al 2023)
# running shift fit with updated forster 2024 data
observed_warming_path = os.path.join(datadir, 'gmst', 'gmst-obs/forster2024/decadal_averages.csv') #OLD: gmst-obs/forster2023/decadal_averages.csv
observed_warming_path_annual = os.path.join(datadir, 'gmst', 'gmst-obs/forster2024/annual_averages.csv') #OLD: gmst-obs/forster2023/decadal_averages.csv

# For saving with job submission 
#ver = sys.argv[1]

#trynew
job_id = os.getenv("SLURM_JOB_ID")  # This gives the job ID
task_id = os.getenv("SLURM_ARRAY_TASK_ID")  # This gives the task ID within the array
ver = f"{job_id}_{task_id}" if task_id else job_id

#ISIMIP3b models 

indir_p = os.path.join(os.environ['VSC_DATA_VO'], 'data/dataset/ISIMIP/ISIMIP3b/InputData/climate/atmosphere/bias-adjusted/global/daily/') # here there is historical of the primary models
indir_s = os.path.join(os.environ['VSC_DATA_VO'], 'data/dataset/ISIMIP/ISIMIP3b/SecondaryInputData/climate/atmosphere/bias-adjusted/global/daily/') # here there is historical of secondary models and hist-nat of all 6 models that have it


GCMs = ['CanESM5', 'CNRM-CM6-1', 'GFDL-ESM4', 'IPSL-CM6A-LR', 'MIROC6', 'MRI-ESM2-0','EC-Earth3', 'UKESM1-0-LL', 'MPI-ESM1-2-HR', 'CNRM-ESM2-1'] # all 10 
GCMs_p = ['GFDL-ESM4', 'IPSL-CM6A-LR', 'MPI-ESM1-2-HR', 'MRI-ESM2-0', 'UKESM1-0-LL']
GCMs_s = ['CanESM5', 'CNRM-CM6-1', 'CNRM-ESM2-1', 'EC-Earth3', 'MIROC6']


dir_gmst_models = os.path.join(datadir, 'gmst', 'gmst-models-isimip3b')
    
start_pi = 1850
end_pi = 1900

    
    
    
    
    
#ISIMIP3a observations

indir_obs =  os.path.join(os.environ['VSC_DATA_VO'],'data/dataset/ISIMIP/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/')

indir_counterclim =  os.path.join(os.environ['VSC_DATA_VO'],'data/dataset/ISIMIP/ISIMIP3a/InputData/climate/atmosphere/counterclim/global/daily/historical/')

datasets = ['GSWP3-W5E5','20CRv3-ERA5','20CRv3-W5E5'] #1901-2019, 1901-2021, 1901-2019 + '20CRv3' 1901-2015



# Set in/outdir for empirical_percentiles and shift fit and clean this up a bit ? 
    
    
    
    
# different metrics 

metric = flags['metric']

if 'TX' in flags['metric']:
    var = flags['var'] = 'tasmax'


if 'WBGT' in flags['metric']:
    if flags['method'] == 'calculate':  # variables used to calculate WBGT: 
        VARs=['tasmax','huss', 'ps']    # tasmax: K (daily max 2-m air temp)
                                        # huss: kg kg-1 (daily mean near surface specific humidity)
                                        # ps: Pa (daily mean surface air pressure
        var=None
    else: 
        var = flags['var'] = 'wbgt'
        
        
if '99' in flags['metric']:
    percentile = 0.99
elif '95' in flags['metric']:
    percentile = 0.95
elif '90' in flags['metric']:
    percentile = 0.90    
    
    
if '25' in flags['metric']:
    fixed_threshold = 25    
elif '28' in flags['metric']:
    fixed_threshold = 28
elif '30' in flags['metric']:
    fixed_threshold = 30
elif '33' in flags['metric']:
    fixed_threshold = 33       
elif '35' in flags['metric']:
    fixed_threshold = 35      



   

def start_message(): 
    
    def print_existing_variables(variables):
        for var_name in variables:
            try:
                var_value = globals()[var_name]
                print(f'{var_name}: {var_value}')
            except KeyError:
                print(f'{var_name}: None')
            
    print_existing_variables(['flags',
                              'var', 'VARs',
                              'indir_p', 'indir_s', 'indir_obs', 'indir_counterclim',
                              'GCMs', 'datasets', 
                              'lat_chunk', 'lon_chunk','time_chunk', 
                              'outdirs', 'figdirs', 'datadirs',
                              'start_pi', 'end_pi',
                              'target_years',
                              'observed_warming_path',
                              'observed_warming_path_annual',
                              'dir_gmst_models',
                              'outdirnames',
                              'percentile', 'fixed_threshold', 
                             'warming_period_method', 'warming_period_match'])
    
    print('\n')


