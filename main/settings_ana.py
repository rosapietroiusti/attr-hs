# attributable heatwaves research

# Created June 26 2023
# rosa.pietroiusti@vub.be
# Update Feb 2024 



import os, glob, sys
import numpy



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

flags['models'] = 'ISIMIP3b' 
                             # 'ISIMIP3b'
                             # 'ISIMIP3a' 
    

flags['experiment'] = None 
                                # obsclim
                                # counterclim
                                # None if ISIMIP3b


flags['metric'] = 'WBGT28' # ['WBGT90', 'WBGT95', 'WBGT99'][int(sys.argv[2])]  
                            # submit as job array e.g. ['WBGT90', 'WBGT95', 'WBGT99'][int(sys.argv[2])-1]
                            # 'WBGT' : to calculate it or run shift_fit on full distribution
                            # 'TX90', 'TX95', 'TX99' : to get return levels and periods
                            # 'WBGT90', 'WBGT95', 'WBGT99' 
                            # 'WBGT28', 'WBGT30', 'WBGT33'

                            
flags['method'] = None
                            # calculate: to calculate WBGT 
                            # empirical_percentile: calculates return levels and periods with empirical percentiles
                            # fixed_threshold: empirical percentiles but based on a fixed magnitude threshold
                            # shift_fit: fits non-stationary distribution per pixel based on dist_cov (Hauser et al)
                            # TODO: make emp percentile and fixed threshold a single flag ?? that it recognized based on metric 

flags['time_method']=  None
                            # 'single-year' analysis on just one year (target year in obs models are matched to!)
                            # 'timeseries-gmt-mapping' - NOT WORKING - DELETE?
                            # 'timeseries-model-years' - IS IT WORKING? DELTE? 
                            # None: if not running empirical percentiles or fixed threshold
                
flags['shift_sigma'] = False # False
                            # True = one model per month, loc and scale both vary
                            # False = one model per month, only loc varies
                            # None: if not running shift fit 

flags['shift_period'] = None #[(1901, 2019),(1950, 2019)][int(sys.argv[2])-1]
                             # 1901, 2019
                             # 1950, 2019 
                             # None for emp percentiles or calc wbgt 

flags['shift_loglike']=None

            
flags['chunk_version']=None     
                            # 0=specified dask chunks - best on Jup Nb (and for shift fit?)
                            # 1=auto dask chunks
                            # 2=no dask chunks - fastest on HPC for empirical percentiles
                            # all of them unchunk time 
                            # test time they take on shift fit! 
            
target_years = None #  [2022,2023][int(sys.argv[3])]  
                # int e.g. 2022, 2023 - for empirical percentiles and fixed magnitude
                # None: to calc WBGT or shift fit 
            

        
        
        
        
        
        

# do not edit below this after development 
# --------------------------------------------------

# set paths 
datadir = os.path.join('..', '..', 'data')

# observed warming decadal averages 10 years leading up to final year (AR6 method) or annual average (Forster et al 2023)
# running shift fit with updated forster 2024 data
observed_warming_path = os.path.join(datadir, 'gmst', 'gmst-obs/forster2024/decadal_averages.csv') #OLD: gmst-obs/forster2023/decadal_averages.csv
observed_warming_path_annual = os.path.join(datadir, 'gmst', 'gmst-obs/forster2024/annual_averages.csv') #OLD: gmst-obs/forster2023/decadal_averages.csv




#ISIMIP3b models 

indir_p = os.path.join(os.environ['VSC_DATA_VO'], 'data/dataset/ISIMIP/ISIMIP3b/InputData/climate/atmosphere/bias-adjusted/global/daily/') # here there is historical of the primary models
indir_s = os.path.join(os.environ['VSC_DATA_VO'], 'data/dataset/ISIMIP/ISIMIP3b/SecondaryInputData/climate/atmosphere/bias-adjusted/global/daily/') # here there is historical of secondary models and hist-nat of all 6 models that have it

# GCMs = ['CanESM5', 'CNRM-CM6-1', 'GFDL-ESM4', 'IPSL-CM6A-LR', 'MIROC6', 'MRI-ESM2-0'] # all the ISIMIP3b GCMs that have both hist and hist-nat (could extend to all hist models)
# GCMs_p = ['GFDL-ESM4', 'IPSL-CM6A-LR', 'MRI-ESM2-0']
# GCMs_s = ['CanESM5', 'CNRM-CM6-1', 'MIROC6']

GCMs = ['CanESM5', 'CNRM-CM6-1', 'GFDL-ESM4', 'IPSL-CM6A-LR', 'MIROC6', 'MRI-ESM2-0','EC-Earth3', 'UKESM1-0-LL', 'MPI-ESM1-2-HR', 'CNRM-ESM2-1'] # all 10 
GCMs_p = ['GFDL-ESM4', 'IPSL-CM6A-LR', 'MPI-ESM1-2-HR', 'MRI-ESM2-0', 'UKESM1-0-LL']
GCMs_s = ['CanESM5', 'CNRM-CM6-1', 'CNRM-ESM2-1', 'EC-Earth3', 'MIROC6']

# temp! the ones that have run for wbgt28
dir_gmst_models = os.path.join(datadir, 'gmst', 'gmst-models-isimip3b')
    
start_pi = 1850
end_pi = 1900

    
    
    
    
    
#ISIMIP3a observations

indir_obs =  os.path.join(os.environ['VSC_DATA_VO'],'data/dataset/ISIMIP/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/')

indir_counterclim =  os.path.join(os.environ['VSC_DATA_VO'],'data/dataset/ISIMIP/ISIMIP3a/InputData/climate/atmosphere/counterclim/global/daily/historical/')

datasets = ['GSWP3-W5E5','20CRv3-ERA5','20CRv3-W5E5'] #1901-2019, 1901-2021, 1901-2019 + '20CRv3' 1901-2015


    
    
    
    
    
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
                              'percentile', 'fixed_threshold'])
    
    print('\n')


