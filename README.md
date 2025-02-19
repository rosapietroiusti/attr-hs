# Children disproportionately exposed to attributable heat stress days

Work in progress (Pietroiusti et al. in prep). 

Calculate gridscale attributable heat stress days and assess whether there is a disproportionate burden on children vs. adults, based on GCMs and reanalysis. 

## Additional necessary code:

Code for preprocessing of demographic data is available at https://github.com/rosapietroiusti/dem4cli. Additional code necessary for the calculation of wet bulb temperature is available at https://github.com/robwarrenwx/atmos, and for non-stationary distribution modelling at https://github.com/mathause/dist_cov (adapted for this project)

## Data availability 

All data used in the analysis is publicly available: reanalysis data (https://doi.org/10.48364/ISIMIP.982724.2) and CMIP6 climate model data bias-adjusted and statistically downscaled (https://doi.org/10.48364/ISIMIP.842396.1) are available through the ISIMIP data repository. Indoor WBGT data produced in this analysis will also be made available in the ISIMIP data repository.

Gridded population reconstructions (https://doi.org/10.48364/ISIMIP.889136.2) and projections (DOI missing!) are also available through the ISIMIP data repository. Country masks are available via ISIpedia (https://github.com/ISI-MIP/isipedia-countries). Cohort size data is available from the Wittgenstein Center Data Explorer v2 (http://dataexplorer.wittgensteincentre.org/wcde-v2/). SSP-RCP pathways are available through the AR6 scenario explorer hosted by IIASA (https://data.ece.iiasa.ac.at/ar6)  

## How to use this code

All code is in ``` main/ ```


### Running 

```main.py``` can be used to run main processing.

In ```settings.py```, choose between:

1) Calculating indoor WBGT temperature from daily maximum temperature, daily mean pressure and daily mean specific humidity, using the NEWT algorithm
Set ```flags['method'] = 'calculate' ```

2) Calculating present-day and pre-industrial return periods of WBGT thresholds from GCM data using empirical percentiles
Set ```flags['method'] = 'empirical_percentile' ``` or ```fixed threshold``` 

3)  Calculating present-day and pre-industrial return periods of WBGT thresholds from reanalysis data using shift fit method 
Set ```flags['method'] = 'shift_fit' ``` 

Set other flags and settings in ```settings.py``` and ```main.py``` according to your analysis needs. 



### Visualization & analysis

Visualization and analysis of processed results is done in .ipynb Jupyter notebooks. 


# Contact

rosa.pietroiusti@vub.be 