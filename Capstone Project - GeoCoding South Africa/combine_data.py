## ensure that the data folder is located in the same folder as this file
## if you don't have the data folder, run the get_data.sh script first
import pandas as pd
import geopandas as gpd
import numpy as np
import os

def load_geodataframes():
    print('Reading geonames')
    geo_cols = ['geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude',
                'feature_class', 'feature_code', 'country_code', 'cc2',
                'admin1_code', 'admin2_code', 'admin3_code', 'admin4_code',
                'population', 'elevation', 'dem', 'timezone', 'modification_date',]
    geonames = pd.read_csv('data/geonames.tsv', sep='\t', names=geo_cols, dtype=object)
    geonames['f_code'] = geonames.feature_class.astype(str)+'.'+(geonames.feature_code).astype(str)
    
    print('Reading geofeatures')
    geofeats_cols = ['f_code','desc_short','desc_long']
    geofeats = pd.read_csv('data/geonames_features.tsv', sep='\t', names=geofeats_cols)
    geofeats.head()

    print('Merging names/features')
    geos = geonames.merge(geofeats, how='left',on='f_code')
    geos[['latitude','longitude']] = geos[['latitude','longitude']].astype(float)
    geos[['geonameid','population']] = geos[['geonameid','population']].astype(int)
    names_geo = gpd.GeoDataFrame(geos, geometry=gpd.points_from_xy(geos.longitude,geos.latitude))

    print('Reading postal codes')
    pc_cols = ['country_code', 'postal_code', 'place_name', 
               'admin_name1', 'admin_code1', 'admin_name2', 'admin_code2', 
               'admin_name3', 'admin_code3', 'latitude', 'longitude', 'accuracy',]
    pc = pd.read_csv('data/postal_codes.tsv', sep='\t', names=pc_cols, dtype={1:object})
    pc[['latitude','longitude']] = pc[['latitude','longitude']].astype(float)
    pc_geo = gpd.GeoDataFrame(pc, geometry=gpd.points_from_xy(pc.longitude,pc.latitude))

    print('Reading wards')
    wards_geo = gpd.read_file('data/MDBWard2016.gdb/').set_geometry('geometry')
    names_geo.crs = {'init': 'epsg:4326'}
    pc_geo.crs = {'init': 'epsg:4326'}

    print('Joining wards to geonames and postal codes')
    wards_names = gpd.sjoin(wards_geo, names_geo, op='intersects')
    wards_pc = gpd.sjoin(wards_geo, pc_geo, op='intersects')
    
    print('Done!')
    return names_geo, pc_geo, wards_geo, wards_names, wards_pc

def cartesian_product(*arrays):
    ndim = len(arrays)
    return np.stack(np.meshgrid(*arrays), axis=-1).reshape(-1, ndim)

def _create_smaller_grid(lats, longs, accuracy_m, verbose):
    mlat, mlong = len(lats), len(longs)
    if max(mlat,mlong) >= 500:
        dfs = []
        if mlat>mlong: 
            for lat in np.array_split(lats,mlat//150):
                dfs += [_create_smaller_grid(lat,longs,accuracy_m,verbose)]
        else:
            for lon in np.array_split(longs,mlong//150):
                dfs += [_create_smaller_grid(lats,lon,accuracy_m,verbose)]
        grid = pd.concat(dfs)
        if verbose: print('collecting results')


    else:
        if verbose: print('.',end='',flush=True)
        cols=['longitude','latitude']
        grid = pd.DataFrame(cartesian_product(lats,longs), columns=cols)
        grid = gpd.GeoDataFrame(grid, geometry=gpd.points_from_xy(grid.longitude,grid.latitude))
        grid['key'] = (grid.latitude*1000).round().astype(int).astype(str)+';'+ \
                      (grid.longitude*1000).round().astype(int).astype(str)
        grid.crs = {'init': 'epsg:4326'}
    return grid

def create_grid(lats, longs, accuracy_m=None, verbose=False):
    '''
    longs(array-like): min,max values for longitude range or actual range of values
    lats(array-like): min,max values for latitude range or actual range of values
    accuracy_m(int): (optional) desired approximate accuracy in meters (min 100)
    verbose(bool): set verbosity level
    - will create accuracy_m spaced grid using min/max from longitudes/latitudes
    Returns: GeoDataFrame with latitudes, longitudes, coordinates and constructed key
    '''
    try:
        accuracy_m = int(accuracy_m)
    except Exception: 
        raise Exception('accuracy_m must be numeric: \n'
                        'ideally integer indicating the desired '
                        'accuracy level of the generated dataset')
    steps = 10**max(len(str(int(accuracy_m))),3)/100000
    lats = np.arange(np.min(lats), np.max(lats), steps)
    longs = np.arange(np.min(longs), np.max(longs), steps)
    print('Generating point grid')
    return _create_smaller_grid(lats,longs,accuracy_m,verbose)

def _chunker(df, chunksize=10000, chunks=None):
    '''
    df(dataframe): Pandas-like df to be split into chunks
    chunksize(int): desired size of smaller dfs
    chunks(int): (optional) desired number of equal-sized chunks
    returns: list of dfs with at most chunksize rows or at most chunks entries
    '''
    df_in = df.copy()
    if chunks:
        chunksize = df_in.shape[0]//(chunks+1)
    for (g,df) in df_in.groupby(np.arange(df_in.shape[0]) // chunksize):
        yield df
        
def _do_join(dfs):
    '''
    Helper to do the geometry join
    '''
    small_grid, small_geometry = dfs
    small_join = gpd.sjoin(small_grid, small_geometry, how='inner', op='within', rsuffix='_R')
    return small_join

def locate_grid(grid, geometries, chunksize=10000, verbose=False):
    '''
    Locate the grid within the provided geography
    Store results in output_file, if provided
    grid(dataframe)
    '''
    print('Trimming the grid')
    results = []
    if grid.shape[0] > 1000000:
        print('Grid is large - this is going to take a while!')
    n,steps = 0,0
    for small_geometry in _chunker(geometries, chunksize//50):
        for small_grid in _chunker(grid, chunksize):
            if verbose: print('.',flush=True,end='')
            small_join = _do_join((small_grid,small_geometry))
            results += [small_join]
            steps += 1
            n += min(small_grid.shape[0],small_geometry.shape[0])
            if steps%20==0:
                print(f' ({n} of {grid.shape[0]} [{n*100//grid.shape[0]}] processed)')
        if verbose: print('collecting results')
    print('Combining trim results')
    ret = pd.concat(results)
    if verbose: print('Done!')
    return ret

if __name__=='__main__':
    if not os.path.exists('data'):
        print('Please run the get_data.sh `sh ./get_data` script before running this script')
        print('This script will not produce results if the data folder containing the data',
              'does not exist in the root of the location of this script')
        import sys; sys.exit()
    names_geo, pc_geo, wards_geo, wards_names, wards_pc = load_geodataframes()
    longs = np.arange(-35,-21,0.01)
    lats = np.arange(15.9,33.5,0.01)
    grid = create_grid(lats, longs, accuracy_m=100, verbose=True)
    joined = locate_grid(grid, wards_geo, grid.shape[0], verbose=True)
    
    # clean and store main dataset
    keep_cols=['key','latitude','longitude',
               'WardID','WardNumber','Shape_Length','Shape_Area',
               'LocalMunicipalityName','DistrictMunicipalityCode',
               'DistrictMunicipalityName','ProvinceName','ProvinceCode']
    col_renames = {'WardID':'ward_id','WardNumber':'ward_number',
                   'Shape_Length':'ward_length','Shape_Area':'ward_area',
                   'LocalMunicipalityName':'local_municipality',
                   'DistrictMunicipalityCode':'district_minicipal_code',
                   'DistrictMunicipalityName':'district_municipality',
                   'ProvinceName':'province_code','ProvinceCode':'province_name'}
    location_grid = pd.DataFrame(joined[keep_cols].rename(columns=col_renames)).reset_index(drop=True)
    print('Storing location_grid.json.gz in the data folder')
    location_grid.to_json('data/location_grid.json.gz',orient='records')
    print('Done!')
    print(f'names_geo size     : {names_geo.shape}')
    print(f'pc_geo size        : {pc_geo.shape}')
    print(f'wards_geo size     : {wards_geo.shape}')
    print(f'wards_names size   : {wards_names.shape}')
    print(f'wards_pc size      : {wards_pc.shape}')
    print(f'grid size          : {grid.shape}')
    print(f'joined size        : {joined.shape}')
    print(f'location_grid size : {location_grid.shape}')
