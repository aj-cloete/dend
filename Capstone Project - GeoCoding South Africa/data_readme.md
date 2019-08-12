## About the data
### * Geography names dataset
To load the geonames dataset correctly, use the following code:
```
geo_cols = ['geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude',
            'feature_class', 'feature_code', 'country_code', 'cc2',
            'admin1_code', 'admin2_code', 'admin3_code', 'admin4_code',
            'population', 'elevation', 'dem', 'timezone', 'modification_date',]
geonames = pd.read_csv('data/geonames.tsv', sep='\t', names=geo_cols, dtype=object)
geonames['f_code'] = geonames.apply(lambda x: str(x.feature_class)+'.'+str(x.feature_code), axis=1)
geonames.head()
```

### * Geography features dataset
To load the geonames features dataset correctly, use the following code:
```
geofeats_cols = ['f_code','desc_short','desc_long']
geofeats = pd.read_csv('data/geonames_features.tsv', sep='\t', names=geofeats_cols)
geofeats.head()
```

To merge the above two datasets, use `geos = geonames.merge(geofeats, how='left',on='f_code')`

### * Postal Codes dataset
To load the geonames features dataset correctly, use the following code:
```
pc_cols = ['country_code', 'postal_code', 'place_name', 
           'admin_name1', 'admin_code1', 'admin_name2', 'admin_code2', 
           'admin_name3', 'admin_code3', 'latitude', 'longitude', 'accuracy',]
pc = pd.read_csv('/home/workspace/data/postal_codes.tsv', sep='\t', names=pc_cols, dtype={1:object})
pc.head()
```
