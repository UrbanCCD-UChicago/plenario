# EAGER API

# Phase I

## Dataset

use a unique, non-semantic id for the dataset, we should mint our own ids.

```
https://base_url/dataset-id/
```

Returns all the data limited to 1000 records at the finest spatial resolutions. All responses will be JSON objects.

## Metadata
Each geospatial area of the city should include metadata about the available data. This data should use the schema of http://schema.org/Dataset

the meta data must include the following fields:

* `name`
* `description`
* `url` (endpoint to the data for this area)
* `isBasedOnUrl` source url, i.e https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2"
* `temporal` The range of temporal applicability of a dataset, e.g. for a 2011 census dataset, the year 2011 (in ISO 8601 time interval format).
* `spatial` The range of spatial applicability of a dataset, e.g. for a dataset of New York weather, the state of New York.
* `frequency` Frequency with which dataset is published. 	accrualPeriodicity


In addition to these schema.org standards the meta data must also include

* `temporal-resolutions` list of available temporal resolutions {continuous, minute, hour, day, week, month, year} 
* `spatial-resolutions` list of available temporal resolutions {point, street segment, grid, building, parcel, city-block, census-block, tract, beat, community area... etc)

## Time range
specificy a time range for the data

or, if you wanted to find all crimes reported between May 23, 2012 and June 25, 2012 it would be formatted in to a [UNIX timestamp](http://en.wikipedia.org/wiki/Unix_time).

```
http://base_url/api/datset-id/?date__lte=1340582400&date__gte=1337731200
```

## Temporal resolution

Supported temporal resolutions:

* continuous (_default, no aggregation_)
* minute
* hour
* day
* week
* month
* year

Example 
```
http://base_url/api/datset-id/?temporal_resolution=minute
```

## Spatial resolution 

Possible spatial resolutions:

* point
* street segment
* grid
* building
* parcel
* city block
* census block
* tract
* beat
* community area
* PUMAs
* congresional districts

Example
```
http://base_url/api/datset-id/?spatial_resolution=city-block
```

### Grid resolution
Define a set of finer nested grid resolutions, similar in spirit to tile maps. See [Tile Map Service specification](http://wiki.osgeo.org/wiki/Tile_Map_Service_Specification).
* 0.25 mile grid
* 0.5 mile grid
* 1 mile grid
* 2 mile grid
* 4 mile grid
* 8 mile grid

Example:
![1940 City of Chicago Population](https://lh3.googleusercontent.com/-l8h_RrxQppg/TzgMdfDpHqI/AAAAAAAAAnw/XZMJJ0Qu-nk/w378-h588-no/EWBP.B50.F8_Gregg3.tif)

## Location queries

### Within a radius of a point.

Specify:

* `lat` latitude
* `lon` longitude
* `radius` in meters

Example

```
http://base_url/api/datset-id/?lat=41.878114&lon=-87.629798&radius=100
```

### Within a given polygon

Specify a GeoJSON polygon:

``` javascript 
{
    "coordinates": [
        [
            [
                -87.66865611076355, 
                42.00809838577665
            ], 
            [
                -87.66855955123901, 
                42.004662333308616
            ], 
            [
                -87.66045928001404, 
                42.004869617835695
            ], 
            [
                -87.66071677207947, 
                42.00953334115145
            ], 
            [
                -87.6644504070282, 
                42.01010731423809
            ], 
            [
                -87.66865611076355, 
                42.00809838577665
            ]
        ]
    ], 
    "type": "Polygon"
}
```

which gets stringified and appended as a query parameter:

``` bash 
http://base_url/api/datset-id/?location__geoWithin=%7B%22type%22%3A%22Polygon%22%2C%22coordinates%22%3A%5B%5B%5B-87.66865611076355%2C42.00809838577665%5D%2C%5B-87.66855955123901%2C42.004662333308616%5D%2C%5B-87.66045928001404%2C42.004869617835695%5D%2C%5B-87.66071677207947%2C42.00953334115145%5D%2C%5B-87.6644504070282%2C42.01010731423809%5D%2C%5B-87.66865611076355%2C42.00809838577665%5D%5D%5D%7D&date__lte=1369285199&date__gte=1368594000&type=violent%2Cproperty&_=1369866788554
```


## Phase 2 (TBD)
Advanced filtering, i.e. `WHERE` type statements.

---
Much inspiration can be taken from Eric's (Crime API)[https://github.com/evz/crimeapi] and the dataset schema http://schema.org/Dataset
