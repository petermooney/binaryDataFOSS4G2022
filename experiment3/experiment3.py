import geopandas as gpd
from geojson import Point, Feature, FeatureCollection, dump
import time
import os.path
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from fastavro import writer, reader, parse_schema
from fastavro.schema import load_schema

#import address_pb2
from decimal import *
import numpy as np

getcontext().prec = 16
file_name = "experiment3"
avro_schema_file_name = "clc"
INPUT_GPKG_FILE = './input-data/Firenze_land-use_CLC_WGS_84_SELECTED.gpkg'
#INPUT_GPKG_FILE = 'test-geopackage.gpkg'
#INPUT_GPKG_FILE_LAYER = 'FI_Addresses_Sample_Aug23rd2021'

INPUT_GPKG_FILE_LAYER = 'Firenze_land-use_CLC_WGS_84_SELECTED'

geopkg_geojson_timing = []
geojson_pbf_timing = []
geojson_avro_timing = []
avro_geojson_timing = []
pbf_geojson_timing = []
load_geojson_timing = []

for test in range (0,1):

    ## Read the GeoPackage using GeoPandas and convert to GeoJSON file.

    tic = time.perf_counter()
    print ("===========GeoPackage to GeoJSON==============")
    print ("Begin: Converting GeoPackage to GeoJSON...")

    file_size = os.path.getsize(INPUT_GPKG_FILE)
    print("GeoPackge File size is {} Kb".format(round(file_size/1024),2))

    finland_gdf = gpd.read_file(INPUT_GPKG_FILE, layer=INPUT_GPKG_FILE_LAYER)
    finland_gdf.to_file("./geojson-output/{}.geojson".format(file_name), driver='GeoJSON')
    print ("End: Converting GeoPackage to GeoJSON...")
    toc = time.perf_counter()

    geopkg_geojson_timing.append(toc - tic)
    print(f"Timing: Converting GeoPackage to GeoJSON : {toc - tic:0.4f} seconds")
    file_size = os.path.getsize("./geojson-output/{}.geojson".format(file_name))
    print("GeoJSON File size is {} Kb".format(round(file_size/1024),2))

    tic = time.perf_counter()
    print ("\nBegin: Loading GeoJSON file for processing...")
    geojson_data = gpd.read_file("./geojson-output/{}.geojson".format(file_name))
    ## obtain the CRS of the data from GeoPandas.
    ## This is important if the CRS is not WGS 84 (EPSG:4326).
    ## Without the CRS specified, a GIS such as QGIS cannot render the GeoJSON file correctly.
    data_CRS = geojson_data.crs
    print ("Data CRS {}".format(data_CRS))

    toc = time.perf_counter()

    load_geojson_timing.append(toc - tic)
    print(f"Timing: Load GeoJSON file (using GeoPandas) : {toc - tic:0.4f} seconds")
    print ("GeoJSON file: CRS {}".format(geojson_data.crs))
    print ("GeoJSON file: Total Geometry Objects: {}".format(len(geojson_data['geometry'])))
    print ("GeoJSON file: Dataset Properties: {}, List: {}".format(len(list(geojson_data)),list(geojson_data)))
    print ("End: Loading GeoJSON file for processing...")

    ## fast avro
    print ("===========GeoJSON to Avro ==============")
    print ("Serialize GeoJSON to Apache Avro")
    fast_avro_clc_schema = load_schema("{}.avsc".format(avro_schema_file_name))

    tic = time.perf_counter()


    clc_fast_avro = []

    for index, row in geojson_data.iterrows():
        #fid =  row["fid"]
        addrHousenumber = row["addr:housenumber"]
        addrStreet = row["addr:street"]
        addrCity = row["addr:city"]
        source = row["source"]
        addrUnit = row["addr:unit"]
        fullAddress = row["fullAddress"]
        geometry = row["geometry"]

        tempCLCAvro = {}
        tempCLCAvro["addrHousenumber"] = addrHousenumber
        tempCLCAvro["addrStreet"] = addrStreet
        tempCLCAvro["addrCity"] = addrCity
        tempCLCAvro["source"] = source
        tempCLCAvro["addrUnit"] = addrUnit
        tempCLCsAvro["fullAddress"] = fullAddress


        tempCLCAvro["geometry"] = geometry.to_wkt()
        tempCLCAvro["fid"] = 1

        clc_fast_avro.append(tempCLCAvro)


    with open("./binary-output/{}_fast.avro".format(file_name), "wb") as out:
        writer(out,fast_avro_address_schema,addresses_fast_avro)


    print ("Finished serializing JSON to GeoJSON")
    toc = time.perf_counter()
    geojson_avro_timing.append(toc - tic)
    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize("./binary-output/{}_fast.avro".format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))




print ("\n\n\n==== Statistical Report ====")
pbf_geojson_timing_np = np.array(pbf_geojson_timing)
geojson_pbf_timing_np = np.array(geojson_pbf_timing)
geojson_avro_timing_np = np.array(geojson_avro_timing)
avro_geojson_timing_np = np.array(avro_geojson_timing)
geopkg_geojson_timing_np  = np.array(geopkg_geojson_timing)
load_geojson_timing_np  = np.array(load_geojson_timing)

print ("=====File Sizes=====")

file_size = os.path.getsize(INPUT_GPKG_FILE)
print("Input GPKG file size is {} Kb".format(round(file_size/1024),2))

file_size = os.path.getsize('./geojson-output/{}.geojson'.format(file_name))
print("./geojson-output/{}.geojson size is {} Kb".format(file_name,round(file_size/1024),2))


print ("=====Run Times=====")
print("Convert GPKG -> GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geopkg_geojson_timing_np, dtype=np.float64),np.std(geopkg_geojson_timing_np, dtype=np.float64)))

print("Load GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(load_geojson_timing_np, dtype=np.float64),np.std(load_geojson_timing_np, dtype=np.float64)))
