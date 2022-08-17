# Geospatial data exchange using binary data serialization approaches
## Abstract

In this work we investigate the benefits of binary data serialization as a means of storing and share large amounts of geospatial data in an interoperable way. De-facto text-based exchange formats typically exposed by modern APIs, including XML and JSON, are generally inefficient for an increasingly higher number of applications. Their inefficiencies are related to inflated volumes of data, low speed and the high computational cost for parsing and processing. In this work we consider comparisons of JSON/GeoJSON and two popular binary data formats called Protocol Buffers and Apache Avro for storing and sharing geographical data. Using a number of experiments we illustrate the advantages and disadvantages of both approaches for commonly used geospatial data formats such as Geopackages and GeoJSON produced by API calls. The work contributes a number of practical recommendations around the potential for binary data serialization for interoperable (geospatial) data storage and sharing in the future. This GitHub respository is provided to support the reproducibility of this work.

## This is the readme file for Experiment 1, 2 and 3
The Python code contained here was originally written in Python 3.8.10 on Ubuntu 20.04.3 LTS (focal) x86_64 (64 bit). The laptop computer was a DELL Inspiron 5567 with 16Gb memory and Intel Core(TM) i7-7500U CPU @ 2.70G processor.

### Required libraries and code.
To reproduce and replicate the experiments outlined here in this repository you will need to install a number of libraries within Python.

* The easiest method of installation of the libraries below is using pip. All installations are required except where stated otherwise.
* geojson https://pypi.org/project/geojson/
* Shapely https://pypi.org/project/Shapely/ (only required for Experiment 3)
* FastAvro https://fastavro.readthedocs.io/en/latest/ https://pypi.org/project/fastavro/
* Avro https://avro.apache.org/docs/current/gettingstartedpython.html
* ProtoBuf https://pypi.org/project/protobuf/
* The Protoc compiler must be downloaded and built for your system. You will only need to take this step if you are creating new Protocol Buffers and need to generate the new Python code. https://developers.google.com/protocol-buffers/docs/downloads
* Requests - this is only used where the JSON data is being downloaded from an API. The source code provided uses a sample JSON file to replace the need for a URL in the source code. https://docs.python-requests.org/en/master/index.html (only required for Experiment 2)
* GeoPandas is an open source project to make working with geospatial data in python easier. GeoPandas extends the datatypes used by pandas to allow spatial operations on geometric types. Geometric operations are performed by shapely. GeoPandas is used in Experiment 1 to handle the GPKG format directly within the Python code. https://geopandas.org/
* Numpy - The numpy library is required to calculate some simple statistics. You may already have this installed in your Python setup. https://pypi.org/project/numpy/

### Original Datasets
Within the `experiment1` and `experiment2/response-data` folder you will find the original datasets used in the testing and analysis using the Python code provided. In the experiment1 folder you will find a file called `original-dataset-experiment1.gpkg` which is a GeoPackage file containing 20,000 randomly generated point locations. In the experiment2 folder you will find a file called `original-dataset-experiment2.json` which also contains 20,000 point locations but the data is from an API call. Both files are provided for reproducibilty assistance. To use these two datasets in the experiments you will need to change the filenames where indicated in the source code for both experiments. For experiment3 in the `experiment3` we retain the same folder structure as 'experiment1' with a folder 'geojson-output' and 'binary-output' storing the deserialized and binary data files respectively.

## REPORT PUBLISHED
This report was based on previous work related to this repository.
* https://publications.jrc.ec.europa.eu/repository/bitstream/JRC127730/jrc-tr_ddi_2021_v9_online.pdf

The work is also published Int. Arch. Photogramm. Remote Sens. Spatial Inf. Sci., XLVIII-4/W1-2022, 307–313, 2022 (see link https://www.int-arch-photogramm-remote-sens-spatial-inf-sci.net/XLVIII-4-W1-2022/307/2022/) as part of the published proceedings of the Academic Track of FOSS4G 2022 https://2022.foss4g.org/


## ACKNOWLEDGEMENTS

The authors acknowledge the support of the European Commission - Joint Research Centre (JRC) through contract number CT-EX2014D166355-104, entitled "Evaluation of Novel approaches for governing (location) data and technology. Combined use of public sector and citizen-generated data".

## DISCLAIMER
The views expressed are purely those of the author and may not in any circumstances be regarded as stating an official position of the European Commission.
