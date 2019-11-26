# VIGSI Data Processing

This repository contains tools for manipulating GHI data encoded in GeoJson and
originating from the NREL Wind Dataset. This strip is run as a series of steps
to produce data that is stored in an S3 bucket for efficient display.

## Running

You need Python 3 to run the scripts. It was tested with Python 3.7.2 64-bit on
Windows. Other platforms have not been tested and are not supported. Only the
standard Python library is required.

You can get information about how to run using the built-in help.

```
python geojon-transformer.py --help
```

The basic pattern is a command followed by the input directory and the output
directory.

## Workflows

This section describes the ordered workflows to produce the prepared data sets.
The input to each workflow is the numbered GeoJSON files extracted using the
downloader application.

### NREL (Daily)

Use the `daily` transformation to perform step-wise integration:

```
python geojson_transformer.py daily <DOWNLOADED FILES DIR> <INTERMEDIATE DIR>
```

Then use the `addlatlon` transformation to add latitude and longtitude information
to the data set.

```
python geojson_transformer.py addlatlon <INTERMEDIATE DIR> <OUTPUT DIR>
```

### NREL (Monthly)

Use the `monthly` transformation to perform step-wise integration:

```
python geojson_transformer.py monthly <DOWNLOADED FILES DIR> <INTERMEDIATE DIR>
```

Then use the `addlatlon` transformation to add latitude and longtitude information
to the data set.

```
python geojson_transformer.py addlatlon <INTERMEDIATE DIR> <OUTPUT DIR>
```

### NREL (Yearly)

Use the `yearly` transformation to perform step-wise integration:

```
python geojson_transformer.py yearly <DOWNLOADED FILES DIR> <INTERMEDIATE DIR>
```

Then use the `addlatlon` transformation to add latitude and longtitude information
to the data set.

```
python geojson_transformer.py addlatlon <INTERMEDIATE DIR> <OUTPUT DIR>
```

### ARIMA (Daily)

Use the `tocsv` transformation to convert to input files for ARIMA learning.

```
python geojson_transformer.py tocsv <DOWNLOADED FILES DIR> <LEARNING IN DIR>
```

Then train the ARIMA on the learning directory using the files in
`<LEARNING IN DIR>`. This produces files in `<LEARNING OUT DIR>`. Then apply
the following transformation to fix the structure.

```
python geojson_transformer.py fix <LEARNING OUT DIR> <INTERMEDIATE DIR>
```

Then use the `addlatlon` transformation to add latitude and longtitude information
to the data set.

```
python geojson_transformer.py addlatlon <INTERMEDIATE DIR> <OUTPUT DIR>
```

### ARIMA (Monthly)

Use the `tocsv` transformation to convert to input files for ARIMA learning.

```
python geojson_transformer.py tocsv <DOWNLOADED FILES DIR> <LEARNING IN DIR>
```

Then train the ARIMA on the learning directory using the files in
`<LEARNING IN DIR>`. This produces files in `<LEARNING OUT DIR>`. Then apply
the following transformation to fix the structure.

```
python geojson_transformer.py fix <LEARNING OUT DIR> <INTERMEDIATE1 DIR>
```

Use the `emonthly` transformation to aggregate monthly energy.

```
python geojson_transformer.py emonthly <INTERMEDIATE1 DIR> <INTERMEDIATE2 DIR>
```

Then use the `addlatlon` transformation to add latitude and longtitude information
to the data set.

```
python geojson_transformer.py addlatlon <INTERMEDIATE2 DIR> <OUTPUT DIR>
```

### ARIMA (Yearly)

Use the `tocsv` transformation to convert to input files for ARIMA learning.

```
python geojson_transformer.py tocsv <DOWNLOADED FILES DIR> <LEARNING IN DIR>
```

Then train the ARIMA on the learning directory using the files in
`<LEARNING IN DIR>`. This produces files in `<LEARNING OUT DIR>`. Then apply
the following transformation to fix the structure.

```
python geojson_transformer.py fix <LEARNING OUT DIR> <INTERMEDIATE1 DIR>
```

Use the `emonthly` transformation to aggregate monthly energy.

```
python geojson_transformer.py emonthly <INTERMEDIATE1 DIR> <INTERMEDIATE2 DIR>
```

Then use the `addlatlon` transformation to add latitude and longtitude information
to the data set.

```
python geojson_transformer.py addlatlon <INTERMEDIATE2 DIR> <OUTPUT DIR>
```
