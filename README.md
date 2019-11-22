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
