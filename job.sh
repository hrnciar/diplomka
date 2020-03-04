#!/bin/bash

month=$(date +'%-m')
year=$(date +'%Y')

cd /home/ckan/Software/diplomka/
python3 /home/ckan/Software/diplomka/evmapy.py -in -sy $year -sm $((month-1)) -ey $year -em $((month-1))
