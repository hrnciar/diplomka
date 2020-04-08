#!/bin/bash

month=$(date +'%-m')
year=$(date +'%Y')

cd /home/ckan/Software/diplomka/
if [ $(( $i  % 12 -1)) -eq "-1" ] # november
then
	python3 /home/ckan/Software/diplomka/evmapy.py -in -sy $year -sm 11 -ey $year -em 11
	echo $(( $i  % 12 -1 ))
	echo "import novembert"
elif [ $(( $i  % 12 -1)) -eq "0" ] # december
then
	python3 /home/ckan/Software/diplomka/evmapy.py -in -sy $((year-1)) -sm 12 -ey $((year-1)) -em 12
	echo $(( $i  % 12 -1 ))
	echo "import december"
else # rest of the year
	python3 /home/ckan/Software/diplomka/evmapy.py -in -sy $year -sm $((month-1)) -ey $year -em $((month-1))
	echo $(( $i  % 12 -1 ))
	echo "import zvysok"
fi

