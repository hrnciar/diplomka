#!/bin/bash

month=$(date +'%-m')
year=$(date +'%Y')

cd /home/ckan/Software/diplomka/elektronabijecky
if [ $(( $month  % 12 -1)) -eq "-1" ] # november
then
	python3 /home/ckan/Software/diplomka/elektronabijecky/elektronabijecky.py -in -sy $year -sm 11 -ey $year -em 11 --no-head
elif [ $(( $month  % 12 -1)) -eq "0" ] # december
then
	python3 /home/ckan/Software/diplomka/elektronabijecky/elektronabijecky.py -in -sy $((year-1)) -sm 12 -ey $((year-1)) -em 12 --no-head
elif [ $(( $month  % 12 -1)) -eq "1" ] # january
then
	python3 /home/ckan/Software/diplomka/elektronabijecky/elektronabijecky.py -in -sy $year -sm $((month-1)) -ey $year -em $((month-1)) --head
else # rest of the year
	python3 /home/ckan/Software/diplomka/elektronabijecky/elektronabijecky.py -in -sy $year -sm $((month-1)) -ey $year -em $((month-1)) --no-head
fi

