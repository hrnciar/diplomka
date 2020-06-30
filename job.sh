#!/bin/bash

month=$(date +'%-m')
year=$(date +'%Y')

cd /home/ckan/Software/diplomka/elektronabijecky
if [ $(( $month  % 12 -1)) -eq "-1" ] # november
then
	python3 /home/ckan/Software/diplomka/elektronabijecky/elektronabijecky.py -sy $year -sm 11 -ey $year -em 11 --no-head
elif [ $(( $month  % 12 -1)) -eq "0" ] # december
then
	python3 /home/ckan/Software/diplomka/elektronabijecky/elektronabijecky.py -sy $((year-1)) -sm 12 -ey $((year-1)) -em 12 --no-head
elif [ $(( $month  % 12 -1)) -eq "1" ] # january
then
	python3 /home/ckan/Software/diplomka/elektronabijecky/elektronabijecky.py -sy $year -sm $((month-1)) -ey $year -em $((month-1)) --head
else # rest of the year
	python3 /home/ckan/Software/diplomka/elektronabijecky/elektronabijecky.py -sy $year -sm $((month-1)) -ey $year -em $((month-1)) --no-head
fi
cd ..

cd /home/ckan/Software/diplomka/teplota
if [ $(( $month  % 12 -1)) -eq "-1" ] # november
then
	python3 /home/ckan/Software/diplomka/teplota/teplota.py -sy $year -sm 11 -ey $year -em 11 --no-head
elif [ $(( $month  % 12 -1)) -eq "0" ] # december
then
	python3 /home/ckan/Software/diplomka/teplota/teplota.py -sy $((year-1)) -sm 12 -ey $((year-1)) -em 12 --no-head
elif [ $(( $month  % 12 -1)) -eq "1" ] # january
then
	python3 /home/ckan/Software/diplomka/teplota/teplota.py -sy $year -sm $((month-1)) -ey $year -em $((month-1)) --head
else # rest of the year
	python3 /home/ckan/Software/diplomka/teplota/teplota.py -sy $year -sm $((month-1)) -ey $year -em $((month-1)) --no-head
fi
cd ..

IFS=$'\t\n\r' read -d '' -r -a ids < ids.txt
for id in "${ids[@]}"
do
	echo $id
	yes | docker exec -i ckan /usr/local/bin/ckan-paster --plugin=ckan datapusher submit $id -c /etc/ckan/production.ini 
done

rm ids.txt

cd /home/ckan/Software/diplomka/uredni-deska

# get ID from file
ID=$(cat /home/ckan/Software/diplomka/uredni-deska/.ID)

python3 /home/ckan/Software/diplomka/uredni-deska/uredni-deska.py -sid $ID -eid $ID

#check if there is something to upload
if [ $? -eq 4 ];
then
	exit
else
	# new dataset detected, bump ID and store it to file for next check
	echo $(($ID+1)) > /home/ckan/Software/diplomka/uredni-deska/.ID

	# try to update again with bumped ID, incase of two City Council's sessions in one month
	python3 /home/ckan/Software/diplomka/uredni-deska/uredni-deska.py -sid $(($ID+1)) -eid $(($ID+1))

	#check if there is something to upload
	if [ $? -eq 4 ];
	then
		exit
	else
		# new dataset detected, bump ID and store it to file for next check
		echo $(($ID+2)) > /home/ckan/Software/diplomka/uredni-deska/.ID
	fi
fi
