#!/bin/bash

cd /home/elmer/frontPage

psql -P format=unaligned -P tuples_only -P fieldsep=\, -c "SELECT * FROM backpage" > backpage_dump.csv
