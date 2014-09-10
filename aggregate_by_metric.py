#!/usr/bin/python

import csv
import sys

def csv_to_dict(filepath):
	datafile = open(filepath, 'r')
	datareader = csv.reader(datafile)
	records = []
	headers = None
	for row in datareader:
		if headers is None:
			headers = row
		else:
			record = {}
			for i in range(0, len(headers)):
				record[headers[i].lower()] = row[i]
			records.append(record)
	return records

#check for aguments
if len(sys.argv) < 2:
	print("You must supply a metric name, e.g. acct_010")
	sys.exit()

#init variables
metric = sys.argv[1]
qtrs = range(2009, 2014)
credit_union_info = csv_to_dict('data/QCR200912/foicu.txt')
metric_cheatsheet = csv_to_dict('data/QCR200912/AcctDesc.txt')
stuff_to_print = ['cu_number', 'cu_name', 'acct_010_2009', 'acct_010_2010', 'acct_010_2011', 'acct_010_2012', 'acct_010_2013']
credit_union_name_lookup = {}
for x in credit_union_info:
	credit_union_name_lookup[x['cu_number']] = x['cu_name']

#gather data per credit union
data_per_credit_union = {}
for q in qtrs:
	datasheet = csv_to_dict('data/QCR%i12/fs220.txt' % q)
	for record in datasheet:
		i = record['cu_number']
		if i not in data_per_credit_union:
			data_per_credit_union[i] = {
				'cu_number': int(i),
				'cu_name': credit_union_name_lookup[i] if i in credit_union_name_lookup else 'UNKNOWN'
			}
		data_per_credit_union[i]['%s_%s' % (metric, q)] = int(float(record[metric]))

#write results to file
results_file = open('results/aggregate_%s' % metric, 'w')
results_file.write(str(stuff_to_print)[1:-1].upper().replace(", ",",").replace("'", "\"") + "\n")
for cu in data_per_credit_union:
	results_file.write(str([(data_per_credit_union[cu][x] if x in data_per_credit_union[cu] else None) for x in stuff_to_print])[1:-1].replace(", ",",").replace("'", "\"").replace("None", "") + "\n")
results_file.close()
print('Results written to results/aggregate_%s' % metric)

