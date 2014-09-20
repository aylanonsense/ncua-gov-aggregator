#!/usr/bin/python

import sys, cu_utils

#get the credit number as an argument
if len(sys.argv) < 2:
	print("You must supply an account id, e.g. ACCT_010")
	sys.exit()
acct_id = sys.argv[1].upper()

#init vars
years = cu_utils.find_years_of_data()
cu_lookup = cu_utils.generate_credit_union_lookup(years)
acct_lookup = cu_utils.generate_account_lookup(years)

#Warn if credit union might not exist
if acct_id in acct_lookup:
	cu_utils.start_timer("Aggregating data for %s (%s) from %i to %i..." % (acct_lookup[acct_id]['ACCT_NAME'], acct_id, years[0], years[-1]))
else:
	cu_utils.start_timer("Aggregating data for %s from %i to %i... (Warning: account id not found, data may be blank)" % (acct_id, years[0], years[-1]))

#aggregate data
data_per_credit_union = {}
for y in years:
	for record in cu_utils.csv_to_dict('data/QCR%i12/fs220.txt' % y):
		cu_number = record['CU_NUMBER']
		if acct_id in record:
			if cu_number not in data_per_credit_union:
				data_per_credit_union[cu_number] = {}
			if cu_utils.is_float(record[acct_id]):
				data_per_credit_union[cu_number][y] = float(record[acct_id])
				if int(data_per_credit_union[cu_number][y]) == data_per_credit_union[cu_number][y]:
					data_per_credit_union[cu_number][y] = int(data_per_credit_union[cu_number][y])
			else:
				data_per_credit_union[cu_number][y] = record[acct_id]
cu_utils.end_timer()

#write results to file
results_file = open('results/aggregate_account_%s' % acct_id.lower(), 'w')
results_file.write('"CU_NUMBER","CU_NAME",' + ','.join(['"VAL_%i"' % x for x in years]))
for cu_number in data_per_credit_union:
	s = '\n%s,"%s"' % (cu_number, cu_lookup[cu_number]['CU_NAME'].replace(',', ''))
	for y in years:
		if y in data_per_credit_union[cu_number]:
			if cu_utils.is_float(data_per_credit_union[cu_number][y]):
				s += ',%s' % str(data_per_credit_union[cu_number][y])
			else:
				s += ',"%s"' % data_per_credit_union[cu_number][y]
		else:
			s += ','
	results_file.write(s)
results_file.close()
print('Results written to results/aggregate_account_%s' % acct_id.lower())

