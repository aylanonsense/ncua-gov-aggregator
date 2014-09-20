#!/usr/bin/python

import sys, cu_utils

#get the credit number as an argument
if len(sys.argv) < 2:
	print("You must supply a credit union number, e.g. 1234")
	sys.exit()
cu_number = sys.argv[1]

#init vars
years = cu_utils.find_years_of_data()
cu_lookup = cu_utils.generate_credit_union_lookup(years)
acct_lookup = cu_utils.generate_account_lookup(years)

#Warn if credit union might not exist
if cu_number in cu_lookup:
	cu_utils.start_timer("Aggregating data for %s (%s) from %i to %i..." % (cu_lookup[cu_number]['CU_NAME'], cu_number, years[0], years[-1]))
else:
	cu_utils.start_timer("Aggregating data for credit union %s from %i to %i... (Warning: credit union not found, data may be blank)" % (cu_number, years[0], years[-1]))

#aggregate data
data_per_account = {}
for y in years:
	for record in cu_utils.csv_to_dict('data/QCR%i12/fs220.txt' % y):
		if record['CU_NUMBER'] == cu_number:
			for acct_id in acct_lookup:
				if acct_id in record:
					if acct_id not in data_per_account:
						data_per_account[acct_id] = {}
					if cu_utils.is_float(record[acct_id]):
						data_per_account[acct_id][y] = float(record[acct_id])
						if int(data_per_account[acct_id][y]) == data_per_account[acct_id][y]:
							data_per_account[acct_id][y] = int(data_per_account[acct_id][y])
					else:
						data_per_account[acct_id][y] = record[acct_id]
			break
cu_utils.end_timer()

#write results to file
results_file = open('results/aggregate_credit_union_%s' % cu_number, 'w')
results_file.write('"ACCT_ID","ACCT_NAME",' + ','.join(['"VAL_%i"' % x for x in years]))
for acct_id in data_per_account:
	s = '\n"%s","%s"' % (acct_id.replace(',', ''), acct_lookup[acct_id]['ACCT_NAME'].replace(',', ''))
	for y in years:
		if y in data_per_account[acct_id]:
			if cu_utils.is_float(data_per_account[acct_id][y]):
				s += ',%s' % str(data_per_account[acct_id][y])
			else:
				s += ',"%s"' % data_per_account[acct_id][y]
		else:
			s += ','
	results_file.write(s)
results_file.close()
print('Results written to results/aggregate_credit_union_%s' % cu_number)

