import os, csv, time

timer = None

def start_timer(s):
	global timer
	timer = time.clock()
	print((s + "...").ljust(80)),

def end_timer():
	global timer
	print("took %f seconds" % (time.clock() - timer))

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
				record[headers[i]] = row[i]
			records.append(record)
	return records

def is_float(value):
	try:
		float(value)
		return True
	except ValueError:
		return False

def find_years_of_data():
	return [int(x[3:-2]) for x in os.walk("./data").next()[1]]

def generate_credit_union_lookup(years):
	cu_lookup = {}
	for y in years[::-1]:
		for cu in csv_to_dict('data/QCR%i12/foicu.txt' % y):
			cu_number = cu['CU_NUMBER']
			if cu_number not in cu_lookup:
				cu_lookup[cu_number] = { 'CU_NAME': cu['CU_NAME'] }
	return cu_lookup

def generate_account_lookup(years):
	acct_lookup = {}
	for y in years[::-1]:
		for acct in csv_to_dict('data/QCR%i12/AcctDesc.txt' % y):
			acct_id = acct['Account'].upper()
			if acct_id not in acct_lookup:
				acct_lookup[acct_id] = { 'ACCT_NAME': acct['AcctName'] }
	return acct_lookup
