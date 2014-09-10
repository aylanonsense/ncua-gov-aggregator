#!/usr/bin/python
import MySQLdb, csv, sys, time

if len(sys.argv) < 5:
	print("You must include database connection variables as arguments, e.g. localhost root '' NCUA")
	sys.exit()

#init variales
very_start_time = time.clock()
suppress_warnings = True
years = range(2009, 2014)
limit_to_important_accounts = False
important_accounts = ['ACCT_010']
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

start_timer("Connecting to the database")
db = MySQLdb.connect(sys.argv[1], sys.argv[2] ,sys.argv[3] ,sys.argv[4])
cursor = db.cursor()
end_timer()

start_timer("Dropping tables")
cursor.execute("DROP TABLE IF EXISTS CREDIT_UNION_ACCOUNTS")
cursor.execute("DROP TABLE IF EXISTS CREDIT_UNIONS")
cursor.execute("DROP TABLE IF EXISTS ACCOUNTS")
end_timer()

start_timer("Recreating tables")
cursor.execute("CREATE TABLE CREDIT_UNIONS (CU_ID INT NOT NULL, CU_NAME VARCHAR(100), PRIMARY KEY(CU_ID))")
cursor.execute("CREATE TABLE ACCOUNTS (ACCT_ID CHAR(30) NOT NULL, ACCT_NAME VARCHAR(200), PRIMARY KEY(ACCT_ID))")
sql = "CREATE TABLE CREDIT_UNION_ACCOUNTS (CU_ID INT, ACCT_ID CHAR(30) NOT NULL, "
sql += ", ".join([ "VAL_%i FLOAT(20,2)" % y for y in years ]) + ", "
sql += "PRIMARY KEY(CU_ID,ACCT_ID),INDEX(ACCT_ID), "
sql += "FOREIGN KEY (CU_ID) REFERENCES CREDIT_UNIONS(CU_ID), "
sql += "FOREIGN KEY (ACCT_ID) REFERENCES ACCOUNTS(ACCT_ID))"
cursor.execute(sql)
end_timer()

start_timer("Aggregating credit union info")
credit_union_lookup = {}
for y in years:
	for credit_union in csv_to_dict('data/QCR%i12/foicu.txt' % y):
		if credit_union['CU_NUMBER'] not in credit_union_lookup:
			credit_union_lookup[credit_union['CU_NUMBER']] = { 'CU_NAME': credit_union['CU_NAME'] }
		elif credit_union['CU_NAME'] != credit_union_lookup[credit_union['CU_NUMBER']]['CU_NAME'] and not suppress_warnings:
			print("  Warning: In %i credit union %s changed its name from %s to %s" % (y, credit_union['CU_NUMBER'], credit_union_lookup[credit_union['CU_NUMBER']]['CU_NAME'], credit_union['CU_NAME']))
end_timer()

start_timer("Inserting credit union info into database")
for x in credit_union_lookup:
	cursor.execute("INSERT INTO CREDIT_UNIONS VALUES ('%s', '%s')" % (x, credit_union_lookup[x]['CU_NAME'].replace("'", "\\'")))
end_timer()

start_timer("Aggregating account info")
account_lookup = {}
for y in years:
	for account in csv_to_dict('data/QCR%i12/AcctDesc.txt' % y):
		account_id = account['Account'].upper()
		if account_id not in account_lookup:
			account_lookup[account_id] = { 'ACCT_NAME': account['AcctName'] }
		elif account['AcctName'] != account_lookup[account_id]['ACCT_NAME'] and not suppress_warnings:
			print("  Warning: In %i account %s changed definitions from '%s' to '%s'" % (y, account_id, account_lookup[account_id]['ACCT_NAME'], account['AcctName']))
end_timer()

start_timer("Inserting account info into database")
for x in account_lookup:
	cursor.execute("INSERT INTO ACCOUNTS VALUES ('%s', '%s')" % (x, account_lookup[x]['ACCT_NAME'].replace("'", "\\'")))
end_timer()

start_timer("Gathering credit union account data across time intervals")
credit_union_account_data = {}
for y in years:
	for record in csv_to_dict('data/QCR%i12/fs220.txt' % y):
		if record['CU_NUMBER'] not in credit_union_account_data:
			credit_union_account_data[record['CU_NUMBER']] = {}
		for x in account_lookup:
			if x in record and (not limit_to_important_accounts or x in important_accounts):
				if x not in credit_union_account_data[record['CU_NUMBER']]:
					credit_union_account_data[record['CU_NUMBER']][x] = {}
				credit_union_account_data[record['CU_NUMBER']][x][y] = record[x]
end_timer()

start_timer("Inserting credit union account data into database")
for cu_id in credit_union_account_data:
	for acct_id in credit_union_account_data[cu_id]:
		sql = "INSERT INTO CREDIT_UNION_ACCOUNTS VALUES (%s, '%s'" % (cu_id, acct_id)
		for y in years:
			sql += ", %s" % (credit_union_account_data[cu_id][acct_id][y] if y in credit_union_account_data[cu_id][acct_id] else 'null')
		sql += ")"
		cursor.execute(sql)
end_timer()

start_timer("Committing changes and closing database connection")
db.commit()
db.close()
end_timer()

print("Done! Script ran for a total of %f seconds" % (time.clock() - very_start_time))
