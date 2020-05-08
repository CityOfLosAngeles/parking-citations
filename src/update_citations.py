# update_citations.py
# download the parking citations data from this month and last month, then upload it to socrata

# import required packages and login information
import pysftp
import datetime
import re
import os
import pandas as pd
import json
import math
from sodapy import Socrata as sc

print("Succesfully import librarys, beginging FTP session")
# tell pysftp that it's ok if the sftp site doesn't have a hostkey
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

# connect to sftp

host = '138.69.165.148'
username = os.environ.get('FTP_PARKING_USERNAME')
password = os.environ.get('FTP_PARKING_PASSWORD')
srv = pysftp.Connection(host=host, username=username,
password=password, cnopts = cnopts)

# get the file listing
all_files = srv.listdir()


print(all_files)


# get today's date
today = str(datetime.datetime.today()).split()[0]
yyyy = today[0:4]
mm = today[5:7]

# get last month's date
lm = int(mm) - 1
ly = int(yyyy)
if lm==0:
	lm=12
	ly=ly - 1

lm = str(lm)
ly = str(ly)

if len(lm)==1:
	lm = '0' + lm

# pull all files from this month or last month
last_month = re.compile(ly + lm + '\d\d.csv')
this_month = re.compile(yyyy + mm + '\d\d.csv')
file_requests = []
for file in all_files:
	m1 = last_month.search(file)
	m2 = this_month.search(file)
	if (m1 or m2):
		file_requests.append(file)


# download the files
for file in file_requests:
	srv.get(file)

# close the connection to the sftp
srv.close()



### merge the files into a single csv to upload

# column names to include in the file
colnames = ["Ticket number","Issue Date","Issue time","Meter Id","Marked Time","RP State Plate",
            "Plate Expiry Date","VIN","Make","Body Style","Color","Location","Route","Agency",
            "Violation code","Violation Description","Fine amount","Latitude","Longitude",
            "Agency Desc","Color Desc","Make","Body Style Desc"]
f = open('colnames.csv','w')
f.write('"' + '","'.join(colnames) + '"' + '\n')
f.close()

system_call = 'type colnames.csv ' + ' '.join(file_requests) + ' > ' + today + '-upload.csv'
os.system(system_call)

# read in the data and convert to json format
data = pd.read_csv(today + '-upload.csv')

data=data[["Ticket number","Issue Date","Issue time","Meter Id","Marked Time","RP State Plate",
            "Plate Expiry Date","VIN","Make","Body Style","Color","Location","Route","Agency",
            "Violation code","Violation Description","Fine amount","Latitude","Longitude",
            "Agency Desc","Color Desc","Body Style Desc"]]

data.columns=["Ticket number","Issue Date","Issue time","Meter Id","Marked Time","RP State Plate",
            "Plate Expiry Date","VIN","Make","Body Style","Color","Location","Route","Agency",
            "Violation code","Violation Description","Fine amount","Latitude","Longitude",
            "Agency Description","Color Description","Body Style Description"]

json_str = data.to_json(orient='records')
json_data = json.loads(json_str)


### Upload to socrata
# establish connection to api

client = sc(domain = "data.lacity.org", app_token = "K0lgodxtUCxf7AH5Q3qegOwCJ", 
	username = os.environ.get('SOCRATA_USERNAME'), password = os.environ.get('SOCRATA_PASSWORD'))

# upsert in small batches to avoid timeout
batch_size = 1000
n_batches = math.ceil(len(json_data) / batch_size)

for i in range(n_batches):
	b1 = i * batch_size
	b2 = (i + 1) * batch_size
	try:
		client.upsert('wjz9-h9np', json_data[b1:b2])
		print("upload succeeded for rows " + str(b1) + " to " + str(b2) + ".")
	except Exception as e:
		print("upload failed for rows " + str(b1) + " to " + str(b2) + ".")
		print(e)

# close the client
client.close()

# remove the data files
system_call = 'del ' + ' '.join(file_requests) + ' ' + today + '-upload.csv'
os.system(system_call)

