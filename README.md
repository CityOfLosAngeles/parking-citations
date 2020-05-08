# parking-citations
ETL on Civis of the Parking Citations from Xerox to Civis to ODP 


## To Run Locally 

Attached the environment variables in a `.env`. specifically 

```
1. FTP_PARKING_USERNAME
1. FTP_PARKING_PASSWORD
1. SOCRATA_USERNAME
1. SOCRATA_PASSWORD
``` 

This might fail locally, since you are not on whitelist IP for the conduent firewall. See .env.sample 

## To Run on Civis. 

Attach the correct credentials and run `src/update-citations.py`
