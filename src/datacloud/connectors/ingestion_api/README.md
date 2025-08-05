# Data Cloud Ingestion API Connector

This is an OAuth based Data Cloud Ingestion API Connector for writing memories into Data Cloud.

The connector works over REST API with the Data Cloud with Bearer Auth.

The bearer auth token needs to be created with the Bearer OAuth flow with the Salesforce OAuth Server at login.salesforce.com.
For this you will need to setup a connected App in your Salesforce Org, and enable Bearer Auth flow on the app.
This connector assumes 

### Environment Variables for load_env:
#### Salesforce Vars:
- SALESFORCE_APP_SUBJECT
- SALESFORCE_ORGANIZATION_ID
- SALESFORCE_APP_PRIVATE_KEY
- SALESFORCE_APP_CONSUMER_KEY
- SALESFORCE_TOKEN_GRANT_TYPE
- SALESFORCE_INSTANCE_URL
#### Data Cloud Vars:
- DATA_CLOUD_INSTANCE_URL
- DATA_CLOUD_TOKEN_GRANT_TYPE
- DATA_CLOUD_SUBJECT_TOKEN_TYPE
