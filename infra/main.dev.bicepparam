using 'main.bicep'

param namePrefix = 'pubsub-dev'
param environmentName = 'dev'

// Replace with the object id and name of the Entra group that should administer SQL.
param sqlAdministratorObjectId = '00000000-0000-0000-0000-000000000000'
param sqlAdministratorName = 'PubSub SQL Admins'

param registryServer = 'REPLACE.azurecr.io'
param brokerImage = 'REPLACE.azurecr.io/pubsub-broker:latest'
param ordersImage = 'REPLACE.azurecr.io/pubsub-orders:latest'
param shippingImage = 'REPLACE.azurecr.io/pubsub-shipping:latest'

param authority = 'https://login.microsoftonline.com/REPLACE-TENANT-ID/v2.0'
param audience = 'api://REPLACE-APP-ID'
