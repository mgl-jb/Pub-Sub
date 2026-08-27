metadata description = '''
The PubSub messaging system on Azure.

The broker is built rather than bought, so what is deployed here is Azure's non-messaging
primitives: SQL Database as the durable store, Cache for Redis to accelerate dispatch, Container
Apps to run the workloads, and Entra ID plus a managed identity so no connection secret exists
anywhere in this template or its outputs.
'''

targetScope = 'resourceGroup'

@description('Location for every resource. Defaults to the resource group\'s.')
param location string = resourceGroup().location

@description('Short name distinguishing this deployment, for example "pubsub-prod".')
@minLength(3)
@maxLength(20)
param namePrefix string

@description('Object id of the Entra group administering the SQL server.')
param sqlAdministratorObjectId string

@description('Display name of that group.')
param sqlAdministratorName string

@description('Container registry login server, for example "myregistry.azurecr.io".')
param registryServer string

@description('Broker image, including tag.')
param brokerImage string

@description('Orders API image, including tag.')
param ordersImage string

@description('Shipping worker image, including tag.')
param shippingImage string

@description('Entra ID authority used to validate broker tokens.')
param authority string

@description('Expected audience for broker tokens.')
param audience string

@description('Environment name, used for tagging and sizing.')
@allowed(['dev', 'prod'])
param environmentName string = 'dev'

var tags = {
  application: 'pubsub'
  environment: environmentName
  managedBy: 'bicep'
}

// Production gets a redundant cache and a larger database; dev gets neither, because paying for
// resilience in an environment that is redeployed daily buys nothing.
var isProduction = environmentName == 'prod'

module identity 'modules/identity.bicep' = {
  name: 'identity'
  params: {
    location: location
    namePrefix: namePrefix
    tags: tags
  }
}

module monitor 'modules/monitor.bicep' = {
  name: 'monitor'
  params: {
    location: location
    namePrefix: namePrefix
    retentionInDays: isProduction ? 90 : 30
    tags: tags
  }
}

module sql 'modules/sql.bicep' = {
  name: 'sql'
  params: {
    location: location
    namePrefix: namePrefix
    administratorObjectId: sqlAdministratorObjectId
    administratorName: sqlAdministratorName
    skuName: isProduction ? 'GP_Gen5_4' : 'GP_S_Gen5_2'
    maxSizeBytes: isProduction ? 137438953472 : 34359738368
    tags: tags
  }
}

module redis 'modules/redis.bicep' = {
  name: 'redis'
  params: {
    location: location
    namePrefix: namePrefix
    principalId: identity.outputs.principalId
    skuName: isProduction ? 'Premium' : 'Standard'
    skuCapacity: 1
    tags: tags
  }
}

module apps 'modules/containerapps.bicep' = {
  name: 'containerapps'
  params: {
    location: location
    namePrefix: namePrefix
    identityId: identity.outputs.identityId
    identityClientId: identity.outputs.clientId
    workspaceId: monitor.outputs.workspaceId
    applicationInsightsConnectionString: monitor.outputs.applicationInsightsConnectionString
    sqlConnectionString: sql.outputs.connectionString
    redisConnectionString: redis.outputs.connectionString
    registryServer: registryServer
    brokerImage: brokerImage
    ordersImage: ordersImage
    shippingImage: shippingImage
    authority: authority
    audience: audience
    tags: tags
  }
}

@description('The broker ingress host name.')
output brokerFqdn string = apps.outputs.brokerFqdn

@description('The orders API ingress host name.')
output ordersFqdn string = apps.outputs.ordersFqdn

@description('The SQL server address.')
output sqlServerFqdn string = sql.outputs.serverFqdn

@description('''
The managed identity's principal id. This principal still needs a database user created for it —
Bicep cannot execute SQL, so that step is a script. See docs/operations.md.
''')
output identityPrincipalId string = identity.outputs.principalId

@description('The managed identity name, used when creating the database user.')
output identityName string = identity.outputs.name
