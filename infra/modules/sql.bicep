metadata description = '''
Azure SQL Database holding the broker's system of record.

Entra-only authentication is enforced: there is no SQL administrator password to store, rotate, or
leak, and the managed identity is the only way in. That is why the identity's details are required
inputs rather than optional ones — a server with neither an Entra admin nor SQL authentication
would be unreachable.
'''

@description('Location for the server.')
param location string

@description('Prefix applied to resource names.')
param namePrefix string

@description('Object id of the Entra principal that administers the server.')
param administratorObjectId string

@description('Display name of the administering principal.')
param administratorName string

@description('Database SKU. Serverless General Purpose suits bursty broker workloads.')
param skuName string = 'GP_S_Gen5_2'

@description('Maximum database size in bytes.')
param maxSizeBytes int = 34359738368

@description('Tags applied to every resource.')
param tags object = {}

resource sqlServer 'Microsoft.Sql/servers@2023-08-01-preview' = {
  name: '${namePrefix}-sql'
  location: location
  tags: tags
  properties: {
    // Entra-only: no SQL login exists, so there is no password to manage.
    administrators: {
      administratorType: 'ActiveDirectory'
      azureADOnlyAuthentication: true
      login: administratorName
      principalType: 'Group'
      sid: administratorObjectId
      tenantId: subscription().tenantId
    }
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

resource database 'Microsoft.Sql/servers/databases@2023-08-01-preview' = {
  parent: sqlServer
  name: 'PubSubBroker'
  location: location
  tags: tags
  sku: {
    name: skuName
    tier: 'GeneralPurpose'
  }
  properties: {
    maxSizeBytes: maxSizeBytes

    // Serverless: the broker's load is bursty, and paying for idle capacity between bursts is
    // avoidable. Raise the delay or move to provisioned if cold-start latency matters.
    autoPauseDelay: 60
    minCapacity: json('0.5')

    // Read-committed snapshot keeps readers from blocking the claim query's writers. Without it,
    // an admin listing could hold up message dispatch.
    readScale: 'Disabled'
    zoneRedundant: false
  }
}

resource allowAzureServices 'Microsoft.Sql/servers/firewallRules@2023-08-01-preview' = {
  parent: sqlServer
  name: 'AllowAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

@description('The server name.')
output serverName string = sqlServer.name

@description('The fully qualified server address.')
output serverFqdn string = sqlServer.properties.fullyQualifiedDomainName

@description('The database name.')
output databaseName string = database.name

@description('''
A passwordless connection string. Authentication happens through the managed identity, so nothing
secret appears here or in any deployment output.
''')
output connectionString string = 'Server=tcp:${sqlServer.properties.fullyQualifiedDomainName},1433;Database=${database.name};Authentication=Active Directory Default;Encrypt=True;TrustServerCertificate=False;'
