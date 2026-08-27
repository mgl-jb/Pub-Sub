metadata description = '''
Azure Cache for Redis, used only to accelerate dispatch and elect the sweeper leader.

The broker treats this as optional at runtime, so losing it costs latency rather than messages.
That is why access keys are disabled: with Entra authentication there is no key to rotate, and a
cache the broker can survive losing is not worth introducing a secret for.
'''

@description('Location for the cache.')
param location string

@description('Prefix applied to resource names.')
param namePrefix string

@description('Principal id of the identity granted data access.')
param principalId string

@description('Cache SKU.')
@allowed(['Basic', 'Standard', 'Premium'])
param skuName string = 'Standard'

@description('Cache size family and capacity.')
param skuCapacity int = 1

@description('Tags applied to every resource.')
param tags object = {}

resource cache 'Microsoft.Cache/redis@2024-03-01' = {
  name: '${namePrefix}-redis'
  location: location
  tags: tags
  properties: {
    sku: {
      name: skuName
      family: skuName == 'Premium' ? 'P' : 'C'
      capacity: skuCapacity
    }
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
    redisConfiguration: {
      // Entra only: no access keys to store or rotate.
      'aad-enabled': 'True'

      // The broker's use is pub/sub signalling and a small leadership key, so evicting the
      // least-recently-used entry under pressure is harmless.
      'maxmemory-policy': 'allkeys-lru'
    }
    disableAccessKeyAuthentication: true
  }
}

resource dataOwner 'Microsoft.Cache/redis/accessPolicyAssignments@2024-03-01' = {
  parent: cache
  name: guid(cache.id, principalId, 'Data Owner')
  properties: {
    accessPolicyName: 'Data Owner'
    objectId: principalId
    objectIdAlias: 'pubsub-broker'
  }
}

@description('The cache host name.')
output hostName string = cache.properties.hostName

@description('A connection string using Entra authentication; it carries no secret.')
output connectionString string = '${cache.properties.hostName}:6380,ssl=True,abortConnect=False'
