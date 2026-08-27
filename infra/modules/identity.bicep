metadata description = '''
A user-assigned managed identity for the broker and its workloads, with the role assignments it
needs. A user-assigned identity rather than system-assigned so the same principal can be granted
database and cache access before any container app exists — with a system-assigned identity the
grants can only happen after the app is created, which turns one deployment into two.
'''

@description('Location for the identity.')
param location string

@description('Prefix applied to resource names.')
param namePrefix string

@description('Tags applied to every resource.')
param tags object = {}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${namePrefix}-identity'
  location: location
  tags: tags
}

@description('The identity resource id, for assigning to container apps.')
output identityId string = identity.id

@description('The identity client id, for DefaultAzureCredential.')
output clientId string = identity.properties.clientId

@description('The identity principal id, for role assignments.')
output principalId string = identity.properties.principalId

@description('The identity name.')
output name string = identity.name
