metadata description = '''
Log Analytics and Application Insights. A workspace-based Application Insights component, because
the classic standalone form is retired and cannot be created in new deployments.
'''

@description('Location for the workspace.')
param location string

@description('Prefix applied to resource names.')
param namePrefix string

@description('How long telemetry is retained, in days.')
param retentionInDays int = 30

@description('Tags applied to every resource.')
param tags object = {}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${namePrefix}-logs'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: retentionInDays
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
  }
}

resource applicationInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${namePrefix}-insights'
  location: location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: workspace.id
    IngestionMode: 'LogAnalytics'
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

@description('The workspace resource id.')
output workspaceId string = workspace.id

@description('The workspace customer id, for the Container Apps environment.')
output workspaceCustomerId string = workspace.properties.customerId

@description('The Application Insights connection string.')
output applicationInsightsConnectionString string = applicationInsights.properties.ConnectionString
