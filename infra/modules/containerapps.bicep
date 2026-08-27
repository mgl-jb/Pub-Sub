metadata description = '''
The Container Apps environment and the broker, publisher, and consumer apps.

Scale rules differ by role on purpose. The broker scales on HTTP concurrency, because that is what
its load actually is. The consumer cannot: its work arrives as messages, not requests, so scaling
it on HTTP would leave it at one replica under any backlog.
'''

@description('Location for the environment.')
param location string

@description('Prefix applied to resource names.')
param namePrefix string

@description('Resource id of the user-assigned identity.')
param identityId string

@description('Client id of the user-assigned identity, for DefaultAzureCredential.')
param identityClientId string

@description('Log Analytics workspace resource id.')
param workspaceId string

@description('Application Insights connection string.')
param applicationInsightsConnectionString string

@description('Passwordless SQL connection string.')
param sqlConnectionString string

@description('Redis connection string.')
param redisConnectionString string

@description('Container registry login server.')
param registryServer string

@description('Broker image, including tag.')
param brokerImage string

@description('Orders API image, including tag.')
param ordersImage string

@description('Shipping worker image, including tag.')
param shippingImage string

@description('Entra ID authority for token validation.')
param authority string

@description('Expected audience for broker tokens.')
param audience string

@description('Tags applied to every resource.')
param tags object = {}

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${namePrefix}-env'
  location: location
  tags: tags
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: reference(workspaceId, '2023-09-01').customerId
        sharedKey: listKeys(workspaceId, '2023-09-01').primarySharedKey
      }
    }
    zoneRedundant: false
  }
}

var identityConfiguration = {
  type: 'UserAssigned'
  userAssignedIdentities: {
    '${identityId}': {}
  }
}

var registries = [
  {
    server: registryServer
    identity: identityId
  }
]

var commonEnvironment = [
  {
    name: 'AZURE_CLIENT_ID'
    value: identityClientId
  }
  {
    name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
    value: applicationInsightsConnectionString
  }
]

resource broker 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-broker'
  location: location
  tags: tags
  identity: identityConfiguration
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
        allowInsecure: false
      }
      registries: registries
    }
    template: {
      containers: [
        {
          name: 'broker'
          image: brokerImage
          resources: {
            cpu: json('1.0')
            memory: '2Gi'
          }
          env: concat(commonEnvironment, [
            {
              name: 'ConnectionStrings__Broker'
              value: sqlConnectionString
            }
            {
              name: 'Redis__ConnectionString'
              value: redisConnectionString
            }
            {
              name: 'Broker__Authority'
              value: authority
            }
            {
              name: 'Broker__Audience'
              value: audience
            }
          ])
          probes: [
            {
              // Liveness deliberately ignores the database: restarting the process would not fix
              // a database outage, and would turn a brief blip into a restart loop.
              type: 'Liveness'
              httpGet: {
                path: '/health/live'
                port: 8080
              }
              initialDelaySeconds: 10
              periodSeconds: 30
            }
            {
              // Readiness does check it, so an instance that cannot reach SQL stops receiving
              // traffic without being killed.
              type: 'Readiness'
              httpGet: {
                path: '/health/ready'
                port: 8080
              }
              initialDelaySeconds: 5
              periodSeconds: 10
              failureThreshold: 3
            }
          ]
        }
      ]
      scale: {
        // Never scales to zero: the broker holds long-polling connections and runs the sweeper,
        // and a cold start would stall dispatch for everyone.
        minReplicas: 2
        maxReplicas: 10
        rules: [
          {
            name: 'http-concurrency'
            http: {
              metadata: {
                concurrentRequests: '50'
              }
            }
          }
        ]
      }
    }
  }
}

resource ordersApi 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-orders'
  location: location
  tags: tags
  identity: identityConfiguration
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
        allowInsecure: false
      }
      registries: registries
    }
    template: {
      containers: [
        {
          name: 'orders'
          image: ordersImage
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: concat(commonEnvironment, [
            {
              name: 'ConnectionStrings__Orders'
              value: replace(sqlConnectionString, 'Database=PubSubBroker', 'Database=SampleOrders')
            }
            {
              name: 'PubSub__BrokerUri'
              value: 'https://${broker.properties.configuration.ingress.fqdn}'
            }
          ])
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 5
        rules: [
          {
            name: 'http-concurrency'
            http: {
              metadata: {
                concurrentRequests: '100'
              }
            }
          }
        ]
      }
    }
  }
}

resource shippingWorker 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-shipping'
  location: location
  tags: tags
  identity: identityConfiguration
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      // No ingress: this app only consumes.
      registries: registries
    }
    template: {
      containers: [
        {
          name: 'shipping'
          image: shippingImage
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: concat(commonEnvironment, [
            {
              name: 'ConnectionStrings__Shipping'
              value: replace(sqlConnectionString, 'Database=PubSubBroker', 'Database=SampleShipping')
            }
            {
              name: 'PubSub__BrokerUri'
              value: 'https://${broker.properties.configuration.ingress.fqdn}'
            }
          ])
        }
      ]
      scale: {
        // A consumer's load is its backlog, which no HTTP metric can see. Scaling on queue depth
        // needs a KEDA scaler reading the broker's metrics; until one is wired up, a fixed replica
        // count is honest about that rather than pretending HTTP concurrency is a proxy for it.
        minReplicas: 2
        maxReplicas: 2
      }
    }
  }
}

@description('The broker environment name.')
output environmentName string = environment.name

@description('The broker ingress host name.')
output brokerFqdn string = broker.properties.configuration.ingress.fqdn

@description('The orders API ingress host name.')
output ordersFqdn string = ordersApi.properties.configuration.ingress.fqdn

@description('The shipping worker app name.')
output shippingAppName string = shippingWorker.name
