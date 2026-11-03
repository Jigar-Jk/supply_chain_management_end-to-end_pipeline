# The mock carrier API (services/carrier_api) on Azure Container Apps, so the daily pipeline's
# fetch_carrier_api task calls a real HTTPS endpoint, as it would a carrier's (DHL, FedEx, ...).
#
#   scripts/up.ps1 -> docker build (today's shipments in the image) -> push to this registry
#                  -> az containerapp update --image   (the app below then pulls it)
#   Databricks fetch_carrier_api -> https://<fqdn>/v1/shipments with X-API-Key
#
# Terraform owns everything but the image tag: the app is created with a public placeholder image and
# up.ps1 rolls out each session's image (ignore_changes below), the way an app pipeline deploys onto
# infrastructure. No passwords: the registry admin user is off and the app pulls with its own
# managed identity (AcrPull). The API key is the one Databricks already has (secrets.tf).
# Consumption plan, no VM cores (the 6-core student quota is untouched); destroyed with the stack.

# Container Apps is not among the providers Terraform registers by itself on this subscription
resource "azurerm_resource_provider_registration" "container_apps" {
  name = "Microsoft.App"
}

resource "azurerm_container_registry" "acr" {
  name                = "acr${replace(local.base, "-", "")}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  sku                 = "Basic"
  admin_enabled       = false
  tags                = local.tags
}

resource "azurerm_user_assigned_identity" "carrier_api" {
  name                = "id-carrier-api-${local.base}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  tags                = local.tags
}

resource "azurerm_role_assignment" "carrier_api_acr_pull" {
  scope                = azurerm_container_registry.acr.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.carrier_api.principal_id
}

resource "azurerm_container_app_environment" "cae" {
  name                = "cae-${local.base}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  tags                = local.tags
  depends_on          = [azurerm_resource_provider_registration.container_apps]
}

resource "azurerm_container_app" "carrier_api" {
  name                         = "ca-carrier-api-${var.name_suffix}"
  resource_group_name          = azurerm_resource_group.rg.name
  container_app_environment_id = azurerm_container_app_environment.cae.id
  revision_mode                = "Single"
  tags                         = local.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.carrier_api.id]
  }

  registry {
    server   = azurerm_container_registry.acr.login_server
    identity = azurerm_user_assigned_identity.carrier_api.id
  }

  secret {
    name  = "carrier-api-key"
    value = random_password.carrier_api_key.result
  }

  ingress {
    external_enabled           = true
    target_port                = 80
    allow_insecure_connections = false # HTTPS only: the key travels in a header
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }

  template {
    # One replica, always on: no cold start in the middle of a pipeline run, and the in-memory data
    # is the same on every request
    min_replicas = 1
    max_replicas = 1

    container {
      name   = "carrier-api"
      image  = "mcr.microsoft.com/k8se/quickstart:latest" # placeholder until up.ps1 rolls out the real image
      cpu    = 0.25
      memory = "0.5Gi"

      env {
        name        = "CARRIER_API_KEY"
        secret_name = "carrier-api-key"
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].container[0].image]
  }

  depends_on = [azurerm_role_assignment.carrier_api_acr_pull]
}
