# The mock carrier API (services/carrier_api) on Azure Container Apps, so the daily pipeline's
# fetch_carrier_api task calls a real HTTPS endpoint, as it would a carrier's (DHL, FedEx, ...).
#
#   scripts/up.ps1 -> docker build (today's shipments in the image) -> push to this registry
#                  -> terraform apply -var carrier_api_image=<that image>   (the app below pulls it)
#   Databricks fetch_carrier_api -> https://<fqdn>/v1/shipments with X-API-Key
#
# The first apply creates the app with a public placeholder image (var.carrier_api_image's default):
# the session's image does not exist yet then, its data is generated later. up.ps1 then applies the
# app again with the image it pushed (not `az containerapp update`, a preview CLI extension that
# rewrote the app's other settings).
#
# Registry access: the admin user is off; the app pulls with a token that may only read the
# carrier-api repository (its password generated here, held only as a Container App secret). Not a
# managed identity: Azure creates the environment in "Express" mode on this subscription, and Express
# cannot pull with one (found on Azure: "Authentication failed" with a user-assigned identity,
# ExpressRegistrySystemIdentityNotSupported with the system one).
# The API key is the one Databricks already has (secrets.tf).
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

# Pull-only, one repository: the token cannot push, delete or read any other image
resource "azurerm_container_registry_scope_map" "carrier_api_pull" {
  name                    = "carrier-api-pull"
  container_registry_name = azurerm_container_registry.acr.name
  resource_group_name     = azurerm_resource_group.rg.name
  actions = [
    "repositories/carrier-api/content/read",
    "repositories/carrier-api/metadata/read",
  ]
}

resource "azurerm_container_registry_token" "carrier_api_pull" {
  name                    = "carrier-api-pull"
  container_registry_name = azurerm_container_registry.acr.name
  resource_group_name     = azurerm_resource_group.rg.name
  scope_map_id            = azurerm_container_registry_scope_map.carrier_api_pull.id
}

resource "azurerm_container_registry_token_password" "carrier_api_pull" {
  container_registry_token_id = azurerm_container_registry_token.carrier_api_pull.id
  password1 {}
}

resource "azurerm_container_app_environment" "cae" {
  name                = "cae-${local.base}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  tags                = local.tags

  # Azure reports this profile on the environment anyway; declaring it keeps plans free of drift
  workload_profile {
    name                  = "Consumption"
    workload_profile_type = "Consumption"
  }

  depends_on = [azurerm_resource_provider_registration.container_apps]
}

resource "azurerm_container_app" "carrier_api" {
  name                         = "ca-carrier-api-${var.name_suffix}"
  resource_group_name          = azurerm_resource_group.rg.name
  container_app_environment_id = azurerm_container_app_environment.cae.id
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  tags                         = local.tags

  registry {
    server               = azurerm_container_registry.acr.login_server
    username             = azurerm_container_registry_token.carrier_api_pull.name
    password_secret_name = "acr-pull-token"
  }

  secret {
    name  = "acr-pull-token"
    value = azurerm_container_registry_token_password.carrier_api_pull.password1[0].value
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
      image  = var.carrier_api_image
      cpu    = 0.25
      memory = "0.5Gi"

      env {
        name        = "CARRIER_API_KEY"
        secret_name = "carrier-api-key"
      }
    }
  }

  # Express manages these itself (it reports transport "http", no traffic split and 0 s scale timers),
  # so without this every plan would show the same change again
  lifecycle {
    ignore_changes = [
      ingress[0].transport,
      ingress[0].traffic_weight,
      template[0].cooldown_period_in_seconds,
      template[0].polling_interval_in_seconds,
    ]
  }
}
