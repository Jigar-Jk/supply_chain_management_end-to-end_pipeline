# Azure for Students subscription. Not secret: the subscription id comes from the ARM_SUBSCRIPTION_ID
# environment variable, never from this file.
#   terraform apply -var-file=envs/student.tfvars
location    = "Korea Central" # allowed by the subscription policy and offers Azure Databricks (UAE North is the other option)
name_suffix = "jk1"           # 1-5 lowercase letters/digits; makes storage / Key Vault / Event Hubs names unique
