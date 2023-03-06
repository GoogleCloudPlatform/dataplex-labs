terraform {
  required_version = ">= 0.13"
  required_providers {
    google = {
      source  = "hashicorp/google-beta"
      version = "4.15.0"
    }
  }
  provider_meta "google" {
    module_name = "blueprints/terraform/test/v0.0.1"
  }
}
