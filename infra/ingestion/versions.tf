terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Paths are relative to the working directory, so this layer must always be
  # run via `terraform -chdir=infra/ingestion` (what deploy-ingestion.sh does).
  backend "local" {
    path = "../states/ingestion/terraform.tfstate"
  }
}
