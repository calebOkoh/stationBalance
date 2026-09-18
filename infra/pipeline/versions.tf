terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Paths are relative to the working directory, so this layer must always be
  # run via `terraform -chdir=infra/pipeline` (what deploy-pipeline.sh does).
  backend "local" {
    path = "../states/pipeline/terraform.tfstate"
  }
}
