provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  # Applied to every taggable resource in this layer, so nothing has to be
  # tagged individually and nothing can be missed. The `step` tag is NOT set
  # here — it varies per resource and is merged in at each resource instead.
  default_tags {
    tags = {
      service = var.service
    }
  }
}
