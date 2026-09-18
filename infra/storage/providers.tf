provider "aws" {
  region = var.aws_region

  # Deliberately no `profile` argument. The AWS provider's `profile` takes
  # precedence over AWS_PROFILE, so setting it here would silently ignore
  # whatever the deploy script exported — and fail with "failed to get shared
  # config profile" if the pinned name did not happen to exist. Credentials
  # come from AWS_PROFILE and the default chain, nothing else.

  # Applied to every taggable resource in this layer, so nothing has to be
  # tagged individually and nothing can be missed. The `step` tag is NOT set
  # here — it varies per resource and is merged in at each resource instead.
  default_tags {
    tags = {
      service = var.service
    }
  }
}
