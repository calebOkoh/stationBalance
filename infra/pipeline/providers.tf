provider "aws" {
  region = var.aws_region

  # Deliberately no `profile` argument. The AWS provider's `profile` takes
  # precedence over AWS_PROFILE, so setting it here would silently ignore
  # whatever the deploy script exported — and fail with "failed to get shared
  # config profile" if the pinned name did not happen to exist. Credentials
  # come from AWS_PROFILE and the default chain, nothing else.

  default_tags {
    tags = {
      service = var.service
    }
  }
}
