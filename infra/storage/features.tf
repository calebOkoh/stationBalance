###############################################################################
# features.yaml, published as JSON.
#
# features.yaml is the human-facing train/serve contract (pipelines.md 0.4) and
# stays the single source of truth. But nothing that runs has a YAML parser:
# the Lambda runtime has no PyYAML, and relying on the EMR Serverless image
# happening to carry it is exactly the kind of implicit dependency that breaks
# on a release-label bump.
#
# Terraform has `yamldecode`, so the conversion happens at apply time. Editing
# features.yaml and re-applying is what republishes it -- there is no generated
# file to forget to regenerate, and a malformed edit fails the plan rather than
# a job three phases later.
###############################################################################
resource "aws_s3_object" "features_json" {
  bucket       = aws_s3_bucket.model.id
  key          = "code/features.json"
  content      = jsonencode(yamldecode(file("${path.module}/../../features.yaml")))
  content_type = "application/json"

  # Without this the object is replaced on every apply regardless of content.
  etag = md5(jsonencode(yamldecode(file("${path.module}/../../features.yaml"))))

  tags = {
    step = "catalog"
  }
}

# The YAML itself is published alongside it, unparsed. It carries the comments
# -- every "why" in the contract lives there and none of it survives yamldecode.
resource "aws_s3_object" "features_yaml" {
  bucket       = aws_s3_bucket.model.id
  key          = "code/features.yaml"
  source       = "${path.module}/../../features.yaml"
  etag         = filemd5("${path.module}/../../features.yaml")
  content_type = "text/yaml"

  tags = {
    step = "catalog"
  }
}
