#!/bin/bash

# Setup script for Apache Iceberg V4 Real Benchmarks
# Creates AWS resources needed for benchmarking

set -e

echo "=========================================="
echo "Apache Iceberg V4 Real Benchmarks - Setup"
echo "=========================================="

# Configuration
BUCKET_NAME="${1:-iceberg-v4-benchmarks}"
AWS_REGION="${2:-us-east-1}"

echo ""
echo "Configuration:"
echo "  S3 Bucket: $BUCKET_NAME"
echo "  AWS Region: $AWS_REGION"
echo ""

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI not found. Please install: https://aws.amazon.com/cli/"
    exit 1
fi

# Check AWS credentials
echo "Checking AWS credentials..."
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS credentials not configured. Run: aws configure"
    exit 1
fi
echo "✅ AWS credentials configured"

# Create S3 bucket
echo ""
echo "Creating S3 bucket: $BUCKET_NAME..."
if aws s3 ls "s3://$BUCKET_NAME" 2>&1 | grep -q 'NoSuchBucket'; then
    aws s3 mb "s3://$BUCKET_NAME" --region "$AWS_REGION"
    echo "✅ Bucket created"
else
    echo "✅ Bucket already exists"
fi

# Enable versioning (optional, for safety)
echo "Enabling versioning..."
aws s3api put-bucket-versioning \
    --bucket "$BUCKET_NAME" \
    --versioning-configuration Status=Enabled
echo "✅ Versioning enabled"

# Create folder structure
echo ""
echo "Creating folder structure..."
aws s3api put-object --bucket "$BUCKET_NAME" --key data/
aws s3api put-object --bucket "$BUCKET_NAME" --key manifests/
aws s3api put-object --bucket "$BUCKET_NAME" --key delete-vectors/
aws s3api put-object --bucket "$BUCKET_NAME" --key results/
echo "✅ Folders created"

# Set lifecycle policy (auto-delete after 7 days)
echo ""
echo "Setting lifecycle policy (auto-delete after 7 days)..."
cat > /tmp/lifecycle.json <<EOF
{
  "Rules": [
    {
      "Id": "DeleteAfter7Days",
      "Status": "Enabled",
      "Prefix": "",
      "Expiration": {
        "Days": 7
      }
    }
  ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
    --bucket "$BUCKET_NAME" \
    --lifecycle-configuration file:///tmp/lifecycle.json
echo "✅ Lifecycle policy set"

echo ""
echo "=========================================="
echo "✅ Setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. cd ../java"
echo "  2. mvn clean install"
echo "  3. java -jar target/iceberg-v4-benchmarks.jar --benchmark DeleteStorm --s3-bucket $BUCKET_NAME"
echo ""
echo "Estimated AWS costs: ~\$5-10 for full benchmark suite"
echo ""
