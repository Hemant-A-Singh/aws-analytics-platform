#!/bin/bash

set -euo pipefail

FUNCTION_NAME="analytics_platform"
REGION="ap-south-1"
RUNTIME="python3.12"
HANDLER="orchestrator.lambda_handler.lambda_handler"
TIMEOUT=900
MEMORY=512
ZIP_FILE="centralised_analytics_platform.zip"
PACKAGE_DIR="package"
DRY_RUN=false

for arg in "$@"; do
  case $arg in
    --dry-run) DRY_RUN=true
      ;;
  esac
done

echo "=================================================="
echo " Analytics Pipeline — Deployment"
echo " Function : $FUNCTION_NAME"
echo " Region   : $REGION"
echo " Dry run  : $DRY_RUN"
echo "=================================================="

echo ""
echo "── Step 1: Running unit tests ──"
pytest /tests -m "not integration" -q
echo "All unit tests passed..."

echo ""
echo "── Step 3: Installing dependencies ──"
pip install -r requirements.txt\
            --target "./$PACKAGE_DIR" \
            --upgrade \
            --quiet
echo "Dependencies installed successfully..."

echo ""
echo "── Step 4: Copying source code ──"
cp -r extractors    "$PACKAGE_DIR/"
cp -r loaders       "$PACKAGE_DIR/"
cp -r transformers  "$PACKAGE_DIR/"
cp -r orchestrator  "$PACKAGE_DIR/"
cp -r config        "$PACKAGE_DIR/"
cp -r sql           "$PACKAGE_DIR/"
echo "Source code copied"

echo ""
echo "── Step 5: Building deployment zip ──"
cd "$PACKAGE_DIR"
zip -r "../$ZIP_FILE" . -q
cd ..

ZIP_SIZE=$(du -h "$ZIP_FILE" | cut -f1)
echo "Zip built: $ZIP_FILE ($ZIP_SIZE)"

ZIP_BYTES=$(wc -c < "$ZIP_FILE")
if [ "$ZIP_BYTES" -gt 52428800 ]; then
  echo "  Zip exceeds 50MB — must upload via S3"
  UPLOAD_VIA_S3=true
else
  UPLOAD_VIA_S3=false
fi

if [ "$DRY_RUN" = true ]; then
  echo ""
  echo "── Dry run — skipping deploy ──"
  echo "Build validated. Deploy with: ./deploy.sh"
  exit 0
fi

echo ""
echo "── Step 6: Deploying to Lambda ──"

if [ "$UPLOAD_VIA_S3" = true ]; then

  S3_KEY="deployments/$ZIP_FILE"
  S3_BUCKET=$(grep S3_BUCKET .env | cut -d= -f2)
    echo "Uploading zip to S3 (file too large for direct upload)..."
    aws s3 cp "$ZIP_FILE" "s3://$S3_BUCKET/$S3_KEY" --region "$REGION"

    aws lambda update-function-code \
      --function-name "$FUNCTION_NAME" \
      --s3-bucket "$S3_BUCKET" \
      --s3-key "$S3_KEY" \
      --region "$REGION" \
      --output json | python3 -m json.tool
else
    echo "Uploading zip directly to Lambda..."
    aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_FILE" \
    --region "$REGION" \
    --output json | python3 -m json.tool
fi

# ── Step 7: Wait for update to complete ──────────────────────────
echo ""
echo "── Step 7: Waiting for Lambda update ──"
aws lambda wait function-updated \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION"
echo " Lambda updated"

# ── Step 8: Verify deployment ─────────────────────────────────────
echo ""
echo "── Step 8: Verifying deployment ──"
LAST_MODIFIED=$(aws lambda get-function \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --query 'Configuration.LastModified' \
  --output text)

CODE_SIZE=$(aws lambda get-function \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --query 'Configuration.CodeSize' \
  --output text)

echo "  Last modified : $LAST_MODIFIED"
echo "  Code size     : $CODE_SIZE bytes"
echo " Deployment verified"

echo ""
echo "── Step 9: (Running dry invoke) ──"
RESPONSE=$(aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --payload '{"source": "deploy-smoke-test"}' \
  --region "$REGION" \
  --log-type Tail \
  --query 'LogResult' \
  --output text \
  /tmp/lambda_response.json 2>/dev/null | base64 --decode | tail -5)

STATUS=$(cat /tmp/lambda_response.json | python3 -c "
import json,sys
r = json.load(sys.stdin)
print(r.get('statusCode', 'unknown'))
")

echo "  Lambda status code: $STATUS"

if [ "$STATUS" = "200" ]; then
  echo "✅ Smoke test passed"
else
  echo "⚠️  Lambda returned status $STATUS — check CloudWatch logs"
fi

echo ""
echo "=================================================="
echo " Deployment complete"
echo " Function  : $FUNCTION_NAME"
echo " Region    : $REGION"
echo " Timestamp : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=================================================="
