#!/bin/bash
# VM startup script — rendered by Terraform templatefile().
# Terraform substitutes: cluster_name, region, project_id, bucket_name, groq_api_key
# All other $ signs are plain bash variables (no conflict).
set -euo pipefail

LOG=/var/log/kafka-app.log
exec > >(tee -a $LOG) 2>&1

echo "================================================================"
echo " Kafka HandsOn — VM Startup"
echo " $(date)"
echo "================================================================"

# ── System packages ──────────────────────────────────────────────────────────
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv python3-full curl

# google-cloud-cli is pre-installed on GCP Debian images; ensure gsutil works
gsutil version || apt-get install -y -qq google-cloud-cli

# ── App directory ────────────────────────────────────────────────────────────
APP=/opt/kafka-app
mkdir -p $APP
cd $APP

echo ""
echo "── Downloading application code from GCS ───────────────────────"
gsutil -m cp "gs://${bucket_name}/app/*" $APP/
echo "Files in $APP:"
ls -1 $APP/

# ── Python virtual environment ────────────────────────────────────────────────
echo ""
echo "── Installing Python dependencies ──────────────────────────────"
python3 -m venv .venv
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet \
  kafka-python-ng \
  scikit-learn \
  numpy \
  python-dotenv \
  groq \
  requests \
  "google-auth>=2.0" \
  google-auth-httplib2

# ── Fetch bootstrap address from GCP Managed Kafka ───────────────────────────
echo ""
echo "── Fetching Kafka bootstrap address ────────────────────────────"
BOOTSTRAP=$(gcloud managed-kafka clusters describe ${cluster_name} \
  --location=${region} \
  --project=${project_id} \
  --format="value(bootstrapAddress)" 2>/dev/null || echo "")

if [ -z "$BOOTSTRAP" ]; then
  echo "ERROR: Could not fetch bootstrap address — cluster may still be provisioning."
  echo "       Retry: gcloud managed-kafka clusters describe ${cluster_name} --location=${region} --project=${project_id}"
  exit 1
fi
echo "Bootstrap: $BOOTSTRAP"

# ── Generate gcp_kafka_config.py with baked-in bootstrap address ──────────────
cat > $APP/gcp_kafka_config.py << PYEOF
import os
import google.auth
import google.auth.transport.requests

GCP_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "$BOOTSTRAP")
_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


class _GCPTokenProvider:
    def token(self):
        creds, _ = google.auth.default(scopes=_SCOPES)
        creds.refresh(google.auth.transport.requests.Request())
        return creds.token

    def token_lifetime_ms(self):
        return 3600 * 1000


def gcp_sasl_config() -> dict:
    return dict(
        bootstrap_servers=GCP_BOOTSTRAP,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=_GCPTokenProvider(),
    )
PYEOF

echo "gcp_kafka_config.py written with bootstrap=$BOOTSTRAP"

# ── .env file ────────────────────────────────────────────────────────────────
cat > $APP/.env << ENVEOF
GROQ_API_KEY=${groq_api_key}
ENVEOF

# ── Kafka connectivity verification ──────────────────────────────────────────
echo ""
echo "── Running Kafka verification ──────────────────────────────────"
cd $APP
USE_GCP=true python3 verify_kafka.py
VERIFY_EXIT=$?

echo ""
if [ $VERIFY_EXIT -eq 0 ]; then
  echo "================================================================"
  echo " ✓  KAFKA VERIFY: PASSED"
  echo "================================================================"
else
  echo "================================================================"
  echo " ✗  KAFKA VERIFY: FAILED  (see log above for details)"
  echo "================================================================"
fi

# ── Usage instructions ────────────────────────────────────────────────────────
echo ""
echo "── Ready to run demos ──────────────────────────────────────────"
echo " SSH command: gcloud compute ssh kafka-app-vm --zone=${region}-a --project=${project_id}"
echo ""
echo " Once inside the VM:"
echo "   cd /opt/kafka-app && source .venv/bin/activate"
echo ""
echo "   # Fraud detection demo (2 terminals):"
echo "   USE_GCP=true python3 consumer_ml.py    # terminal 1"
echo "   USE_GCP=true python3 producer.py       # terminal 2"
echo ""
echo "   # Order routing demo (2 terminals):"
echo "   USE_GCP=true python3 simple_processor.py  # terminal 1"
echo "   USE_GCP=true python3 simple_producer.py   # terminal 2"
echo ""
echo "   # GenAI pipeline:"
echo "   USE_GCP=true python3 genai_pipeline.py"
echo ""
echo "================================================================"
echo " Startup complete: $(date)"
echo "================================================================"
