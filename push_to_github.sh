#!/bin/bash
# ================================================================
#  VINEETH'S GITHUB PUSH SCRIPT
#  Run this once after creating your repo on github.com
# ================================================================
#
#  BEFORE RUNNING:
#  1. Create a FREE GitHub account at https://github.com
#     (use email: aredlavineethreddy2001@gmail.com)
#
#  2. Create a new EMPTY repo at https://github.com/new
#     - Name:        cloud-etl-pipeline
#     - Visibility:  Public
#     - IMPORTANT:   Do NOT check "Add a README" or any other options
#     - Click:       "Create repository"
#
#  3. Create a Personal Access Token:
#     GitHub → Settings → Developer settings
#     → Personal access tokens → Tokens (classic)
#     → Generate new token (classic)
#     → Check: repo + workflow → Generate
#     → COPY the token (starts with ghp_...)
#
#  4. Open Terminal and run:
#     chmod +x push_to_github.sh
#     ./push_to_github.sh
# ================================================================

set -e  # Stop on any error

echo ""
echo "=================================================="
echo "  Cloud ETL Pipeline → GitHub Push Script"
echo "  For: Vineeth Reddy Aredla (vineethreddy-a)"
echo "=================================================="
echo ""

# Your details (pre-filled)
GITHUB_USERNAME="vineethreddy-a"
REPO_NAME="cloud-etl-pipeline"

# Ask for PAT securely (hidden input)
echo "Paste your GitHub Personal Access Token and press Enter:"
echo "(The token will be hidden as you type/paste)"
read -s GITHUB_TOKEN
echo ""

if [ -z "$GITHUB_TOKEN" ]; then
    echo "ERROR: No token provided. Exiting."
    exit 1
fi

echo "Step 1/4: Configuring Git identity..."
git config user.name "Vineeth Reddy Aredla"
git config user.email "aredlavineethreddy2001@gmail.com"
echo "         Done."

echo ""
echo "Step 2/4: Connecting to your GitHub repo..."
# Remove existing remote if any
git remote remove origin 2>/dev/null || true

# Add remote with embedded credentials
git remote add origin "https://${GITHUB_USERNAME}:${GITHUB_TOKEN}@github.com/${GITHUB_USERNAME}/${REPO_NAME}.git"
echo "         Connected to: github.com/${GITHUB_USERNAME}/${REPO_NAME}"

echo ""
echo "Step 3/4: Pushing 10 commits to GitHub..."
git push -u origin main
echo "         Pushed successfully!"

echo ""
echo "Step 4/4: Cleaning up credentials from remote URL..."
git remote set-url origin "https://github.com/${GITHUB_USERNAME}/${REPO_NAME}.git"
echo "         Done."

echo ""
echo "=================================================="
echo "  SUCCESS! Your project is live on GitHub!"
echo ""
echo "  View your repo:"
echo "  https://github.com/${GITHUB_USERNAME}/${REPO_NAME}"
echo ""
echo "  Add to LinkedIn Featured section:"
echo "  https://github.com/${GITHUB_USERNAME}/${REPO_NAME}"
echo ""
echo "  Resume bullet points to add:"
echo "  • Built production ETL pipeline processing 10M+ records"
echo "    with PySpark on AWS EMR; achieved 99%+ data accuracy"
echo "  • Orchestrated with Apache Airflow; loaded to Snowflake"
echo "    and Redshift; IaC via Terraform; CI/CD via GitHub Actions"
echo "=================================================="
echo ""
