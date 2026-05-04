#!/bin/bash

echo "🚀 Installing Meta Dashboard..."

APP_DIR="$HOME/meta-dashboard"

# 1. Copy files
mkdir -p "$APP_DIR"
cp -R . "$APP_DIR"

cd "$APP_DIR"

# 2. Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

# 3. Activate
source venv/bin/activate

# 4. Install dependencies
echo "📥 Installing dependencies..."
pip install --upgrade pip
pip install streamlit pandas requests

# 5. Make run script executable
chmod +x run.command

echo ""
echo "✅ Installation complete!"
echo ""
echo "👉 To run:"
echo "Double-click your Meta Dashboard.app"
echo "OR run:"
echo "bash $APP_DIR/run.command"