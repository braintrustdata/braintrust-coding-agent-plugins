#!/bin/bash
###
# Installation script for opencode-braintrust plugin
###

set -e

echo "Installing opencode-braintrust plugin..."

# Check if pnpm is available
if ! command -v pnpm &> /dev/null; then
    echo "Error: pnpm is required but not found. Install it from https://pnpm.io"
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
pnpm install --frozen-lockfile

# Build the plugin
echo "Building plugin..."
pnpm run build

# Create OpenCode plugin directory if it doesn't exist
PLUGIN_DIR="$HOME/.config/opencode/plugin"
mkdir -p "$PLUGIN_DIR"

# Copy plugin to OpenCode
echo "Installing plugin to $PLUGIN_DIR/trace-opencode.js"
cp dist/index.mjs "$PLUGIN_DIR/trace-opencode.js"

echo ""
echo "✓ Plugin installed successfully!"
echo ""
echo "Next steps:"
echo "1. Authenticate the bt CLI:"
echo "   bt auth login"
echo ""
echo "2. (Optional) Configure project name:"
echo "   bt trace setup opencode --project my-project"
echo ""
echo "3. Run OpenCode:"
echo "   opencode"
echo ""
echo "4. Your sessions will be traced to Braintrust automatically!"
echo ""
