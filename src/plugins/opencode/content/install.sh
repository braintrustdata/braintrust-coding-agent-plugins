#!/bin/bash
###
# Installation script for opencode-braintrust plugin
###

set -e

echo "Installing opencode-braintrust plugin..."

# Check if bun is available
if ! command -v bun &> /dev/null; then
    echo "Error: bun is required but not found. Install it from https://bun.sh"
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
bun install

# Build the plugin
echo "Building plugin..."
bun run build

# Create OpenCode plugin directory if it doesn't exist
PLUGIN_DIR="$HOME/.config/opencode/plugin"
mkdir -p "$PLUGIN_DIR"

# Copy plugin to OpenCode
echo "Installing plugin to $PLUGIN_DIR/trace-opencode.js"
cp dist/index.js "$PLUGIN_DIR/trace-opencode.js"

echo ""
echo "✓ Plugin installed successfully!"
echo ""
echo "Next steps:"
echo "1. Set your Braintrust API key:"
echo "   export BRAINTRUST_API_KEY='your-api-key'"
echo ""
echo "2. (Optional) Configure project name:"
echo "   export BRAINTRUST_PROJECT='my-project'"
echo ""
echo "3. Run OpenCode:"
echo "   opencode"
echo ""
echo "4. Your sessions will be traced to Braintrust automatically!"
# TODO: add org to url to fix the link
# echo "   View at: https://www.braintrust.dev/app/projects/${BRAINTRUST_PROJECT:-opencode}/logs"
echo ""
