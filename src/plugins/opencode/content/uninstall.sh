#!/bin/bash

PLUGIN_DIR="$HOME/.config/opencode/plugin"
mkdir -p "$PLUGIN_DIR"

echo "Uninstalling plugin at $PLUGIN_DIR/trace-opencode.js"
rm "$PLUGIN_DIR/trace-opencode.js"

echo ""
echo "✓ Plugin uninstalled successfully!"
echo ""
