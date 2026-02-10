#!/bin/bash

# --- Configuration ---
PROJECT="p"
INSTANCE="i"

# Helper function to keep commands clean and consistent
run_cbt() {
    cbt -project="$PROJECT" -instance="$INSTANCE" "$@"
}

echo ">>> Starting Bigtable Emulator Population..."
echo "If it takes too long for you, comment out some parts of this script"
echo "But for the original emulator it is also slow"
export BIGTABLE_EMULATOR_HOST=localhost:8888

# ==========================================
# TABLE 1: users
# Use Case: Storing user data with history
# ==========================================

echo "--- Creating Table 1: users ---"
run_cbt createtable users
run_cbt createfamily users id
run_cbt createfamily users preferences

# --- Row 1: user-001 (Demonstrating Multiple Columns) ---
echo "Populating user-001..."
run_cbt set users user-001 \
    id:name="Alice Smith" \
    id:email="alice@example.com" \
    preferences:theme="dark" \
    preferences:notifications="true"

# --- Row 2: user-002 (Demonstrating Multiple Timestamps/Versioning) ---
echo "Populating user-002 with version history..."

# 1. Initial write (Timestamp T1)
run_cbt set users user-002 \
    id:name="Jon Jones" \
    id:email="jones@ufc.com" \
    preferences:theme="OS" \
    preferences:notifications="false"

# 2. Update write (Timestamp T2) - Simulates a move
run_cbt set users user-002 preferences:notifications="true"

# ==========================================
# TABLE 2: cars
# ==========================================

echo "--- Creating Table 2: cars ---"
run_cbt createtable cars
run_cbt createfamily cars id
run_cbt createfamily cars params

echo "Populating car-1..."
run_cbt set cars car-1 \
    params:power="97HP" \
    id:brand="Toyota"

echo "Populating car-2..."
run_cbt set cars car-2 \
    params:power="140HP" \
    id:brand="Hyundai"

echo ">>> Population Complete!"