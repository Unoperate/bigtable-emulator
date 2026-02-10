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
# run_cbt createfamily users preferences

# --- Row 1: user-001 ---
echo "Populating user-001..."
run_cbt set users user-001 \
    id:name="Alice Smith" \
    id:email="alice@example.com"
    # preferences:theme="dark" \
    # preferences:notifications="true"

# --- Row 2: user-002 (Demonstrating Multiple Timestamps/Versioning) ---
echo "Populating user-002 with version history..."

# 1. Initial write (Timestamp T1) - Consolidated
run_cbt set users user-002 \
    id:name="Jon Jones" \
    id:email="jones@ufc.com" \
    id:location="New York"

# 2. Update write (Timestamp T2) - Simulates a move
run_cbt set users user-002 id:location="London"

run_cbt set users user-002 id:location="Paris"

# ==========================================
# TABLE 2: iot-metrics
# Use Case: Time-series data
# ==========================================

echo "--- Creating Table 2: iot-metrics ---"
run_cbt createtable iot-metrics
run_cbt createfamily iot-metrics device_meta
run_cbt createfamily iot-metrics sensor_data
run_cbt createfamily iot-metrics device_params

# --- Row 1: sensor-A ---
# Note: Moved the stray 'height' parameter (originally found under sensor-B) up here
echo "Populating sensor-A..."
run_cbt set iot-metrics sensor-A \
    device_meta:model="v1.0" \
    sensor_data:temp="20.5" \
    sensor_data:temp="21.0" \
    device_params:weight="92 g" \
    device_params:height="8 mm"

# --- Row 2: sensor-B ---
echo "Populating sensor-B..."
run_cbt set iot-metrics sensor-B \
    device_meta:model="v2.0" \
    sensor_data:temp="18.2" \
    sensor_data:humidity="60" \
    device_params:weight="104 g"

# ==========================================
# TABLE 3: cars
# ==========================================

echo "--- Creating Table 3: cars ---"
run_cbt createtable cars
run_cbt createfamily cars id
run_cbt createfamily cars params
run_cbt createfamily cars state

echo "Populating car-1..."
run_cbt set cars car-1 \
    params:weight="1400kg" \
    params:power="97HP" \
    params:displacement="1.4 l" \
    id:brand="Toyota" \
    id:model="Corolla" \
    id:generation="VIII" \
    state:mileage="175000km"

echo "Populating car-2..."
run_cbt set cars car-2 \
    params:weight="1800kg" \
    params:power="140HP" \
    params:displacement="1.5 l" \
    id:brand="Hyundai" \
    id:model="Tuscon" \
    id:generation="3" \
    state:mileage="110000km"

echo ">>> Population Complete!"