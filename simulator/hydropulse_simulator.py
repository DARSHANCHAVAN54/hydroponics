import asyncio
import json
import math
import os
import random
import sys
import uuid

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from azure.eventhub import EventData, TransportType
from azure.eventhub.aio import EventHubProducerClient


# ============================================================
# 1. AZURE EVENT HUB CONFIGURATION
# ============================================================

EVENT_HUB_CONNECTION_STRING = os.getenv(
    "EVENT_HUB_CONNECTION_STRING"
)

EVENT_HUB_NAME = os.getenv(
    "EVENT_HUB_NAME",
    "eh-hydropulse-telemetry"
)

if not EVENT_HUB_CONNECTION_STRING:
    print("ERROR: EVENT_HUB_CONNECTION_STRING is not set.")
    sys.exit(1)


# ============================================================
# 2. SIMULATOR CONFIGURATION
# ============================================================

SEND_INTERVAL_SECONDS = 10

ZONES = [
    "BAY-01",
    "BAY-02"
]

DEVICES_PER_ZONE = 5

PACKET_LOSS_RATE = 0.007

SENSOR_FAULT_RATE = 0.00005

INCIDENT_START_RATE = 0.00015


# ============================================================
# 3. FARM PROFILES
#
# Used internally by simulator.
# Location/timezone are NOT sent in Event Hub telemetry.
# ============================================================

FARM_PROFILES = {

    "FARM-AUSTIN-01": {
        "location": "Austin",
        "timezone": "America/Chicago",
        "temp_offset": 2.0,
        "humidity_offset": -4.0
    },

    "FARM-CHICAGO-01": {
        "location": "Chicago",
        "timezone": "America/Chicago",
        "temp_offset": -1.5,
        "humidity_offset": 2.0
    },

    "FARM-SEATTLE-01": {
        "location": "Seattle",
        "timezone": "America/Los_Angeles",
        "temp_offset": -3.0,
        "humidity_offset": 8.0
    },

    "FARM-DENVER-01": {
        "location": "Denver",
        "timezone": "America/Denver",
        "temp_offset": -1.0,
        "humidity_offset": -8.0
    },

    "FARM-MIAMI-01": {
        "location": "Miami",
        "timezone": "America/New_York",
        "temp_offset": 3.0,
        "humidity_offset": 10.0
    },

    "FARM-BOSTON-01": {
        "location": "Boston",
        "timezone": "America/New_York",
        "temp_offset": -2.0,
        "humidity_offset": 0.0
    },

    "FARM-DALLAS-01": {
        "location": "Dallas",
        "timezone": "America/Chicago",
        "temp_offset": 2.5,
        "humidity_offset": -3.0
    },

    "FARM-ATLANTA-01": {
        "location": "Atlanta",
        "timezone": "America/New_York",
        "temp_offset": 1.5,
        "humidity_offset": 4.0
    },

    "FARM-PHOENIX-01": {
        "location": "Phoenix",
        "timezone": "America/Phoenix",
        "temp_offset": 4.0,
        "humidity_offset": -12.0
    },

    "FARM-SANJOSE-01": {
        "location": "San Jose",
        "timezone": "America/Los_Angeles",
        "temp_offset": 0.5,
        "humidity_offset": -2.0
    },

    "FARM-DETROIT-01": {
        "location": "Detroit",
        "timezone": "America/Detroit",
        "temp_offset": -2.0,
        "humidity_offset": 1.0
    },

    "FARM-ORLANDO-01": {
        "location": "Orlando",
        "timezone": "America/New_York",
        "temp_offset": 3.0,
        "humidity_offset": 8.0
    },

    "FARM-PORTLAND-01": {
        "location": "Portland",
        "timezone": "America/Los_Angeles",
        "temp_offset": -2.5,
        "humidity_offset": 6.0
    },

    "FARM-HOUSTON-01": {
        "location": "Houston",
        "timezone": "America/Chicago",
        "temp_offset": 2.8,
        "humidity_offset": 6.0
    },

    "FARM-NEWYORK-01": {
        "location": "New York",
        "timezone": "America/New_York",
        "temp_offset": -1.0,
        "humidity_offset": 1.0
    }
}


# ============================================================
# 4. INTERNAL CROP PROFILES
#
# Used ONLY to generate realistic readings.
# crop_type is NOT sent to Event Hub.
# ============================================================

CROP_PROFILES = {

    "Lettuce": {
        "ph_min": 6.0,
        "ph_max": 7.0,
        "ec_min": 1.2,
        "ec_max": 1.8,
        "ambient_target_c": 20.0,
        "humidity_target_pct": 65.0
    },

    "Basil": {
        "ph_min": 5.5,
        "ph_max": 6.0,
        "ec_min": 1.0,
        "ec_max": 1.6,
        "ambient_target_c": 23.0,
        "humidity_target_pct": 60.0
    },

    "Spinach": {
        "ph_min": 6.0,
        "ph_max": 7.0,
        "ec_min": 1.8,
        "ec_max": 2.3,
        "ambient_target_c": 19.0,
        "humidity_target_pct": 65.0
    },

    "Strawberry": {
        "ph_min": 5.8,
        "ph_max": 6.2,
        "ec_min": 1.8,
        "ec_max": 2.2,
        "ambient_target_c": 21.0,
        "humidity_target_pct": 65.0
    },

    "Cucumber": {
        "ph_min": 5.0,
        "ph_max": 5.5,
        "ec_min": 1.7,
        "ec_max": 2.0,
        "ambient_target_c": 24.0,
        "humidity_target_pct": 70.0
    },

    "Cherry Tomatoes": {
        "ph_min": 6.0,
        "ph_max": 6.5,
        "ec_min": 2.0,
        "ec_max": 4.0,
        "ambient_target_c": 24.0,
        "humidity_target_pct": 65.0
    }
}


# ============================================================
# 5. INTERNAL STATE
# ============================================================

CROP_NAMES = list(CROP_PROFILES.keys())

ZONE_CROPS = {}

ZONE_STATES = {}

DEVICE_STATES = {}


def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


# ============================================================
# 6. INITIALIZE SIMULATOR
# ============================================================

def initialize_simulator():

    crop_index = 0

    for farm_id in FARM_PROFILES:

        for zone_id in ZONES:

            zone_key = f"{farm_id}:{zone_id}"

            crop_type = CROP_NAMES[
                crop_index % len(CROP_NAMES)
            ]

            crop_index += 1

            ZONE_CROPS[zone_key] = crop_type

            crop = CROP_PROFILES[crop_type]

            target_ph = (
                crop["ph_min"] + crop["ph_max"]
            ) / 2

            target_ec = (
                crop["ec_min"] + crop["ec_max"]
            ) / 2

            # -----------------------------------------------
            # Shared farm-zone/reservoir state
            # -----------------------------------------------

            ZONE_STATES[zone_key] = {

                "ph": (
                    target_ph
                    + random.uniform(-0.05, 0.05)
                ),

                "ec_ms_cm": (
                    target_ec
                    + random.uniform(-0.05, 0.05)
                ),

                "water_level_cm": random.uniform(
                    18.5,
                    21.5
                ),

                "water_temp_c": random.uniform(
                    19.5,
                    21.5
                ),

                "dissolved_oxygen_mg_l": random.uniform(
                    7.0,
                    8.2
                ),

                "is_ph_reducer_on": False,
                "is_ph_increaser_on": False,
                "is_water_refill_on": False,
                "is_nutrient_pump_on": False,
                "is_exhaust_fan_on": False,
                "is_humidifier_on": False,
                "is_aerator_on": True,

                "incident_type": None,
                "incident_cycles_remaining": 0
            }

            # -----------------------------------------------
            # Individual device state
            # -----------------------------------------------

            for dev_num in range(
                1,
                DEVICES_PER_ZONE + 1
            ):

                device_id = (
                    f"HYDRO-NODE-{dev_num:03d}"
                )

                device_key = (
                    f"{zone_key}:{device_id}"
                )

                DEVICE_STATES[device_key] = {

                    "seq_num": 0,

                    "battery_voltage": random.uniform(
                        4.05,
                        4.20
                    ),

                    "temp_bias": random.gauss(
                        0,
                        0.12
                    ),

                    "humidity_bias": random.gauss(
                        0,
                        0.50
                    ),

                    "ph_bias": random.gauss(
                        0,
                        0.015
                    ),

                    "ec_bias": random.gauss(
                        0,
                        0.015
                    )
                }


# ============================================================
# 7. PHYSICAL INCIDENT SIMULATION
# ============================================================

def update_incident(zone_state):

    if zone_state["incident_cycles_remaining"] > 0:

        zone_state["incident_cycles_remaining"] -= 1

        if zone_state["incident_cycles_remaining"] == 0:
            zone_state["incident_type"] = None

        return

    if random.random() < INCIDENT_START_RATE:

        zone_state["incident_type"] = random.choice(
            [
                "PH_DRIFT_HIGH",
                "EC_DEPLETION",
                "WATER_TEMP_HIGH",
                "WATER_LEVEL_LOW",
                "DISSOLVED_OXYGEN_LOW"
            ]
        )

        zone_state["incident_cycles_remaining"] = (
            random.randint(12, 60)
        )


# ============================================================
# 8. UPDATE FARM-ZONE PHYSICS
# ============================================================

def update_zone_physics(farm_id, zone_id):

    zone_key = f"{farm_id}:{zone_id}"

    state = ZONE_STATES[zone_key]

    crop_type = ZONE_CROPS[zone_key]

    crop = CROP_PROFILES[crop_type]

    profile = FARM_PROFILES[farm_id]

    now_utc = datetime.now(timezone.utc)

    local_time = now_utc.astimezone(
        ZoneInfo(profile["timezone"])
    )

    hour = (
        local_time.hour
        + local_time.minute / 60.0
        + local_time.second / 3600.0
    )

    # --------------------------------------------------------
    # Day/night cycle
    # --------------------------------------------------------

    solar_strength = max(
        0.0,
        math.sin(
            math.pi * (hour - 6.0) / 12.0
        )
    )

    update_incident(state)

    incident = state["incident_type"]

    # --------------------------------------------------------
    # Ambient temperature
    # --------------------------------------------------------

    ambient_temp = (
        crop["ambient_target_c"]
        + profile["temp_offset"]
        + solar_strength * 2.5
        - 1.0
        + random.gauss(0, 0.20)
    )

    if state["is_exhaust_fan_on"]:
        ambient_temp -= 0.8

    ambient_temp = clamp(
        ambient_temp,
        12.0,
        38.0
    )

    # --------------------------------------------------------
    # Humidity
    # --------------------------------------------------------

    humidity = (
        crop["humidity_target_pct"]
        + profile["humidity_offset"]
        - solar_strength * 4.0
        + random.gauss(0, 0.8)
    )

    if state["is_exhaust_fan_on"]:
        humidity -= 1.5

    if state["is_humidifier_on"]:
        humidity += 2.5

    humidity = clamp(
        humidity,
        25.0,
        95.0
    )

    # --------------------------------------------------------
    # pH drift
    # --------------------------------------------------------

    state["ph"] += random.uniform(
        0.00002,
        0.00015
    )

    if incident == "PH_DRIFT_HIGH":
        state["ph"] += 0.004

    ph_target = (
        crop["ph_min"]
        + crop["ph_max"]
    ) / 2

    if (
        not state["is_ph_reducer_on"]
        and state["ph"] > crop["ph_max"] - 0.05
    ):
        state["is_ph_reducer_on"] = True

    if state["is_ph_reducer_on"]:

        state["ph"] -= 0.003

        if state["ph"] <= ph_target:
            state["is_ph_reducer_on"] = False

    if (
        not state["is_ph_increaser_on"]
        and state["ph"] < crop["ph_min"] + 0.05
    ):
        state["is_ph_increaser_on"] = True

    if state["is_ph_increaser_on"]:

        state["ph"] += 0.0025

        if state["ph"] >= ph_target:
            state["is_ph_increaser_on"] = False

    # --------------------------------------------------------
    # EC / nutrient consumption
    # --------------------------------------------------------

    state["ec_ms_cm"] -= random.uniform(
        0.00002,
        0.00010
    )

    if incident == "EC_DEPLETION":
        state["ec_ms_cm"] -= 0.004

    ec_target = (
        crop["ec_min"]
        + crop["ec_max"]
    ) / 2

    if (
        not state["is_nutrient_pump_on"]
        and state["ec_ms_cm"] < ec_target - 0.10
    ):
        state["is_nutrient_pump_on"] = True

    if state["is_nutrient_pump_on"]:

        state["ec_ms_cm"] += 0.003

        if state["ec_ms_cm"] >= ec_target + 0.03:
            state["is_nutrient_pump_on"] = False

    # --------------------------------------------------------
    # Water level
    # --------------------------------------------------------

    water_consumption = (
        random.uniform(0.001, 0.004)
        + solar_strength * 0.001
    )

    state["water_level_cm"] -= water_consumption

    if incident == "WATER_LEVEL_LOW":
        state["water_level_cm"] -= 0.035

    if (
        not state["is_water_refill_on"]
        and state["water_level_cm"] < 16.5
    ):
        state["is_water_refill_on"] = True

    if state["is_water_refill_on"]:

        state["water_level_cm"] += 0.03

        if state["water_level_cm"] >= 21.0:
            state["is_water_refill_on"] = False

    # --------------------------------------------------------
    # Water temperature
    # --------------------------------------------------------

    target_water_temp = (
        20.5
        + (ambient_temp - 22.0) * 0.20
    )

    state["water_temp_c"] += (
        target_water_temp
        - state["water_temp_c"]
    ) * 0.01

    if incident == "WATER_TEMP_HIGH":
        state["water_temp_c"] += 0.025

    # --------------------------------------------------------
    # Dissolved oxygen
    # --------------------------------------------------------

    do_target = (
        7.7
        - max(
            0.0,
            state["water_temp_c"] - 20.0
        ) * 0.12
    )

    if state["is_aerator_on"]:
        do_target += 0.35

    state["dissolved_oxygen_mg_l"] += (
        (
            do_target
            - state["dissolved_oxygen_mg_l"]
        ) * 0.05
        + random.gauss(0, 0.015)
    )

    if incident == "DISSOLVED_OXYGEN_LOW":
        state["dissolved_oxygen_mg_l"] -= 0.025

    if (
        not state["is_aerator_on"]
        and state["dissolved_oxygen_mg_l"] < 6.3
    ):
        state["is_aerator_on"] = True

    elif (
        state["is_aerator_on"]
        and state["dissolved_oxygen_mg_l"] > 7.5
    ):
        state["is_aerator_on"] = False

    # --------------------------------------------------------
    # Exhaust fan
    # --------------------------------------------------------

    fan_on_temp = (
        crop["ambient_target_c"] + 2.0
    )

    fan_off_temp = (
        crop["ambient_target_c"] + 0.5
    )

    if not state["is_exhaust_fan_on"]:

        if (
            ambient_temp > fan_on_temp
            or humidity > 78.0
        ):
            state["is_exhaust_fan_on"] = True

    else:

        if (
            ambient_temp < fan_off_temp
            and humidity < 74.0
        ):
            state["is_exhaust_fan_on"] = False

    # --------------------------------------------------------
    # Humidifier
    # --------------------------------------------------------

    humidity_target = crop[
        "humidity_target_pct"
    ]

    if not state["is_humidifier_on"]:

        if humidity < humidity_target - 7.0:
            state["is_humidifier_on"] = True

    else:

        if humidity > humidity_target + 2.0:
            state["is_humidifier_on"] = False

    # --------------------------------------------------------
    # CO2
    # --------------------------------------------------------

    co2_ppm = (
        650.0
        - solar_strength * 120.0
        + random.gauss(0, 10.0)
    )

    if state["is_exhaust_fan_on"]:

        co2_ppm += (
            450.0 - co2_ppm
        ) * 0.25

    co2_ppm = clamp(
        co2_ppm,
        380.0,
        1000.0
    )

    # --------------------------------------------------------
    # Safety bounds
    # --------------------------------------------------------

    state["ph"] = clamp(
        state["ph"],
        4.0,
        8.0
    )

    state["ec_ms_cm"] = clamp(
        state["ec_ms_cm"],
        0.2,
        5.0
    )

    state["water_level_cm"] = clamp(
        state["water_level_cm"],
        3.0,
        25.0
    )

    state["water_temp_c"] = clamp(
        state["water_temp_c"],
        12.0,
        35.0
    )

    state["dissolved_oxygen_mg_l"] = clamp(
        state["dissolved_oxygen_mg_l"],
        3.0,
        10.0
    )

    return {
        "timestamp": now_utc,
        "ambient_temp_c": ambient_temp,
        "humidity_pct": humidity,
        "co2_ppm": co2_ppm
    }


# ============================================================
# 9. BUILD DEVICE EVENT
# ============================================================

def build_device_payload(
    farm_id,
    zone_id,
    device_id,
    zone_environment
):

    zone_key = f"{farm_id}:{zone_id}"

    device_key = (
        f"{zone_key}:{device_id}"
    )

    zone_state = ZONE_STATES[zone_key]

    device_state = DEVICE_STATES[
        device_key
    ]

    # Increment before simulated packet loss
    device_state["seq_num"] += 1

    # --------------------------------------------------------
    # Battery drain
    # --------------------------------------------------------

    device_state["battery_voltage"] -= (
        random.uniform(
            0.000002,
            0.000008
        )
    )

    device_state["battery_voltage"] = max(
        3.2,
        device_state["battery_voltage"]
    )

    # --------------------------------------------------------
    # Sensor readings
    # --------------------------------------------------------

    ph = (
        zone_state["ph"]
        + device_state["ph_bias"]
        + random.gauss(0, 0.008)
    )

    ec = (
        zone_state["ec_ms_cm"]
        + device_state["ec_bias"]
        + random.gauss(0, 0.008)
    )

    water_temp = (
        zone_state["water_temp_c"]
        + random.gauss(0, 0.05)
    )

    water_level = (
        zone_state["water_level_cm"]
        + random.gauss(0, 0.05)
    )

    dissolved_oxygen = (
        zone_state["dissolved_oxygen_mg_l"]
        + random.gauss(0, 0.04)
    )

    ambient_temp = (
        zone_environment["ambient_temp_c"]
        + device_state["temp_bias"]
        + random.gauss(0, 0.05)
    )

    humidity = (
        zone_environment["humidity_pct"]
        + device_state["humidity_bias"]
        + random.gauss(0, 0.20)
    )

    # --------------------------------------------------------
    # Rare individual sensor fault
    #
    # We deliberately DO NOT label the fault.
    # Silver layer must detect abnormal readings.
    # --------------------------------------------------------

    if random.random() < SENSOR_FAULT_RATE:

        fault_type = random.choice(
            [
                "PH_SPIKE",
                "EC_SPIKE",
                "TEMP_SPIKE",
                "LEVEL_SPIKE"
            ]
        )

        if fault_type == "PH_SPIKE":

            ph += random.choice(
                [-1.4, 1.4]
            )

        elif fault_type == "EC_SPIKE":

            ec += random.choice(
                [-0.8, 1.2]
            )

        elif fault_type == "TEMP_SPIKE":

            water_temp += random.choice(
                [-5.0, 7.0]
            )

        elif fault_type == "LEVEL_SPIKE":

            water_level -= random.uniform(
                5.0,
                9.0
            )

    ec = max(
        0.05,
        ec
    )

    tds_ppm = (
        ec * 500.0
    )

    # ========================================================
    # TELEMETRY ONLY
    #
    # NOT sent:
    # crop_type
    # location
    # timezone
    # thresholds
    # anomaly flag
    # severity
    #
    # These are added through master data and Silver logic.
    # ========================================================

    return {

        "event_id": str(
            uuid.uuid4()
        ),

        "event_type":
            "hydroponics_telemetry",

        "schema_version":
            "5.0",

        "farm_id":
            farm_id,

        "zone_id":
            zone_id,

        "device_id":
            device_id,

        "seq_num":
            device_state["seq_num"],

        "timestamp":
            zone_environment[
                "timestamp"
            ].isoformat(),

        # Sensor metrics

        "ph":
            round(ph, 2),

        "ec_ms_cm":
            round(ec, 2),

        "tds_ppm":
            round(tds_ppm, 1),

        "water_level_cm":
            round(
                water_level,
                1
            ),

        "water_temp_c":
            round(
                water_temp,
                1
            ),

        "dissolved_oxygen_mg_l":
            round(
                dissolved_oxygen,
                2
            ),

        "ambient_temp_c":
            round(
                ambient_temp,
                1
            ),

        "humidity_pct":
            round(
                clamp(
                    humidity,
                    20.0,
                    100.0
                ),
                1
            ),

        "co2_ppm":
            round(
                zone_environment["co2_ppm"]
                + random.gauss(
                    0,
                    3.0
                ),
                1
            ),

        # Device health

        "battery_voltage":
            round(
                device_state[
                    "battery_voltage"
                ],
                3
            ),

        "device_status":
            "ONLINE",

        # Controller

        "controller_mode":
            "AUTO",

        # Actuators

        "is_ph_reducer_on":
            zone_state[
                "is_ph_reducer_on"
            ],

        "is_ph_increaser_on":
            zone_state[
                "is_ph_increaser_on"
            ],

        "is_water_refill_on":
            zone_state[
                "is_water_refill_on"
            ],

        "is_nutrient_pump_on":
            zone_state[
                "is_nutrient_pump_on"
            ],

        "is_humidifier_on":
            zone_state[
                "is_humidifier_on"
            ],

        "is_exhaust_fan_on":
            zone_state[
                "is_exhaust_fan_on"
            ],

        "is_aerator_on":
            zone_state[
                "is_aerator_on"
            ]
    }


# ============================================================
# 10. EVENT HUB PRODUCER
#
# Important:
# One Event Hub batch is created per farm.
# farm_id is used as the partition key.
# ============================================================

async def run_producer():

    initialize_simulator()

    producer = (
        EventHubProducerClient
        .from_connection_string(

            conn_str=
                EVENT_HUB_CONNECTION_STRING,

            eventhub_name=
                EVENT_HUB_NAME,

            transport_type=
                TransportType.AmqpOverWebsocket
        )
    )

    total_devices = (
        len(FARM_PROFILES)
        * len(ZONES)
        * DEVICES_PER_ZONE
    )

    print(
        "=========================================================="
    )

    print(
        "HydroPulse Real-Time IoT Simulator V5"
    )

    print(
        f"{len(FARM_PROFILES)} Farms | "
        f"{len(FARM_PROFILES) * len(ZONES)} Zones | "
        f"{total_devices} Devices"
    )

    print(
        f"Event Hub: {EVENT_HUB_NAME}"
    )

    print(
        f"Send interval: {SEND_INTERVAL_SECONDS} seconds"
    )

    print(
        "Partition Key: farm_id"
    )

    print(
        "Telemetry-only Event Hub payload"
    )

    print(
        "Crop/master metadata handled separately"
    )

    print(
        "==========================================================\n"
    )

    async with producer:

        while True:

            sent_count = 0
            dropped_count = 0

            # =================================================
            # One batch per farm
            # farm_id = partition key
            # =================================================

            for farm_id in FARM_PROFILES:

                event_data_batch = (
                    await producer.create_batch(
                        partition_key=farm_id
                    )
                )

                for zone_id in ZONES:

                    # Update shared zone physics once per cycle.

                    zone_environment = (
                        update_zone_physics(
                            farm_id,
                            zone_id
                        )
                    )

                    for dev_num in range(
                        1,
                        DEVICES_PER_ZONE + 1
                    ):

                        device_id = (
                            f"HYDRO-NODE-{dev_num:03d}"
                        )

                        payload = (
                            build_device_payload(
                                farm_id,
                                zone_id,
                                device_id,
                                zone_environment
                            )
                        )

                        # -------------------------------------
                        # Simulated packet loss
                        #
                        # seq_num already incremented.
                        # Missing sequence number can therefore
                        # be detected later in Silver.
                        # -------------------------------------

                        if (
                            random.random()
                            < PACKET_LOSS_RATE
                        ):

                            dropped_count += 1

                            continue

                        event_data = EventData(
                            json.dumps(payload)
                        )

                        event_data.content_type = (
                            "application/json"
                        )

                        event_data.properties = {

                            "farm_id":
                                farm_id,

                            "zone_id":
                                zone_id,

                            "event_type":
                                "hydroponics_telemetry",

                            "schema_version":
                                "5.0"
                        }

                        try:

                            event_data_batch.add(
                                event_data
                            )

                        except ValueError:

                            # Batch reached maximum Event Hub size.
                            # Send it and create another batch
                            # with the SAME farm partition key.

                            await producer.send_batch(
                                event_data_batch
                            )

                            event_data_batch = (
                                await producer.create_batch(
                                    partition_key=farm_id
                                )
                            )

                            event_data_batch.add(
                                event_data
                            )

                        sent_count += 1

                # Send this farm's batch

                if len(event_data_batch) > 0:

                    await producer.send_batch(
                        event_data_batch
                    )

            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"Sent: {sent_count}/{total_devices} | "
                f"Dropped: {dropped_count}"
            )

            await asyncio.sleep(
                SEND_INTERVAL_SECONDS
            )


# ============================================================
# 11. START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            run_producer()
        )

    except KeyboardInterrupt:

        print(
            "\nSimulator stopped manually."
        )