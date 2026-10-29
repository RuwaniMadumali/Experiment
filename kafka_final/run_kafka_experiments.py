import subprocess
import time
import os
import csv


# ============================================================
# CONFIG
# ============================================================

model_files = [
    "model_10MB.pth",
    "model_50MB.pth",
    "model_100MB.pth"
]

device_counts = [1, 3, 5]

approaches = ["full", "metadata"]

# Kafka Docker container name
KAFKA_CONTAINER = "kafka"

# Results file
RESULTS_FILE = "results_kafka.csv"

# Network conditions
network_conditions = {
    "normal": {
        "delay": "0ms",
        "bandwidth": None
    },

    "moderate": {
        "delay": "50ms",
        "bandwidth": "10mbit"
    },

    "constrained": {
        "delay": "100ms",
        "bandwidth": "5mbit"
    }
}


# ============================================================
# CSV INITIALIZATION
# ============================================================

if os.path.exists(RESULTS_FILE):
    os.remove(RESULTS_FILE)

with open(RESULTS_FILE, "w", newline="") as f:

    writer = csv.writer(f)

    writer.writerow([
        "Approach",
        "Network Condition",
        "Configured Delay (ms)",
        "Configured Bandwidth (Mbps)",
        "Latency (ms)",
        "Throughput (Mbps)",
        "Model File",
        "Model Size (MB)",
        "Device Count",
        "Status",
        "Failure Reason"
    ])


# ============================================================
# GET MODEL SIZE
# ============================================================

def get_model_size_mb(model):

    if not os.path.exists(model):
        return None

    size_bytes = os.path.getsize(model)

    return size_bytes / (1024 * 1024)


# ============================================================
# CHECK tc
# ============================================================

def check_tc():

    print("\nChecking tc inside Kafka container...")

    result = subprocess.run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "which",
            "tc"
        ],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:

        print("WARNING: tc was not found inside the Kafka container.")

        print(
            "Network throttling cannot be applied until tc "
            "is installed inside the Kafka container."
        )

        return False

    print("tc found:", result.stdout.strip())

    return True


# ============================================================
# RESET NETWORK
# ============================================================

def reset_network_condition():

    print("\nResetting network condition...")

    result = subprocess.run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "tc",
            "qdisc",
            "del",
            "dev",
            "eth0",
            "root"
        ],
        capture_output=True,
        text=True
    )

    time.sleep(1)


# ============================================================
# APPLY NETWORK CONDITION
# ============================================================

def apply_network_condition(delay, bandwidth):

    # Remove previous condition
    reset_network_condition()

    # --------------------------------------------------------
    # NORMAL
    # --------------------------------------------------------

    if delay == "0ms" and bandwidth is None:

        print("\nNetwork condition: NORMAL")
        print("Delay: 0 ms")
        print("Bandwidth: Unlimited")

        return True


    # --------------------------------------------------------
    # MODERATE / CONSTRAINED
    # --------------------------------------------------------

    print("\nApplying network condition...")
    print("Delay:", delay)

    if bandwidth:
        print("Bandwidth:", bandwidth)
    else:
        print("Bandwidth: Unlimited")


    command = [
        "docker",
        "exec",
        KAFKA_CONTAINER,
        "tc",
        "qdisc",
        "add",
        "dev",
        "eth0",
        "root",
        "netem",
        "delay",
        delay
    ]


    if bandwidth:

        command.extend([
            "rate",
            bandwidth
        ])


    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )


    if result.returncode != 0:

        print("\nERROR applying network condition:")

        if result.stderr:
            print(result.stderr)

        return False


    print("Network condition successfully applied.")

    return True


# ============================================================
# START CONSUMERS
# ============================================================

def start_consumers(approach, devices):

    consumers = []

    for i in range(devices):

        print(
            f"Starting consumer {i + 1}/{devices} "
            f"for {approach}"
        )


        if approach == "full":

            p = subprocess.Popen(
                [
                    "python",
                    "kafka_consumer_full.py"
                ]
            )

        else:

            p = subprocess.Popen(
                [
                    "python",
                    "kafka_consumer_meta.py"
                ]
            )


        consumers.append(p)


    return consumers


# ============================================================
# STOP CONSUMERS
# ============================================================

def stop_consumers(consumers):

    for p in consumers:

        try:

            p.terminate()

        except Exception:

            pass


    time.sleep(2)


# ============================================================
# RUN PRODUCER
# ============================================================

def run_producer(approach, model):

    start_time = time.perf_counter()


    try:

        if approach == "full":

            result = subprocess.run(
                [
                    "python",
                    "kafka_producer_full.py",
                    model
                ],
                capture_output=True,
                text=True
            )


        else:

            # IMPORTANT:
            # Pass the model so metadata corresponds
            # to the correct 10/50/100 MB model.

            result = subprocess.run(
                [
                    "python",
                    "kafka_producer_meta.py",
                    model
                ],
                capture_output=True,
                text=True
            )


    except Exception as e:

        end_time = time.perf_counter()

        return (
            end_time - start_time,
            False,
            str(e),
            ""
        )


    end_time = time.perf_counter()

    elapsed = end_time - start_time


    output = result.stdout

    error = result.stderr


    success = result.returncode == 0


    return (
        elapsed,
        success,
        error,
        output
    )


# ============================================================
# EXTRACT FAILURE REASON
# ============================================================

def get_failure_reason(error, output):

    combined = ""

    if error:
        combined += error

    if output:
        combined += "\n" + output


    combined = combined.strip()


    if not combined:

        return "Producer process failed"


    # Specifically identify Kafka message-size problem
    if (
        "MessageSizeTooLargeError" in combined
        or "Message too large" in combined
        or "message size" in combined.lower()
    ):

        return "MessageSizeTooLargeError"


    # Kafka connection problems
    if "KafkaTimeoutError" in combined:

        return "KafkaTimeoutError"


    if "NoBrokersAvailable" in combined:

        return "NoBrokersAvailable"


    return combined.replace("\n", " ")[:1000]


# ============================================================
# SAVE RESULT
# ============================================================

def save_result(
    approach,
    condition,
    delay,
    bandwidth,
    latency,
    throughput,
    model,
    model_size,
    devices,
    status,
    failure_reason
):

    if bandwidth is None:

        bandwidth_value = "Unlimited"

    else:

        bandwidth_value = bandwidth.replace(
            "mbit",
            ""
        )


    with open(
        RESULTS_FILE,
        "a",
        newline=""
    ) as f:

        writer = csv.writer(f)


        writer.writerow([

            approach,

            condition,

            delay.replace(
                "ms",
                ""
            ),

            bandwidth_value,

            round(
                latency * 1000,
                3
            )
            if latency is not None
            else "",

            round(
                throughput,
                3
            )
            if throughput is not None
            else "",

            model,

            round(
                model_size,
                2
            )
            if model_size is not None
            else "",

            devices,

            status,

            failure_reason
        ])


# ============================================================
# MAIN EXPERIMENT
# ============================================================

print("\n")

print("=" * 70)

print("KAFKA MODEL DISTRIBUTION EXPERIMENT")

print("=" * 70)


tc_available = check_tc()


# ============================================================
# APPROACH LOOP
# ============================================================

for approach in approaches:

    print("\n")

    print("=" * 70)

    print(
        f"TESTING APPROACH: "
        f"{approach.upper()}"
    )

    print("=" * 70)


    # ========================================================
    # NETWORK CONDITION LOOP
    # ========================================================

    for condition, settings in network_conditions.items():

        print("\n")

        print("-" * 70)

        print(
            f"NETWORK CONDITION: "
            f"{condition.upper()}"
        )

        print("-" * 70)


        delay = settings["delay"]

        bandwidth = settings["bandwidth"]


        # ----------------------------------------------------
        # APPLY NETWORK CONDITION
        # ----------------------------------------------------

        if tc_available:

            network_applied = apply_network_condition(
                delay,
                bandwidth
            )

        else:

            network_applied = False


            if condition != "normal":

                print(
                    "WARNING: Network condition "
                    "was NOT applied."
                )


        # ====================================================
        # MODEL LOOP
        # ====================================================

        for model in model_files:

            model_size = get_model_size_mb(model)


            if model_size is None:

                print(
                    f"WARNING: Model file not found: "
                    f"{model}"
                )

                continue


            # =================================================
            # DEVICE LOOP
            # =================================================

            for devices in device_counts:

                print("\n")

                print("=" * 70)


                print(
                    f"TEST: Kafka | "
                    f"{approach.upper()} | "
                    f"{condition.upper()} | "
                    f"{model} | "
                    f"devices={devices}"
                )


                print("=" * 70)


                print(
                    f"Model size: "
                    f"{model_size:.2f} MB"
                )

                print(
                    f"Network delay: "
                    f"{delay}"
                )

                print(
                    f"Network bandwidth: "
                    f"{bandwidth if bandwidth else 'Unlimited'}"
                )


                # ------------------------------------------------
                # START CONSUMERS
                # ------------------------------------------------

                consumers = start_consumers(
                    approach,
                    devices
                )


                # Allow consumers to connect
                time.sleep(3)


                # ------------------------------------------------
                # RUN PRODUCER
                # ------------------------------------------------

                latency, success, error, output = run_producer(
                    approach,
                    model
                )


                # ------------------------------------------------
                # CALCULATE THROUGHPUT
                # ------------------------------------------------

                throughput = None


                if (
                    success
                    and approach == "full"
                    and model_size is not None
                    and latency > 0
                ):

                    throughput = (
                        model_size
                        * 8
                        / latency
                    )


                # ------------------------------------------------
                # STATUS
                # ------------------------------------------------

                if success:

                    status = "Success"

                    failure_reason = ""


                else:

                    status = "Failed"

                    failure_reason = get_failure_reason(
                        error,
                        output
                    )


                # ------------------------------------------------
                # DISPLAY PRODUCER OUTPUT
                # ------------------------------------------------

                print("\n")

                print("=" * 70)

                print("PRODUCER OUTPUT")

                print("=" * 70)


                if output:

                    print(output)


                # ------------------------------------------------
                # DISPLAY ERROR
                # ------------------------------------------------

                if error:

                    print("\n")

                    print("=" * 70)

                    print("PRODUCER ERROR")

                    print("=" * 70)

                    print(error)


                # ------------------------------------------------
                # DISPLAY EXPERIMENT RESULT
                # ------------------------------------------------

                print("\n")

                print("=" * 70)

                print("EXPERIMENT RESULT")

                print("=" * 70)


                print(
                    "Approach:",
                    approach
                )

                print(
                    "Network:",
                    condition
                )

                print(
                    "Model:",
                    model
                )

                print(
                    "Model size:",
                    f"{model_size:.2f} MB"
                )

                print(
                    "Devices:",
                    devices
                )

                print(
                    "Status:",
                    status
                )


                print(
                    "Latency:",
                    f"{latency * 1000:.3f} ms"
                )


                if throughput is not None:

                    print(
                        "Throughput:",
                        f"{throughput:.3f} Mbps"
                    )

                else:

                    print(
                        "Throughput: N/A"
                    )


                if failure_reason:

                    print(
                        "Failure reason:",
                        failure_reason
                    )


                # ------------------------------------------------
                # SAVE RESULT
                # ------------------------------------------------

                save_result(

                    approach,

                    condition,

                    delay,

                    bandwidth,

                    latency,

                    throughput,

                    model,

                    model_size,

                    devices,

                    status,

                    failure_reason
                )


                # ------------------------------------------------
                # STOP CONSUMERS
                # ------------------------------------------------

                stop_consumers(
                    consumers
                )


                time.sleep(2)


# ============================================================
# RESET NETWORK
# ============================================================

if tc_available:

    reset_network_condition()


# ============================================================
# COMPLETE
# ============================================================

print("\n")

print("=" * 70)

print(
    "ALL KAFKA EXPERIMENTS COMPLETED!"
)

print("=" * 70)


print("\nResults saved to:")

print(
    os.path.abspath(
        RESULTS_FILE
    )
)