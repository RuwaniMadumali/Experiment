import subprocess
import time
import os
import csv
import sys
import re


# ==================================================
# CONFIGURATION
# ==================================================

model_files = [
    "model_10MB.pth",
    "model_50MB.pth",
    "model_100MB.pth"
]

device_counts = [1, 3, 5]

network_conditions = [
    {
        "name": "normal",
        "delay": "0ms",
        "rate": None
    },
    {
        "name": "moderate",
        "delay": "50ms",
        "rate": "10mbit"
    },
    {
        "name": "constrained",
        "delay": "100ms",
        "rate": "5mbit"
    }
]

approaches = [
    {
        "name": "Full Model",
        "producer": "producer_full.py",
        "consumer": "consumer_full.py",
        "queue": "model_queue"
    },
    {
        "name": "Metadata",
        "producer": "producer_metadata.py",
        "consumer": "consumer_metadata.py",
        "queue": "meta_queue"
    }
]


csv_file = "results.csv"


# ==================================================
# REMOVE OLD CSV
# ==================================================

if os.path.exists(csv_file):
    os.remove(csv_file)


with open(csv_file, "w", newline="") as file:

    writer = csv.writer(file)

    writer.writerow([
        "Approach",
        "Network Condition",
        "Latency (ms)",
        "Bandwidth",
        "Model File",
        "Model Size (MB)",
        "Device Count",
        "Status"
    ])


# ==================================================
# NETWORK CONTROL
# ==================================================

def apply_network(condition):

    print(
        f"Applying network: "
        f"{condition['name']} | "
        f"Delay: {condition['delay']} | "
        f"Rate: {condition['rate']}",
        flush=True
    )

    # Remove previous qdisc
    subprocess.run(
        [
            "docker",
            "exec",
            "rabbitmq",
            "tc",
            "qdisc",
            "del",
            "dev",
            "eth0",
            "root"
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # Normal
    if condition["rate"] is None:

        subprocess.run(
            [
                "docker",
                "exec",
                "rabbitmq",
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "root",
                "netem",
                "delay",
                condition["delay"]
            ],
            check=False
        )

    # Moderate / constrained
    else:

        subprocess.run(
            [
                "docker",
                "exec",
                "rabbitmq",
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "root",
                "handle",
                "1:",
                "netem",
                "delay",
                condition["delay"]
            ],
            check=False
        )

        subprocess.run(
            [
                "docker",
                "exec",
                "rabbitmq",
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "parent",
                "1:",
                "handle",
                "2:",
                "tbf",
                "rate",
                condition["rate"],
                "burst",
                "32kbit",
                "latency",
                "400ms"
            ],
            check=False
        )


def reset_network():

    subprocess.run(
        [
            "docker",
            "exec",
            "rabbitmq",
            "tc",
            "qdisc",
            "del",
            "dev",
            "eth0",
            "root"
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


# ==================================================
# PURGE QUEUE
# ==================================================

def purge_queue(queue):

    subprocess.run(
        [
            "docker",
            "exec",
            "rabbitmq",
            "rabbitmqctl",
            "purge_queue",
            queue
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


# ==================================================
# GET TIMEOUT
# ==================================================

def get_timeout(model, network):

    size = int(
        model
        .replace("model_", "")
        .replace("MB.pth", "")
    )

    if network == "normal":
        return max(60, size * 2)

    if network == "moderate":
        return max(120, size * 5)

    if network == "constrained":
        return max(180, size * 5)

    return 180


# ==================================================
# EXTRACT LATENCY
# ==================================================

def extract_latency(output):

    match = re.search(
        r"Latency:\s*([0-9.]+)s",
        output
    )

    if match:
        seconds = float(match.group(1))
        return seconds * 1000

    return None


# ==================================================
# RUN EXPERIMENT
# ==================================================

for approach in approaches:

    print("\n========================================")
    print(f"APPROACH: {approach['name']}")
    print("========================================")

    for net in network_conditions:

        apply_network(net)

        for model in model_files:

            model_size = int(
                model
                .replace("model_", "")
                .replace("MB.pth", "")
            )

            for devices in device_counts:

                print("\n----------------------------------------")
                print(
                    f"Test: "
                    f"{approach['name']} | "
                    f"{net['name']} | "
                    f"{model} | "
                    f"devices={devices}"
                )
                print("----------------------------------------")

                # ----------------------------------
                # CLEAR OLD MESSAGES
                # ----------------------------------

                purge_queue(
                    approach["queue"]
                )

                # ----------------------------------
                # START CONSUMERS
                # ----------------------------------

                consumers = []

                for i in range(devices):

                    p = subprocess.Popen(
                        [
                            sys.executable,
                            "-u",
                            approach["consumer"],
                            model
                        ],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1
                    )

                    consumers.append(p)

                print(
                    f"Started {devices} device process(es)",
                    flush=True
                )

                # Give consumers time to connect
                time.sleep(3)

                # ----------------------------------
                # START PRODUCER
                # ----------------------------------

                print(
                    f"Starting producer for {model}...",
                    flush=True
                )

                producer_result = subprocess.run(
                    [
                        sys.executable,
                        "-u",
                        approach["producer"],
                        model,
                        str(devices)
                    ],
                    capture_output=True,
                    text=True
                )

                if producer_result.stdout:
                    print(
                        producer_result.stdout,
                        end="",
                        flush=True
                    )

                if producer_result.stderr:
                    print(
                        producer_result.stderr,
                        end="",
                        flush=True
                    )

                # ----------------------------------
                # WAIT FOR CONSUMERS
                # ----------------------------------

                timeout = get_timeout(
                    model,
                    net["name"]
                )

                print(
                    f"Waiting up to {timeout}s "
                    f"for devices...",
                    flush=True
                )

                consumer_outputs = []

                for index, p in enumerate(consumers):

                    try:

                        output, _ = p.communicate(
                            timeout=timeout
                        )

                        print(
                            output,
                            end="",
                            flush=True
                        )

                        consumer_outputs.append(
                            output
                        )

                    except subprocess.TimeoutExpired:

                        print(
                            f"Device process {index + 1} "
                            f"timed out.",
                            flush=True
                        )

                        p.kill()

                        output, _ = p.communicate()

                        if output:
                            print(
                                output,
                                end="",
                                flush=True
                            )

                        consumer_outputs.append(
                            output or ""
                        )

                # ----------------------------------
                # SAVE RESULTS
                # ----------------------------------

                successful = 0

                for output in consumer_outputs:

                    latency_ms = extract_latency(
                        output
                    )

                    if latency_ms is not None:

                        status = "Completed"
                        successful += 1

                    else:

                        if "timed out" in output.lower():
                            status = "Timeout"
                        else:
                            status = "Failed"

                    with open(
                        csv_file,
                        "a",
                        newline=""
                    ) as file:

                        writer = csv.writer(file)

                        writer.writerow([
                            approach["name"],
                            net["name"],
                            round(latency_ms, 2)
                            if latency_ms is not None
                            else "",
                            net["rate"]
                            if net["rate"]
                            else "Unlimited",
                            model,
                            model_size,
                            devices,
                            status
                        ])

                # ----------------------------------
                # SUMMARY
                # ----------------------------------

                print("\nTest Summary:")
                print(
                    f"Approach: {approach['name']}"
                )
                print(
                    f"Network: {net['name']}"
                )
                print(
                    f"Model: {model}"
                )
                print(
                    f"Devices: {devices}"
                )
                print(
                    f"Successful devices: "
                    f"{successful}/{devices}"
                )

        reset_network()


# ==================================================
# FINISHED
# ==================================================

print("\n========================================")
print("ALL EXPERIMENTS COMPLETED!")
print("========================================")

print(
    f"Results saved to: {csv_file}"
)