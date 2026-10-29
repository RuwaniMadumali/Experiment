
"""
Chapter 4 figure generator: RabbitMQ vs Kafka AI model distribution.

Input files:
    results_kafka.csv
    results_rabbitmq.csv

Output:
    thesis_figures/
        Figure_4_1_successful_latency_by_size.png
        Figure_4_2_latency_by_network.png
        Figure_4_3_latency_by_device_count.png
        Figure_4_4_rabbitmq_full_model_throughput.png
        Figure_4_5_success_rate.png
        Figure_4_6_full_model_failures.png
        Figure_4_7_metadata_latency.png
        Figure_4_8_latency_heatmap.png
        Figure_4_9_legacy_rabbitmq_latency.png
        Figure_4_10_legacy_rabbitmq_bandwidth.png
        Figure_4_11_metadata_latency_vs_model_size.png
        Figure_4_12_delivery_outcomes.png
        cleaned_combined_results.csv
        legacy_rabbitmq_results.csv
        summary_by_broker_approach.csv
        summary_by_network.csv
        throughput_results.csv

Important:
- Successful full-model throughput is calculated from file size and
  delivery latency.
- Failed deliveries are excluded from throughput calculations.
- Metadata latency is NOT treated as full-model download latency.
- Legacy RabbitMQ rows are kept separate because their columns do not
  describe the same experiment format as the Full Model/Metadata rows.
"""

from pathlib import Path
import re
import warnings

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ============================================================
# 1. FILE SETTINGS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

KAFKA_FILE = BASE_DIR / "results_kafka.csv"
RABBITMQ_FILE = BASE_DIR / "results.csv"

OUTPUT_DIR = BASE_DIR / "thesis_figures"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MODEL_SIZE_ORDER = [10, 50, 100]
NETWORK_ORDER = ["normal", "moderate", "constrained"]
DEVICE_ORDER = [1, 3, 5]

# Set True only if the supplied latency measures the COMPLETE
# metadata-based model download, not just the broker metadata message.
METADATA_LATENCY_IS_FULL_DISTRIBUTION = False

# The legacy RabbitMQ rows contain file sizes in bytes in the
# "Network Condition" column. This divisor converts bytes to MB.
BYTES_PER_MB = 1_000_000

plt.rcParams.update({
    "figure.dpi": 120,
    "savefig.dpi": 300,
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "legend.fontsize": 9,
    "figure.autolayout": True,
})


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def save_figure(filename):
    """Save the current figure and close it."""
    path = OUTPUT_DIR / filename
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")


def numeric(value):
    """Convert a value to float, returning NaN when invalid."""
    if pd.isna(value):
        return np.nan

    text = str(value).strip().replace(",", "")

    if not text or text.lower() in {
        "nan", "none", "null", "failed", "unlimited", "-"
    }:
        return np.nan

    try:
        return float(text)
    except (TypeError, ValueError):
        return np.nan


def normalize_network(value):
    """Standardize network condition labels."""
    if pd.isna(value):
        return "unknown"

    value = str(value).strip().lower()

    if value in {"normal", "moderate", "constrained"}:
        return value

    return value


def normalize_approach(value):
    """Standardize experiment approach labels."""
    if pd.isna(value):
        return "unknown"

    value = str(value).strip().lower()

    if "metadata" in value:
        return "metadata"

    if "full" in value:
        return "full"

    if value == "rabbitmq":
        return "legacy_rabbitmq"

    return value


def normalize_status(value):
    """Standardize delivery status."""
    if pd.isna(value):
        return "Unknown"

    value = str(value).strip().lower()

    if value in {"success", "completed", "complete", "successful"}:
        return "Success"

    if value in {"failed", "failure", "error"}:
        return "Failed"

    return "Unknown"


def parse_model_size(value, filename=None):
    """Get model size in MB from the size column or filename."""
    size = numeric(value)

    if np.isfinite(size) and size > 0:
        return size

    if filename and not pd.isna(filename):
        match = re.search(r"(\d+(?:\.\d+)?)\s*MB", str(filename), re.I)
        if match:
            return float(match.group(1))

    return np.nan


def parse_device_count(value):
    """Convert device count to a numeric value."""
    result = numeric(value)

    if np.isfinite(result) and result > 0:
        return int(result)

    return np.nan


def parse_bandwidth(value):
    """Extract a numeric configured bandwidth from strings such as 10mbit."""
    if pd.isna(value):
        return np.nan

    text = str(value).strip().lower()

    if text in {"unlimited", ""}:
        return np.nan

    match = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(match.group(1)) if match else np.nan


def infer_legacy_model_size(size_bytes):
    """Map legacy byte sizes to approximate model sizes in MB."""
    if not np.isfinite(size_bytes):
        return np.nan

    mb = size_bytes / BYTES_PER_MB

    # Match the approximately 10, 50, and 100 MB model files.
    if 8 <= mb <= 15:
        return 10
    if 40 <= mb <= 60:
        return 50
    if 80 <= mb <= 120:
        return 100

    return mb


# ============================================================
# 3. LOAD AND CLEAN KAFKA DATA
# ============================================================

def load_kafka(path):
    if not path.exists():
        raise FileNotFoundError(
            f"Kafka file not found: {path}\n"
            "Place results_kafka.csv beside this script."
        )

    df = pd.read_csv(path, dtype=object)
    df.columns = [str(c).strip() for c in df.columns]

    # Map likely variations of column names.
    aliases = {
        "Approach": ["Approach", "Strategy"],
        "Network Condition": [
            "Network Condition", "Network", "Condition"
        ],
        "Configured Delay (ms)": [
            "Configured Delay (ms)", "Delay (ms)", "Configured Delay"
        ],
        "Configured Bandwidth (Mbps)": [
            "Configured Bandwidth (Mbps)",
            "Configured Bandwidth",
            "Bandwidth"
        ],
        "Latency (ms)": ["Latency (ms)", "Latency", "Delivery Latency (ms)"],
        "Throughput (Mbps)": ["Throughput (Mbps)", "Throughput"],
        "Model File": ["Model File", "Filename", "Model"],
        "Model Size (MB)": ["Model Size (MB)", "Model Size (MB)", "Model Size"],
        "Device Count": ["Device Count", "Devices", "Number of Devices"],
        "Status": ["Status", "Delivery Status", "Result"],
        "Failure Reason": ["Failure Reason", "Error", "Error Message"],
    }

    selected = {}
    for canonical, candidates in aliases.items():
        for candidate in candidates:
            if candidate in df.columns:
                selected[canonical] = df[candidate]
                break

        if canonical not in selected:
            selected[canonical] = pd.Series(
                [np.nan] * len(df), index=df.index
            )

    out = pd.DataFrame(selected)

    out["Broker"] = "Kafka"
    out["Approach"] = out["Approach"].apply(normalize_approach)
    out["Network Condition"] = out["Network Condition"].apply(normalize_network)
    out["Latency (ms)"] = out["Latency (ms)"].apply(numeric)
    out["Model Size (MB)"] = [
        parse_model_size(size, filename)
        for size, filename in zip(out["Model Size (MB)"], out["Model File"])
    ]
    out["Device Count"] = out["Device Count"].apply(parse_device_count)
    out["Status"] = out["Status"].apply(normalize_status)
    out["Configured Bandwidth (Mbps)"] = (
        out["Configured Bandwidth (Mbps)"].apply(parse_bandwidth)
    )
    out["Recorded Throughput (Mbps)"] = (
        out["Throughput (Mbps)"].apply(numeric)
    )

    out["Failure Reason"] = out["Failure Reason"].fillna("").astype(str)
    out["Model File"] = out["Model File"].fillna("").astype(str)

    return out


# ============================================================
# 4. LOAD AND CLEAN RABBITMQ DATA
# ============================================================

def load_rabbitmq(path):
    if not path.exists():
        raise FileNotFoundError(
            f"RabbitMQ file not found: {path}\n"
            "Change RABBITMQ_FILE near the top of the script if needed."
        )

    raw = pd.read_csv(path, dtype=object)
    raw.columns = [str(c).strip() for c in raw.columns]

    required = [
        "Approach", "Network Condition", "Latency (ms)", "Bandwidth",
        "Model File", "Model Size (MB)", "Device Count", "Status"
    ]

    # Add missing columns if the file uses a slightly different schema.
    for col in required:
        if col not in raw.columns:
            raw[col] = np.nan

    cleaned_rows = []
    legacy_rows = []

    for _, row in raw.iterrows():
        approach_original = str(row.get("Approach", "")).strip()
        approach = normalize_approach(approach_original)

        network = row.get("Network Condition", np.nan)
        latency_raw = row.get("Latency (ms)", np.nan)
        bandwidth_raw = row.get("Bandwidth", np.nan)
        model_file = row.get("Model File", np.nan)
        model_size_raw = row.get("Model Size (MB)", np.nan)
        devices_raw = row.get("Device Count", np.nan)
        status_raw = row.get("Status", np.nan)

        # ----------------------------------------------------
        # Legacy rows:
        # Approach = RabbitMQ
        # Network Condition = file size in bytes
        # Latency = recorded latency
        # Bandwidth = recorded numeric value
        # ----------------------------------------------------
        if approach == "legacy_rabbitmq":
            size_bytes = numeric(network)
            legacy_rows.append({
                "Broker": "RabbitMQ",
                "Raw Size (Bytes)": size_bytes,
                "Inferred Model Size (MB)": infer_legacy_model_size(size_bytes),
                "Recorded Latency (ms)": numeric(latency_raw),
                "Recorded Bandwidth Value": numeric(bandwidth_raw),
                "Original Row": str(row.to_dict()),
            })
            continue

        # ----------------------------------------------------
        # Repair rows shifted because the latency field was
        # blank, for example:
        # Full Model | constrained | 5mbit | model_10MB.pth |
        # 10 | 5 | Failed
        #
        # In that case, pandas places 5mbit in the latency
        # field, the filename in the bandwidth field, etc.
        # ----------------------------------------------------
        latency_text = (
            "" if pd.isna(latency_raw) else str(latency_raw).strip().lower()
        )

        if re.fullmatch(r"\d+(?:\.\d+)?\s*(?:mbit|mbps)", latency_text):
            configured_bandwidth = latency_raw
            shifted_model_file = bandwidth_raw
            shifted_model_size = model_file
            shifted_devices = model_size_raw
            shifted_status = devices_raw

            latency_raw = np.nan
            bandwidth_raw = configured_bandwidth
            model_file = shifted_model_file
            model_size_raw = shifted_model_size
            devices_raw = shifted_devices
            status_raw = shifted_status

        # Some files may use an empty latency field but still have
        # the expected model filename shifted one column to the left.
        if (
            pd.isna(model_file)
            and isinstance(bandwidth_raw, str)
            and ".pth" in bandwidth_raw.lower()
        ):
            model_file = bandwidth_raw
            bandwidth_raw = np.nan

        size_mb = parse_model_size(model_size_raw, model_file)

        # If model size is still missing, infer it from filename.
        if not np.isfinite(size_mb):
            size_mb = parse_model_size(np.nan, model_file)

        cleaned_rows.append({
            "Broker": "RabbitMQ",
            "Approach": approach,
            "Network Condition": normalize_network(network),
            "Latency (ms)": numeric(latency_raw),
            "Configured Bandwidth (Mbps)": parse_bandwidth(bandwidth_raw),
            "Model File": "" if pd.isna(model_file) else str(model_file),
            "Model Size (MB)": size_mb,
            "Device Count": parse_device_count(devices_raw),
            "Status": normalize_status(status_raw),
            "Failure Reason": "",
            "Recorded Throughput (Mbps)": np.nan,
        })

    cleaned = pd.DataFrame(cleaned_rows)
    legacy = pd.DataFrame(legacy_rows)

    # The legacy rows are not labelled with network conditions
    # in the same way as the main experiment records.
    if not legacy.empty:
        legacy["Network Condition"] = "not_recorded"

    return cleaned, legacy


# ============================================================
# 5. CALCULATE VALID THROUGHPUT
# ============================================================

def calculate_throughput(df):
    out = df.copy()
    out["Calculated Throughput (Mbps)"] = np.nan

    # Only calculate throughput when:
    # 1. Approach is full-model transmission.
    # 2. Delivery succeeded.
    # 3. Model size and latency are available.
    # 4. Latency is positive.
    #
    # MB * 8 gives megabits; multiply by 1000 / milliseconds.
    valid_full = (
        out["Approach"].eq("full")
        & out["Status"].eq("Success")
        & out["Latency (ms)"].notna()
        & out["Model Size (MB)"].notna()
        & out["Latency (ms)"].gt(0)
    )

    out.loc[valid_full, "Calculated Throughput (Mbps)"] = (
        out.loc[valid_full, "Model Size (MB)"] * 8 * 1000
        / out.loc[valid_full, "Latency (ms)"]
    )

    # Metadata throughput is not calculated unless the metadata
    # latency is known to represent the entire model download.
    if METADATA_LATENCY_IS_FULL_DISTRIBUTION:
        valid_meta = (
            out["Approach"].eq("metadata")
            & out["Status"].eq("Success")
            & out["Latency (ms)"].notna()
            & out["Model Size (MB)"].notna()
            & out["Latency (ms)"].gt(0)
        )

        out.loc[valid_meta, "Calculated Throughput (Mbps)"] = (
            out.loc[valid_meta, "Model Size (MB)"] * 8 * 1000
            / out.loc[valid_meta, "Latency (ms)"]
        )

    return out


# ============================================================
# 6. LOAD DATA AND EXPORT CLEANED TABLES
# ============================================================

kafka = load_kafka(KAFKA_FILE)
rabbitmq, legacy_rabbitmq = load_rabbitmq(RABBITMQ_FILE)

combined = pd.concat([kafka, rabbitmq], ignore_index=True, sort=False)

combined["Approach"] = combined["Approach"].replace({
    "Full Model": "full",
    "Metadata": "metadata",
})

combined["Approach"] = combined["Approach"].apply(normalize_approach)
combined["Status"] = combined["Status"].apply(normalize_status)
combined["Network Condition"] = combined["Network Condition"].apply(
    normalize_network
)

combined = calculate_throughput(combined)

combined.to_csv(OUTPUT_DIR / "cleaned_combined_results.csv", index=False)
legacy_rabbitmq.to_csv(
    OUTPUT_DIR / "legacy_rabbitmq_results.csv", index=False
)

# Keep only the main experiment rows for the main charts.
main = combined[
    combined["Approach"].isin(["full", "metadata"])
].copy()

main = main[main["Network Condition"].isin(NETWORK_ORDER)].copy()

main["Model Size (MB)"] = pd.to_numeric(
    main["Model Size (MB)"], errors="coerce"
)
main["Device Count"] = pd.to_numeric(
    main["Device Count"], errors="coerce"
)
main["Latency (ms)"] = pd.to_numeric(
    main["Latency (ms)"], errors="coerce"
)

main["Broker-Approach"] = (
    main["Broker"] + " - " + main["Approach"].str.title()
)

# Successful delivery measurements for latency comparisons.
successful = main[main["Status"].eq("Success")].copy()

# Successful full-model transmissions only for throughput.
throughput_df = main[
    main["Approach"].eq("full")
    & main["Status"].eq("Success")
    & main["Calculated Throughput (Mbps)"].notna()
].copy()

throughput_df.to_csv(
    OUTPUT_DIR / "throughput_results.csv", index=False
)


# ============================================================
# 7. SUMMARY TABLES
# ============================================================

summary = (
    main.groupby(["Broker", "Approach"], dropna=False)
    .agg(
        Total_Attempts=("Status", "size"),
        Successful=("Status", lambda x: (x == "Success").sum()),
        Failed=("Status", lambda x: (x == "Failed").sum()),
        Unknown_Status=("Status", lambda x: (x == "Unknown").sum()),
        Success_Rate_Percent=(
            "Status",
            lambda x: 100 * (x == "Success").sum() / len(x)
            if len(x) else np.nan
        ),
        Mean_Successful_Latency_ms=(
            "Latency (ms)",
            lambda x: x[
                main.loc[x.index, "Status"].eq("Success")
            ].mean()
        ),
    )
    .reset_index()
)

summary.to_csv(
    OUTPUT_DIR / "summary_by_broker_approach.csv", index=False
)

network_summary = (
    main.groupby(
        ["Broker", "Approach", "Network Condition"],
        dropna=False
    )
    .agg(
        Attempts=("Status", "size"),
        Successful=("Status", lambda x: (x == "Success").sum()),
        Failed=("Status", lambda x: (x == "Failed").sum()),
        Mean_Latency_ms=(
            "Latency (ms)",
            lambda x: x[
                main.loc[x.index, "Status"].eq("Success")
            ].mean()
        ),
    )
    .reset_index()
)

network_summary.to_csv(
    OUTPUT_DIR / "summary_by_network.csv", index=False
)


# ============================================================
# 8. FIGURE 4.1 — SUCCESSFUL LATENCY BY MODEL SIZE
# ============================================================

plot_data = successful[
    successful["Latency (ms)"].notna()
    & successful["Model Size (MB)"].isin(MODEL_SIZE_ORDER)
].copy()

fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=False)

for ax, network in zip(axes, NETWORK_ORDER):
    subset = plot_data[plot_data["Network Condition"] == network]

    grouped = subset.groupby(
        ["Broker", "Approach", "Model Size (MB)"]
    )["Latency (ms)"].mean()

    categories = [
        (broker, approach)
        for broker in ["RabbitMQ", "Kafka"]
        for approach in ["full", "metadata"]
    ]

    x = np.arange(len(MODEL_SIZE_ORDER))
    width = 0.18

    for i, (broker, approach) in enumerate(categories):
        values = []
        for size in MODEL_SIZE_ORDER:
            key = (broker, approach, size)
            values.append(grouped.get(key, np.nan))

        offset = (i - (len(categories) - 1) / 2) * width
        label = f"{broker} {approach.title()}"

        if np.isfinite(np.array(values, dtype=float)).any():
            ax.bar(x + offset, values, width, label=label)

    ax.set_title(network.title())
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s} MB" for s in MODEL_SIZE_ORDER])
    ax.set_xlabel("Model size")
    ax.set_ylabel("Mean successful latency (ms)")
    ax.grid(axis="y", alpha=0.25)

axes[-1].legend(loc="upper left", bbox_to_anchor=(1.02, 1))
fig.suptitle("Figure 4.1. Successful model-distribution latency by model size")
save_figure("Figure_4_1_successful_latency_by_size.png")


# ============================================================
# 9. FIGURE 4.2 — SUCCESSFUL LATENCY BY NETWORK CONDITION
# ============================================================

fig, axes = plt.subplots(1, 3, figsize=(15, 5))

for ax, size in zip(axes, MODEL_SIZE_ORDER):
    subset = plot_data[plot_data["Model Size (MB)"] == size]

    grouped = subset.groupby(
        ["Broker", "Approach", "Network Condition"]
    )["Latency (ms)"].mean()

    labels = [
        ("RabbitMQ", "full"),
        ("RabbitMQ", "metadata"),
        ("Kafka", "full"),
        ("Kafka", "metadata"),
    ]

    x = np.arange(len(NETWORK_ORDER))
    width = 0.18

    for i, (broker, approach) in enumerate(labels):
        values = [
            grouped.get((broker, approach, network), np.nan)
            for network in NETWORK_ORDER
        ]

        offset = (i - (len(labels) - 1) / 2) * width
        if np.isfinite(np.array(values, dtype=float)).any():
            ax.bar(
                x + offset, values, width,
                label=f"{broker} {approach.title()}"
            )

    ax.set_title(f"{size} MB model")
    ax.set_xticks(x)
    ax.set_xticklabels(
        [n.title() for n in NETWORK_ORDER], rotation=15
    )
    ax.set_xlabel("Network condition")
    ax.set_ylabel("Mean successful latency (ms)")
    ax.grid(axis="y", alpha=0.25)

axes[-1].legend(loc="upper left", bbox_to_anchor=(1.02, 1))
fig.suptitle("Figure 4.2. Successful latency across network conditions")
save_figure("Figure_4_2_latency_by_network.png")


# ============================================================
# 10. FIGURE 4.3 — LATENCY BY DEVICE COUNT
# ============================================================

fig, axes = plt.subplots(1, 3, figsize=(15, 5))

for ax, size in zip(axes, MODEL_SIZE_ORDER):
    subset = plot_data[plot_data["Model Size (MB)"] == size]

    grouped = subset.groupby(
        ["Broker", "Approach", "Device Count"]
    )["Latency (ms)"].mean()

    categories = [
        ("RabbitMQ", "full"),
        ("RabbitMQ", "metadata"),
        ("Kafka", "full"),
        ("Kafka", "metadata"),
    ]

    x = np.arange(len(DEVICE_ORDER))
    width = 0.18

    for i, (broker, approach) in enumerate(categories):
        values = [
            grouped.get((broker, approach, device), np.nan)
            for device in DEVICE_ORDER
        ]

        offset = (i - (len(categories) - 1) / 2) * width

        if np.isfinite(np.array(values, dtype=float)).any():
            ax.bar(
                x + offset, values, width,
                label=f"{broker} {approach.title()}"
            )

    ax.set_title(f"{size} MB model")
    ax.set_xticks(x)
    ax.set_xticklabels(DEVICE_ORDER)
    ax.set_xlabel("Device count")
    ax.set_ylabel("Mean successful latency (ms)")
    ax.grid(axis="y", alpha=0.25)

axes[-1].legend(loc="upper left", bbox_to_anchor=(1.02, 1))
fig.suptitle("Figure 4.3. Successful latency by number of receiving devices")
save_figure("Figure_4_3_latency_by_device_count.png")


# ============================================================
# 11. FIGURE 4.4 — RABBITMQ FULL-MODEL THROUGHPUT
# ============================================================

if not throughput_df.empty:
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    for ax, network in zip(axes, NETWORK_ORDER):
        subset = throughput_df[
            throughput_df["Network Condition"] == network
        ]

        grouped = subset.groupby("Model Size (MB)")[
            "Calculated Throughput (Mbps)"
        ].mean()

        values = [
            grouped.get(size, np.nan)
            for size in MODEL_SIZE_ORDER
        ]

        ax.bar(
            [f"{s} MB" for s in MODEL_SIZE_ORDER],
            values
        )
        ax.set_title(network.title())
        ax.set_xlabel("Model size")
        ax.set_ylabel("Calculated throughput (Mbps)")
        ax.grid(axis="y", alpha=0.25)

    fig.suptitle(
        "Figure 4.4. RabbitMQ full-model throughput for successful deliveries"
    )
    save_figure("Figure_4_4_rabbitmq_full_model_throughput.png")
else:
    print(
        "Figure 4.4 skipped: no successful full-model rows with valid "
        "latency and model size were available."
    )


# ============================================================
# 12. FIGURE 4.5 — SUCCESS RATE BY BROKER AND APPROACH
# ============================================================

rate_data = (
    main.groupby(["Broker", "Approach"])
    .agg(
        Attempts=("Status", "size"),
        Successes=("Status", lambda x: (x == "Success").sum()),
        Failures=("Status", lambda x: (x == "Failed").sum()),
    )
    .reset_index()
)

rate_data["Success Rate (%)"] = (
    rate_data["Successes"] / rate_data["Attempts"] * 100
)

rate_data["Label"] = (
    rate_data["Broker"] + "\n" + rate_data["Approach"].str.title()
)

plt.figure(figsize=(9, 5))
plt.bar(rate_data["Label"], rate_data["Success Rate (%)"])
plt.ylabel("Successful deliveries (%)")
plt.xlabel("Broker and approach")
plt.ylim(0, 105)
plt.title("Figure 4.5. Delivery success rate by broker and approach")
plt.grid(axis="y", alpha=0.25)

for i, value in enumerate(rate_data["Success Rate (%)"]):
    plt.text(i, value + 1, f"{value:.1f}%", ha="center")

save_figure("Figure_4_5_success_rate.png")


# ============================================================
# 13. FIGURE 4.6 — FULL-MODEL FAILURES BY MODEL SIZE
# ============================================================

full_data = main[main["Approach"] == "full"].copy()

failure_counts = (
    full_data.assign(
        Failed=full_data["Status"].eq("Failed").astype(int),
        Successful=full_data["Status"].eq("Success").astype(int),
    )
    .groupby(["Broker", "Model Size (MB)"])[["Successful", "Failed"]]
    .sum()
    .reset_index()
)

pivot_fail = failure_counts.pivot(
    index="Model Size (MB)",
    columns="Broker",
    values="Failed"
).reindex(MODEL_SIZE_ORDER)

pivot_success = failure_counts.pivot(
    index="Model Size (MB)",
    columns="Broker",
    values="Successful"
).reindex(MODEL_SIZE_ORDER)

fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)

for ax, broker in zip(axes, ["RabbitMQ", "Kafka"]):
    failed = (
        pivot_fail[broker].fillna(0)
        if broker in pivot_fail.columns
        else pd.Series(0, index=MODEL_SIZE_ORDER)
    )
    succeeded = (
        pivot_success[broker].fillna(0)
        if broker in pivot_success.columns
        else pd.Series(0, index=MODEL_SIZE_ORDER)
    )

    x = np.arange(len(MODEL_SIZE_ORDER))
    ax.bar(x, succeeded, label="Successful")
    ax.bar(x, failed, bottom=succeeded, label="Failed")

    ax.set_title(broker)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s} MB" for s in MODEL_SIZE_ORDER])
    ax.set_xlabel("Model size")
    ax.set_ylabel("Number of attempts")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()

fig.suptitle("Figure 4.6. Full-model transmission outcomes by model size")
save_figure("Figure_4_6_full_model_failures.png")


# ============================================================
# 14. FIGURE 4.7 — METADATA LATENCY BY MODEL SIZE
# ============================================================

metadata = successful[
    successful["Approach"] == "metadata"
].copy()

if not metadata.empty:
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    for ax, network in zip(axes, NETWORK_ORDER):
        subset = metadata[metadata["Network Condition"] == network]

        grouped = subset.groupby(
            ["Broker", "Model Size (MB)"]
        )["Latency (ms)"].mean()

        x = np.arange(len(MODEL_SIZE_ORDER))
        width = 0.35

        for i, broker in enumerate(["RabbitMQ", "Kafka"]):
            values = [
                grouped.get((broker, size), np.nan)
                for size in MODEL_SIZE_ORDER
            ]
            ax.bar(
                x + (i - 0.5) * width,
                values,
                width,
                label=broker
            )

        ax.set_title(network.title())
        ax.set_xticks(x)
        ax.set_xticklabels([f"{s} MB" for s in MODEL_SIZE_ORDER])
        ax.set_xlabel("Model size")
        ax.set_ylabel("Mean metadata latency (ms)")
        ax.grid(axis="y", alpha=0.25)

    axes[-1].legend()
    fig.suptitle(
        "Figure 4.7. Metadata delivery latency by model size and network"
    )
    save_figure("Figure_4_7_metadata_latency.png")
else:
    print("Figure 4.7 skipped: no successful metadata rows were found.")


# ============================================================
# 15. FIGURE 4.8 — LATENCY HEATMAP
# ============================================================

heat = successful[
    successful["Latency (ms)"].notna()
    & successful["Model Size (MB)"].isin(MODEL_SIZE_ORDER)
    & successful["Device Count"].isin(DEVICE_ORDER)
].copy()

if not heat.empty:
    groups = heat.groupby(
        ["Broker", "Approach", "Network Condition"]
    )

    heatmaps = list(groups)

    # Create a single image with one panel per observed combination.
    number = len(heatmaps)
    columns = 3
    rows = int(np.ceil(number / columns))

    fig, axes = plt.subplots(
        rows, columns, figsize=(14, 4 * rows), squeeze=False
    )
    axes_flat = axes.flatten()

    for ax, ((broker, approach, network), subset) in zip(
        axes_flat, heatmaps
    ):
        table = subset.pivot_table(
            index="Device Count",
            columns="Model Size (MB)",
            values="Latency (ms)",
            aggfunc="mean"
        ).reindex(index=DEVICE_ORDER, columns=MODEL_SIZE_ORDER)

        image = ax.imshow(table.values, aspect="auto")
        ax.set_title(f"{broker} / {approach.title()} / {network}")
        ax.set_xticks(range(len(MODEL_SIZE_ORDER)))
        ax.set_xticklabels([f"{s} MB" for s in MODEL_SIZE_ORDER])
        ax.set_yticks(range(len(DEVICE_ORDER)))
        ax.set_yticklabels(DEVICE_ORDER)
        ax.set_xlabel("Model size")
        ax.set_ylabel("Device count")

        for i in range(table.shape[0]):
            for j in range(table.shape[1]):
                value = table.iloc[i, j]
                if pd.notna(value):
                    ax.text(
                        j, i, f"{value:.0f}",
                        ha="center", va="center"
                    )

        fig.colorbar(image, ax=ax, label="Latency (ms)")

    for ax in axes_flat[len(heatmaps):]:
        ax.axis("off")

    fig.suptitle(
        "Figure 4.8. Mean successful latency heatmaps by broker, approach, "
        "network and device count"
    )
    save_figure("Figure_4_8_latency_heatmap.png")
else:
    print("Figure 4.8 skipped: no valid latency data for heatmap.")


# ============================================================
# 16. FIGURE 4.9 — LEGACY RABBITMQ LATENCY
# ============================================================

if (
    not legacy_rabbitmq.empty
    and legacy_rabbitmq["Inferred Model Size (MB)"].notna().any()
):
    legacy = legacy_rabbitmq.dropna(
        subset=["Inferred Model Size (MB)", "Recorded Latency (ms)"]
    ).copy()

    if not legacy.empty:
        grouped = legacy.groupby(
            "Inferred Model Size (MB)"
        )["Recorded Latency (ms)"].mean()

        plt.figure(figsize=(8, 5))
        plt.bar(
            grouped.index.astype(str) + " MB",
            grouped.values
        )
        plt.xlabel("Inferred model size from recorded byte count")
        plt.ylabel("Mean recorded latency (ms)")
        plt.title(
            "Figure 4.9. Legacy RabbitMQ recorded latency "
            "(separate dataset)"
        )
        plt.grid(axis="y", alpha=0.25)
        save_figure("Figure_4_9_legacy_rabbitmq_latency.png")


# ============================================================
# 17. FIGURE 4.10 — LEGACY RABBITMQ BANDWIDTH VALUES
# ============================================================

if (
    not legacy_rabbitmq.empty
    and legacy_rabbitmq["Inferred Model Size (MB)"].notna().any()
):
    legacy_bw = legacy_rabbitmq.dropna(
        subset=[
            "Inferred Model Size (MB)",
            "Recorded Bandwidth Value"
        ]
    ).copy()

    if not legacy_bw.empty:
        grouped = legacy_bw.groupby(
            "Inferred Model Size (MB)"
        )["Recorded Bandwidth Value"].mean()

        plt.figure(figsize=(8, 5))
        plt.bar(
            grouped.index.astype(str) + " MB",
            grouped.values
        )
        plt.xlabel("Inferred model size")
        plt.ylabel("Recorded bandwidth value (source unit unknown)")
        plt.title(
            "Figure 4.10. Legacy RabbitMQ recorded bandwidth values"
        )
        plt.grid(axis="y", alpha=0.25)
        save_figure("Figure_4_10_legacy_rabbitmq_bandwidth.png")


# ============================================================
# 18. FIGURE 4.11 — METADATA LATENCY VS MODEL SIZE
# ============================================================

if not metadata.empty:
    plt.figure(figsize=(9, 6))

    styles = [
        ("RabbitMQ", "normal"),
        ("RabbitMQ", "moderate"),
        ("RabbitMQ", "constrained"),
        ("Kafka", "normal"),
        ("Kafka", "moderate"),
        ("Kafka", "constrained"),
    ]

    for broker, network in styles:
        subset = metadata[
            (metadata["Broker"] == broker)
            & (metadata["Network Condition"] == network)
        ]

        grouped = subset.groupby("Model Size (MB)")[
            "Latency (ms)"
        ].mean().reindex(MODEL_SIZE_ORDER)

        if grouped.notna().any():
            plt.plot(
                grouped.index,
                grouped.values,
                marker="o",
                label=f"{broker} - {network}"
            )

    plt.xlabel("Model size (MB)")
    plt.ylabel("Mean metadata latency (ms)")
    plt.title(
        "Figure 4.11. Relationship between model size and metadata latency"
    )
    plt.xticks(MODEL_SIZE_ORDER)
    plt.grid(alpha=0.25)
    plt.legend()
    save_figure("Figure_4_11_metadata_latency_vs_model_size.png")


# ============================================================
# 19. FIGURE 4.12 — DELIVERY OUTCOMES BY NETWORK
# ============================================================

outcomes = (
    main.groupby(["Broker", "Approach", "Network Condition", "Status"])
    .size()
    .rename("Count")
    .reset_index()
)

outcomes["Group"] = (
    outcomes["Broker"] + "\n"
    + outcomes["Approach"].str.title() + "\n"
    + outcomes["Network Condition"].str.title()
)

group_names = list(outcomes["Group"].drop_duplicates())
statuses = ["Success", "Failed", "Unknown"]

fig, ax = plt.subplots(figsize=(max(10, len(group_names) * 0.6), 6))
bottom = np.zeros(len(group_names))

for status in statuses:
    values = []
    for group in group_names:
        row = outcomes[
            (outcomes["Group"] == group)
            & (outcomes["Status"] == status)
        ]
        values.append(int(row["Count"].sum()) if not row.empty else 0)

    ax.bar(group_names, values, bottom=bottom, label=status)
    bottom += np.array(values)

ax.set_ylabel("Number of attempts")
ax.set_xlabel("Broker / approach / network condition")
ax.set_title("Figure 4.12. Delivery outcomes across experimental conditions")
ax.tick_params(axis="x", labelrotation=70)
ax.grid(axis="y", alpha=0.25)
ax.legend()

save_figure("Figure_4_12_delivery_outcomes.png")


# ============================================================
# 20. FINAL REPORT
# ============================================================

print("\n" + "=" * 65)
print("CHAPTER 4 FIGURE GENERATION COMPLETE")
print("=" * 65)

print(f"\nOutput folder: {OUTPUT_DIR.resolve()}")

print("\nSummary by broker and approach:")
print(summary.to_string(index=False))

print("\nSummary by network condition:")
print(network_summary.to_string(index=False))

print("\nThroughput calculation:")
print(
    "Calculated throughput = Model Size (MB) * 8 * 1000 "
    "/ Latency (ms)"
)
print(
    "Only successful full-model transfers are included in the "
    "calculated-throughput dataset."
)

if not throughput_df.empty:
    print("\nMean calculated full-model throughput:")
    print(
        throughput_df.groupby(
            ["Broker", "Network Condition", "Model Size (MB)"]
        )["Calculated Throughput (Mbps)"]
        .mean()
        .round(3)
        .to_string()
    )

print("\nAll generated figures and summary tables are saved.")
