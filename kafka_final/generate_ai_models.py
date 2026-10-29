
from pathlib import Path
import math
import torch
import torch.nn as nn

# Save the model files separately from your existing experiment results.
OUTPUT_DIR = Path("")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_SIZES = {
    "model_10MB.pth": 10 * 1024 * 1024,
    "model_50MB.pth": 50 * 1024 * 1024,
    "model_100MB.pth": 100 * 1024 * 1024,
}


class DistributionMLP(nn.Module):
    """A feed-forward neural network with MNIST-shaped input."""
    def __init__(self, hidden_size):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(784, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 10),
        )

    def forward(self, x):
        return self.network(x)


def parameter_count(hidden_size):
    # Weights and biases in all three Linear layers.
    return (
        784 * hidden_size + hidden_size
        + hidden_size * hidden_size + hidden_size
        + hidden_size * 10 + 10
    )


def choose_width(target_bytes):
    # Each float32 parameter uses 4 bytes.
    target_parameters = target_bytes // 4

    # Find a hidden width that gets close to the target.
    width = max(
        1,
        int(
            (-796 + math.sqrt(796**2 + 4 * (target_parameters - 10)))
            / 2
        ),
    )

    # Check nearby widths and choose the closest parameter count.
    candidates = range(max(1, width - 3), width + 4)
    return min(
        candidates,
        key=lambda h: abs(parameter_count(h) * 4 - target_bytes),
    )


for filename, target_bytes in TARGET_SIZES.items():
    hidden_size = choose_width(target_bytes)
    model = DistributionMLP(hidden_size)

    checkpoint = {
        "architecture": "DistributionMLP",
        "input_features": 784,
        "hidden_size": hidden_size,
        "output_classes": 10,
        "state_dict": model.state_dict(),
    }

    output_path = OUTPUT_DIR / filename
    torch.save(checkpoint, output_path)

    actual_bytes = output_path.stat().st_size
    print(f"\nFile: {output_path}")
    print(f"Hidden width: {hidden_size}")
    print(f"Target size: {target_bytes:,} bytes")
    print(f"Actual size: {actual_bytes:,} bytes")
    print(f"Actual size: {actual_bytes / (1024 * 1024):.2f} MiB")

print("\nModel generation complete.")
