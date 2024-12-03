from pathlib import Path
from core.utils import parse_args
from core.task2 import calculate_metrics
from core.task3 import calculate_lowest_avg_prices
from core.task4 import top_prescribed_qty


PROJECT_DIR = Path.cwd()
OUTPUT_DIR = Path(PROJECT_DIR, "output")
OUTPUT_TASK2 = Path(OUTPUT_DIR, "task2.json")
OUTPUT_TASK3 = Path(OUTPUT_DIR, "task3.json")
OUTPUT_TASK4 = Path(OUTPUT_DIR, "task4.json")


if __name__ == "__main__":
    args = parse_args()

    num_cores: int = args["cores"]
    input_claims: Path = args["input_claims"]
    input_reverts: Path = args["input_reverts"]
    input_pharmacies: Path = args["input_pharmacies"]

    calculate_metrics(
        input_claims_dir=input_claims,
        input_reverts_dir=input_reverts,
        workers=num_cores,
        output_path=OUTPUT_TASK2
    )

    calculate_lowest_avg_prices(
        metrics_json_path=OUTPUT_TASK2,
        input_pharmacies_dir=input_pharmacies,
        top_n=2,
        output_path=OUTPUT_TASK3
    )

    top_prescribed_qty(
        input_claims_dir=input_claims,
        workers=10,
        top_n=5,
        output_path=OUTPUT_TASK4
    )