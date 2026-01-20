from pathlib import Path
import sys
import os
import time
from datetime import datetime

# Configurer Prefect pour fonctionner sans serveur (mode ephemeral)
os.environ["PREFECT_API_URL"] = ""

from prefect import flow, task
from prefect.states import Completed, Failed

# Ajouter le dossier parent au path pour importer les modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Importer les sous-flows
from flows.bronze_ingestion import bronze_ingestion_flow
from flows.silver_transformation import silver_transformation_flow
from flows.gold_transformation import gold_transformation_flow


@task(name="log_step")
def log_step(step_number: int, step_name: str, status: str = "start") -> None:
    """
    Log pipeline step progress for visibility.

    Args:
        step_number: Current step number (1, 2, or 3)
        step_name: Name of the step
        status: Either "start" or "complete"
    """
    if status == "start":
        print(f"\n[{step_number}/3] 🔄 {step_name}...")
    else:
        print(f"[{step_number}/3] ✅ {step_name} complete!")


@task(name="validate_source_files")
def validate_source_files(data_dir: str) -> bool:
    """
    Validate that required source files exist before starting pipeline.

    This pre-flight check prevents the pipeline from failing midway
    due to missing input files.

    Args:
        data_dir: Directory containing source CSV files

    Returns:
        True if all files exist

    Raises:
        FileNotFoundError: If any required file is missing
    """
    data_path = Path(data_dir)
    required_files = ["clients.csv", "achats.csv"]
    
    missing_files = []
    for file_name in required_files:
        file_path = data_path / file_name
        if not file_path.exists():
            missing_files.append(str(file_path))
    
    if missing_files:
        raise FileNotFoundError(
            f"Missing required files: {', '.join(missing_files)}"
        )
    
    print(f"✅ All source files validated in {data_dir}")
    return True


@task(name="generate_pipeline_report")
def generate_pipeline_report(
    start_time: float,
    bronze_result: dict,
    silver_result: dict,
    gold_result: dict
) -> dict:
    """
    Generate a comprehensive report of the pipeline execution.

    Consolidates results from all three layers into a single report
    with timing information and row counts.

    Args:
        start_time: Pipeline start timestamp
        bronze_result: Results from bronze ingestion
        silver_result: Results from silver transformation
        gold_result: Results from gold transformation

    Returns:
        Complete pipeline report dictionary
    """
    end_time = time.time()
    duration = end_time - start_time
    
    report = {
        "pipeline_run": {
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": round(duration, 2),
            "status": "SUCCESS"
        },
        "bronze_layer": {
            "files_ingested": list(bronze_result.keys()),
            "details": bronze_result
        },
        "silver_layer": {
            "clients": {
                "rows_input": silver_result["clients"]["rows_raw"],
                "rows_output": silver_result["clients"]["rows_clean"],
                "rows_filtered": (
                    silver_result["clients"]["rows_raw"] - 
                    silver_result["clients"]["rows_clean"]
                )
            },
            "achats": {
                "rows_input": silver_result["achats"]["rows_raw"],
                "rows_output": silver_result["achats"]["rows_clean"],
                "rows_filtered": (
                    silver_result["achats"]["rows_raw"] - 
                    silver_result["achats"]["rows_clean"]
                )
            }
        },
        "gold_layer": {
            table_name: info["rows"] 
            for table_name, info in gold_result.items()
        }
    }
    
    return report


@task(name="print_final_report")
def print_final_report(report: dict) -> None:
    """
    Print a formatted final report to console.

    Provides clear visibility into what the pipeline accomplished,
    useful for debugging and monitoring.

    Args:
        report: Complete pipeline report dictionary
    """
    print("\n" + "=" * 60)
    print("🎉 PIPELINE EXECUTION COMPLETE")
    print("=" * 60)
    
    # Timing info
    run_info = report["pipeline_run"]
    print(f"\n⏱️  Duration: {run_info['duration_seconds']} seconds")
    print(f"📅 Timestamp: {run_info['timestamp']}")
    
    # Bronze layer
    print(f"\n🥉 BRONZE LAYER")
    print(f"   Files ingested: {', '.join(report['bronze_layer']['files_ingested'])}")
    
    # Silver layer
    print(f"\n🥈 SILVER LAYER")
    silver = report["silver_layer"]
    print(f"   Clients: {silver['clients']['rows_input']} → {silver['clients']['rows_output']} rows")
    print(f"   Achats:  {silver['achats']['rows_input']} → {silver['achats']['rows_output']} rows")
    
    # Gold layer
    print(f"\n🥇 GOLD LAYER")
    for table_name, row_count in report["gold_layer"].items():
        print(f"   {table_name}: {row_count} rows")
    
    print("\n" + "=" * 60)
    print("✅ All data successfully processed!")
    print("=" * 60 + "\n")


@flow(name="Main Data Pipeline", log_prints=True)
def main_pipeline(
    data_dir: str = "./data/sources",
    skip_validation: bool = False
) -> dict:
    """
    Main orchestrator flow that runs the complete data pipeline.

    This flow coordinates the execution of:
    1. Bronze Ingestion: Raw data from sources to bronze bucket
    2. Silver Transformation: Clean and standardize data
    3. Gold Transformation: Create business aggregations

    The flow ensures proper sequencing, error handling, and reporting.

    Args:
        data_dir: Directory containing source CSV files
        skip_validation: Skip source file validation (for testing)

    Returns:
        Complete pipeline execution report
    """
    print("\n" + "=" * 60)
    print("🚀 STARTING FULL DATA PIPELINE")
    print("=" * 60)
    
    start_time = time.time()
    
    # Pre-flight validation
    if not skip_validation:
        validate_source_files(data_dir)
    
    # Step 1: Bronze Ingestion
    log_step(1, "Bronze Ingestion", "start")
    bronze_result = bronze_ingestion_flow(data_dir=data_dir)
    log_step(1, "Bronze Ingestion", "complete")
    
    # Step 2: Silver Transformation
    log_step(2, "Silver Transformation", "start")
    silver_result = silver_transformation_flow()
    log_step(2, "Silver Transformation", "complete")
    
    # Step 3: Gold Transformation
    log_step(3, "Gold Transformation", "start")
    gold_result = gold_transformation_flow()
    log_step(3, "Gold Transformation", "complete")
    
    # Generate and print report
    report = generate_pipeline_report(
        start_time, 
        bronze_result, 
        silver_result, 
        gold_result
    )
    print_final_report(report)
    
    return report


if __name__ == "__main__":
    # Allow custom data directory from command line
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the full data pipeline")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data/sources",
        help="Directory containing source CSV files"
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip source file validation"
    )
    
    args = parser.parse_args()
    
    result = main_pipeline(
        data_dir=args.data_dir,
        skip_validation=args.skip_validation
    )
