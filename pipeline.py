"""
Pipeline orchestrator: Generate data -> Bronze -> Silver -> Gold -> Dashboard

This script runs the complete data pipeline:
1. Generate synthetic data (clients & purchases)
2. Bronze ingestion (raw data)
3. Silver transformation (cleaned data)
4. Gold aggregation (business metrics)
5. Launch Streamlit dashboard
"""

import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Add flows directory to path
sys.path.insert(0, str(Path(__file__).parent / "flows"))
sys.path.insert(0, str(Path(__file__).parent / "script"))

from generate_data import generate_clients, generate_purchases
from flows.bronze_ingestion import bronze_ingestion_flow
from flows.silver_transformation import silver_transformation_flow
from flows.gold_aggregation import gold_aggregation_flow


def run_pipeline(n_clients: int = 500, n_purchases_per_client: int = 5, launch_dashboard: bool = True):
    """
    Run the complete data pipeline.
    
    Args:
        n_clients: Number of clients to generate
        n_purchases_per_client: Average number of purchases per client
        launch_dashboard: Whether to launch Streamlit dashboard after pipeline
    """
    print("\n" + "="*70)
    print(" "*20 + "DATA PIPELINE EXECUTION")
    print("="*70)
    
    # Generate timestamp for this pipeline run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\n📅 Pipeline Run ID: {timestamp}")
    print(f"👥 Clients to generate: {n_clients}")
    print(f"🛒 Purchases per client: ~{n_purchases_per_client}")
    
    # === STEP 1: GENERATE DATA ===
    print("\n" + "="*70)
    print("STEP 1/4: DATA GENERATION")
    print("="*70)
    
    data_dir = Path("./data/sources")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    clients_path = data_dir / "clients.csv"
    purchases_path = data_dir / "purchases.csv"
    
    print(f"\n📝 Generating {n_clients} clients...")
    client_ids = generate_clients(n_clients, str(clients_path))
    
    print(f"\n🛒 Generating purchases...")
    generate_purchases(client_ids, str(purchases_path), avg_purchases_per_client=n_purchases_per_client)
    
    print(f"\n✅ Data generation complete!")
    print(f"   - {clients_path}")
    print(f"   - {purchases_path}")
    
    # === STEP 2: BRONZE INGESTION ===
    print("\n" + "="*70)
    print("STEP 2/4: BRONZE LAYER - RAW DATA INGESTION")
    print("="*70)
    
    bronze_result = bronze_ingestion_flow(data_dir=str(data_dir), timestamp=timestamp)
    
    print(f"\n✅ Bronze ingestion complete!")
    print(f"   - Clients: {bronze_result['clients']}")
    print(f"   - Purchases: {bronze_result['achats']}")
    
    # === STEP 3: SILVER TRANSFORMATION ===
    print("\n" + "="*70)
    print("STEP 3/4: SILVER LAYER - DATA CLEANING & TRANSFORMATION")
    print("="*70)
    
    silver_result = silver_transformation_flow(
        clients_file=bronze_result['clients'],
        purchases_file=bronze_result['achats']
    )
    
    print(f"\n✅ Silver transformation complete!")
    print(f"   - Clients: {silver_result['clients']['silver_object']}")
    print(f"   - Purchases: {silver_result['purchases']['silver_object']}")
    
    # === STEP 4: GOLD AGGREGATION ===
    print("\n" + "="*70)
    print("STEP 4/4: GOLD LAYER - BUSINESS AGGREGATIONS & KPIs")
    print("="*70)
    
    gold_result = gold_aggregation_flow(
        clients_file=silver_result['clients']['silver_object'],
        purchases_file=silver_result['purchases']['silver_object']
    )
    
    print(f"\n✅ Gold aggregation complete!")
    print(f"   - {len(gold_result['fact_dimension_tables'])} fact/dimension tables")
    print(f"   - {len(gold_result['aggregations'])} aggregation tables")
    print(f"   - {len(gold_result['statistics'])} statistical tables")
    
    # === PIPELINE SUMMARY ===
    print("\n" + "="*70)
    print(" "*25 + "PIPELINE SUMMARY")
    print("="*70)
    
    print(f"\n✅ Pipeline Run ID: {timestamp}")
    print(f"✅ Data Generated: {n_clients} clients, ~{n_clients * n_purchases_per_client} purchases")
    print(f"✅ Bronze Layer: 2 raw files")
    print(f"✅ Silver Layer: 2 cleaned files")
    print(f"✅ Gold Layer: 13 aggregated files")
    print(f"\n🎯 Total: 17 files created in MinIO")
    
    print(f"\n📊 Data Quality:")
    print(f"   - Clients retained: {silver_result['clients']['cleaning_stats']['final_rows']} / {silver_result['clients']['cleaning_stats']['initial_rows']}")
    print(f"   - Purchases retained: {silver_result['purchases']['cleaning_stats']['final_rows']} / {silver_result['purchases']['cleaning_stats']['initial_rows']}")
    
    # === LAUNCH DASHBOARD ===
    if launch_dashboard:
        print("\n" + "="*70)
        print("🚀 LAUNCHING STREAMLIT DASHBOARD")
        print("="*70)
        print("\n📊 Opening dashboard in your browser...")
        print("   Press Ctrl+C to stop the dashboard")
        print("\n" + "="*70 + "\n")
        
        # Launch Streamlit with Python module
        subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard.py"])
    else:
        print("\n" + "="*70)
        print("\n✅ Pipeline complete! To view the dashboard, run:")
        print("   streamlit run dashboard.py")
        print("\n" + "="*70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the complete data pipeline")
    parser.add_argument("--clients", type=int, default=500, help="Number of clients to generate (default: 500)")
    parser.add_argument("--purchases", type=int, default=5, help="Average purchases per client (default: 5)")
    parser.add_argument("--no-dashboard", action="store_true", help="Don't launch dashboard after pipeline")
    
    args = parser.parse_args()
    
    run_pipeline(
        n_clients=args.clients,
        n_purchases_per_client=args.purchases,
        launch_dashboard=True  # Always launch dashboard
    )
