"""
Benchmark simple Pandas vs PySpark sans Prefect
"""
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
import subprocess
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration
CLIENTS_FILE = "data/sources/clients.csv"
PURCHASES_FILE = "data/sources/purchases.csv"


def benchmark_pandas():
    """Transformation Pandas"""
    print("\n" + "="*60)
    print("PANDAS TRANSFORMATION")
    print("="*60)
    
    start_time = time.time()
    
    # Charger les données
    df_clients = pd.read_csv(CLIENTS_FILE)
    df_purchases = pd.read_csv(PURCHASES_FILE)
    
    print(f"✓ Clients chargés: {len(df_clients)} lignes")
    print(f"✓ Purchases chargés: {len(df_purchases)} lignes")
    
    # Nettoyage clients
    df_clients['date_inscription'] = pd.to_datetime(df_clients['date_inscription'], errors='coerce')
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df_clients = df_clients[df_clients['email'].str.match(email_pattern, na=False)]
    df_clients = df_clients.drop_duplicates(subset=['client_id'])
    
    print(f"✓ Clients nettoyés: {len(df_clients)} lignes")
    
    # Nettoyage purchases
    df_purchases['date_purchase'] = pd.to_datetime(df_purchases['date_purchase'], errors='coerce')
    df_purchases = df_purchases.dropna(subset=['purchase_id', 'client_id', 'amount', 'date_purchase'])
    
    # Outliers (IQR method)
    Q1 = df_purchases['amount'].quantile(0.25)
    Q3 = df_purchases['amount'].quantile(0.75)
    IQR = Q3 - Q1
    upper_bound = Q3 + 3 * IQR
    df_purchases = df_purchases[df_purchases['amount'] <= upper_bound]
    
    df_purchases = df_purchases.drop_duplicates(subset=['purchase_id'])
    
    print(f"✓ Purchases nettoyés: {len(df_purchases)} lignes")
    
    elapsed = time.time() - start_time
    return elapsed, len(df_clients), len(df_purchases)


def benchmark_spark():
    """Transformation PySpark"""
    print("\n" + "="*60)
    print("SPARK TRANSFORMATION")
    print("="*60)
    
    start_time = time.time()
    
    # Créer session Spark
    spark = SparkSession.builder \
        .appName("Benchmark Spark") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Charger les données
        df_clients = spark.read.csv(CLIENTS_FILE, header=True, inferSchema=True)
        df_purchases = spark.read.csv(PURCHASES_FILE, header=True, inferSchema=True)
        
        clients_count = df_clients.count()
        purchases_count = df_purchases.count()
        
        print(f"✓ Clients chargés: {clients_count} lignes")
        print(f"✓ Purchases chargés: {purchases_count} lignes")
        
        # Nettoyage clients
        df_clients = df_clients.withColumn(
            'date_inscription',
            F.to_date(F.col('date_inscription'))
        )
        
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        df_clients = df_clients.filter(F.col('email').rlike(email_regex))
        df_clients = df_clients.dropDuplicates(['client_id'])
        
        clients_clean = df_clients.count()
        print(f"✓ Clients nettoyés: {clients_clean} lignes")
        
        # Nettoyage purchases
        df_purchases = df_purchases.withColumn(
            'date_purchase',
            F.to_date(F.col('date_purchase'))
        )
        
        df_purchases = df_purchases.dropna(
            subset=['purchase_id', 'client_id', 'amount', 'date_purchase']
        )
        
        # Outliers (approxQuantile)
        quantiles = df_purchases.approxQuantile('amount', [0.25, 0.75], 0.01)
        Q1, Q3 = quantiles[0], quantiles[1]
        IQR = Q3 - Q1
        upper_bound = Q3 + 3 * IQR
        
        df_purchases = df_purchases.filter(F.col('amount') <= upper_bound)
        df_purchases = df_purchases.dropDuplicates(['purchase_id'])
        
        purchases_clean = df_purchases.count()
        print(f"✓ Purchases nettoyés: {purchases_clean} lignes")
        
        elapsed = time.time() - start_time
        
        return elapsed, clients_clean, purchases_clean
        
    finally:
        spark.stop()


def main():
    print("\n" + "="*80)
    print("                    BENCHMARK: PANDAS vs PYSPARK")
    print("="*80)
    
    # Vérifier que les fichiers existent
    if not Path(CLIENTS_FILE).exists() or not Path(PURCHASES_FILE).exists():
        print("❌ Fichiers de données manquants!")
        print("   Exécutez d'abord: python script/generate_data.py --clients 500 --purchases 7")
        return
    
    # Benchmark Pandas
    try:
        pandas_time, pandas_clients, pandas_purchases = benchmark_pandas()
        pandas_total = pandas_clients + pandas_purchases
        pandas_throughput = pandas_total / pandas_time if pandas_time > 0 else 0
    except Exception as e:
        print(f"❌ Erreur Pandas: {e}")
        return
    
    # Benchmark Spark
    try:
        spark_time, spark_clients, spark_purchases = benchmark_spark()
        spark_total = spark_clients + spark_purchases
        spark_throughput = spark_total / spark_time if spark_time > 0 else 0
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        return
    
    # Résultats
    print("\n" + "="*80)
    print("                         RÉSULTATS DU BENCHMARK")
    print("="*80)
    
    print(f"\n{'Engine':<15} {'Temps (s)':<15} {'Throughput':<20} {'Records':<15}")
    print("-" * 80)
    print(f"{'Pandas':<15} {pandas_time:<15.2f} {pandas_throughput:<20.0f} records/sec   {pandas_total:<15}")
    print(f"{'Spark':<15} {spark_time:<15.2f} {spark_throughput:<20.0f} records/sec   {spark_total:<15}")
    print("-" * 80)
    
    # Comparaison
    if spark_time < pandas_time:
        speedup = pandas_time / spark_time
        winner = "Spark"
        print(f"\n🏆 {winner} est {speedup:.2f}x plus rapide!")
    else:
        speedup = spark_time / pandas_time
        winner = "Pandas"
        print(f"\n🏆 {winner} est {speedup:.2f}x plus rapide!")
    
    print(f"⏱️  Différence: {abs(spark_time - pandas_time):.2f} secondes")
    
    # Sauvegarder les résultats
    results = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'pandas_time': pandas_time,
        'spark_time': spark_time,
        'pandas_throughput': pandas_throughput,
        'spark_throughput': spark_throughput,
        'winner': winner,
        'speedup': speedup
    }
    
    results_df = pd.DataFrame([results])
    results_file = "benchmark_results.csv"
    
    if Path(results_file).exists():
        results_df.to_csv(results_file, mode='a', header=False, index=False)
    else:
        results_df.to_csv(results_file, index=False)
    
    print(f"\n📊 Résultats sauvegardés dans {results_file}")
    print("="*80)
    
    # Lancer le dashboard Streamlit
    print("\n🚀 Lancement du dashboard benchmark...")
    try:
        streamlit_cmd = [sys.executable, "-m", "streamlit", "run", "dashboard_benchmark.py"]
        subprocess.Popen(streamlit_cmd)
        print("✓ Dashboard lancé sur http://localhost:8501")
    except Exception as e:
        print(f"❌ Erreur lors du lancement du dashboard: {e}")
        print("   Lancez manuellement: streamlit run dashboard_benchmark.py")


if __name__ == "__main__":
    main()
