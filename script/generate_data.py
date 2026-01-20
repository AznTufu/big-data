import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_clients(n_clients: int, output_path: str) -> list[int]:
    """
    Generate fake client data
    
    Args:
        n_clients (int): Number of clients to generate
        output_path (str): Path to save the generated CSV file
    
    Returns:
        list[int]: List of generated client IDs
    """
    
    countries = ['France', 'Germany', 'Spain', 'Italy', 'Belgium', 
            'Netherlands', 'Switzerland', 'UK', 'Canada']
            
    clients = []
    client_ids = []
    
    for i in range(1, n_clients + 1):
        date_inscription = fake.date_between(start_date='-3y', end_date='-1m')
        clients.append(
            {
                "client_id": i,
                "name": fake.name(),
                "email": fake.email(),
                "date_inscription": date_inscription.strftime("%Y-%m-%d"),
                "country": random.choice(countries)
            }
        )
        client_ids.append(i)
        
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=clients[0].keys())
        writer.writeheader()
        writer.writerows(clients)
        
    print(f"Generated {n_clients} clients and saved to {output_path}")
    
    return client_ids

def generate_purchases(client_ids: list[int], output_path: str, avg_purchases_per_client: int = 5) -> None:
    """
    Generate fake purchase data
    
    Args:
        client_ids (list[int]): List of client IDs
        average_purchases_per_client (int): Average number of purchases per client
        output_path (str): Path to save the generated CSV file
    """
    
    products = [
        "Laptop", "Phone", "Tablet", "Headphones", "Monitor",
        "Keyboard", "Mouse", "Webcam", "Speaker", "Charger"
    ]
    
    purchases = []
    purchase_id = 1
    
    for client_id in client_ids:
        n_purchases = max(1, int(random.gauss(avg_purchases_per_client, 2)))
        
        for _ in range(n_purchases):
            date_purchase = fake.date_between(start_date='-1y', end_date='today')
            purchases.append(
                {
                    "purchase_id": purchase_id,
                    "client_id": client_id,
                    "product": random.choice(products),
                    "amount": round(random.uniform(20.0, 2000.0), 2),
                    "date_purchase": date_purchase.strftime("%Y-%m-%d")
                }
            )
            purchase_id += 1
            
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=purchases[0].keys())
        writer.writeheader()
        writer.writerows(purchases)
        
    print(f"Generated {len(purchases)} purchases and saved to {output_path}")

if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"
    
    clients_ids = generate_clients(
        n_clients=1500, 
        output_path=output_dir / "clients.csv"
    )
    
    generate_purchases(
        client_ids=clients_ids,
        output_path=output_dir / "purchases.csv",
        avg_purchases_per_client=5
    )

