import neo4j
import time
import os
from dotenv import load_dotenv
import json
# Load environment variables from the .env file
load_dotenv()

# It's a good practice to handle potential connection errors
def connect_to_neo4j(uri, user, password, database_name="system"):
    """
    Connects to a Neo4j database. By default, it connects to the "system" database,
    which is required for administrative tasks like creating new databases.
    """
    try:
        # Create a driver instance, specifying the target database
        driver = neo4j.GraphDatabase.driver(uri, auth=(user, password), database=database_name)
        # Verify the connection by checking if the driver is alive
        driver.verify_connectivity()
        print(f"Connection to Neo4j at {uri} on database '{database_name}' successful!")
        return driver
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def create_database(driver, new_db_name):
    """
    Creates a new database using an administrative session on the system database.
    """
    with driver.session() as session:
        try:
            # Use backticks to safely handle database names with special characters like dashes
            session.run(f"CREATE DATABASE `{new_db_name}` IF NOT EXISTS")
            print(f"Successfully created database '{new_db_name}'.")
        except Exception as e:
            print(f"An error occurred during database creation: {e}")


# --- Data Ingestion Logic ---
def create_nodes_and_relationships(driver, database_name, product_data ):
    """Ingest a single product entry from JSON into the Neo4j database.
    It creates nodes for the product, its attributes and relationships
    """

    with driver.session(database=database_name) as session:
        # use transaction for efficient data loading
        def ingest_product_transaction(tx, product):
            # 1. MERGE the main Product node
            tx.run("""
                MERGE (p:Product {product_id: $product_id})
                ON CREATE SET
                    p.sku = $sku,
                    p.name = $name,
                    p.short_description = $short_description,
                    p.description = $description,
                    p.list_price = toString($list_price),
                    p.aggregate_stock = toString($aggregate_stock),
                    p.physical_attributes = toString($physical_attributes),
                    p.status = $status,
                    p.deleted = $deleted,
                    p.created_at = $created_at
            """,
            product_id = product["product_id"],
            sku = product["sku"],
            name = product["name"],
            short_description = product["short_description"],
            description = product["description"],
            list_price = json.dumps(product["list_price"]),
            aggregate_stock = json.dumps(product["aggregate_stock"]),
            physical_attributes = json.dumps(product["physical_attributes"]),
            status = product["status"],
            deleted = product["deleted"],
            created_at = product["created_at"]
        )

            # Use UNWIND to iterate through a list in a single cypher statement
            # 2. MERGE categories and their relationships
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                UNWIND $categories AS category
                MERGE (c:Category {category_id: category.category_id})
                ON CREATE SET c.name = category.name, c.slug = category.slug
                MERGE (p)-[:BELONGS_TO]->(c)
                """,
                product_id=product["product_id"],
                categories=product["categories"]
            )

            # 3. MERGE Collections and their relationships
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                UNWIND $collections AS collection
                MERGE (c:Collection {collection_id: collection.collection_id})
                ON CREATE SET c.name = collection.name, c.slug = collection.slug
                MERGE (p)-[:PART_OF]->(c)
                """,
                product_id=product["product_id"],
                collections=product["collections"]
            )

            # 4. MERGE Partners and their relationships
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                UNWIND $partners AS partner
                MERGE (pa:Partner {partner_id: partner.partner_id})
                ON CREATE SET pa.name = partner.name, pa.type = partner.type
                MERGE (p)-[:SUPPLIED_BY]->(pa)
                """,
                product_id=product["product_id"],
                partners=product["partners"]
            )

            # 5. MERGE Variants and their relationships
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                UNWIND $variants AS variant
                MERGE (v:Variant {variant_id: variant.variant_id})
                ON CREATE SET
                    v.name = variant.name,
                    v.sku = variant.sku,
                    v.color = variant.variations.color,
                    v.material = variant.variations.material
                MERGE (p)-[:HAS_VARIANT]->(v)
                """,
                product_id=product["product_id"],
                variants=product["variants"]
            )
            # 6. MERGE Media nodes and their relationships
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                UNWIND $media AS media
                MERGE (m:Media {url: media.url})
                ON CREATE SET
                    m.type = media.type,
                    m.alt_text = media.alt_text,
                    m.is_primary = media.is_primary
                MERGE (p)-[:HAS_MEDIA]->(m)
                """,
                product_id=product["product_id"],
                media=product["media"]
            )

            # 7. MERGE Brand node and its relationship
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                MERGE (b:Brand {brand_id: $brand_id})
                ON CREATE SET b.name = $brand_name
                MERGE (p)-[:HAS_BRAND]->(b)
                """,
                product_id=product["product_id"],
                brand_id=product["brand"]["id"],
                brand_name=product["brand"]["name"]
            )

            # 8. Set Marketing properties on Product node.
            # Note: since marketing is a nested dictionary, it's better to store it as string for simplicity
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                SET p.marketing = toString($marketing)
                """,
                product_id=product["product_id"],
                marketing=json.dumps(product["marketing"])
            )

            # 9. Set Tags properties on Product node
            # The JSON array of tags is converted into a list of strings and stored directly on the node.
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                SET p.tags = $tags
                """,
                product_id=product["product_id"],
                tags=product["tags"]
            )

            print(f"Successfully ingested product: {product['name']}")

        #loop through each product and run the transaction
        for product in product_data:
            session.execute_write(ingest_product_transaction, product)




if __name__ == "__main__":
    # Corrected database name
    db_name = "knowledge-graph"

    # Use "system" for administrative tasks
    system_db_name = "system"

    # Replace with your actual Neo4j URI, user, and password
    neo4j_uri = "neo4j://127.0.0.1:7687"
    neo4j_user = "neo4j"
    # Load the password from the environment variable
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    if not neo4j_password:
        print("Error: NEO4J_PASSWORD not found in .env file. Please check your setup.")
        exit()

    print("--- STEP 1: Connecting to the 'system' database to create the new database ---")
    #create db if it does not exist
    system_driver = connect_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, database_name=system_db_name)
    if system_driver:
        create_database(system_driver, db_name)
        system_driver.close()

    print("\n--- STEP 2: Connecting to the newly created 'knowledge-graph' database to run queries ---")
    # Now that the database exists, we can connect to it directly, but we'll add a retry loop
    data_driver = None
    max_retries = 5
    for attempt in range(max_retries):
        print(f"Attempting to connect to '{db_name}' database... (Attempt {attempt + 1}/{max_retries})")
        data_driver = connect_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, database_name=db_name)
        if data_driver:
            # Load the JSON data from the file
            try:
                with open('ekyam_chat_v3.products.json', 'r') as f:
                    products_data = json.load(f)
            except FileNotFoundError:
                print("Error: 'ekyam_chat_v3.products.json' file not found. Please create it and add your data.")
                products_data = []  # Prevents crashing if file is missing
        if products_data:
            print("\n--- Starting data ingestion from JSON file ---")
            create_nodes_and_relationships(data_driver, db_name, products_data)
            print("--- Data ingestion complete ---")
        else:
            print("No data found in 'ekyam_chat_v3.products.json'. Ingestion skipped.")
            print("Connection failed. The database may not be ready yet. Retrying in 5 seconds...")
            time.sleep(5)
        data_driver.close()

print("\nAll driver connections closed.")