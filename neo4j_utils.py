import neo4j
import time
import os
from dotenv import load_dotenv
import json

# Load environment variables from the .env file
load_dotenv()


# --- Connection and Admin Functions ---
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
def create_nodes_and_relationships(driver, database_name, product_data, variants_data, orders_data, customers_data,
                                   inventories_data):
    """Ingests data from all JSON files into the Neo4j database,
    creating nodes for all entities and the relationships between them.
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
                   product_id=product["product_id"],
                   sku=product["sku"],
                   name=product["name"],
                   short_description=product["short_description"],
                   description=product["description"],
                   list_price=json.dumps(product["list_price"]),
                   aggregate_stock=json.dumps(product["aggregate_stock"]),
                   physical_attributes=json.dumps(product["physical_attributes"]),
                   status=product["status"],
                   deleted=product["deleted"],
                   created_at=product["created_at"]
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
            # 7. MERGE Brand node and its relationship
            tx.run("""
                MATCH (p:Product {product_id: $product_id})
                MERGE (b:Brand {brand_id: $brand_id})
                ON CREATE SET b.name = $brand_name
                MERGE (p)-[:HAS]->(b)
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

        def ingest_variant_transaction(tx, variant):
            tx.run("""
                MERGE (v:Variant {variant_id: $variant_id})
                ON CREATE SET
                    v.sku = $sku,
                    v.name = $name,
                    v.status = $status,
                    v.deleted = $deleted,
                    v.variation_type = $variation_type,
                    v.created_at = $created_at,
                    v.updated_at = $updated_at,
                    v.list_price = toString($list_price),
                    v.variations = toString($variations),
                    v.physical_attributes = toString($physical_attributes),
                    v.media = toString($media),
                    v.inventory_summary = toString($inventory_summary),
                    v.sales_channels = toString($sales_channels),
                    v.external_identifiers = toString($external_identifiers)
            """,
                   variant_id=variant["variant_id"],
                   sku=variant["sku"],
                   name=variant["name"],
                   status=variant["status"],
                   deleted=variant["deleted"],
                   variation_type=variant["variation_type"],
                   created_at=variant["created_at"],
                   updated_at=variant["updated_at"],
                   list_price=json.dumps(variant.get("list_price", {})),
                   variations=json.dumps(variant.get("variations", {})),
                   physical_attributes=json.dumps(variant.get("physical_attributes", {})),
                   media=json.dumps(variant.get("media", [])),
                   inventory_summary=json.dumps(variant.get("inventory_summary", [])),
                   sales_channels=json.dumps(variant.get("sales_channels", [])),
                   external_identifiers=json.dumps(variant.get("external_identifiers", []))
                   )
            # match the corresponding nodes and MERGE the relationship
            tx.run(""" MATCH (p:Product {product_id: $product_id})
                MATCH (v:Variant {variant_id: $variant_id})
                MERGE (p)-[:HAS]->(v)""",
                   product_id=variant["product_id"],
                   variant_id=variant["variant_id"]
                   )
            print(f"Successfully ingested variant: {variant['name']}")

        def ingest_order_transaction(tx, order):
            """
            Ingests a single order entry from JSON.
            """
            # MERGE the main Order node
            tx.run("""
                MERGE (o:Order {order_id: $order_id})
                ON CREATE SET
                    o.order_number = $order_number,
                    o.status = $status,
                    o.currency = $currency,
                    o.totals = toString($totals),
                    o.payments = toString($payments),
                    o.shipments = toString($shipments),
                    o.applied_promotions = toString($applied_promotions),
                    o.external_references = toString($external_references),
                    o.notes = $notes,
                    o.created_at = $created_at,
                    o.updated_at = $updated_at,
                    o.order_created_date = $order_created_date
            """,
                   order_id=order["order_id"],
                   order_number=order["order_number"],
                   status=order["status"],
                   currency=order["currency"],
                   totals=json.dumps(order.get("totals", {})),
                   payments=json.dumps(order.get("payments", [])),
                   shipments=json.dumps(order.get("shipments", [])),
                   applied_promotions=json.dumps(order.get("applied_promotions", [])),
                   external_references=json.dumps(order.get("external_references", [])),
                   notes=order.get("notes", ""),
                   created_at=order["created_at"],
                   updated_at=order["updated_at"],
                   order_created_date=order["order_created_date"]
                   )

            # Match and MERGE relationships to Customer and BusinessEntity
            tx.run("""
                MATCH (o:Order {order_id: $order_id})
                MERGE (c:Customer {customer_id: $customer_id})
                MERGE (b:BusinessEntity {business_entity_id: $business_entity_id})
                MERGE (c)-[:PLACED]->(o)
                MERGE (o)-[:FOR_BUSINESS_ENTITY]->(b)
            """,
                   order_id=order["order_id"],
                   customer_id=order["customer_id"],
                   business_entity_id=order["business_entity_id"]
                   )

            # MERGE the SalesChannel node and relationship
            sales_channel = order.get("sales_channel", {})
            if sales_channel:
                tx.run("""
                    MATCH (o:Order {order_id: $order_id})
                    MERGE (sc:SalesChannel {channel_id: $channel_id})
                    ON CREATE SET
                        sc.name = $name,
                        sc.type = $type,
                        sc.status = $status
                    MERGE (o)-[:THROUGH_CHANNEL]->(sc)
                """,
                       order_id=order["order_id"],
                       channel_id=sales_channel["channel_id"],
                       name=sales_channel["name"],
                       type=sales_channel["type"],
                       status=sales_channel["status"]
                       )

            # Loop through order_items to create CONTAINS relationships to Variant nodes
            for item in order["order_items"]:
                tx.run("""
                    MATCH (o:Order {order_id: $order_id})
                    MERGE (v:Variant {variant_id: $variant_id})
                    MERGE (o)-[r:CONTAINS]->(v)
                    ON CREATE SET
                        r.quantity = $quantity,
                        r.price_per_unit = $price_per_unit,
                        r.line_item_total = $line_item_total,
                        r.name_at_sale = $name_at_sale,
                        r.sku = $sku,
                        r.variations = toString($variations),
                        r.physical_attributes = toString($physical_attributes)
                """,
                       order_id=order["order_id"],
                       variant_id=item["variant_id"],
                       quantity=item["quantity"],
                       price_per_unit=item["price_per_unit"],
                       line_item_total=item["line_item_total"],
                       name_at_sale=item["name_at_sale"],
                       sku=item["sku"],
                       variations=json.dumps(item["variations"]),
                       physical_attributes=json.dumps(item["physical_attributes"])
                       )
            print(f"Successfully ingested order: {order['order_number']}")

        def ingest_inventory_transaction(tx, inventory):
            """
            Ingests a single inventory entry from JSON.
            """
            tx.run("""
                MERGE (inv:Inventory {inventory_id: $inventory_id})
                ON CREATE SET
                    inv.quantity = toString($quantity),
                    inv.created_at = $created_at,
                    inv.updated_at = $updated_at
            """,
                   inventory_id=inventory["inventory_id"],
                   quantity=json.dumps(inventory["quantity"]),
                   created_at=inventory["created_at"],
                   updated_at=inventory["updated_at"]
                   )
            # Create a relationship to the Variant node
            tx.run("""
                MATCH (v:Variant {variant_id: $variant_id})
                MATCH (inv:Inventory {inventory_id: $inventory_id})
                MERGE (inv)-[:RECORDS_STOCK_FOR]->(v)
            """,
                   variant_id=inventory["variant_id"],
                   inventory_id=inventory["inventory_id"]
                   )
            print(f"Successfully ingested inventory for variant: {inventory['variant_id']}")

        def ingest_customer_transaction(tx, customer):
            """
            Ingests a single customer entry from JSON.
            """
            tx.run("""
                MERGE (c:Customer {customer_id: $customer_id})
                ON CREATE SET
                    c.email = $email,
                    c.first_name = $first_name,
                    c.last_name = $last_name,
                    c.phone = $phone,
                    c.customer_segment = $customer_segment,
                    c.marketing_consent = $marketing_consent,
                    c.personalization_details = toString($personalization_details),
                    c.notes = $notes,
                    c.addresses = toString($addresses),
                    c.payment_methods = toString($payment_methods),
                    c.wishlist = toString($wishlist),
                    c.status = $status,
                    c.deleted = $deleted,
                    c.created_at = $created_at,
                    c.updated_at = $updated_at
            """,
                   customer_id=customer["customer_id"],
                   email=customer["email"],
                   first_name=customer["first_name"],
                   last_name=customer["last_name"],
                   phone=customer["phone"],
                   customer_segment=customer["customer_segment"],
                   marketing_consent=customer["marketing_consent"],
                   personalization_details=json.dumps(customer.get("personalization_details", {})),
                   notes=customer["notes"],
                   addresses=json.dumps(customer.get("addresses", [])),
                   payment_methods=json.dumps(customer.get("payment_methods", [])),
                   wishlist=json.dumps(customer.get("wishlist", [])),
                   status=customer["status"],
                   deleted=customer["deleted"],
                   created_at=customer["created_at"],
                   updated_at=customer["updated_at"]
                   )
            print(f"Successfully ingested customer: {customer['email']}")

        # Loop through each data type and run the corresponding transaction
        for product in product_data:
            session.execute_write(ingest_product_transaction, product)

        for variant in variants_data:
            session.execute_write(ingest_variant_transaction, variant)

        for order in orders_data:
            session.execute_write(ingest_order_transaction, order)

        for inventory in inventories_data:
            session.execute_write(ingest_inventory_transaction, inventory)

        for customer in customers_data:
            session.execute_write(ingest_customer_transaction, customer)


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
    # create db if it does not exist
    system_driver = connect_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, database_name=system_db_name)
    if system_driver:
        create_database(system_driver, db_name)
        system_driver.close()

    print("\n--- STEP 2: Connecting to the newly created 'knowledge-graph' database to run queries ---")
    data_driver = None
    max_retries = 5
    for attempt in range(max_retries):
        print(f"Attempting to connect to '{db_name}' database... (Attempt {attempt + 1}/{max_retries})")
        data_driver = connect_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, database_name=db_name)
        if data_driver:
            # Initialize data variables outside the try...except block
            products_data = []
            variants_data = []
            orders_data = []
            inventories_data = []
            customers_data = []
            try:
                with open('ekyam_chat_v3.products.json', 'r') as f:
                    products_data = json.load(f)
                with open('ekyam_chat_v3.variants.json', 'r') as f:
                    variants_data = json.load(f)
                with open('ekyam_chat_v3.orders.json', 'r') as f:
                    orders_data = json.load(f)
                with open('ekyam_chat_v3.inventories.json', 'r') as f:
                    inventories_data = json.load(f)
                with open('ekyam_chat_v3.customers.json', 'r') as f:
                    customers_data = json.load(f)
            except FileNotFoundError as e:
                print(f"Error: {e}. Please ensure all JSON files exist.")


            # Call the ingestion function once with all the data
            if products_data or variants_data or orders_data or inventories_data or customers_data:
                print("\n--- Starting data ingestion from JSON files ---")
                create_nodes_and_relationships(data_driver, db_name, products_data, variants_data, orders_data,
                                               customers_data, inventories_data)
                print("--- Data ingestion complete ---")
            else:
                print("No data found in any JSON files. Ingestion skipped.")

            data_driver.close()
            break  # Break the retry loop on successful connection and ingestion
        else:
            print("Connection failed. The database may not be ready yet. Retrying in 5 seconds...")
            time.sleep(5)

print("\nAll driver connections closed.")
