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
class Neo4jDataIngestor:
    def __init__(self, driver, database_name):
        self.driver = driver
        self.database_name = database_name

    def ingest_data(self, product_data, variants_data, orders_data, customers_data, inventories_data):
        """Main method to ingest all data"""
        with self.driver.session(database=self.database_name) as session:
            for product in product_data:
                session.execute_write(self._ingest_product, product)

            for variant in variants_data:
                session.execute_write(self._ingest_variant, variant)

            for order in orders_data:
                session.execute_write(self._ingest_order, order)

            for inventory in inventories_data:
                session.execute_write(self._ingest_inventory, inventory)

            for customer in customers_data:
                session.execute_write(self._ingest_customer, customer)

    @staticmethod
    def _ingest_product(tx, product):
        """
        Ingests a single product, creating the product node and all its
        associated relationships in a single, optimized Cypher query.
        """
        tx.run("""
            // MERGE the Product node first, as it is the central point
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
                p.created_at = $created_at,
                p.marketing = toString($marketing),
                p.tags = $tags,
                p.media = toString($media),
                p.compliances = toString($compliances),
                p.handling_instructions = toString($handling_instructions),
                p.external_identifiers = toString($external_identifiers)

            // Handle Categories: MERGE a Category node and link it to the Product
            WITH p, $categories AS categories
            UNWIND categories AS category
            MERGE (c:Category {category_id: category.category_id})
            ON CREATE SET c.name = category.name, c.slug = category.slug
            MERGE (p)-[:BELONGS_TO]->(c)

            // Handle Collections: MERGE a Collection node and link it
            WITH p, $collections AS collections
            UNWIND collections AS collection
            MERGE (coll:Collection {collection_id: collection.collection_id})
            ON CREATE SET coll.name = collection.name, coll.slug = collection.slug
            MERGE (p)-[:PART_OF]->(coll)

            // Handle Partners: MERGE a Partner node and link it
            WITH p, $partners AS partners
            UNWIND partners AS partner
            MERGE (pa:Partner {partner_id: partner.partner_id})
            ON CREATE SET pa.name = partner.name, pa.type = partner.type
            MERGE (p)-[:SUPPLIED_BY]->(pa)

            // Handle Brand: MERGE a Brand node and link it
            WITH p, $brand AS brand
            MERGE (b:Brand {brand_id: brand.id})
            ON CREATE SET b.name = brand.name
            MERGE (p)-[:BELONGS_TO]->(b)
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
               created_at=product["created_at"],
               marketing=json.dumps(product["marketing"]),
               tags=product["tags"],
               media=json.dumps(product["media"]),
               compliances=json.dumps(product["compliances"]),
               handling_instructions=json.dumps(product["handling_instructions"]),
               external_identifiers=json.dumps(product["external_identifiers"]),
               categories=product["categories"],
               collections=product["collections"],
               partners=product["partners"],
               brand=product["brand"]
               )
        print(f"Successfully ingested product: {product['name']}")

    @staticmethod
    def _create_variant_node(tx, variant):
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

    @staticmethod
    def _create_product_variant_relationship(tx, variant):
        tx.run(""" 
            MATCH (p:Product {product_id: $product_id})
            MATCH (v:Variant {variant_id: $variant_id})
            MERGE (p)-[:HAS]->(v)""",
               product_id=variant["product_id"],
               variant_id=variant["variant_id"]
               )

    def _ingest_variant(self, tx, variant):
        self._create_variant_node(tx, variant)
        self._create_product_variant_relationship(tx, variant)
        print(f"Successfully ingested variant: {variant['name']}")

    @staticmethod
    def _create_order_node(tx, order):
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

    @staticmethod
    def _create_order_customer_relationships(tx, order):
        tx.run("""
            MATCH (o:Order {order_id: $order_id})
            MERGE (c:Customer {customer_id: $customer_id})
            MERGE (b:BusinessEntity {business_entity_id: $business_entity_id})
            MERGE (c)-[:PLACED]->(o)
            MERGE (o)-[:PLACED_ON_THIS]->(b)
        """,
               order_id=order["order_id"],
               customer_id=order["customer_id"],
               business_entity_id=order["business_entity_id"]
               )

    @staticmethod
    def _create_order_sales_channel_relationship(tx, order):
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

    @staticmethod
    def _create_order_variant_relationships(tx, order):
        for item in order["order_items"]:
            tx.run("""
                MATCH (o:Order {order_id: $order_id})
                MERGE (v:Variant {variant_id: $variant_id})
                MERGE (o)-[r:CONTAINS]->(v)
                ON CREATE SET
                    r.quantity = $quantity,
                    r.line_item_total = $line_item_total
            """,
                   order_id=order["order_id"],
                   variant_id=item["variant_id"],
                   quantity=item["quantity"],
                   line_item_total=item["line_item_total"]
                   )

    def _ingest_order(self, tx, order):
        self._create_order_node(tx, order)
        self._create_order_customer_relationships(tx, order)
        self._create_order_sales_channel_relationship(tx, order)
        self._create_order_variant_relationships(tx, order)
        print(f"Successfully ingested order: {order['order_number']}")

    @staticmethod
    def _create_inventory_node(tx, inventory):
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

    @staticmethod
    def _create_inventory_variant_relationship(tx, inventory):
        tx.run("""
            MATCH (v:Variant {variant_id: $variant_id})
            MATCH (inv:Inventory {inventory_id: $inventory_id})
            MERGE (inv)-[:RECORDS_STOCK_FOR]->(v)
        """,
               variant_id=inventory["variant_id"],
               inventory_id=inventory["inventory_id"]
               )

    def _ingest_inventory(self, tx, inventory):
        self._create_inventory_node(tx, inventory)
        self._create_inventory_variant_relationship(tx, inventory)
        print(f"Successfully ingested inventory for variant: {inventory['variant_id']}")

    @staticmethod
    def _create_customer_node(tx, customer):
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

    def _ingest_customer(self, tx, customer):
        self._create_customer_node(tx, customer)
        print(f"Successfully ingested customer: {customer['email']}")


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
                with open('products.json', 'r') as f:
                    products_data = json.load(f)
                with open('variants.json', 'r') as f:
                    variants_data = json.load(f)
                with open('orders.json', 'r') as f:
                    orders_data = json.load(f)
                with open('Inventories.json', 'r') as f:
                    inventories_data = json.load(f)
                with open('customers.json', 'r') as f:
                    customers_data = json.load(f)
            except FileNotFoundError as e:
                print(f"Error: {e}. Please ensure all JSON files exist.")


            # Call the ingestion function once with all the data
            if products_data or variants_data or orders_data or inventories_data or customers_data:
                print("\n--- Starting data ingestion from JSON files ---")
                ingestor = Neo4jDataIngestor(data_driver, db_name)
                ingestor.ingest_data(
                    products_data,
                    variants_data,
                    orders_data,
                    customers_data,
                    inventories_data
                )
                print("--- Data ingestion complete ---")
            else:
                print("No data found in any JSON files. Ingestion skipped.")

            data_driver.close()
            break  # Break the retry loop on successful connection and ingestion
        else:
            print("Connection failed. The database may not be ready yet. Retrying in 5 seconds...")
            time.sleep(5)

    print("\nAzll driver connections closed.")