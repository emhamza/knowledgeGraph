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
    def _ingest_variant(tx, variant):
        """
        Ingests a single variant, creating the variant node and its
        relationship to the Product node in a single, optimized Cypher query.
        """
        tx.run(""" 
               // MERGE the Variant node
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

               // MATCH the existing Product node
               WITH v, $product_id AS product_id
               MATCH (p:Product {product_id: product_id})

               // MERGE the relationship
               MERGE (p)-[:HAS]->(v)
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
               external_identifiers=json.dumps(variant.get("external_identifiers", [])),
               product_id=variant["product_id"]  # Pass product_id for relationship
               )
        print(f"Successfully ingested variant: {variant['name']}")

    @staticmethod
    def _ingest_order(tx, order):
        """
        Ingests a single order, creating the Order node and all its
        associated relationships, including dedicated Shipment nodes.
        """
        sales_channel = order.get("sales_channel", {})

        # Prepare the shipments data by converting the shipping_address map to a JSON string
        prepared_shipments = []
        for shipment in order.get("shipments", []):
            shipment_copy = shipment.copy()
            if "shipping_address" in shipment_copy:
                shipment_copy["shipping_address"] = json.dumps(shipment_copy["shipping_address"])
            prepared_shipments.append(shipment_copy)

        tx.run("""
                // MERGE the Order node first, as it is the central point
                MERGE (o:Order {order_id: $order_id})
                ON CREATE SET
                    o.order_number = $order_number,
                    o.status = $status,
                    o.currency = $currency,
                    o.notes = $notes,
                    o.created_at = $created_at,
                    o.updated_at = $updated_at,
                    o.order_created_date = $order_created_date,
                    o.totals = toString($totals),
                    o.payments = toString($payments),
                    o.applied_promotions = toString($applied_promotions),
                    o.external_references = toString($external_references)

                // MERGE the Customer and its relationship to the Order
                WITH o, $customer_id AS customer_id
                MERGE (c:Customer {customer_id: customer_id})
                MERGE (c)-[:PLACED]->(o)

                // Handle Sales Channel (now also representing the BusinessEntity)
                WITH o, $sales_channel AS sales_channel
                FOREACH (sc IN CASE WHEN sales_channel.channel_id IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (salesChannel:SalesChannel {channel_id: sales_channel.channel_id})
                    ON CREATE SET
                        salesChannel.name = sales_channel.name,
                        salesChannel.type = sales_channel.type,
                        salesChannel.status = sales_channel.status
                    MERGE (o)-[:PLACED_ON_CHANNEL]->(salesChannel)
                )

                // Handle Shipments and their items
                WITH o, $shipments AS shipments
                UNWIND shipments AS shipment
                MERGE (s:Shipment {shipment_id: shipment.shipment_id})
                ON CREATE SET
                    s.status = shipment.status,
                    s.carrier = shipment.carrier,
                    s.tracking_number = shipment.tracking_number,
                    s.shipped_date = shipment.shipped_date,
                    s.estimated_delivery_date = shipment.estimated_delivery_date,
                    s.shipping_address = shipment.shipping_address
                MERGE (o)-[:HAS_SHIPMENT]->(s)

                // Link Shipment items to Variants
                WITH o, s, shipment.items AS items
                UNWIND items AS item
                MERGE (v:Variant {variant_id: item.variant_id})
                MERGE (s)-[r:CONTAINS]->(v)
                ON CREATE SET
                    r.quantity = item.quantity
                // The item_name is removed from the relationship.

                // Handle Order Items (Variants) - this is for all items on the order, not just shipments
                WITH o, $order_items AS order_items
                UNWIND order_items AS item
                MERGE (v_order:Variant {variant_id: item.variant_id})
                MERGE (o)-[ro:CONTAINS]->(v_order)
                ON CREATE SET
                    ro.quantity = item.quantity,
                    ro.line_item_total = item.line_item_total
                ON MATCH SET
                    ro.quantity = item.quantity,
                    ro.line_item_total = item.line_item_total
            """,
               order_id=order["order_id"],
               order_number=order["order_number"],
               status=order["status"],
               currency=order["currency"],
               notes=order.get("notes", ""),
               created_at=order["created_at"],
               updated_at=order["updated_at"],
               order_created_date=order["order_created_date"],
               totals=json.dumps(order.get("totals", {})),
               payments=json.dumps(order.get("payments", [])),
               applied_promotions=json.dumps(order.get("applied_promotions", [])),
               external_references=json.dumps(order.get("external_references", [])),
               customer_id=order["customer_id"],
               sales_channel=sales_channel,
               order_items=order["order_items"],
               shipments=prepared_shipments
               )
        print(f"Successfully ingested order: {order['order_number']}")

    @staticmethod
    def _ingest_inventory(tx, inventory):
        """
        Ingests a single inventory record, creating the Inventory node and its
        relationship to the Variant node in a single, optimized Cypher query.
        """
        tx.run("""
                // MATCH the Variant node that this inventory record is for
                MATCH (v:Variant {variant_id: $variant_id})

                // MERGE the Inventory node, creating it if it doesn't exist
                MERGE (inv:Inventory {inventory_id: $inventory_id})
                ON CREATE SET
                    inv.created_at = $created_at,
                    inv.updated_at = $updated_at

                // MERGE the relationship and add properties to it
                MERGE (inv)-[r:RECORDS_STOCK_FOR]->(v)
                ON CREATE SET 
                    r.total = $total, 
                    r.sellable = $sellable, 
                    r.reserved = $reserved
                ON MATCH SET
                    r.total = $total, 
                    r.sellable = $sellable, 
                    r.reserved = $reserved
                """,
               inventory_id=inventory["inventory_id"],
               variant_id=inventory["variant_id"],
               created_at=inventory["created_at"],
               updated_at=inventory["updated_at"],
               total=inventory["quantity"]["total"],
               sellable=inventory["quantity"]["sellable"],
               reserved=inventory["quantity"]["reserved"]
               )
        print(f"Successfully ingested inventory for variant: {inventory['variant_id']}")

    @staticmethod
    def _ingest_customer(tx, customer):
        """
        Ingests a single customer, creating the Customer node and
        all its associated nodes and relationships in one query.
        """
        # Preprocess wishlist to convert 'price_at_add' from a map to a JSON string
        prepared_wishlist = []
        for item in customer.get("wishlist", []):
            item_copy = item.copy()
            if "price_at_add" in item_copy and isinstance(item_copy["price_at_add"], dict):
                item_copy["price_at_add"] = json.dumps(item_copy["price_at_add"])
            prepared_wishlist.append(item_copy)

        # The rest of the ingestion logic remains the same
        tx.run("""
                // MERGE the Customer node first
                MERGE (c:Customer {customer_id: $customer_id})
                ON CREATE SET
                    c.email = $email,
                    c.first_name = $first_name,
                    c.last_name = $last_name,
                    c.phone = $phone,
                    c.customer_segment = $customer_segment,
                    c.marketing_consent = $marketing_consent,
                    c.notes = $notes,
                    c.status = $status,
                    c.deleted = $deleted,
                    c.created_at = $created_at,
                    c.updated_at = $updated_at,
                    c.personalization_details = toString($personalization_details)

                // Handle Addresses
                WITH c, $addresses AS addresses
                UNWIND addresses AS address
                MERGE (a:Address {address_id: address.address_id})
                ON CREATE SET
                    a.label = address.label,
                    a.receiver_name = address.receiver_name,
                    a.receiver_phone = address.receiver_phone,
                    a.street = address.street,
                    a.city = address.city,
                    a.state = a.state,
                    a.zip_code = address.zip_code,
                    a.country = address.country
                MERGE (c)-[rel:HAS_ADDRESS]->(a)
                ON CREATE SET
                    rel.is_default = address.is_default

                // Handle Payment Methods
                WITH c, $payment_methods AS payment_methods
                UNWIND payment_methods AS method
                MERGE (p:PaymentMethod {payment_method_id: method.payment_method_id})
                ON CREATE SET
                    p.type = method.type,
                    p.gateway_token = method.gateway_token,
                    p.card_last_four = method.card_last_four,
                    p.card_brand = method.card_brand,
                    p.card_expiry_month = method.card_expiry_month,
                    p.card_expiry_year = method.card_expiry_year
                MERGE (c)-[rel:HAS_PAYMENT_METHOD]->(p)
                ON CREATE SET
                    rel.is_default = method.is_default

                // Handle Wishlist Items
                WITH c, $wishlist AS wishlist
                UNWIND wishlist AS item
                MERGE (v:Variant {variant_id: item.variant_id})
                MERGE (c)-[w:WISHES_FOR]->(v)
                ON CREATE SET
                    w.added_at = item.added_at,
                    w.price_at_add = item.price_at_add // This is now a JSON string
                """,
               customer_id=customer["customer_id"],
               email=customer["email"],
               first_name=customer["first_name"],
               last_name=customer["last_name"],
               phone=customer["phone"],
               customer_segment=customer["customer_segment"],
               marketing_consent=customer["marketing_consent"],
               notes=customer["notes"],
               status=customer["status"],
               deleted=customer["deleted"],
               created_at=customer["created_at"],
               updated_at=customer["updated_at"],
               personalization_details=json.dumps(customer.get("personalization_details", {})),
               addresses=customer.get("addresses", []),
               payment_methods=customer.get("payment_methods", []),
               wishlist=prepared_wishlist  # Pass the prepared list
               )
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