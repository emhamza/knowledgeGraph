import neo4j
import time
import os
from dotenv import load_dotenv
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


def run_data_operations(driver, database_name):
    """
    Runs example queries on the specified database.
    """
    with driver.session(database=database_name) as session:
        try:
            # Clear any existing test nodes to start fresh
            session.run("MATCH (n:TestNode) DETACH DELETE n")

            # Example query to show it's working
            session.run("MERGE (n:TestNode {name: 'Hello-Node'})")
            result = session.run("MATCH (n:TestNode) RETURN n.name as name")
            for record in result:
                print(f"Query successful: Found a node with name '{record['name']}'")
        except Exception as e:
            print(f"An error occurred during data operation: {e}")


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

    print("--- STEP 1: Connecting to the 'system' database to create the new database ---")
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
            print("Successfully connected after database creation.")
            break
        else:
            print("Connection failed. The database may not be ready yet. Retrying in 5 seconds...")
            time.sleep(5)

    if data_driver:
        run_data_operations(data_driver, db_name)
        data_driver.close()
    else:
        print(
            f"Failed to connect to the '{db_name}' database after {max_retries} attempts. Please check your Neo4j instance.")

    print("\nAll driver connections closed.")