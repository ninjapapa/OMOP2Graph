import psycopg

# Connect to PostgreSQL database using graph_user
try:
    with psycopg.connect(
        host="localhost",
        dbname="graphdb",  # Replace with your database name
        user="graph_user",
        password="asdf1234"
    ) as conn:
        print("Connected to the database successfully.")

        # Create a cursor object
        with conn.cursor() as cursor:

            # Load the AGE extension and set up search path
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            print("Apache AGE loaded and search path set.")

            # List all node types
            print("\nListing all types of nodes:")
            cursor.execute("""
                SELECT DISTINCT name 
                FROM ag_catalog.ag_label 
                WHERE kind = 'v';  -- 'v' for vertex (node) types
            """)
            nodes = cursor.fetchall()
            for node in nodes:
                print(f"Node Type: {node[0]}")

            # List all relationship (edge) types
            print("\nListing all types of relationships:")
            cursor.execute("""
                SELECT DISTINCT name 
                FROM ag_catalog.ag_label 
                WHERE kind = 'e';  -- 'e' for edge (relationship) types
            """)
            relationships = cursor.fetchall()
            for relationship in relationships:
                print(f"Relationship Type: {relationship[0]}")

except Exception as e:
    print("Error:", e)
