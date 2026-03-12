import psycopg2

# CHANGE THIS to your PostgreSQL password
POSTGRES_PASSWORD = "Wertypop87"

try:
    # Connect to default postgres database
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Create database
    cursor.execute("SELECT 1 FROM pg_database WHERE datname='riot_stats'")
    if not cursor.fetchone():
        cursor.execute('CREATE DATABASE riot_stats')
        print("✅ Database 'riot_stats' created successfully!")
    else:
        print("✅ Database 'riot_stats' already exists")
    
    cursor.close()
    conn.close()
    
    print("\n🎉 PostgreSQL is set up and ready!")
    print("Next step: Run riot_to_db_postgres.py")
    
except psycopg2.OperationalError as e:
    print("❌ Connection failed. Check:")
    print("   1. PostgreSQL is running")
    print("   2. Password is correct")
    print(f"   Error: {e}")
except Exception as e:
    print(f"❌ Error: {e}")