from pyssandra import Pyssandra


def sync_table_safely(db: Pyssandra, table_class):
    try:
        db[table_class].sync()
        print(f"{table_class.__name__} table created successfully!")
    
    except Exception as e:
        print(f"Failed to create {table_class.__name__} table: {e}")
