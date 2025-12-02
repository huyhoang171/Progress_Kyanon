import sqlite3
import pandas as pd

DB_FILE = 'places_data.db'
conn = sqlite3.connect(DB_FILE)
table_name = 'places'

sql_query = f"""
            SELECT 
                name, 
                rating, 
                user_ratings_total,
                types,
                DENSE_RANK() OVER (
                    PARTITION BY types 
                    ORDER BY rating DESC, user_ratings_total DESC
                ) AS rank_in_type
            FROM 
                {table_name}
            WHERE rating IS NOT NULL
            ORDER BY 
                types, 
                rank_in_type
        """
        
result_df = pd.read_sql_query(sql_query, conn)
        
print("\n=== KẾT QUẢ XẾP HẠNG (Top 5 ví dụ) ===")
print(result_df.to_string(index=False))
        
conn.close()
