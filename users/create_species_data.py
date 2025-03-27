import asyncio
import pandas as pd
import asyncpg

# Load species data file
species_df = pd.read_csv(
    "../users/cartig/Atlas case study example/input/spec.dat")

# Define async function to insert species data for a specific project


async def insert_species_data(project_id):
    conn = await asyncpg.connect(
        user='postgres', password='postgres',
        database='bioprotect', host='localhost'
    )

    # Prepare the data for batch insertion
    values = [
        (project_id, row['id'], row['prop'], row['spf'])
        for _, row in species_df.iterrows()
    ]
    # Insert into species_data table
    query = """
        INSERT INTO species_data (project_id, feature_unique_id, prop, spf)
        VALUES ($1, $2, $3, $4)
    """
    await conn.executemany(query, values)
    await conn.close()

# Run the insertion for a specific project
asyncio.run(insert_species_data(project_id=2))
