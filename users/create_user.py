from services.user_service import create_user
from classes.postgis_class import get_pg
import asyncio


async def create_a_user():
    """
    Creates a user using the custom pg connection class.
    """
    pg = await get_pg()  # Assuming get_pg() establishes the database connection
    try:
        user_id = await create_user(pg, "cartig", "carlostighe@gmail.com", "carlos")
        print(f"User created successfully with ID: {user_id}")
    except Exception as e:
        print(f"Failed to create user: {e}")

# Call the function with an event loop
if __name__ == "__main__":
    asyncio.run(create_a_user())
