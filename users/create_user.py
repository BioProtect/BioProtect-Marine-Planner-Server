from classes.db_config import DBConfig
from sqlalchemy import create_engine, text
from passlib.hash import bcrypt

# Setup database connection
db_config = DBConfig()
db_url = (
    f"postgresql://{db_config.DATABASE_USER}:"
    f"{db_config.DATABASE_PASSWORD}@"
    f"{db_config.DATABASE_HOST}/"
    f"{db_config.DATABASE_NAME}"
)
engine = create_engine(db_url, future=True)


def create_a_user(username, email, password, role="User"):
    """
    Creates a user in the bioprotect.users table.
    """
    password_hash = bcrypt.hash(password)

    insert_query = text("""
        INSERT INTO bioprotect.users (
            username, email, password_hash, role,
            report_units, basemap, date_created, show_popup, use_feature_colours
        )
        VALUES (
            :username, :email, :password_hash, :role,
            'Km2', 'Light', CURRENT_TIMESTAMP, FALSE, FALSE
        )
        RETURNING id
    """)

    try:
        with engine.begin() as conn:
            result = conn.execute(insert_query, {
                'username': username,
                'email': email,
                'password_hash': password_hash,
                'role': role
            })
            user_id = result.scalar()
            print(f"✅ User created successfully with ID: {user_id}")
    except Exception as e:
        print(f"❌ Failed to create user: {e}")


if __name__ == "__main__":
    create_a_user(username="maylis",
                  email="Maylis.Sontot-Marjary@marine.ie",
                  password="Paltry3-Rundown-Okay",
                  role="Admin")
