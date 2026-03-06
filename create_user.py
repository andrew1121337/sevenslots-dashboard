"""Create a user in the database. Usage: python create_user.py <username> <password>"""
import sys
import db

if len(sys.argv) != 3:
    print("Usage: python create_user.py <username> <password>")
    sys.exit(1)

db.init_db()
try:
    db.create_user(sys.argv[1], sys.argv[2])
    print(f"User '{sys.argv[1]}' created.")
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
