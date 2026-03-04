import os
import json
import glob
import sys
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "lab5")


class MongoDBManager:
    def __init__(self):
        if not MONGO_URI:
            raise ValueError("❌ MONGO_URI is not set. Please configure your .env file.")
        self.client = None
        self.db = None

    def test_connection(self):
        """Test the connection to MongoDB and print server info."""
        try:
            self.client = MongoClient(MONGO_URI)
            self.client.admin.command("ping")
            print("✅ Connection successful!")
            print(f"   Database: '{MONGO_DB}'")
            server_info = self.client.server_info()
            print(f"   MongoDB version: {server_info.get('version', 'unknown')}")
            self.client.close()
            print("🔒 Connection closed.")
        except Exception as e:
            print(f"❌ Connection failed: {e}")

    def insert_data(self):
        """Read all JSON files and insert them into MongoDB."""
        try:
            self.client = MongoClient(MONGO_URI)
            self.db = self.client[MONGO_DB]
            print(f"🔗 Connected to database: '{MONGO_DB}'")

            data_dir = os.path.join(os.path.dirname(__file__), "data", "Datos_para_MongoDB")
            json_files = glob.glob(os.path.join(data_dir, "*.json"))

            if not json_files:
                print("⚠️  No JSON files found in", data_dir)
                return

            print(f"📂 Found {len(json_files)} JSON file(s)\n")

            total_inserted = 0

            for filepath in sorted(json_files):
                filename = os.path.basename(filepath)
                collection_name = os.path.splitext(filename)[0]

                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)

                documents = data if isinstance(data, list) else [data]
                collection = self.db[collection_name]
                collection.drop()

                result = collection.insert_many(documents)
                count = len(result.inserted_ids)
                total_inserted += count

                print(f"  📄 {filename}")
                print(f"     → Collection: '{collection_name}'")
                print(f"     → Documents inserted: {count}\n")

            print("=" * 50)
            print(f"🎉 Done! Inserted {total_inserted} documents into '{MONGO_DB}'.")

            print(f"\n📋 Collections in '{MONGO_DB}':")
            for name in sorted(self.db.list_collection_names()):
                count = self.db[name].count_documents({})
                print(f"   • {name} ({count} documents)")

            self.client.close()
            print("\n🔒 Connection closed.")

        except Exception as e:
            print(f"❌ Error: {e}")


if __name__ == "__main__":
    manager = MongoDBManager()

    if len(sys.argv) < 2:
        print("Usage: python main.py [--test | --insert]")
        print("  --test    Test MongoDB connection")
        print("  --insert  Insert JSON data into MongoDB")
        sys.exit(1)

    flag = sys.argv[1]

    if flag == "--test":
        manager.test_connection()
    elif flag == "--insert":
        manager.insert_data()
    else:
        print(f"❌ Unknown flag: {flag}")
        print("Use --test or --insert")
        sys.exit(1)
