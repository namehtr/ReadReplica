from pymongo import MongoClient
import concurrent.futures

# Connect to MongoDB
client = MongoClient('localhost:27022')
db = client.bookstoreDB
collection = db.books

# Function to insert a document
def insert_book(book_id):
    book = {
        "book_id": book_id,
        "title": f"Book Title {book_id}"
    }
    collection.insert_one(book)

# Parallel insertion using ThreadPoolExecutor
start_book_id = 100001
end_book_id = 200000

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(insert_book, range(start_book_id, end_book_id + 1))