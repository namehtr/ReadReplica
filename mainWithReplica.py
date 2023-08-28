from pymongo import MongoClient
import time,random
import concurrent.futures

# Connect to MongoDB
client = MongoClient([
    "localhost:27020",
    "localhost:27021",
    "localhost:27022"
], replicaSet='bookstore', readPreference='secondaryPreferred')

db = client.bookstoreDB
collection = db.books

# Function to fetch a random book
def fetch_random_book(dummy):
    book_id = random.randint(1, 100000)  # Assuming book_id range is from 1 to 100000
    book = collection.find_one({"book_id": book_id})

# Parallel fetching using ThreadPoolExecutor
num_reads = 10000

# Start the timer
start_time = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    # Using a dummy list of 100,00 elements to map the function
    executor.map(fetch_random_book, [None] * num_reads)

# End the timer
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"\nTime taken to query {num_reads} books: {elapsed_time:.2f} seconds")