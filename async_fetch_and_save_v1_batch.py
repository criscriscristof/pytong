
import asyncio
import csv
import aiohttp

async def fetch_json(session, url):
    """Asynchronously fetches JSON data from a given URL."""
    print(f"Fetching data from {url}...")
    try:
        async with session.get(url) as response:
            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()
            print(f"Successfully fetched data from {url}")
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"Error fetching data from {url}: {e}")
        return None

async def main():
    """
    Main function to fetch data from multiple API endpoints asynchronously
    and save the combined results to a CSV file.
    """
    # Using JSONPlaceholder as an example API
    api_urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3"
    ]

    async with aiohttp.ClientSession() as session:
        # Create a list of tasks to run concurrently
        tasks = [fetch_json(session, url) for url in api_urls]
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)

    # Filter out any None results from failed requests
    successful_results = [res for res in results if res]

    if not successful_results:
        print("No data was fetched successfully. Exiting.")
        return

    # Define the output CSV file name
    output_csv_file = "api_data_batch.csv"

    # The headers for the CSV file should match the keys in the JSON objects
    # Assuming all JSON objects have the same structure
    headers = successful_results[0].keys()

    print(f"Writing data to {output_csv_file}...")
    try:
        with open(output_csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)

            # Write the header row
            writer.writeheader()

            # Write the data rows
            writer.writerows(successful_results)
        
        print(f"Data successfully saved to {output_csv_file}")

    except IOError as e:
        print(f"Error writing to CSV file: {e}")

if __name__ == "__main__":
    # To run the async main function, we use asyncio.run()
    # This is available in Python 3.7+
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Process interrupted by user.")
