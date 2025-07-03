
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
    and save the results to a CSV file as they arrive.
    """
    # Using JSONPlaceholder as an example API
    api_urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3",
        "https://jsonplaceholder.typicode.com/posts/4", # Added a few more for demonstration
        "https://jsonplaceholder.typicode.com/posts/5"
    ]

    # Define the specific fields you want to extract
    fields_to_extract = ['id', 'title', 'userId']
    output_csv_file = "api_data_streamed.csv"

    print(f"Streaming fetched data to {output_csv_file}...")

    try:
        with open(output_csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fields_to_extract)
            header_written = False

            async with aiohttp.ClientSession() as session:
                tasks = [fetch_json(session, url) for url in api_urls]
                
                # Process tasks as they complete
                for future in asyncio.as_completed(tasks):
                    result = await future
                    if result:
                        # Write header only once, using the keys from the first result
                        if not header_written:
                            writer.writeheader()
                            header_written = True

                        # Extract and write the desired fields
                        processed_item = {key: result.get(key) for key in fields_to_extract}
                        writer.writerow(processed_item)
                        print(f"Wrote item with id: {processed_item.get('id')}")

        if header_written:
            print(f"Data successfully streamed to {output_csv_file}")
        else:
            print("No data was fetched successfully. No file was written.")

    except IOError as e:
        print(f"Error writing to CSV file: {e}")
        
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

