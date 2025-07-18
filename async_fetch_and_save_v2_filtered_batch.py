
import asyncio
import csv
import aiohttp

async def fetch_json(session, url):
    """Asynchronously fetches JSON data from a given URL."""
    print(f"Fetching data from {url}...")
    try:
        async with session.get(url) as response:
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
    api_urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3"
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_json(session, url) for url in api_urls]
        results = await asyncio.gather(*tasks)

    # Filter out any None results from failed requests
    successful_results = [res for res in results if res]

    if not successful_results:
        print("No data was fetched successfully. Exiting.")
        return

    # Define the specific fields you want to extract from the JSON response
    fields_to_extract = ['id', 'title']

    # Process the results to extract only the desired fields
    processed_data = []
    for item in successful_results:
        processed_item = {key: item.get(key) for key in fields_to_extract}
        processed_data.append(processed_item)

    if not processed_data:
        print("No data to write after processing. Exiting.")
        return

    # Define the output CSV file name
    output_csv_file = "api_data_filtered.csv"

    # The headers for the CSV file are the fields we just extracted
    headers = fields_to_extract

    print(f"Writing filtered data to {output_csv_file}...")
    try:
        with open(output_csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(processed_data)
        
        print(f"Data successfully saved to {output_csv_file}")

    except IOError as e:
        print(f"Error writing to CSV file: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Process interrupted by user.")
