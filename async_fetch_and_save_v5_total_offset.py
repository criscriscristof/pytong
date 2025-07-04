
import asyncio
import csv
import aiohttp
import math

async def fetch_json(session, url):
    """Asynchronously fetches JSON data from a given URL."""
    print(f"Fetching data from {url}...")
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            # We are returning the full response to extract 'total' and 'items' later
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"Error fetching data from {url}: {e}")
        return None

async def fetch_all_with_total_offset(session, base_url, max_results_per_page):
    """
    Fetches all items from a paginated API that uses a 'total' and 'start'/'offset' pattern.
    """
    # == Step 1: Make the initial request to get the total number of items ==
    initial_url = f"{base_url}?_start=0&_limit={max_results_per_page}"
    # Note: Using JSONPlaceholder's _start and _limit, you would replace these.
    # Your API might use: f"{base_url}?start=0&max_results={max_results_per_page}"
    
    initial_response = await fetch_json(session, initial_url)
    if not initial_response:
        return [] # Return empty list if the first request fails

    # == Step 2: Extract total and calculate pages needed ==
    # IMPORTANT: You must adapt these keys to your API.
    # JSONPlaceholder doesn't return a total, so we'll simulate it. For a real API, you'd do:
    # total_items = initial_response.get('total', 0)
    total_items = 100 # Simulating a total of 100 posts for the example
    all_items = initial_response # The first page of items is already fetched

    if total_items == 0:
        return all_items

    # == Step 3: Generate the list of all other URLs to fetch ==
    urls_to_fetch = []
    for start_index in range(max_results_per_page, total_items, max_results_per_page):
        next_url = f"{base_url}?_start={start_index}&_limit={max_results_per_page}"
        urls_to_fetch.append(next_url)

    # == Step 4: Fetch all other pages concurrently ==
    tasks = [fetch_json(session, url) for url in urls_to_fetch]
    remaining_pages = await asyncio.gather(*tasks)

    # == Step 5: Combine the results ==
    for page_items in remaining_pages:
        if page_items:
            # IMPORTANT: Adapt this if your items are in a nested key, e.g., page_items.get('results', [])
            all_items.extend(page_items)

    print(f"Finished fetching. Found {len(all_items)} items in total.")
    return all_items

async def main():
    """
    Main function to fetch offset-paginated data and save to a CSV.
    """
    # Using JSONPlaceholder posts endpoint as our example base URL
    base_api_url = "https://jsonplaceholder.typicode.com/posts"
    max_results = 20 # We'll fetch 20 items per page

    # Define the fields you want to extract
    fields_to_extract = ['userId', 'id', 'title', 'body']
    output_csv_file = "api_data_total_offset.csv"

    async with aiohttp.ClientSession() as session:
        all_items = await fetch_all_with_total_offset(session, base_api_url, max_results)

    if not all_items:
        print("No items were fetched. Exiting.")
        return

    print(f"Writing {len(all_items)} items to {output_csv_file}...")
    try:
        with open(output_csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fields_to_extract, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_items)
        print(f"Successfully wrote data to {output_csv_file}")
    except IOError as e:
        print(f"Error writing to {output_csv_file}: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Process interrupted by user.")
