
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
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"Error fetching data from {url}: {e}")
        return None

async def main():
    """
    Main function to fetch offset-paginated data and stream it to a CSV file
    to handle very large datasets with low memory usage.
    """
    # --- CONFIGURATION: ADAPT THESE TO YOUR API ---
    base_api_url = "https://dummyjson.com/products" # Example API that uses total/limit/skip
    max_results_per_page = 20
    
    # The query parameter names your API uses
    start_param = "skip"
    limit_param = "limit"

    # The keys in the JSON response
    total_key = "total"
    items_key = "products" # e.g., 'items', 'results', 'products'

    # The fields from each item you want to save
    fields_to_extract = ['id', 'title', 'brand', 'category', 'price', 'rating']
    output_csv_file = "api_data_streaming_offset.csv"
    # ---

    try:
        with open(output_csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fields_to_extract, extrasaction='ignore')
            header_written = False

            async with aiohttp.ClientSession() as session:
                # == Step 1: Make the initial request to get the total and first page ==
                initial_url = f"{base_api_url}?{limit_param}={max_results_per_page}&{start_param}=0"
                initial_response = await fetch_json(session, initial_url)

                if not initial_response:
                    print("Failed to fetch initial page. Aborting.")
                    return

                total_items = initial_response.get(total_key, 0)
                print(f"API reports a total of {total_items} items.")

                # == Step 2: Write the first page of items immediately ==
                first_page_items = initial_response.get(items_key, [])
                if first_page_items:
                    writer.writeheader()
                    header_written = True
                    writer.writerows(first_page_items)
                    print(f"Wrote initial page with {len(first_page_items)} items.")
                
                # == Step 3: Calculate and create tasks for all remaining pages ==
                urls_to_fetch = []
                for start_index in range(max_results_per_page, total_items, max_results_per_page):
                    next_url = f"{base_api_url}?{limit_param}={max_results_per_page}&{start_param}={start_index}"
                    urls_to_fetch.append(next_url)

                if not urls_to_fetch:
                    print("All items were on the first page. Done.")
                    return

                print(f"Fetching {len(urls_to_fetch)} remaining pages...")
                tasks = [fetch_json(session, url) for url in urls_to_fetch]

                # == Step 4: Stream the results of the remaining tasks to the file ==
                for future in asyncio.as_completed(tasks):
                    page_response = await future
                    if page_response:
                        items_on_page = page_response.get(items_key, [])
                        if items_on_page:
                            writer.writerows(items_on_page)
                            print(f"Wrote a page with {len(items_on_page)} items.")

            if header_written:
                print(f"Successfully streamed all data to {output_csv_file}")
            else:
                print("No data was found or written.")

    except IOError as e:
        print(f"Error writing to file: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Process interrupted by user.")
