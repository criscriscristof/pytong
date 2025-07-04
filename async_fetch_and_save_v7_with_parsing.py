import asyncio
import csv
import aiohttp
import math

# ==============================================================================
# == HELPER FUNCTION: This is where you put your custom parsing logic       ==
# ==============================================================================
def get_item_data(raw_item):
    """
    Parses a raw item dictionary from the API into the desired format for the CSV.
    
    Args:
        raw_item (dict): A single item dictionary from the API response.
        
    Returns:
        dict: A new dictionary with the desired structure and keys.
    """
    # --- Replace this with your actual parsing logic ---
    return {
        'product_id': raw_item.get('id'),
        'product_name': raw_item.get('title'),
        'brand_name': raw_item.get('brand'),
        'category': raw_item.get('category'),
        'price_usd': raw_item.get('price'),
        'customer_rating': raw_item.get('rating')
        # Example of more complex logic:
        # 'in_stock': raw_item.get('stock', 0) > 0,
        # 'description_snippet': raw_item.get('description', '')[:50] + '...'
    }
# ==============================================================================

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
    Main function to fetch, parse, and stream offset-paginated data to a CSV file.
    """
    # --- CONFIGURATION: ADAPT THESE TO YOUR API ---
    base_api_url = "https://dummyjson.com/products"
    max_results_per_page = 20
    
    start_param = "skip"
    limit_param = "limit"

    total_key = "total"
    items_key = "products"

    # IMPORTANT: These fields must now match the keys returned by get_item_data()
    fields_to_extract = ['product_id', 'product_name', 'brand_name', 'category', 'price_usd', 'customer_rating']
    output_csv_file = "api_data_parsed_and_streamed.csv"
    # ---

    try:
        with open(output_csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fields_to_extract, extrasaction='ignore')
            header_written = False

            async with aiohttp.ClientSession() as session:
                initial_url = f"{base_api_url}?{limit_param}={max_results_per_page}&{start_param}=0"
                initial_response = await fetch_json(session, initial_url)

                if not initial_response:
                    print("Failed to fetch initial page. Aborting.")
                    return

                total_items = initial_response.get(total_key, 0)
                print(f"API reports a total of {total_items} items.")

                first_page_items = initial_response.get(items_key, [])
                if first_page_items:
                    # Apply the parsing function to the first page of items
                    parsed_items = [get_item_data(item) for item in first_page_items]
                    writer.writeheader()
                    header_written = True
                    writer.writerows(parsed_items)
                    print(f"Wrote initial page with {len(parsed_items)} parsed items.")
                
                urls_to_fetch = []
                for start_index in range(max_results_per_page, total_items, max_results_per_page):
                    next_url = f"{base_api_url}?{limit_param}={max_results_per_page}&{start_param}={start_index}"
                    urls_to_fetch.append(next_url)

                if not urls_to_fetch:
                    print("All items were on the first page. Done.")
                    return

                print(f"Fetching {len(urls_to_fetch)} remaining pages...")
                tasks = [fetch_json(session, url) for url in urls_to_fetch]

                for future in asyncio.as_completed(tasks):
                    page_response = await future
                    if page_response:
                        items_on_page = page_response.get(items_key, [])
                        if items_on_page:
                            # Apply the parsing function to each subsequent page
                            parsed_items = [get_item_data(item) for item in items_on_page]
                            writer.writerows(parsed_items)
                            print(f"Wrote a page with {len(parsed_items)} parsed items.")

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