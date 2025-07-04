
import asyncio
import csv
import aiohttp

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

async def fetch_all_pages(session, initial_url):
    """
    Fetches data from an API endpoint, automatically following pagination links.
    Assumes the API response is a dictionary containing a 'results' list and a 'next' URL.
    """
    all_items = []
    next_url = initial_url

    while next_url:
        response_data = await fetch_json(session, next_url)
        if not response_data:
            break  # Stop if there was an error

        # IMPORTANT: This assumes a specific JSON structure, like {"results": [...], "next": "..."}
        # You must adapt `response_data.get('results', [])` and `response_data.get('next')`
        # to match the actual structure of your API.
        items_on_page = response_data.get('results', [])
        all_items.extend(items_on_page)

        next_url = response_data.get('next')
        if next_url:
            print(f"Found next page: {next_url}")

    print(f"Finished fetching all pages for {initial_url}. Found {len(all_items)} items.")
    return all_items

async def main():
    """
    Main function to fetch paginated data from multiple API endpoints and save to a CSV.
    """
    # We will use the Star Wars API (SWAPI) as an example because it uses pagination.
    initial_api_urls = [
        "https://swapi.dev/api/people/",
        "https://swapi.dev/api/planets/"
    ]

    # Define the fields you want. Note that 'people' and 'planets' have different fields.
    # We will handle this by writing to separate files.
    people_fields = ['name', 'height', 'mass', 'hair_color', 'skin_color', 'eye_color', 'birth_year', 'gender']
    planets_fields = ['name', 'rotation_period', 'orbital_period', 'diameter', 'climate', 'gravity', 'terrain']

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_all_pages(session, url) for url in initial_api_urls]
        results_by_url = await asyncio.gather(*tasks)

    # Process each result set separately
    for i, all_items in enumerate(results_by_url):
        if not all_items:
            print(f"No items found for URL: {initial_api_urls[i]}")
            continue

        # Determine which fields and filename to use
        if "people" in initial_api_urls[i]:
            fields_to_extract = people_fields
            output_csv_file = "swapi_people.csv"
        elif "planets" in initial_api_urls[i]:
            fields_to_extract = planets_fields
            output_csv_file = "swapi_planets.csv"
        else:
            continue # Skip if we don't have a mapping

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
