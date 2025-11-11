import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
import csv
import time
from typing import Dict, List
import pandas as pd

class CoachScraper:
    def __init__(self, config_path: str, max_workers: int = 8):
        """Load site configs and set up thread pool."""
        with open(config_path, "r", encoding="utf-8") as f:
            self.sites: Dict = yaml.safe_load(f)

        self.results: List[Dict] = []
        self.max_workers = max_workers
        self.headers = {"User-Agent": "Mozilla/5.0 (compatible; CoachScraper/1.0)"}

    # def fetch_html(self, url: str) -> str:
    #     """Fetch HTML content safely."""
    #     try:
    #         resp = requests.get(url, headers=self.headers, timeout=10)
    #         resp.raise_for_status()
    #         return resp.text
    #     except requests.RequestException as e:
    #         print(f"[!] Error fetching {url}: {e}")
    #         return ""
    def fetch_html(self, url: str, max_retries: int = 3, delay_seconds: int = 5) -> str:
        """
        Fetch HTML content safely, with retry logic for network or server errors.

        Args:
            url (str): The URL to fetch.
            max_retries (int): The maximum number of times to retry the request.
            delay_seconds (int): The time delay (in seconds) between retries.

        Returns:
            str: The HTML content if successful, otherwise an empty string.
        """
        if not url:
            print("[!] Error: Invalid URL received (empty string). Skipping.")
            return ""

        for attempt in range(max_retries):
            try:
                # 1. Attempt the request
                resp = requests.get(url, headers=self.headers, timeout=10)

                # 2. Raise exception for bad status codes (4xx or 5xx)
                resp.raise_for_status()

                # 3. If successful, return the text immediately
                return resp.text

            except requests.exceptions.HTTPError as e:
                # Handle specific 4xx/5xx errors
                if 400 <= resp.status_code < 500:
                    # Client errors (e.g., 404 Not Found, 403 Forbidden) are usually non-recoverable.
                    print(f"[!] Permanent Error fetching {url} (Status {resp.status_code}): {e}. Aborting retries.")
                    return "" 

                # Server errors (5xx) are usually temporary, so proceed to retry logic.
                print(f"[!] Temporary Error fetching {url} (Status {resp.status_code}): {e}. Retrying in {delay_seconds}s...")

            except requests.RequestException as e:
                # Handle connection issues (DNS error, timeout, connection reset, etc.)
                print(f"[!] Network Error fetching {url}: {e}. Retrying in {delay_seconds}s...")

            # This block executes if an exception occurred and it's not the last attempt
            if attempt < max_retries - 1:
                time.sleep(delay_seconds)
            else:
                # This is the last attempt, log final failure
                print(f"[!!!] Max retries ({max_retries}) reached for {url}. Failed to fetch.")
                return "" # Return empty string on final failure

        # Should only be reached if max_retries was 0, but included for completeness
        return ""

    def parse_site(self, school: str, config: Dict) -> Dict:
        """Extract coach info using configured selectors."""
        if 'handler' in config:
            print(f'need custom handler for {school}')
            return pd.DataFrame()
        url = config.get("url")
        html = self.fetch_html(url)
        if not html:
            return pd.DataFrame()

        soup = BeautifulSoup(html, "html.parser")
        staff_data = []
        
        for table_key, config in config.items():
            if table_key.endswith('_TABLE'): # Process only the table configs
                data = self.scrape_generic_table(soup, config)
                for d in data:
                    d['staff_type'] = table_key[:-6].capitalize()
                staff_data.append(data)

        staff_data = [pd.DataFrame(s) for s in staff_data]

        for df in staff_data:
            df['school'] = school

        if len(staff_data) > 1:
            staff_data = pd.concat(staff_data).reset_index().drop(columns='index')
        else:
            staff_data = staff_data[0]

        return staff_data
    
    def scrape_generic_table(self, soup, config):
        staff_data = []

        # 1. Find ALL elements matching the table_container_selector
        # This will return a list containing [Coaches Table, Staff Table, ...]
        all_tables = soup.select(config['table_container_selector'])

        # Use the new index key, defaulting to 0 if not present
        table_index = config.get('wrapper_index', 0) 

        if not all_tables or len(all_tables) <= table_index:
            print(f"Error: Table container not found using selector '{config['table_container_selector']}' at index {table_index}.")
            return staff_data

        # Select the specific table using the index
        table = all_tables[table_index]

        # 2. Find ALL staff rows RELATIVE to that selected table
        all_staff_rows = table.select(config['row_selector'])

        # 3. Iterate through rows and apply field selectors (Updated logic)
        for row in all_staff_rows:
            member = {}

            for field_name, field_config in config['field_selectors'].items():

                # --- NEW DICTIONARY HANDLING START ---

                attribute_name = None
                if isinstance(field_config, dict):
                    # If it's a dictionary (e.g., for image_url), extract the actual selector string and the attribute name
                    selector_string = field_config.get('selector')
                    attribute_name = field_config.get('attribute')
                else:
                    # If it's a simple string, use it directly as the selector
                    selector_string = field_config

                # Use the extracted selector string to find the tag
                tag = row.select_one(selector_string)

                # --- NEW DICTIONARY HANDLING END ---

                value = None

                if tag:
                    if attribute_name:
                        # Case 1: Handle attribute extraction (for image_url, etc.)
                        value = tag.get(attribute_name)
                    elif field_name == 'email' and tag.has_attr('href'):
                        # Case 2: Your original special email handling
                        value = tag['href'].replace('mailto:', '').strip()
                    else:
                        # Case 3: Your original text extraction handling
                        value = tag.get_text(strip=True)

                member[field_name.lower()] = value

            staff_data.append(member)

        return staff_data

    def scrape_all(self):
        """Scrape all configured schools in parallel."""
        start = time.time()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.parse_site, school, conf): school
                for school, conf in self.sites.items()
            }

            for future in as_completed(futures):
                school = futures[future]
                data = None
                try:
                    data = future.result()
                    if data is not None:
                        self.results.append(data)
                        print(f"completed {school}")
                    else:
                        print(f"No data for {school}")
                except Exception as e:
                    print(f"[!] Error scraping {school}: {e}")

        print(f"\nDone. Scraped {len(self.results)} schools in {time.time() - start:.1f}s")

    def save_csv(self, out_path="wbb_coaches.csv"):
        """Save to CSV."""
        if not self.results:
            print("No results to save.")
            return
        
        results = pd.concat(self.results)
        results.to_csv(out_path, index=False)

        print(f"Saved {len(results)} rows → {out_path}")
        
if __name__ == "__main__":
    cs = CoachScraper('./config.yaml')
    cs.scrape_all()
    cs.save_csv()
    
    
