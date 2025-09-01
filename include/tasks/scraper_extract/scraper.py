import logging
import os
import re
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Config Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AllAirlineReviewScraper:
    """
    Extracts review data from all airlines on AirlineQuality.com.

    Args:
        num_pages_per_airline (int): The number of pages to scrape per airline.
        max_airlines (int): Maximum number of airlines to scrape (None for all).

    Returns:
        pd.DataFrame: A DataFrame containing the extracted review data from all airlines.
    """

    BASE_URL = "https://www.airlinequality.com"
    AIRLINES_LIST_URL = "https://www.airlinequality.com/review-pages/a-z-airline-reviews/"
    PAGE_SIZE = 100

    def __init__(
        self, 
        num_pages_per_airline: int = 5, 
        max_airlines: int = None,
        output_path: str = "/usr/local/airflow/include/data/raw_data.csv"
    ):
        self.num_pages_per_airline = num_pages_per_airline
        self.max_airlines = max_airlines
        self.output_path = output_path
        self.reviews_data = []

    def get_all_airline_urls(self) -> List[Tuple[str, str]]:
        """
        Extracts all airline URLs and names from the A-Z airline reviews page.

        Returns:
            List[Tuple[str, str]]: A list of tuples containing (airline_name, airline_url).
        """
        logging.info("Fetching airline list from A-Z page...")
        
        try:
            response = requests.get(self.AIRLINES_LIST_URL, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Failed to fetch airline list: {e}")
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        
        # Find all airline links on the A-Z page
        airline_links = []
        
        # Look for links that contain "/airline-reviews/" in their href
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if "/airline-reviews/" in href and href != "/airline-reviews/":
                # Extract airline name from the link text or URL
                airline_name = link.get_text(strip=True)
                if not airline_name:
                    # Extract from URL if no text
                    airline_name = href.split("/airline-reviews/")[-1].split("/")[0]
                    airline_name = airline_name.replace("-", " ").title()
                
                # Ensure it's a full URL
                if href.startswith("/"):
                    full_url = urljoin(self.BASE_URL, href)
                else:
                    full_url = href
                
                # Remove duplicates and ensure valid airline pages
                if airline_name and (airline_name, full_url) not in airline_links:
                    airline_links.append((airline_name, full_url))

        # Also try to find links in the content area more specifically
        content_div = soup.find("div", class_="comp_az-reviews-list") or soup.find("div", class_="comp_content")
        if content_div:
            for link in content_div.find_all("a", href=True):
                href = link.get("href", "")
                if "/airline-reviews/" in href and href != "/airline-reviews/":
                    airline_name = link.get_text(strip=True)
                    if not airline_name:
                        airline_name = href.split("/airline-reviews/")[-1].split("/")[0]
                        airline_name = airline_name.replace("-", " ").title()
                    
                    if href.startswith("/"):
                        full_url = urljoin(self.BASE_URL, href)
                    else:
                        full_url = href
                    
                    if airline_name and (airline_name, full_url) not in airline_links:
                        airline_links.append((airline_name, full_url))

        logging.info(f"Found {len(airline_links)} airlines")
        
        # Apply max_airlines limit if specified
        if self.max_airlines and len(airline_links) > self.max_airlines:
            airline_links = airline_links[:self.max_airlines]
            logging.info(f"Limited to first {self.max_airlines} airlines")
        
        return airline_links

    def scrape_airline_reviews(self, airline_name: str, airline_url: str) -> List[Dict[str, str]]:
        """
        Scrapes reviews for a specific airline.

        Args:
            airline_name (str): The name of the airline.
            airline_url (str): The base URL for the airline reviews.

        Returns:
            List[Dict[str, str]]: A list of review data dictionaries.
        """
        airline_reviews = []
        logging.info(f"Scraping {airline_name} reviews...")

        for page in range(1, self.num_pages_per_airline + 1):
            url = f"{airline_url}/page/{page}/?sortby=post_date%3ADesc&pagesize={self.PAGE_SIZE}"
            
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                logging.error(f"Failed to fetch {airline_name} page {page}: {e}")
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            reviews = soup.select('article[class*="comp_media-review-rated"]')

            if not reviews:
                logging.warning(f"No reviews found on page {page} for {airline_name}")
                break

            for review in reviews:
                review_data = self.extract_review_data(review)
                if review_data:
                    review_data["airline_name"] = airline_name
                    airline_reviews.append(review_data)

        logging.info(f"Scraped {len(airline_reviews)} reviews for {airline_name}")
        return airline_reviews

    def scrape_all_airlines(self) -> pd.DataFrame:
        """
        Scrapes reviews for all airlines and combines them into a single DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all review data with airline names.
        """
        airline_urls = self.get_all_airline_urls()
        
        if not airline_urls:
            logging.error("No airline URLs found!")
            return pd.DataFrame()

        all_reviews = []
        
        for airline_name, airline_url in airline_urls:
            try:
                airline_reviews = self.scrape_airline_reviews(airline_name, airline_url)
                all_reviews.extend(airline_reviews)
            except Exception as e:
                logging.error(f"Error scraping {airline_name}: {e}")
                continue

        if all_reviews:
            df = pd.DataFrame(all_reviews)
            self.save_to_csv(df)
            logging.info(f"Total reviews scraped: {len(all_reviews)}")
            return df
        else:
            logging.warning("No reviews were scraped!")
            return pd.DataFrame()

    def extract_review_data(self, review: BeautifulSoup) -> Dict[str, str]:
        """
        Extracts relevant data from a single review.

        Args:
            review (BeautifulSoup): The parsed HTML of a single review.

        Returns:
            Dict[str, str]: A dictionary containing extracted review data.
        """
        review_data = {
            "date": self.extract_text(review, "time", itemprop="datePublished"),
            "customer_name": self.extract_text(review, "span", itemprop="name"),
            "country": self.extract_country(review),
            "review_body": self.extract_text(review, "div", itemprop="reviewBody"),
        }

        self.extract_ratings(review, review_data)
        return review_data

    def extract_text(self, element: BeautifulSoup, tag: str, **attrs) -> str:
        """
        Extracts text from a BeautifulSoup element.

        Args:
            element (BeautifulSoup): The BeautifulSoup element to search within.
            tag (str): The HTML tag to look for.
            **attrs: Additional attributes to filter the search.

        Returns:
            str: The extracted text, or None if not found.
        """
        found = element.find(tag, attrs)
        return found.text.strip() if found else None

    def extract_country(self, review: BeautifulSoup) -> str:
        """
        Extracts the country from a review.

        Args:
            review (BeautifulSoup): The parsed HTML of a single review.

        Returns:
            str: The extracted country, or None if not found.
        """
        country = review.find(string=lambda text: text and "(" in text and ")" in text)
        return country.strip("()") if country else None

    def extract_ratings(
        self, review: BeautifulSoup, review_data: Dict[str, str]
    ) -> None:
        """
        Extracts ratings from a review and adds them to the review_data dictionary.

        Args:
            review (BeautifulSoup): The parsed HTML of a single review.
            review_data (Dict[str, str]): The dictionary to update with extracted ratings.
        """
        ratings_table = review.find("table", class_="review-ratings")
        if not ratings_table:
            return

        for row in ratings_table.find_all("tr"):
            header = row.find("td", class_="review-rating-header")
            if not header:
                continue

            header_text = header.text.strip()
            stars_td = row.find("td", class_="review-rating-stars")

            if stars_td:
                stars = stars_td.find_all("span", class_="star fill")
                review_data[header_text] = len(stars)
            else:
                value_td = row.find("td", class_="review-value")
                if value_td:
                    review_data[header_text] = value_td.text.strip()

    def save_to_csv(self, df: pd.DataFrame) -> None:
        """
        Saves a DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to save.
        """
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)
        logging.info(f"Data saved to {self.output_path}")


# Backward compatibility: Keep the original class for existing code
class AirlineReviewScraper(AllAirlineReviewScraper):
    """
    Backward compatible class for British Airways reviews only.
    """
    
    def __init__(self, num_pages: int, output_path: str = "/usr/local/airflow/include/data/raw_data.csv"):
        super().__init__(num_pages_per_airline=num_pages, max_airlines=1, output_path=output_path)
        # Override to use only British Airways
        self.british_airways_url = "https://www.airlinequality.com/airline-reviews/british-airways"
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrapes British Airways reviews only for backward compatibility.
        """
        return self.scrape_airline_reviews("British Airways", self.british_airways_url)


if __name__ == "__main__":
    # Example usage for all airlines (limited to 3 airlines and 2 pages each for testing)
    # Use appropriate path based on environment
    import os
    
    # Check if running in Airflow environment - use multiple indicators
    script_path = os.path.abspath(__file__)
    is_airflow = (
        '/usr/local/airflow' in script_path or 
        '/opt/airflow' in script_path or 
        os.path.exists('/usr/local/airflow') or
        'AIRFLOW_HOME' in os.environ
    )
    
    if is_airflow:
        # Running in Airflow/Docker environment
        airflow_output_path = "/usr/local/airflow/include/data/raw_data.csv"
        scraper = AllAirlineReviewScraper(
            num_pages_per_airline=100, 
            # max_airlines=3,  # Reove this for all airlines
            output_path=airflow_output_path
        )
        print(f"Running in Airflow environment, output will be saved to: {airflow_output_path}")
    else:
        # Running independently outside Docker
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))))
        local_output_path = os.path.join(project_root, "data", "raw_data.csv")
        scraper = AllAirlineReviewScraper(
            num_pages_per_airline=100, 
            # max_airlines=3,  # Remove this for all airlines
            output_path=local_output_path
        )
        print(f"Running independently, output will be saved to: {local_output_path}")
    
    scraper.scrape_all_airlines()