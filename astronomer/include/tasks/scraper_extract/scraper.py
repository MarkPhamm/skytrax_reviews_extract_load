import logging
import os
import re
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Config Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AirlineReviewScraper:
    """
    Extracts review data from British Airways reviews on AirlineQuality.com.

    Args:
        number_of_pages (int): The number of pages to scrape.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted review data.
    """

    BASE_URL = "https://www.airlinequality.com/airline-reviews/british-airways"
    PAGE_SIZE = 100

    def __init__(
        self,
        num_pages: int,
        output_path: str = "/usr/local/airflow/include/data/raw_data.csv",
    ):
        self.num_pages = num_pages
        self.output_path = output_path
        self.reviews_data = []

    def scrape(self) -> pd.DataFrame:
        for page in range(1, self.num_pages + 1):
            logging.info(f"Scraping page {page}...")
            url = f"{self.BASE_URL}/page/{page}/?sortby=post_date%3ADesc&pagesize={self.PAGE_SIZE}"

            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                logging.error(f"Failed to fetch page {page}: {e}")
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            reviews = soup.select('article[class*="comp_media-review-rated"]')

            for review in reviews:
                review_data = self.extract_review_data(review)
                if review_data:
                    self.reviews_data.append(review_data)

        df = pd.DataFrame(self.reviews_data)
        self.save_to_csv(df)
        return df

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

        Returns:h
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
            file_path (str): The path where the CSV file will be saved.
        """
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)
        logging.info(f"Data saved to {self.output_path}")


if __name__ == "__main__":
    scraper = AirlineReviewScraper(num_pages=30)
    scraper.scrape()
