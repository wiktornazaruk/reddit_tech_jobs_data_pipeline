import os
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Set up logging for debugging
logging.basicConfig(level=logging.INFO)


def scrape_posts(subreddit, start_datetime, end_datetime, extra_check=6):
    """
    Scrapes Reddit posts from a specified subreddit within a given date range.
    Args:
        subreddit (str): The name of the subreddit to scrape.
        start_datetime (datetime): The start datetime for the range filter.
        end_datetime (datetime): The end datetime for the range filter.
        extra_check (int): The number of additional posts to check after finding one that is out of range.

    Returns:
        list: A list of dictionaries containing post data.
    """
    url = f"https://old.reddit.com/r/{subreddit}/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    posts_data = []

    while True:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logging.error(f"Failed to retrieve page: {response.status_code}")
            time.sleep(2)  # Retry delay
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        posts_found = False

        for i, post in enumerate(soup.find_all('div', class_='thing')):
            if i == 0:
                continue  # Skip the first post (pinned post)
            try:
                # Parse post data
                post_id = post['data-fullname']
                title = post.find('p', class_='title').text
                post_url = post.find('a', class_='title')[
                    'href'] if post.find('a', class_='title') else ''
                content = post.find('div', class_='md').text if post.find(
                    'div', class_='md') else ''
                author = post['data-author']

                # Convert Unix timestamp to datetime and check date range
                created_utc = int(post['data-timestamp']) / \
                    1000  # Convert to seconds
                post_datetime = datetime.fromtimestamp(
                    created_utc, tz=timezone.utc)

                # Filter posts within the date range
                if start_datetime <= post_datetime <= end_datetime:
                    posts_found = True
                    upvotes = int(post['data-score'])
                    comments_count_elem = post.find('a', class_='comments')
                    if comments_count_elem:
                        # Extract the number of comments (in most cases, it's in the text of the 'comments' link)
                        comments_count_text = comments_count_elem.text.strip()
                        comments_count = int(comments_count_text.split()[
                                             0]) if comments_count_text.split()[0].isdigit() else 0
                    else:
                        comments_count = 0

                    post_data = {
                        'post_id': post_id,
                        'title': title,
                        'url': post_url,
                        'author': author,
                        'created_datetime': post_datetime,
                        'upvotes': upvotes,
                        'comments_count': comments_count,
                        'subreddit': subreddit
                    }
                    posts_data.append(post_data)
                    logging.info(f"Added post: {title} from {post_datetime}")

                # If the post is outside the range, continue checking a few more posts
                elif post_datetime < start_datetime:
                    logging.info(f"Post outside date range: {
                                 post_datetime}. Checking {extra_check} more posts.")
                    # Check the next `extra_check` posts to make sure we're not skipping valid posts
                    for j in range(1, extra_check):
                        next_post = soup.find_all('div', class_='thing')[i + j]
                        created_utc = int(next_post['data-timestamp']) / 1000
                        next_post_datetime = datetime.fromtimestamp(
                            created_utc, tz=timezone.utc)
                        if next_post_datetime >= start_datetime:
                            logging.info(f"Found valid post: {
                                         next_post_datetime}. Continuing.")
                            break
                    else:
                        # If we don't find a valid post, we exit
                        logging.info(
                            f"All next {extra_check} posts are outside the date range. Stopping.")
                        return posts_data

            except Exception as e:
                logging.error(f"Error processing post: {e}")
                continue

        # Check for pagination link and update URL, if available
        next_button = soup.find('span', class_='next-button')
        if next_button and next_button.find('a'):
            url = next_button.find('a')['href']
            time.sleep(1)  # Short delay for rate limiting
        else:
            break  # Exit if no more pages

    return posts_data


# Define the date range for today
year = date.today().year
month = date.today().month
day = date.today().day
start_datetime = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
end_datetime = datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)

# Scrape data from the subreddit
data = scrape_posts('dataengineeringjobs', start_datetime, end_datetime)

# Save data to a local CSV if any posts found
if data:
    output_dir = 'data/reddit/job_posts/raw'
    os.makedirs(output_dir, exist_ok=True)
    csv_file_path = os.path.join(
        output_dir, f'{str(date.today())}_posts.csv')
    df = pd.DataFrame(data)
    df.to_csv(csv_file_path, encoding='utf-8', index=False)
    logging.info("Data saved to CSV.")

