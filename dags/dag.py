import time
from datetime import date, datetime, timedelta, timezone
from airflow import DAG
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from typing import List, Dict, Any, Optional, Tuple

# Set up logging for debugging
logging.basicConfig(level=logging.INFO)

def extract_salary_details(title: str) -> Optional[Tuple[str, float, float]]:
    """
    Extracts salary range and currency from job title.
    Args:
        title (str): The job title containing the salary information.
    Returns:
        Optional[Tuple[str, float, float]]: Tuple of (currency, lower_salary, upper_salary) or None
    """
    if not title:
        return None
        
    salary_pattern = r"([A-Za-z$€£]*)\s*(\d+(?:\.\d+)?)k\s?-\s?(\d+(?:\.\d+)?)k"
    match = re.search(salary_pattern, title.lower(), re.IGNORECASE)

    if match:
        currency = match.group(1).strip() or 'None'  # Default to None if no currency specified
        return (
            currency,
            float(match.group(2)) * 1000,
            float(match.group(3)) * 1000
        )

    return None

def is_job_post(title: str) -> bool:
    """
    Checks if the post title contains keywords related to job postings.
    Args:
        title (str): The post title to check.
    Returns:
        bool: True if the title contains job-related keywords and do not contain keywords that suggests it is not job post,
        False otherwise.
    """
    if not title:
        return False
        
    positive_job_keywords = {
        "hiring", "job", "position", "opening", "career", "recruitment",
        "employment", "vacancy", "opportunity", "role", "work"
    }

    negative_job_keywords = {
        'help', 'question', 'advice', 'discussion', 'meta', 'feedback', 'suggestion'
    }

    title_lower = title.lower()
    
    # Check for negative keywords first
    if any(keyword in title_lower for keyword in negative_job_keywords):
        return False

    # Then check for positive keywords
    return any(keyword in title_lower for keyword in positive_job_keywords)

def extract_job_details(title: str) -> Dict[str, Any]:
    """
    Extracts job position, location, field, and technologies from the job title.
    Args:
        title (str): The job title to process.
    Returns:
        Dict[str, Any]: Dictionary containing job details.
    """
    if not title:
        return {
            'job_position': None,
            'location': None,
            'field': None,
            'technologies': []
        }

    job_details = {
        'job_position': None,
        'location': None,
        'field': None,
        'technologies': []
    }

    title = title.strip()

    # Compile patterns once for efficiency
    job_position_patterns = [
        re.compile(pattern, re.IGNORECASE) for pattern in [
            r"(Data\s*Engineer|Machine\s*Learning\s*Engineer|AI\s*Engineer|Software\s*Engineer|Backend\s*Engineer|Frontend\s*Engineer|Fullstack\s*Engineer|DevOps\s*Engineer|Cloud\s*Engineer|Data\s*Scientist|Data\s*Analyst|QA\s*Engineer|Security\s*Engineer|Research\s*Scientist)",
            r"(Engineer|Scientist|Manager|Developer|Architect|Analyst|Specialist|Director|Lead|Principal|Coordinator|Consultant|VP|Head)"
        ]
    ]

    location_patterns = [
        re.compile(pattern, re.IGNORECASE) for pattern in [
            r"(Remote|Telecommute|Virtual|Home\s*Office|Hybrid)",
            r"(New\s*York|San\s*Francisco|California|London|Berlin|Toronto|Austin|Boston|Seattle|Chicago|Vancouver|Los\s*Angeles|Dallas|Miami|Washington\s*DC|Montreal|Paris|Singapore|Sydney|Zurich|Gdansk)",
            r"(US|United\s*States|Canada|UK|Germany|Australia|India|Singapore|Switzerland|France|Poland)"
        ]
    ]

    field_patterns = re.compile(r"(AI|Artificial\s*Intelligence|Data\s*Science|Machine\s*Learning|Deep\s*Learning|Computer\s*Vision|NLP|Natural\s*Language\s*Processing|Data\s*Engineering|Software\s*Engineering|Cloud\s*Computing|DevOps|Cyber\s*Security|Blockchain|Robotics|Big\s*Data|Analytics)", re.IGNORECASE)

    # Match patterns
    for pattern in job_position_patterns:
        match = pattern.search(title)
        if match:
            job_details['job_position'] = match.group(1)
            break

    for pattern in location_patterns:
        match = pattern.search(title)
        if match:
            job_details['location'] = match.group(1)
            break

    field_match = field_patterns.search(title)
    if field_match:
        job_details['field'] = field_match.group(1)

    # Technology detection using a more reliable approach
    tech_keywords = {
        'python', 'java', 'javascript', 'typescript', 'c++', 'c#', 'ruby', 'go',
        'sql', 'rust', 'scala', 'react', 'angular', 'vue', 'django', 'flask',
        'spring', 'tensorflow', 'pytorch', 'kubernetes', 'docker', 'aws', 'azure',
        'gcp', 'terraform', 'jenkins', 'redis', 'mongodb', 'postgresql', 'mysql'
    }
    
    words = set(re.findall(r'\b\w+\b', title.lower()))
    job_details['technologies'] = list(words.intersection(tech_keywords))

    return job_details

def scrape_reddit_posts(subreddit: str, start_datetime: datetime, end_datetime: datetime, ti, extra_check: int = 6) -> None:
    """
    Scrapes Reddit posts from a specified subreddit within a given date range.
    Args:
        subreddit (str): The name of the subreddit to scrape.
        start_datetime (datetime): The start datetime for the range filter.
        end_datetime (datetime): The end datetime for the range filter.
        ti (TaskInstance): Airflow task instance.
        extra_check (int): Number of additional posts to check.
    """
    logging.info(f"Processing posts between {start_datetime} and {end_datetime}")

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
                    logging.info(f"Post outside date range: {post_datetime}. Checking {extra_check} more posts.")
                    # Check the next `extra_check` posts to make sure we're not skipping valid posts
                    for j in range(1, extra_check):
                        next_post = soup.find_all('div', class_='thing')[i + j]
                        created_utc = int(next_post['data-timestamp']) / 1000
                        next_post_datetime = datetime.fromtimestamp(
                            created_utc, tz=timezone.utc)
                        if next_post_datetime >= start_datetime:
                            logging.info(f"Found valid post: {next_post_datetime}. Continuing.")
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
            logging.info(f"Moving to next page: {url}")
            time.sleep(1)  # Short delay for rate limiting
        else:
            logging.info("No next page found. Stopping pagination.")
            break  # Exit if no more pages

    if not posts_data:
        logging.warning("No posts were scraped. Check the subreddit or date range.")
        ti.xcom_push(key='raw_posts', value=[])  # Push an empty list to XCom to avoid task failure
        return
    
    logging.info(f"Total posts fetched: {len(posts_data)}")

    # Push posts to XCom
    ti.xcom_push(key='raw_posts', value=post_data)

def process_job_posts(ti) -> None:
    """
    Process raw Reddit posts into structured job posts.
    Args:
        ti (TaskInstance): Airflow task instance.
    """
    # Get raw posts directly from the extract task
    raw_posts = ti.xcom_pull(task_ids='extract_reddit_posts')
    
    if not raw_posts:
        # Try alternative key if direct pull fails
        raw_posts = ti.xcom_pull(task_ids='extract_reddit_posts', key='raw_posts')
    
    if not raw_posts:
        logging.error("No raw posts found in XCom")
        ti.xcom_push(key='posts', value=[])
        return
    
    logging.info(f"Retrieved {len(raw_posts)} raw posts for processing")

    df = pd.DataFrame(raw_posts)
    if df.empty:
        logging.warning("Empty DataFrame created from raw posts")
        ti.xcom_push(key='posts', value=[])
        return

    logging.info(f"Created DataFrame with shape {df.shape}")
    
    df.drop_duplicates(subset=['post_id', 'title'], inplace=True)
    logging.info(f"After removing duplicates: {len(df)} posts")

    # Process salary information
    salary_info = df['title'].apply(extract_salary_details)
    df[['salary_currency', 'lower_salary', 'upper_salary']] = pd.DataFrame(
        salary_info.tolist(),
        index=df.index
    )

    # Process job details
    job_details = df['title'].apply(extract_job_details)
    df = pd.concat([df, pd.DataFrame.from_records(job_details)], axis=1)

    # Filter valid job posts
    df['is_valid_post'] = df['title'].apply(is_job_post) | df[['lower_salary', 'upper_salary']].notna().all(axis=1)
    df = df[df['is_valid_post']].drop(columns=['is_valid_post'])

    if df.empty:
        logging.warning("No valid job posts found after processing")
        ti.xcom_push(key='posts', value=[])
        return

    logging.info(f"Processed {len(df)} job posts")

    # Convert Timestamp objects to ISO format strings before serialization
    if 'created_datetime' in df.columns:
        df['created_datetime'] = df['created_datetime'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

    # Convert DataFrame to dict and ensure all objects are JSON serializable
    posts_dict = df.to_dict('records')
    
    # Convert any remaining Timestamp objects to strings
    for post in posts_dict:
        for key, value in post.items():
            if isinstance(value, pd.Timestamp):
                post[key] = value.isoformat()

    ti.xcom_push(key='posts', value=posts_dict)

def insert_posts_into_postgres(ti) -> None:
    """
    Insert processed posts into PostgreSQL database.
    Args:
        ti (TaskInstance): Airflow task instance.
    """
    posts = ti.xcom_pull(key='posts', task_ids='transform_reddit_posts')
    if not posts:
        logging.warning("No posts to insert")
        return

    postgres_hook = PostgresHook(postgres_conn_id='jobs_connection')
    
    insert_query = """
        INSERT INTO posts (
            post_id, title, url, author, created_datetime, upvotes, 
            comments_count, subreddit, salary_currency, lower_salary, 
            upper_salary, job_position, location, field, technologies
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (post_id) DO UPDATE SET
            title = EXCLUDED.title,
            url = EXCLUDED.url,
            author = EXCLUDED.author,
            created_datetime = EXCLUDED.created_datetime,
            upvotes = EXCLUDED.upvotes,
            comments_count = EXCLUDED.comments_count,
            subreddit = EXCLUDED.subreddit,
            salary_currency = EXCLUDED.salary_currency,
            lower_salary = EXCLUDED.lower_salary,
            upper_salary = EXCLUDED.upper_salary,
            job_position = EXCLUDED.job_position,
            location = EXCLUDED.location,
            field = EXCLUDED.field,
            technologies = EXCLUDED.technologies;
    """

    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for post in posts:
                try:
                    cur.execute(insert_query, (
                        post['post_id'], post['title'], post['url'], post['author'],
                        post['created_datetime'], post['upvotes'], post['comments_count'],
                        post['subreddit'], post['salary_currency'], post['lower_salary'],
                        post['upper_salary'], post['job_position'], post['location'],
                        post['field'], post['technologies']
                    ))
                except Exception as e:
                    logging.error(f"Error inserting post {post['post_id']}: {e}")
                    continue

            conn.commit()

def dag_failure_alert(context):
    """Send an alert on DAG failure"""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    error_msg = str(context.get('exception', 'Unknown error'))
    
    logging.error(f"DAG {dag_id} failed in task {task_id} on {execution_date}")
    logging.error(f"Error: {error_msg}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'wait_for_downstream': True,
    'start_date': datetime(2024, 12, 30),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': dag_failure_alert
}

# Create the DAG
dag = DAG(
    'fetch_and_store_reddit_data_engineering_job_posts',
    default_args=default_args,
    description='Fetch job posts from Reddit and store in Postgres',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Add documentation
dag.doc_md = """
## Reddit Data Engineering Jobs DAG

This DAG scrapes job postings from the r/dataengineeringjobs subreddit and stores them in a PostgreSQL database.

### Tasks:
1. extract_reddit_posts: Scrape posts from Reddit
2. transform_reddit_posts: Process and clean the job posts data
3. create_table: Ensure the database table exists
4. load_posts: Store the processed data in PostgreSQL

### Schedule:
Runs daily to collect the previous day's job posts.

### Dependencies:
- PostgreSQL connection ID: 'jobs_connection'
- Reddit access (old.reddit.com)
"""

# Create tasks

extract_reddit_posts_task = PythonOperator(
    task_id='extract_reddit_posts',
    python_callable=scrape_reddit_posts,
    op_kwargs={
        'subreddit': 'dataengineeringjobs',
        'start_datetime': datetime.now(timezone.utc) - timedelta(days=1),
        'end_datetime': datetime.now(timezone.utc),
    },
    provide_context = True,
    dag=dag,
)

transform_reddit_posts_task = PythonOperator(
    task_id='transform_reddit_posts',
    python_callable=process_job_posts,
    provide_context = True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='jobs_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS posts (
        post_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        url TEXT,
        author TEXT,
        created_datetime TIMESTAMP WITH TIME ZONE,
        upvotes INTEGER,
        comments_count INTEGER,
        subreddit TEXT,
        salary_currency TEXT,
        lower_salary FLOAT,
        upper_salary FLOAT,
        job_position TEXT,
        location TEXT,
        field TEXT,
        technologies TEXT[]
    );
    CREATE INDEX IF NOT EXISTS idx_posts_created_datetime ON posts(created_datetime);
    """,
    dag=dag,
)

load_posts_task = PythonOperator(
    task_id='load_posts',
    python_callable=insert_posts_into_postgres,
    provide_context = True,
    dag=dag,
)

# Set up task dependencies
extract_reddit_posts_task >> transform_reddit_posts_task >> create_table_task >> load_posts_task

if __name__ == "__main__":
    dag.cli()