import pandas as pd
import re
from datetime import date


def extract_salary_details(title):
    """
    Extracts salary range and currency from job title.
    The salary range is assumed to be in the form of 'USD xxxK - yyyK' or 'xxxk-yyyk'.

    Args:
        title (str): The job title containing the salary information.

    Returns:
        tuple: A tuple containing the salary currency, lower salary, and upper salary,
               or None if no salary range is found.
    """
    salary_pattern = r"([A-Za-z$€£]*)\s*(\d+(?:\.\d+)?)k\s?-\s?(\d+(?:\.\d+)?)k"
    match = re.search(salary_pattern, title, re.IGNORECASE)

    if match:
        currency = match.group(1).strip() if match.group(1) else None
        # Convert to full salary amount
        lower_salary = float(match.group(2)) * 1000
        # Convert to full salary amount
        upper_salary = float(match.group(3)) * 1000
        return (currency, lower_salary, upper_salary)

    return None


def is_job_post(title):
    """
    Checks if the post title contains keywords related to job postings.

    Args:
        title (str): The post title to check.

    Returns:
        bool: True if the title contains job-related keywords, False otherwise.
    """
    positive_job_keywords = [
        "hiring",
        "job",
        "position",
        "opening",
        "career",
        "recruitment",
        "employment",
        "vacancy",
        "opportunity",
        "role",
        "work"
    ]

    negative_job_keywords = [
        'help', 'question', 'advice', 'discussion', 'meta', 'feedback', 'suggestion'
    ]

    # Check for negative job-related keywords in the title
    for keyword in negative_job_keywords:
        if keyword in title.lower():
            return False

    # Check for positive job-related keywords in the title
    for keyword in positive_job_keywords:
        if keyword in title.lower():
            return True

    return False


def extract_job_details(title):
    """
    Extracts job position, location, field, and technologies from the job title.

    Args:
        title (str): The job title containing the job position, location, field, and technologies.

    Returns:
        dict: A dictionary with job position, location, field, and technologies.
    """
    job_details = {
        'job_position': None,
        'location': None,
        'field': None,
        'technologies': []
    }

    # Define extended patterns for job positions
    job_position_patterns = [
        r"(Engineer|Scientist|Manager|Developer|Architect|Analyst|Specialist|Director|Lead|Principal|Coordinator|Consultant|VP|Head)",
        r"(Data\s*Engineer|Machine\s*Learning\s*Engineer|AI\s*Engineer|Software\s*Engineer|Backend\s*Engineer|Frontend\s*Engineer|Fullstack\s*Engineer|DevOps\s*Engineer|Cloud\s*Engineer|Data\s*Scientist|Data\s*Analyst|QA\s*Engineer|Security\s*Engineer|Research\s*Scientist)"
    ]

    # Check for job position in title
    for pattern in job_position_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            job_details['job_position'] = match.group(0)
            break

    # Define extended patterns for locations
    location_patterns = [
        r"(Remote|Telecommute|Virtual|Home\s*Office|Hybrid)",
        r"(New\s*York|San\s*Francisco|California|London|Berlin|Toronto|Austin|Boston|Seattle|Chicago|Vancouver|Los\s*Angeles|Dallas|Miami|Washington\s*DC|Montreal|Paris|Singapore|Sydney|Zurich|Gdansk)",
        r"(US|United\s*States|Canada|UK|Germany|Australia|India|Singapore|Switzerland|France|Poland)"
    ]

    # Check for location in title
    for pattern in location_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            job_details['location'] = match.group(0)
            break

    # Define extended patterns for fields
    field_patterns = [
        r"(AI|Artificial\s*Intelligence|Data\s*Science|Machine\s*Learning|Deep\s*Learning|Computer\s*Vision|Natural\s*Language\s*Processing|Data\s*Engineering|Software\s*Engineering|Cloud\s*Computing|DevOps|Cyber\s*Security|Blockchain|Robotics|Big\s*Data|Data\s*Analytics|Business\s*Intelligence|Game\s*Development|Mobile\s*Development|UX/UI\s*Design|QA\s*Engineering|Testing)",
    ]

    # Check for field in title
    for pattern in field_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            job_details['field'] = match.group(0)
            break

    # Define extended patterns for technologies (including programming languages, frameworks, tools)
    tech_patterns = [
        r"(C\+\+|Python|Java|JavaScript|TypeScript|C#|Ruby|Go|Swift|Kotlin|SQL|Rust|Scala|Perl|MATLAB|Haskell|PHP|Shell|Julia|HTML|CSS|React|Node\.js|Angular|Vue\.js|Django|Flask|Spring|Express|TensorFlow|Keras|PyTorch|OpenCV|Scikit-learn|Hadoop|Spark|Kubernetes|Docker|AWS|Azure|Google\s*Cloud|GCP|Terraform|Jenkins|Redis|Elasticsearch|MongoDB|PostgreSQL|MySQL|SQLite|GraphQL|Sass|Bootstrap|JQuery|Vim|Docker|Terraform|Kafka|RabbitMQ|Firebase|Oracle|Solr)",
        r"(Git|GitHub|GitLab|Jira|Confluence|Slack|Trello|Docker|Ansible|Chef|Puppet|Nginx|Apache|Vagrant|CICD|Bash|PowerShell|Raspberry\s*Pi|Android|iOS|Flutter|Xamarin|Unity|Unreal|Cloud\s*Formation)"
    ]

    # Check for technologies in title and ensure clean extraction
    for pattern in tech_patterns:
        match = re.findall(pattern, title, re.IGNORECASE)
        if match:
            # Remove duplicates and standardize case
            unique_technologies = list(set([tech.lower() for tech in match]))
            job_details['technologies'].extend(unique_technologies)

    # Remove duplicates again for the overall technologies list
    job_details['technologies'] = list(set(job_details['technologies']))

    return job_details


def process_reddit_data(input_csv):
    """
    Process the Reddit job data to extract salary ranges, currency, job position, location, field, and technologies,
    and add as new columns.

    Args:
        input_csv (str): Path to the CSV file containing Reddit job data.

    Returns:
        pd.DataFrame: Processed DataFrame with additional columns.
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv)

    # Apply the salary extraction function to the 'title' column
    df[['salary_currency', 'lower_salary', 'upper_salary']] = df['title'].apply(
        lambda x: pd.Series(extract_salary_details(x)))

    # Apply the job details extraction function to the 'title' column
    job_details = df['title'].apply(extract_job_details)

    # Extract and expand the job details into new columns
    df = pd.concat([df, pd.json_normalize(job_details)], axis=1)

    # Filter out rows that aren't job posts unless they have salary details
    df = df[df.apply(lambda row: is_job_post(row['title']) or (
        pd.notna(row['lower_salary']) and pd.notna(row['upper_salary'])), axis=1)]

    # Save the updated DataFrame to a new CSV file
    output_csv = f'data/reddit/job_posts/processed/{str(date.today())}_posts.csv'
    df.to_csv(output_csv, index=False)
    print(f"Processed data saved to {output_csv}")

    return df


# Example usage
processed_df = process_reddit_data(
    f'data/reddit/job_posts/raw/{str(date.today())}_posts.csv')

# Output the first few rows of the processed DataFrame
print(processed_df.head())
