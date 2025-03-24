import os
import json
import time
import requests
import pyrebase
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

# Global variables
BASE_URL = "https://apnews.com/ap-fact-check"
FIREBASE_URL = "https://factcheckers-b377b-default-rtdb.firebaseio.com/"
SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
})

# Firebase configuration
firebase_config = {
    "apiKey": "",  # No API key needed for database-only operations
    "authDomain": "factcheckers-b377b.firebaseapp.com",
    "databaseURL": FIREBASE_URL,
    "storageBucket": "factcheckers-b377b.appspot.com"
}

# Initialize Firebase
firebase = pyrebase.initialize_app(firebase_config)
db = firebase.database()

def load_existing_articles():
    """Load existing articles from Firebase."""
    existing_articles = {}
    next_id = 1
    
    try:
        # Get all articles from the 'articles' node
        articles = db.child("articles").get().val()
        if articles:
            # Handle different possible return types from Firebase
            if isinstance(articles, dict):
                # Process dictionary response
                for id_str, article in articles.items():
                    if isinstance(article, dict) and 'url' in article:
                        existing_articles[article['url']] = article
                        # Store the Firebase ID in the article for later reference
                        existing_articles[article['url']]['firebase_id'] = id_str
                        
                        # Try to convert the ID to an integer to track the highest ID
                        try:
                            article_id = int(id_str)
                            if article_id >= next_id:
                                next_id = article_id + 1
                        except ValueError:
                            # If the ID is not an integer, just continue
                            pass
            elif isinstance(articles, list):
                # Process list response
                for i, article in enumerate(articles):
                    if article and isinstance(article, dict) and 'url' in article:
                        existing_articles[article['url']] = article
                        # For list responses, use the index as a fallback ID
                        existing_articles[article['url']]['firebase_id'] = str(i + 1)
                
                # Set next_id to be one more than the length of the list
                next_id = len(articles) + 1
        
        print(f"Loaded {len(existing_articles)} existing articles from Firebase")
        print(f"Next article ID will be: {next_id}")
    except Exception as e:
        print(f"Error loading existing articles from Firebase: {e}")
        existing_articles = {}
    
    return existing_articles, next_id

def get_article_links(base_url, session):
    """Scrape all article links from the AP Fact Check page."""
    print(f"Fetching article links from {base_url}...")
    response = session.get(base_url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    article_links = []
    
    # Find all article items in the page list
    article_items = soup.select('div.PageList-items-item')
    
    for item in article_items:
        # Find the link element
        link_elem = item.select_one('h3.PagePromo-title a.Link')
        if link_elem and link_elem.has_attr('href'):
            url = link_elem['href']
            title = link_elem.text.strip()
            article_links.append({
                'url': url,
                'title': title
            })
    
    print(f"Found {len(article_links)} article links")
    return article_links

def convert_unix_timestamp(unix_timestamp):
    """Convert Unix timestamp (in milliseconds) to a standard datetime string."""
    if not unix_timestamp:
        return None
    
    try:
        # Convert from milliseconds to seconds
        timestamp_seconds = int(unix_timestamp) / 1000
        # Convert to datetime object
        dt = datetime.fromtimestamp(timestamp_seconds)
        # Format as a standard datetime string
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError) as e:
        print(f"Error converting timestamp {unix_timestamp}: {e}")
        return None

def scrape_article(article_info, session, existing_articles):
    """Scrape content from an individual article."""
    url = article_info['url']
    print(f"Scraping article: {url}")
    
    # Skip if we already have this article
    if url in existing_articles:
        print(f"Article already exists in the database with ID: {existing_articles[url].get('firebase_id', 'unknown')}, skipping: {url}")
        return None
    
    try:
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract article title
        title_elem = soup.select_one('h1.Page-headline')
        title = title_elem.text.strip() if title_elem else article_info.get('title', 'No title found')
        
        # Extract author
        author_elem = soup.select_one('div.Page-authors')
        author = None
        if author_elem:
            author_link = author_elem.select_one('a.Link')
            if author_link:
                author = author_link.text.strip()
            else:
                author = author_elem.text.replace('By', '').strip()
        
        # Extract timestamp - look for the bsp-timestamp element
        timestamp_elem = soup.select_one('bsp-timestamp')
        unix_timestamp = None
        standard_timestamp = None
        
        if timestamp_elem and timestamp_elem.has_attr('data-timestamp'):
            unix_timestamp = timestamp_elem['data-timestamp']
            # Convert Unix timestamp to standard datetime
            standard_timestamp = convert_unix_timestamp(unix_timestamp)
            # No need to get the human-readable timestamp anymore
        else:
            # Fallback to the old method if no Unix timestamp is found
            timestamp_elem = soup.select_one('span[data-date]')
            if timestamp_elem:
                # Try to parse the text timestamp into a standard format
                try:
                    date_text = timestamp_elem.text.strip()
                    # Convert text date to standard timestamp
                    dt = datetime.strptime(date_text, "%b. %d, %Y")
                    standard_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError) as e:
                    print(f"Error parsing text timestamp: {e}")
        
        # Extract article content
        content_elem = soup.select_one('div.RichTextStoryBody')
        
        # Process paragraphs and headers into a single string
        content = ""
        if content_elem:
            # Get all paragraphs and headers
            for elem in content_elem.find_all(['p', 'h2']):
                # Skip elements that are part of infoboxes or other non-content elements
                if elem.parent.get('class') and 'Infobox' in elem.parent.get('class'):
                    continue
                
                text = elem.text.strip()
                if not text:  # Skip empty elements
                    continue
                    
                if elem.name == 'h2':
                    content += f"\n## {text}\n\n"  # Format headings with markdown
                else:
                    content += f"{text}\n\n"  # Add paragraphs with double newlines
        
        # Trim any extra whitespace
        content = content.strip()
        
        # Create article data structure
        article_data = {
            'url': url,
            'title': title,
            'author': author,
            'unix_timestamp': unix_timestamp,
            'standard_timestamp': standard_timestamp,
            'content': content,
            'scraped_at': datetime.now().isoformat()
        }
        
        return article_data
        
    except Exception as e:
        print(f"Error scraping article {url}: {e}")
        return None

def save_article(article_data, existing_articles, next_id):
    """Save a single article to Firebase using sequential numeric IDs."""
    if not article_data:
        return False, next_id
    
    try:
        # Add the article to our existing articles dictionary
        existing_articles[article_data['url']] = article_data
        
        # Save the article with a sequential numeric ID
        db.child("articles").child(str(next_id)).set(article_data)
        
        # Store the ID in the article for future reference
        article_data['firebase_id'] = str(next_id)
        
        # Increment the ID for the next article
        next_id += 1
        
        return True, next_id
    except Exception as e:
        print(f"Error saving article to Firebase: {e}")
        return False, next_id

def run_scraper(base_url=BASE_URL):
    """Run the full scraping process."""
    # Load existing articles and get the next available ID
    existing_articles, next_id = load_existing_articles()
    
    # Get all article links
    article_links = get_article_links(base_url, SESSION)
    
    # Counter for new articles
    new_articles_count = 0
    
    # Scrape each article
    for i, article_info in enumerate(article_links):
        print(f"Processing article {i+1}/{len(article_links)}")
        
        article_data = scrape_article(article_info, SESSION, existing_articles)
        
        if article_data:
            # Save the article immediately
            success, next_id = save_article(article_data, existing_articles, next_id)
            if success:
                new_articles_count += 1
                print(f"Saved article: {article_data['title']} with ID: {article_data['firebase_id']}")
    
    print(f"Scraping complete. Added {new_articles_count} new articles.")
    print(f"Total articles in database: {len(existing_articles)}")
    print(f"All articles saved to Firebase at {FIREBASE_URL}")
    print(f"Next available ID: {next_id}")

if __name__ == "__main__":
    run_scraper()