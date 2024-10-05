import csv
from playwright.sync_api import sync_playwright
import time
import os
from dotenv import load_dotenv
import psycopg2  # Import psycopg2 for PostgreSQL connection

# Load environment variables from .env file
load_dotenv()

# Fetch credentials from environment variables
DB_HOST = os.getenv('SUPABASE_HOST')
DB_NAME = os.getenv('SUPABASE_DB')
DB_USER = os.getenv('SUPABASE_USER')
DB_PASSWORD = os.getenv('SUPABASE_PASSWORD')

def run(playwright):
    browser = playwright.chromium.launch(headless=False)
    page = browser.new_page()

    # Define the list of keywords and hashtags
    keywords = [
        "beautiful destinations", 
        "places to visit", 
        "places to travel", 
        "places that don't feel real", 
        "travel hacks"
    ]
    
    hashtags = [
        "traveltok", 
        "wanderlust", 
        "backpackingadventures", 
        "luxurytravel", 
        "hiddengems", 
        "solotravel", 
        "roadtripvibes", 
        "travelhacks", 
        "foodietravel", 
        "sustainabletravel"
    ]

    # Initialize list to store scraped data
    tiktok_data = []

    # Helper function to scrape search results
    def scrape_results(search_url, is_hashtag=False):
        try:
            page.goto(search_url)
            if is_hashtag:
                page.wait_for_selector('div[data-e2e="challenge-item"]', timeout=10000)
                search_results = page.query_selector_all('div[data-e2e="challenge-item"]')
            else:
                page.wait_for_selector('.css-1soki6-DivItemContainerForSearch', timeout=10000)
                search_results = page.query_selector_all('.css-1soki6-DivItemContainerForSearch')

            for result in search_results:
                if is_hashtag:
                    # Extract video URL
                    video_element = result.query_selector('a')
                    video_url = video_element.get_attribute('href') if video_element else ''

                    # Extract description
                    description_element = result.query_selector('div[data-e2e="search-card-video-caption"] h1')
                    description = description_element.inner_text() if description_element else ''

                    # Extract username
                    username_element = result.query_selector('a[data-e2e="search-card-user-unique-id"] p')
                    username = username_element.inner_text() if username_element else ''
                else:
                    # Extract video URL
                    video_element = result.query_selector('a.css-1g95xhm-AVideoContainer')
                    video_url = video_element.get_attribute('href') if video_element else ''

                    # Extract description
                    description_element = result.query_selector('div[data-e2e="search-card-video-caption"] h1')
                    description = description_element.inner_text() if description_element else ''

                    # Extract username
                    username_element = result.query_selector('a[data-e2e="search-card-user-link"] p')
                    username = username_element.inner_text() if username_element else ''

                # Append the data
                tiktok_data.append({
                    'search_term': search_url,
                    'video_url': video_url,
                    'description': description,
                    'username': username
                })

                # Insert the data into PostgreSQL
                insert_data({
                    'search_term': search_url,
                    'video_url': video_url,
                    'description': description,
                    'username': username
                })

        except Exception as e:
            print(f"Error scraping {search_url}: {e}")

    # Function to insert data into Supabase PostgreSQL
    def insert_data(data):
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO tiktok_results (search_term, video_url, description, username)
                VALUES (%s, %s, %s, %s)
            """, (data['search_term'], data['video_url'], data['description'], data['username']))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error inserting data into database: {e}")


    # Scrape results based on hashtags
    for hashtag in hashtags:
        search_url = f'https://www.tiktok.com/tag/{hashtag}'
        print(f"Scraping hashtag: {hashtag}")
        scrape_results(search_url, is_hashtag=True)
        time.sleep(2)  # Add a delay between searches to prevent rate-limiting


    # Scrape results based on keywords
    for keyword in keywords:
        search_url = f'https://www.tiktok.com/search?q={keyword.replace(" ", "%20")}'
        print(f"Scraping keyword: {keyword}")
        scrape_results(search_url)
        time.sleep(2)  # Add a delay between searches to prevent rate-limiting

    
    # Write the scraped data to a CSV file (optional)
    with open('tiktok_results6.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['search_term', 'video_url', 'description', 'username']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(tiktok_data)

    browser.close()

with sync_playwright() as playwright:
    run(playwright)
    print("Scraping completed. Data stored in the Supabase database.")



# import csv
# from playwright.sync_api import sync_playwright
# import time
# import re

# def run(playwright):
#     browser = playwright.chromium.launch(headless=False)
#     page = browser.new_page()

#     # Define the list of keywords and hashtags
#     keywords = [
#         "beautiful destinations", 
#         "places to visit", 
#         "places to travel", 
#         "places that don't feel real", 
#         "travel hacks"
#     ]
    
#     hashtags = [
#         "traveltok", 
#         "wanderlust", 
#         "backpackingadventures", 
#         "luxurytravel", 
#         "hiddengems", 
#         "solotravel", 
#         "roadtripvibes", 
#         "travelhacks", 
#         "foodietravel", 
#         "sustainabletravel"
#     ]

#     # Initialize list to store scraped data
#     tiktok_data = []
#     top_influencers = {}

#     # Helper function to extract count from string
#     def extract_count(text):
#         if text:
#             match = re.search(r'([\d.]+)([KMB]?)', text)
#             if match:
#                 num, unit = match.groups()
#                 multiplier = {'K': 1000, 'M': 1000000, 'B': 1000000000}.get(unit, 1)
#                 return int(float(num) * multiplier)
#         return 0

#     # Helper function to scrape user profile
#     def scrape_user_profile(username):
#         profile_url = f'https://www.tiktok.com/@{username}'
#         page.goto(profile_url)
#         page.wait_for_selector('strong[title="Followers"]', timeout=10000)
        
#         follower_count = extract_count(page.query_selector('strong[title="Followers"]').inner_text())
#         following_count = extract_count(page.query_selector('strong[title="Following"]').inner_text())
#         like_count = extract_count(page.query_selector('strong[title="Likes"]').inner_text())
        
#         return follower_count, following_count, like_count

#     # Helper function to scrape search results
#     def scrape_results(search_url):
#         try:
#             page.goto(search_url)
#             page.wait_for_selector('.css-1soki6-DivItemContainerForSearch', timeout=20000)
#             search_results = page.query_selector_all('.css-1soki6-DivItemContainerForSearch')

#             for result in search_results:
#                 # Extract video URL
#                 video_element = result.query_selector('a.css-1g95xhm-AVideoContainer')
#                 video_url = video_element.get_attribute('href') if video_element else ''

#                 # Extract description
#                 description_element = result.query_selector('div[data-e2e="search-card-video-caption"] h1')
#                 description = description_element.inner_text() if description_element else ''

#                 # Extract username
#                 username_element = result.query_selector('a[data-e2e="search-card-user-link"] p')
#                 username = username_element.inner_text() if username_element else ''

#                 # Scrape user profile info
#                 if username:
#                     follower_count, following_count, like_count = scrape_user_profile(username)
#                 else:
#                     follower_count, following_count, like_count = 0, 0, 0

#                 # Check if the user meets the top influencer criteria
#                 if follower_count >= 50000 and like_count >= 1000000:
#                     top_influencers[username] = {
#                         'username': username,
#                         'follower_count': follower_count,
#                         'like_count': like_count,
#                         'video_url': video_url
#                     }

#                 # Append the data
#                 tiktok_data.append({
#                     'search_term': search_url,
#                     'video_url': video_url,
#                     'description': description,
#                     'username': username,
#                     'follower_count': follower_count,
#                     'following_count': following_count,
#                     'like_count': like_count
#                 })
#         except Exception as e:
#             print(f"Error scraping {search_url}: {e}")

#     # Scrape results based on keywords
#     for keyword in keywords:
#         search_url = f'https://www.tiktok.com/search?q={keyword.replace(" ", "%20")}'
#         print(f"Scraping keyword: {keyword}")
#         scrape_results(search_url)
#         time.sleep(2)  # Add a delay between searches to prevent rate-limiting

#     # Scrape results based on hashtags
#     for hashtag in hashtags:
#         search_url = f'https://www.tiktok.com/tag/{hashtag}'
#         print(f"Scraping hashtag: {hashtag}")
#         scrape_results(search_url)
#         time.sleep(2)  # Add a delay between searches to prevent rate-limiting

#     # Write the scraped data to a CSV file
#     with open('tiktok_results_with_profile_info1.csv', 'w', newline='', encoding='utf-8') as csvfile:
#         fieldnames = ['search_term', 'video_url', 'description', 'username', 'follower_count', 'following_count', 'like_count']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(tiktok_data)

#     # Write top influencers to a separate CSV file
#     with open('top_influencers1.csv', 'w', newline='', encoding='utf-8') as csvfile:
#         fieldnames = ['username', 'follower_count', 'like_count', 'video_url']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(top_influencers.values())

#     browser.close()

# with sync_playwright() as playwright:
#     run(playwright)
#     print("Scraping completed. Data saved to tiktok_results_with_profile_info.csv")
#     print("Top influencers data saved to top_influencers1.csv")