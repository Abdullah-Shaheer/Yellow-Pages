import json
import random
import time
import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup
from urllib.parse import unquote
import pandas as pd


# Define a retry mechanism with random sleep intervals
def request_with_retry(url, max_retries=5):
    session = HTMLSession()
    retries = 0
    while retries < max_retries:
        try:
            print(f"Attempt {retries + 1} to fetch: {url}")
            response = session.get(url, timeout=10)

            # Check for captchas or other anti-bot mechanisms (simple check for captcha in title)
            if "captcha" in response.text.lower():
                print("Captcha detected! Retrying after a delay...")
                retries += 1
                time.sleep(random.uniform(5, 15))  # Backoff delay before retry
                continue

            response.raise_for_status()
            print(f"Successfully fetched: {url}")
            return response
        except requests.exceptions.RequestException as e:
            print(f"Error occurred: {e}. Retrying...")
            retries += 1
            time.sleep(random.uniform(5, 15))  # Random delay between retries

    print(f"Failed to fetch {url} after {max_retries} attempts.")
    return None


# Function to get the soup (HTML content) of the YellowPages URL
def get_soup(url):
    print("Fetching soup from YellowPages URL...")

    headers = get_random_headers()  # Random user agent for initial request
    response = request_with_retry(url)
    if response:
        soup = BeautifulSoup(response.text, "html.parser")
        print("Soup fetched successfully.")
        return soup
    else:
        print("Failed to fetch soup from YellowPages URL.")
        return None


# Function to extract all job links from the soup
def job_links(soup):
    print("Finding all job links...")
    listings = soup.find_all("div", class_="listing__content listing__content--ltr listingInfo ctaMap2")
    j_l = []
    for listing in listings:
        link = listing.find("a", class_="listing__name--link listing__link jsListingName")
        if link:
            job_link = "https://www.yellowpages.ca" + link["href"]
            print(f"Found listing link: {job_link}")
            j_l.append(job_link)
    print(f"Total links found: {len(j_l)}")
    return j_l


# Function to get random headers for each request
def get_random_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Mobile Safari/537.36",
        "Mozilla/5.0 (iPad; CPU OS 13_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.2; rv:85.0) Gecko/20100101 Firefox/85.0"
    ]

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
        "DNT": "1"
    }

    return headers


def fetch_all_main_data(link):
    print(f"Fetching data from: {link}")
    details = {}

    headers = get_random_headers()  # Rotate user agent for each request

    response = request_with_retry(link)
    if response is None:
        return  # Skip to the next link if all retries fail

    soup1 = BeautifulSoup(response.text, "html.parser")

    try:
        title_element = soup1.find("h1", class_="merchantInfo-title merchant__title")
        if title_element:
            title = title_element.find("span").text.strip()
            details["title"] = title
            print(f"Title found: {title}")
        else:
            title_element = soup1.find("h1", class_="merchantInfo-title merchant__title only-title")
            title = title_element.find("span").text.strip() if title_element else "Not Available"
            details["title"] = title
            print(f"Title fallback: {title}")

    except Exception as e:
        details["title"] = "Not Available"
        print(f"An error occurred while finding the title: {e}")

    try:
        p_n_element_main = soup1.find("ul", class_="mlr__submenu jsMlrSubMenu")

        if p_n_element_main:
            # Find all <li> elements inside the main element
            p_n_elements = p_n_element_main.find_all("li", class_="mlr__submenu__item")

            # Check if there are any phone numbers
            if not p_n_elements:
                print("No phone numbers found.")
            else:
                for i, p_n_element in enumerate(p_n_elements):
                    phone = p_n_element.find("span", class_="mlr__sub-text").text.strip()
                    details[f"phone_{i + 1}"] = phone if phone else "Not Available"
                    print(f"Phone {i + 1} found: {phone}")
        else:
            print("Phone number list not found.")

    except Exception as e:
        details["phone"] = "Not Available"
        print(f"An error occurred while finding the phone: {e}")

    try:
        website_set = set()
        # First, try to find the main website element
        website_element_main = soup1.find("li", class_="mlr__item mlr__item--website")

        if website_element_main:
            website_element = website_element_main.find("a")
            if website_element:
                website = website_element["href"]
                # Decode the redirect URL if it exists
                if "redirect=" in website:
                    website = unquote(website.split("redirect=")[-1])
                website_set.add(website)  # Add to the set to prevent duplicates
                print(f"Main Website found: {website}")
        else:
            print("Main website not available.")

        # Check for nested websites inside the submenu
        submenus = soup1.find_all("ul", class_="mlr__submenu jsMlrSubMenu")
        if submenus:
            for submenu in submenus:
                submenu_items = submenu.find_all("li", class_="mlr__submenu__item mlr__submenu__itemnotprint")
                for submenu_item in submenu_items:
                    anchor_tag = submenu_item.find("a")  # Find the anchor tag within the <li>
                    if anchor_tag:
                        nested_website = anchor_tag["href"]
                        # Decode the redirect URL if it exists
                        if "redirect=" in nested_website:
                            nested_website = unquote(nested_website.split("redirect=")[-1])
                        else:
                            nested_website = "https://www.yellowpages.ca" + nested_website

                        website_set.add(nested_website)  # Add to set to avoid duplicates
                        # print(f"Nested Website found: {nested_website}")
        else:
            print("No submenus or nested websites found.")

        if website_set:
            for index, website in enumerate(website_set, 1):
                details[f"website_{index}"] = website
        else:
            details["website"] = "Not Available"
            print("No websites found at all.")

    except Exception as e:
        details["website"] = "Not Available"
        print(f"An error occurred while finding the websites: {e}")

    try:
        c = soup1.find("div", {'itemprop': 'address'})
        a = c.find("span", {'itemprop': 'streetAddress'}).text.strip()
        b = c.find("span", {'itemprop': 'addressLocality'}).text.strip()
        d = c.find("span", {'itemprop': 'addressRegion'}).text.strip()
        e = c.find("span", {'itemprop': 'postalCode'}).text.strip()
        address = f"{a}, {b}, {d} {e}"
        details["Address"] = address if address else "Not Available"
        print(f"Address found: {address}")

    except Exception as e:
        details["Address"] = "Not Available"
        print(f"An error occurred while finding the address: {e}")

    try:
        s_m_e = soup1.find("div", class_="merchant__useful_item mlr__item--website")
        if not s_m_e:
            print("There is no s_m_e")
        list_items = s_m_e.find_all("li")
        if not list_items:
            print("There are no list_items")

        account_count = 1  # To keep track of how many accounts we've processed

        for list_item in list_items:
            # Find all <a> tags within the list_item
            social_media_elements = list_item.find_all("a")
            if not social_media_elements:
                print("There are no social media elements")

            for social_media_element in social_media_elements:
                social_media_link = social_media_element["href"]

                # Check if the link has a redirect URL
                if "redirect=" in social_media_link:
                    # Extract the redirected URL
                    social_media_final_url = unquote(social_media_link.split("redirect=")[-1])
                    details[f"Social Media Account ({account_count})"] = social_media_final_url
                    print(f"Social Media Account ({account_count}) found: {social_media_final_url}")
                    account_count += 1
                else:
                    # If no redirect parameter, save the original link
                    details[f"Social Media Account ({account_count})"] = social_media_link
                    print(f"Social Media Account ({account_count}) found: {social_media_link}")
                    account_count += 1

    except Exception as e:
        print(f"An error occurred while finding the social media accounts: {e}")

    try:
        main_data_tables = soup1.find_all("div", class_="business__details jsParentContainer")

        for main_data_table in main_data_tables:
            heading_tag_h2 = main_data_table.find("h2", class_="module__title")
            heading_text = heading_tag_h2.text.strip() if heading_tag_h2 else ''

            # Check for Restaurant Type
            if 'restaurant type' in heading_text.lower():
                r_t = main_data_table.find("ul")
                restaurant_type_li = r_t.find_all("li")
                restaurant_types = [li.text.strip().rstrip(',') for li in restaurant_type_li if
                                    'more...' not in li.text.lower() and 'less...' not in li.text.lower()]
                # Remove duplicates and join them into a comma-separated string
                unique_restaurant_types = ', '.join(sorted(set(restaurant_types), key=restaurant_types.index))
                details["Restaurant Type"] = unique_restaurant_types
                print(f"Restaurant Type found: {unique_restaurant_types}")

            # Check for Cuisine Type
            elif 'cuisine type' in heading_text.lower():
                c_t = main_data_table.find("ul")
                cuisines = c_t.find_all("li")
                cuisine_element = [li.text.strip().rstrip(',') for li in cuisines]
                # Remove duplicates and join them into a comma-separated string
                cuisine_type = ', '.join(sorted(set(cuisine_element), key=cuisine_element.index))
                details['Cuisine Type'] = cuisine_type
                print(f"Cuisine Type found: {cuisine_type}")

            # Check for Atmosphere
            elif 'atmosphere' in heading_text.lower():
                atmos = main_data_table.find("ul")
                atmospher = atmos.find_all("li")
                atmospheres = [li.text.strip().rstrip(',') for li in atmospher]
                # Remove duplicates and join them into a comma-separated string
                atmosphere = ', '.join(sorted(set(atmospheres), key=atmospheres.index))
                details['Atmosphere'] = atmosphere
                print(f"Atmosphere found: {atmosphere}")

            # Check for Languages Spoken
            elif 'languages spoken' in heading_text.lower():
                l_s = main_data_table.find("ul")
                languages = l_s.find_all("li")
                lang_lst = [li.text.strip().rstrip(',') for li in languages]
                # Remove duplicates and join them into a comma-separated string
                language = ', '.join(sorted(set(lang_lst), key=lang_lst.index))
                details['Languages Spoken'] = language
                print(f"Languages Spoken: {language}")

    except Exception as e:
        details["Restaurant Type"] = "Not Available"
        print(f"An error occurred while finding the Restaurant Type: {e}")
    print(f"Data fetched: {details}")
    return details


def main():
    print("Starting scraping process...")
    data = []  # Initialize data list here to accumulate results

    for page_num in range(1, 60):
        ur = f"https://www.yellowpages.ca/search/si/{page_num}/Restaurants/Toronto+ON"
        s = get_soup(url=ur)
        if s is None:
            print("Soup could not be fetched. Exiting.")
            return

        links = job_links(soup=s)
        if not links:  # Check if any links were found
            print(f"No links found on page {page_num}. Exiting.")
            return

        for link in links:
            print(f"Scraping data from: {link}")
            fetched_data = fetch_all_main_data(link)
            if fetched_data:
                data.append(fetched_data)

            # Random delay to mimic human browsing behavior
            delay = random.uniform(1, 5)
            print(f"Sleeping for {delay:.2f} seconds before the next request...")
            time.sleep(delay)

    if data:  # Only create the DataFrame if there is data collected
        df = pd.DataFrame(data)
        df.to_excel("job_data.xlsx", index=False)
        with open("job_data.json", "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        print("Scraping complete. Data saved to scraped_data.xlsx")
    else:
        print("No data collected. Exiting.")


if __name__ == "__main__":
    main()
