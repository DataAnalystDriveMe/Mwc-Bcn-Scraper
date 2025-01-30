import time
import requests
import pandas as pd
from bs4 import BeautifulSoup



def extract_extra_info(url:str) -> dict:

    """
    Extracts contact information (email, phone number, website, and LinkedIn) from a given webpage.

    This function makes an HTTP request to the specified URL, parses the HTML content using BeautifulSoup, 
    and searches for specific icons representing contact details. It then extracts and formats the corresponding 
    href attributes.

    :param url: The webpage URL to scrape.
    :return: A dictionary containing extracted contact details:
        - "email": Extracted email address (without "mailto:") or None if not found.
        - "phone_number": Extracted phone number (without "tel:") or None if not found.
        - "web": Extracted website URL or None if not found.
        - "linkedIn": Extracted LinkedIn profile URL or None if not found.
        Returns None if the request fails.
    """

    def extract_href_from_icon(soup, icon_class):
        """
        Searches for an <i> element with a specific class and retrieves the href from its parent <a> tag.

        :param soup: BeautifulSoup object containing the HTML.
        :param icon_class: Class of the <i> element to search for (e.g., "fa-solid fa-envelope mr-2").
        :return: The href value if found, otherwise None.
        """
        icon = soup.find("i", {"class": icon_class})
        if icon:
            a_tag = icon.find_parent("a")
            if a_tag and a_tag.has_attr("href"):
                return a_tag["href"]
        return None

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'es-ES,es;q=0.9',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    }

    r = requests.get(url,headers=headers)
    if r.ok:
        soup = BeautifulSoup(r.text,"html.parser")
        email = extract_href_from_icon(soup = soup , icon_class = "fa-solid fa-envelope mr-2")
        phone_number = extract_href_from_icon(soup = soup , icon_class = "fa-solid fa-phone mr-2")
        web = extract_href_from_icon(soup = soup , icon_class = "fa-solid fa-globe mr-2")
        linkedIn = extract_href_from_icon(soup = soup , icon_class = "fa-brands fa-linkedin mr-2")

        return {
            "email" : email.replace("mailto:","") if email else None ,
            "phone_number" : phone_number.replace("tel:","") if phone_number else None  ,
            "web" : web ,
            "linkedIn" : linkedIn
        }
    else:
        print(r.status_code)
        return None


def parse_json(enterprise_info:dict) -> dict :

    ent_id =  enterprise_info.get("externalId")
    ent_name = enterprise_info.get("name")
    ent_interests = enterprise_info.get("interests")
    ent_url = enterprise_info.get("url")
    ent_country = enterprise_info.get("country")
    is_startup = enterprise_info.get("startUp")
    ent_stage = enterprise_info.get("stage")
    ent_founding = enterprise_info.get("foundingYear")

    extra_information = extract_extra_info(url = ent_url)

    return {
        "id" : ent_id , 
        "name" : ent_name,
        "interests" : ent_interests ,
        "url" : ent_url , 
        "country" : ent_country ,
        "is_startup" : is_startup ,
        "stage" : ent_stage , 
        "founding" : ent_founding
    } | extra_information


def fetch_all_exhibitors(parse_json, start_page=0, max_retries=3, delay=0.2):
    """
    Iterates through pages of exhibitors from the API until no more results are found.

    :param parse_json: Function to parse each exhibitor's data.
    :param start_page: The starting page number (default: 0).
    :param max_retries: Number of retries in case of request failure (default: 3).
    :param delay: Delay in seconds between requests to avoid overloading the server (default: 2).
    :return: List of parsed exhibitor data.
    """
    url = "https://8vvb6vr33k-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.24.0)%3B%20Browser%20(lite)%3B%20instantsearch.js%20(4.75.5)%3B%20react%20(17.0.2)%3B%20react-instantsearch%20(7.13.8)%3B%20react-instantsearch-core%20(7.13.8)%3B%20JS%20Helper%20(3.22.5)&x-algolia-api-key=00422c3d9f3484bccfae011262fcf49a&x-algolia-application-id=8VVB6VR33K"
    
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'es-ES,es;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.mwcbarcelona.com',
        'Referer': 'https://www.mwcbarcelona.com/exhibitors?page=48',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    all_exhibitors = []
    page = start_page

    while True:
        # Prepare the request payload
        data = f'''{{
            "requests": [{{
                "indexName": "exhibitors-default",
                "params": "clickAnalytics=true&facets=%5B%22attributes%22%2C%22building%22%2C%22externalId%22%2C%22interests%22%2C%22letter%22%5D&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=24&maxValuesPerFacet=500&page={page}&query=&userToken=web"
            }}]
        }}'''

        # Retry logic for failed requests
        for attempt in range(max_retries):
            try:
                response = requests.post(url, headers=headers, data=data)
                if response.ok:
                    result = response.json()
                    exhibitors = result['results'][0]['hits']
                    
                    if not exhibitors:  # Stop if no more results
                        print(f"No more results found. Stopping at page {page}.")
                        return all_exhibitors

                    # Process and store data
                    parsed_exhibitors = [parse_json(enterprise_info=ex) for ex in exhibitors]
                    all_exhibitors.extend(parsed_exhibitors)

                    print(f"Page {page} processed successfully with {len(exhibitors)} exhibitors.")
                    page += 1
                    time.sleep(delay)  # Avoid aggressive requests
                    break  # Exit retry loop on success
                
                else:
                    print(f"Failed request on page {page}, attempt {attempt + 1}. Status code: {response.status_code}")
                    time.sleep(delay)

            except requests.RequestException as e:
                print(f"Exception on page {page}, attempt {attempt + 1}: {e}")
                time.sleep(delay)

        else:
            print(f"Max retries reached for page {page}. Skipping...")
            page += 1  # Skip to next page after retries are exhausted

    return all_exhibitors

def main():
    """
    Main function to fetch exhibitors and save them to a CSV file.
    """
    parsed_data = fetch_all_exhibitors(parse_json)
    df_exhibitors = pd.DataFrame(parsed_data)
    df_exhibitors['interests'] = df_exhibitors['interests'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
    df_exhibitors.to_csv("exhibitors_bcn.csv", index=False)
    print("Data saved to exhibitors_bcn.csv")

if __name__ == "__main__":
    main()