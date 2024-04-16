import requests
import os
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time
from requests.exceptions import SSLError, ConnectionError
import google.generativeai as genai
from telegram import Bot
import asyncio
import urllib3, socket
from urllib3.connection import HTTPConnection

HTTPConnection.default_socket_options = (
    HTTPConnection.default_socket_options + [
    (socket.SOL_SOCKET, socket.SO_SNDBUF, 1000000), #1MB in byte
    (socket.SOL_SOCKET, socket.SO_RCVBUF, 1000000)
])

if asyncio.get_event_loop().is_running():
    import nest_asyncio
    nest_asyncio.apply()

data_list = []

telegram_token = '6596910288:AAEhNR0tb_2e5bUQRWbsHyKn55d6PHWnChw'
telegram_chat_id = '7079224492'
bot = Bot(token=telegram_token)


# google_api_key = os.getenv('GOOGLE_API_KEY')
google_api_key = 'AIzaSyA4IjpUxzSAlcWYuG6g90oHNjyN0rPYASA'
genai.configure(api_key=google_api_key)

# Set up generative model
model = genai.GenerativeModel('gemini-pro')

async def send_file_to_telegram(filename):
    with open(filename, 'rb') as file:
        await bot.send_document(chat_id=telegram_chat_id, document=file)

async def send_telegram_message(text):
    await bot.send_message(chat_id=telegram_chat_id, text=text)

def get_gemini_response(input):
    prompt = """Please provide the city name from the given address.
    You must only return the city name and nothing else. If the address does not contain a valid city name,
    return the text 'None'. Note: A valid city name should be recognized as a city, not a village or town.

    Example:
    Address: 815 South Main Street, Grapevine, Texas 76051
    City: Grapevine

    Example:
    Address: 2453 Park Ave, Tustin, CA 92782
    City: Tustin

    Example:
    Address: 9910 Gaston Road, Suite 200, Katy, Texas 77494
    City: Katy

    Example:
    Address: 3201 Orleans Avenue
    City: None

    Example:
    Address: 121 Garlisch Dr (Off Higgins and Lively Blvd), Elk Grove Village, IL 60007
    City: None
    The above returns the text 'None' because Elk Grove Village is a village and not a city.

    Remember the instruction:
    You must only return the city name and nothing else. If the address does not contain a valid city name,
    return the text 'None'. Note: A valid city name should be recognized as a city, not a village or town.

    You should be able to detect invalid city names by running a Google search for accurate result

    Now, please extract and return the city name from the following address:

    """

    retry_attempts = 5
    for attempts in range(retry_attempts+1):
      try:
        response = model.generate_content(prompt + input, safety_settings=[
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ])

        return response
        break
      except Exception as e:
        print(e)

def get_current_directory_path(filename):
    return os.path.join(os.getcwd(), filename)


def get_map_link_with_retry(map_link):
    proxies={
        "http": "http://hagllwuk-rotate:itwc7jf4mnqf@p.webshare.io:80/",
        "https": "http://hagllwuk-rotate:itwc7jf4mnqf@p.webshare.io:80/"
   }

    retry_attempts = 5
    for attempt in range(retry_attempts):
      try:
          response = requests.get(map_link, timeout=150)
          if response.status_code == 200:
              return response
          elif response.status_code == 403 or str(response.status_code).startswith('5'):
              print(f"Retrying... Attempt {attempt+1}/{retry_attempts}")
              time.sleep(2)
          else:
              return response
      except requests.exceptions.RequestException as e:
          print("Error:", e)
      except SSLError as e:
          print("SSLError:", e)
      except ConnectionError as e:
            print("ConnectionError:", e)




def extract_places(map_link,map_list_name,publish_date):
    response = get_map_link_with_retry(map_link)

    soup = BeautifulSoup(response.text, 'lxml')

    try:
      description_map_list= soup.find('p', class_='c-entry-summary p-dek').text
    except:
      try:
        description_map_list = soup.find('div', class_='c-mapstack__methodology').text.strip()
      except:
        description_map_list = ''

    try:
      div_tags = soup.find_all('div', class_='c-mapstack__card-hed')
      address = ''
      for i in range(5):
          div_tag = div_tags[i]
          place_section_parent = div_tag.parent
          address = place_section_parent.find('div', class_='c-mapstack__address').find('a').text.strip() if place_section_parent.find('div', class_='c-mapstack__address') else ''
          response = get_gemini_response(address)
          city = response.text.strip()

          if city != 'None':
              break

    except Exception as e:
        city = ''

    try:
      map_list_description2_div = soup.find('div', class_='c-entry-content c-mapstack__content')
      # map_list_description2 = map_list_description2_div.find('p')

      p_tags = map_list_description2_div.find_all('p')

      # Extract the text content from each <p> tag and concatenate them
      map_list_description2 = ''.join(p_tag.get_text(strip=True) for p_tag in p_tags)

      if not map_list_description2:
         raise Exception("Empty string returned")
    except Exception as e:
     #print(e)
     try:
        map_list_description2 = soup.find('div', class_='c-mapstack__methodology').text.strip()

        #if not map_list_description2:
            #raise Exception("Empty string returned")

        unwanted_text = "Eater maps are curated by editors and aim to reflect a diversity of neighborhoods, cuisines, and prices. Learn more about our editorial process. If you buy something or book a reservation from an Eater link, Vox Media may earn a commission. See our ethics policy."
        if map_list_description2.startswith('Eater maps are curated'):
            raise Exception("Unwanted text found")

     except Exception as e:
        #print(e)
        try:
            map_list_description2_div = soup.find('div', class_='c-entry-content c-mapstack__content')
            map_list_description2 = map_list_description2_div.find('p').text

            if not map_list_description2:
                raise Exception("Empty string returned")
        except Exception as e:
            #print(e)
            try:
                element = soup.find("div", class_="c-entry-content c-mapstack__content")
                map_list_description2 = element.get_text(strip=True)

                #map_list_description2_div = soup.find('div', class_='c-entry-content c-mapstack__content')
                #description_map_list = map_list_description2_div.get_text(strip=True)

                if not map_list_description2:
                    raise Exception("Empty string returned")
            except Exception as e:
                map_list_description2 = ''


    # try:
    #   map_list_description2_div = soup.find('div', class_='c-entry-content c-mapstack__content')
    #   map_list_description2 = map_list_description2_div.find('p').text

    # except:
    #   map_list_description2 = ''
    """try:
      map_list_description2_div = soup.find('div', class_='c-entry-content c-mapstack__content')
      # map_list_description2 = map_list_description2_div.find('p')

      p_tags = map_list_description2_div.find_all('p')

      # Extract the text content from each <p> tag and concatenate them
      map_list_description2 = ''.join(p_tag.get_text(strip=True) for p_tag in p_tags)
    except Exception as e:
      # print(e)
      try:
        map_list_description2 = soup.find('div', class_='c-mapstack__methodology').text.strip()
      except:
        try:
          map_list_description2_div = soup.find('div', class_='c-entry-content c-mapstack__content')
          map_list_description2 = map_list_description2_div.find('p').text

        except:
          map_list_description2 = '' """



    # div_tags = soup.find_all('div', class_='c-mapstack__card-hed')

    place_data_list = []
    for div_tag in div_tags:
        place_section_parent = div_tag.parent
        place_name = place_section_parent.find('h1').text.strip() if place_section_parent.find('h1') else ''
        place_description = place_section_parent.find('div', class_='c-entry-content venu-card').find('p').text.strip() if place_section_parent.find('div', class_='c-entry-content venu-card') else ''
        google_maps_link = place_section_parent.find('a', string='Open in Google Maps')['href'] if place_section_parent.find('a', string='Open in Google Maps') else ''
        foursquare_a_tag = place_section_parent.find('ul', class_='services').find('a', href=lambda href: href.startswith('https://www.foursquare.com'))['href'] if place_section_parent.find('ul', class_='services') and place_section_parent.find('ul', class_='services').find('a', href=lambda href: href.startswith('https://www.foursquare.com')) else ''
        address = place_section_parent.find('div', class_='c-mapstack__address').find('a').text.strip() if place_section_parent.find('div', class_='c-mapstack__address') else ''
        website = place_section_parent.find('div', class_='c-mapstack__info').find('a', string='Visit Website')['href'] if place_section_parent.find('div', class_='c-mapstack__info') and place_section_parent.find('div', class_='c-mapstack__info').find('a', string='Visit Website') else ''

        try:
          # place_grand_parent = place_section_parent.parent
          # instagram_parent = place_grand_parent.find('a', class_='ViewProfileButton')['href'] if place_section_parent.find('a', class_='ViewProfileButton')['href'] else ''
          instagram = place_section_parent.find('blockquote',class_='instagram-media')
          post_shared_tag = instagram.find('p', string=lambda text: text and 'A post shared by' in text)
          if post_shared_tag:
            post_shared_text = post_shared_tag.get_text(strip=True)

          username_start = post_shared_text.find('@') + 1  # Adding 1 to exclude the '@' symbol
          username = post_shared_text[username_start:].split(')')[0]
          instagram_profile_link = f"https://www.instagram.com/{username}/"

        except:
          instagram_profile_link = ''

        place_data = {
        "Map List Link": map_link,
        "Map List Name": map_list_name,
        "Map List Publish Date":publish_date,
        "Map List Description1": description_map_list,
        "Map List Description2": map_list_description2,
        "Map List City": city,
        "Place Name": place_name,
        "Place Description": place_description,
        "Place Google Maps": google_maps_link,
        "Place Foursquare": foursquare_a_tag,
        "Place Address": address,
        "Place Website": website,
        "Instagram": instagram_profile_link }

        print(place_name)
        place_data_list.append(place_data)
        # data_list.append(place_data)

    return place_data_list

def get_map_page_with_retry(page_link):
    # proxies={
    # "http": "http://ixbaddxs-rotate:uvzo8vjxj1up@p.webshare.io:80/",
    # "https": "http://ixbaddxs-rotate:uvzo8vjxj1up@p.webshare.io:80/"
    #   }

    retry_attempts = 6
    for attempt in range(retry_attempts):
      try:
          response = requests.get(page_link, timeout=150)
          if response.status_code == 200:
              return response
          elif response.status_code == 403 or str(response.status_code).startswith('5'):
              print(f"Retrying... Attempt {attempt+1}/{retry_attempts}")
              time.sleep(3)
          else:
              return response
      except requests.exceptions.RequestException as e:
          print("Error:", e)
      except SSLError as e:
          print("SSLError:", e)
      except ConnectionError as e:
            print("ConnectionError:", e)



def extract_map_list():
    global data_list
    start_page = 349
    end_page = 359
    # end_page = 451
    try:

      with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []

        for page in range(start_page,end_page+1):
              url = f'https://www.eater.com/maps/archives/{page}'
              response = get_map_page_with_retry(url)
              print(f"Currently Scraping : {url}")


              soup = BeautifulSoup(response.text, 'lxml')

              map_boxes = soup.find_all('div', class_='c-entry-box--compact c-entry-box--compact--mapstack')
              map_boxes_featured = soup.find_all('div', class_='c-entry-box--compact c-entry-box--compact--featured')
              map_boxes.extend(map_boxes_featured)

              for box in map_boxes:
                try:
                  map_list_name = box.find('h2', class_='c-entry-box--compact__title').text.strip()
                except:
                  map_list_name = ''

                try:
                  map_link = box.find('a', {'data-analytics-link':'mapstack'})['href']
                except:
                  map_link = ''
                try:
                  publish_date = box.find('time', class_='c-byline__item').text.strip()
                except:
                  publish_date= ''

                # extract_places(map_link,map_list_name,publish_date)
                future = executor.submit(extract_places,map_link,map_list_name,publish_date)
                futures.append(future)


        for future in futures:
          try:
            result = future.result()
            if result:
                for item in result:
                  data_list.append(item)
          except Exception as e:
            print(f"Error processing task: {str(e)}")




    except KeyboardInterrupt:
        print("Received KeyboardInterrupt. Stopping gracefully.")
        asyncio.run(send_telegram_message('Received KeyboardInterrupt. Stopping gracefully'))
    except Exception as e:
        print(f"Error while trying to find the seller item element: {str(e)}")
        asyncio.run(send_telegram_message(f'Error happened {e}'))
    finally:
        global_df = pd.DataFrame(data_list)
        output_file = 'eater_data.xlsx'
        global_df.to_excel(output_file, engine='xlsxwriter', index=False)
        asyncio.run(send_file_to_telegram(output_file))
        asyncio.run(send_telegram_message(f'Finished Page {start_page}-{end_page}'))
# Error: HTTPSConnectionPool(host='demo.eater.com', port=443): Max retries exceeded with url: /maps/where-to-drink-goose-island-during-restaurant-week-2 (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x7ee528443be0>: Failed to resolve 'demo.eater.com' ([Errno -5] No address associated with hostname)"))

# 1m 14s
extract_map_list()