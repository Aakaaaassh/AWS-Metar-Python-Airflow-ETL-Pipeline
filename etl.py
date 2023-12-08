import requests
import pandas as pd 
from datetime import datetime
from bs4 import BeautifulSoup
import json
import csv




def ETL():
    
    url = "https://tgftp.nws.noaa.gov/data/observations/metar/stations/"
    response = requests.get(url)

    data_list = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all links on the page
        links = soup.find_all('a')

        for link in links:
            file_name = link.get_text()
            file_link = url + file_name
            if file_name in ('Name', 'Last modified','Size','Parent Directory'):
                continue
            else:
                data = {
                "Name": file_name,
                "Link": file_link
            }
                data_list.append(data)
                
            # Fetch additional details for each link (assuming they are in the same format)
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

        
    df = pd.DataFrame(data_list)
    csv_file_path = 'scraped_data.csv'  # Change this to your desired path

    # Save the DataFrame to a CSV file at the specified path
    df.to_csv(csv_file_path, index=False)

    print(f"Data has been saved to {csv_file_path}")
    return data_list



#Wind Direction
def WD(string):

    """
    Converts a wind direction string to a cardinal direction.

    Args:
        string (str): The wind direction in degrees.

    Returns:
        str: The cardinal direction (e.g., 'North', 'East').
    """
    # Implementation details

    degree = int(string)
    if degree >= 0 and degree <= 89:
        return "North"
    elif degree >= 90 and degree <= 179:
        return "East"
    elif degree >= 180 and degree <= 269:
        return "South"
    else:
        return "West"

#Wind    
def WDV(string):

    """
    Parses wind speed and direction information.

    Args:
        string (str): Wind information string.

    Returns:
        str: Parsed wind information.
    """
    # Implementation details

    if len(string) == 6:
        degree = string[:2]
        speed = string[2:4]
        return "wind is blowing from " + str(degree) + " degrees (true) at a sustained speed of " + str(speed) + " knots"
    elif len(string) == 7:
        degree = string[:3]
        speed = string[3:5]
        return "wind is blowing from " + str(degree) + " degrees (true) at a sustained speed of " + str(speed) + " knots"
    elif len(string) == 9 and string[4] == "G":
        degree = string[:2]
        speed = string[2:4]
        gust = string[5:7]
        return "wind is blowing from " + str(degree) + " degrees (true) at a sustained speed of " + str(speed) + " knots  with " + str(gust) + "-knot gusts."
    elif len(string) == 10 and string[5] == "G":
        degree = string[:3]
        speed = string[3:5]
        gust = string[6:8]
        return "wind is blowing from " + str(degree) + " degrees (true) at a sustained speed of " + str(speed) + " knots  with " + str(gust) + "-knot gusts."
    elif len(string) == 10 and string[4] == "G":
        degree = string[:2]
        speed = string[2:4]
        gust = string[5:8]
        return "wind is blowing from " + str(degree) + " degrees (true) at a sustained speed of " + str(speed) + " knots  with " + str(gust) + "-knot gusts."
    elif len(string) == 11 and string[5] == "G":
        degree = string[:3]
        speed = string[3:5]
        gust = string[6:9]
        return "wind is blowing from " + str(degree) + " degrees (true) at a sustained speed of " + str(speed) + " knots  with " + str(gust) + "-knot gusts."


#Wind Variability
def WV(string):

    """
    Parses wind variability information.

    Args:
        string (str): Wind variability information string.

    Returns:
        str: Parsed wind variability information.
    """
    # Implementation details

    if (len(string) == 5 or len(string) == 6) and ord(string[2]) == 86:
        return "wind direction varying between " + string[:2] + " and " + string[3:]  
    elif (len(string) == 6 or len(string) == 7 ) and ord(string[3]) == 86:
        return "wind direction varying between " + string[:3] + " and " + string[4:]        

#Prevailing Visibility  
def PV(string):

    """
    Parses prevailing visibility information.

    Args:
        string (str): Prevailing visibility information string.

    Returns:
        str: Parsed prevailing visibility information.
    """
    # Implementation details

    if "SM" in string:
        res = string[:-2]
        return res + " statute mile"


#tempearture and Dewpoint
def TAD(string):

    """
    Parses temperature and dewpoint information.

    Args:
        string (str): Temperature and dewpoint information string.

    Returns:
        str: Parsed temperature and dewpoint information.
    """
    # Implementation details

    if (len(string) == 5 and ord(string[2]) == 47 and ord(string[0]) != 47 and ord(string[1]) != 47 and ord(string[3]) != 47 and ord(string[4]) != 47):
        return string[:2] + " is the temperature in degrees Celsius and " + string[3:] + " is the dewpoint in degrees Celsius."
    elif (len(string) == 6 and ord(string[0]) != 47 and ord(string[1]) != 47 and ord(string[2]) == 47 and ord(string[3]) == 77 and ord(string[4]) != 47 and ord(string[5]) != 47 ):
        return string[:2] + " is the temperature in degrees Celsius and " + string[3:] + " is the dewpoint in Minus degrees Celsius."
    elif (len(string) == 6 and (ord(string[3]) == 47 and ord(string[0]) == 77)):
        return string[:3] + " is the temperature in Minus degrees Celsius and " + string[4:] + " is the dewpoint in degrees Celsius."                     
    elif (len(string) == 7 and (ord(string[3]) == 47 and ord(string[0]) == 77 and ord(string[4]) == 77)):
        return string[:3] + " is the temperature in Minus degrees Celsius and " + string[4:] + " is the dewpoint in Minus degrees Celsius."

#Cloud Layers   
import ast

def Clouds(string):
    """
    Parses cloud layers information.

    Args:
        string (str): Cloud layers information string.

    Returns:
        str: Parsed cloud layers information.
    """
    ans = ''
    dig = ''

    if string[:3] == "SKC":
        ans = "Sky Clear "
    elif string[:3] == "FEW":
        ans = "Few Sky Coverage "
    elif string[:3] == "SCT":
        ans = "Scattered Sky Coverage "
    elif string[:3] == "BKN":
        ans = "Broken "
    elif string[:3] == "OVC":
        ans = "Overcast "

    if string[3:6]:
        dig = int(string[3:6])
        dig = str(dig) + "00"

    if string[6:8] == "CB":
        return ans + dig + " Feet AGL " + "Cumulonimbus"
    elif string[6:9] == "TCU":
        return ans + dig + " Feet AGL " + "towering cumulus"
    else:
        return ans + dig + " Feet AGL"
        


    


#Altimeter setting     
def AS(string):

    """
    Parses altimeter setting information.

    Args:
        string (str): Altimeter setting information string.

    Returns:
        str: Parsed altimeter setting information.
    """
    # Implementation details

    if ord(string[0]) == 65 and len(string) == 5:
        return "current altimeter setting of " + string[1:3] + "." + string[3:] + " inches Hg." 
    if ord(string[0]) == 81 and len(string) == 5:
        return "current altimeter setting of " + string[1:]+ " hPa or " + string[1:] + " mb."
    


#Get Weather Information
def get_weather_info(url):

    """
    Fetches and processes weather information.

    Args:
        station_code (str): The station code for weather data.
        nocache (bool): Set to True to bypass caching.

    Returns:
        dict: Processed weather information.
    """
    # Implementation details
    metar_data = fetch_metar_data(url)
    
    if metar_data:
        # Parse METAR data here
        data =  parse_metar_data(metar_data)
        return data
    return None

#Parse Weather Information
def parse_metar_data(metar_text):

    """
    Parses METAR weather data.

    Args:
        metar_text (str): Raw METAR weather data.

    Returns:
        dict: Parsed weather information.
    """
    # Implementation details

    raw_data = {}
    raw_data['Data'] = {}
    data = metar_text.split()
    Udata = data[4:]
    raw_data['Data']['station'] = data[2]  #station  
    raw_data['Data']['last observation'] = data[0] + " at " + data[1] + " GMT"  #last observation
    for i in data[3:]:
        if i in ["METAR","SPECI"]:
            if i == "METAR":
                raw_data['Data']["Report Type"] = "Aviation Routine Weather Report"             #Report Type
            else:
                raw_data['Data']["Report Type"] = "Special Report"                               #Report Type
        if "NOSIG" in i :
            raw_data['Data']['Remarks Overseas'] = "no significant changes"                    #Remarks Overseas
        if "KT" in i:
            raw_data['Data']['wind'] = WDV(i)
        if "AUTO" in i: 
            raw_data['Data']['observation type'] = "AUTO i.e Automatic"  #Observation Type
        if "AO1" in i:
            raw_data['Data']['observation remark'] = i + " i.e CANNOT distinguish between rain and snow."  #Observation Remark
        if "AO2" in i:
            raw_data['Data']['observation remark'] = i + " i.e CAN distinguish between rain and snow."       #Observation Remark
        if "SM" in i:
            raw_data['Data']['prevailing visibility'] = PV(i)
        if (len(i) == 5 or len(i) == 6 or len(i) == 7) and (ord(i[2]) == 86 or ord(i[3]) == 86) :
            raw_data['Data']['Wind Variability'] = WV(i)
        if (ord(i[0]) == 65 and len(i) == 5) or (ord(i[0]) == 81 and len(i) == 5):
            raw_data['Data']["altimeter setting"] = AS(i)
        if (len(i) == 5 and ord(i[2]) == 47) or (len(i) == 6 and (ord(i[2]) == 47 and ord(i[3]) == 77))or(len(i) == 6 and (ord(i[3]) == 47 and ord(i[0]) == 77))or(len(i) == 7 and (ord(i[3]) == 47 and ord(i[0]) == 77 and ord(i[4]) == 77)):
            raw_data['Data']["Temperature and Dewpoint"] = TAD(i)
        if i[:3] in ["SKC","FEW","SCT","BKN","OVC"]:
            if "Clouds Layers" not in raw_data['Data']:
                raw_data['Data']["Clouds Layers"] = []
                raw_data['Data']["Clouds Layers"].append(Clouds(i))
            else:
                raw_data['Data']["Clouds Layers"].append(Clouds(i))
    return raw_data


#Fetch Weather Information
def fetch_metar_data(url):

    """
    Fetches raw METAR weather data from a source.

    Args:
        station_code (str): The station code for weather data.

    Returns:
        str: Raw METAR weather data.
    """
    # Implementation details

    # METAR data source URL for the station code
    metar_url = url
    
    try:
        response = requests.get(metar_url)
        if response.status_code == 200:
            return response.text
    except Exception as e:
        print(f"Error fetching METAR data: {e}")
    
    return None


#Processessed Data
def get_weather():
    processed = []
    data_list = ETL()
    for data in data_list:
        url = data['Link']
        # Implementation details
        weather_data = get_weather_info(url)
        if weather_data:
            processed.append({ "Response": weather_data})
                
    headers = ["station", "last observation", "Report Type", 'Remarks Overseas', 'observation type',
               'observation remark', 'prevailing visibility', 'Wind Variability', "altimeter setting",
               "wind", "Clouds Layers", "Temperature and Dewpoint"]
    rows = []
    
    for item in processed:
        response_data = item.get("Response", {}).get("Data", {})
        row = [response_data.get(header, "") for header in headers]

        # Ensure that the lists are treated as lists
        rows.append(row)

    # Write to CSV file
    csv_file_path = "output.csv"
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"CSV file created: {csv_file_path}")


