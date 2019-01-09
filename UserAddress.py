# -*- coding: utf-8 -*-
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from bs4 import BeautifulSoup
import urllib2
import re

URL_INIT = 'https://twitter.com/'

def getUserAddress(user):
    
    def parse_url(tweet_user):
        url = URL_INIT+ tweet_user.strip('@')
        return url
    
    try:
        url = parse_url(user)
        response = urllib2.urlopen(url)
    except:
        return None
    
    geolocator = Nominatim()
    html = response.read()
    soup = BeautifulSoup(html)
    location = soup.find('span','ProfileHeaderCard-locationText').text.encode('utf8').strip('\n').strip()
    if location:
        if ',' in location:
            splitted_location = location.split(',')
        else:
            splitted_location = re.split('|;|-|/|°|#', location)
        try:
            if splitted_location:
                located_location = geolocator.geocode(splitted_location[0], timeout=100)
            else:
                located_location = geolocator.geocode(location, timeout=100)
            if located_location:
                return located_location
            else:
                return location
        except GeocoderTimedOut as e:
            print("Error: geocode failed on input %s with message %s"%(location, e))