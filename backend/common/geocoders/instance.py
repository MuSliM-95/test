import os

from dotenv import load_dotenv

from common.geocoders.impl.geoapify import Geoapify

load_dotenv()

geocoder = Geoapify(api_key=os.getenv("GEOAPIFY_SECRET"))
