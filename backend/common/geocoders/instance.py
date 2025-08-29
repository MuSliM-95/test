from common.geocoders.impl.geoapify import Geoapify
from dotenv import load_dotenv
import os

load_dotenv()

geocoder = Geoapify(api_key=os.getenv("GEOAPIFY_SECRET"))
