import requests

class OpenSky:
    def __init__(self):
        self.open_sky_api = "https://opensky-network.org/api/states/all"
        self.data = {}
