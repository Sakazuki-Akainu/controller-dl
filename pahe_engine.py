import requests
import random
import string
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("PaheList")

class PaheEngine:
    def __init__(self):
        self.base_url = "https://animepahe.si"
        self.api_url = f"{self.base_url}/api"
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": self.base_url
        }
        self.update_cookie()

    def update_cookie(self):
        cookie_val = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        self.session.cookies.set("__ddg2_", cookie_val, domain="animepahe.si")

    def get_latest_releases(self):
        try:
            res = self.session.get(f"{self.api_url}?m=airing", headers=self.headers, timeout=10)
            
            if res.status_code != 200:
                return []

            data = res.json()
            if "data" not in data:
                return []

            results = []
            for item in data["data"]:
                # 1. Clean Episode Number
                raw_ep = float(item.get('episode', 0))
                ep = int(raw_ep) if raw_ep.is_integer() else raw_ep

                # 2. Format Time
                created_at = item.get('created_at', '')
                try:
                    dt_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                    time_str = dt_obj.strftime("%I:%M %p") 
                except:
                    time_str = created_at

                results.append({
                    "title": item.get('anime_title'),
                    "ep": ep,
                    "time": time_str,
                    "session": item.get('session')
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error: {e}")
            return []
