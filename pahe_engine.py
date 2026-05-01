import requests
import random
import string
import logging
from datetime import datetime
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("PaheList")

class PaheEngine:
    def __init__(self):
        self.base_url = "https://animepahe.pw"
        self.api_url = f"{self.base_url}/api"
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": self.base_url
        }
        self._cookie_domain = urlparse(self.base_url).hostname
        self.update_cookie()

    def update_cookie(self):
        cookie_val = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        # Cookie domain must match the domain we're actually requesting
        self.session.cookies.set("__ddg2_", cookie_val, domain=self._cookie_domain)

    def get_latest_releases(self):
        try:
            # Refresh the DDoS-Guard cookie before each request
            self.update_cookie()

            res = self.session.get(f"{self.api_url}?m=airing", headers=self.headers, timeout=15)

            if res.status_code != 200:
                logger.error(f"API returned HTTP {res.status_code}")
                return []

            data = res.json()
            if "data" not in data:
                logger.error(f"Unexpected API response: {list(data.keys())}")
                return []

            results = []
            for item in data["data"]:
                raw_ep = float(item.get('episode', 0))
                ep = int(raw_ep) if raw_ep.is_integer() else raw_ep

                created_at = item.get('created_at', '')
                try:
                    dt_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                    time_str = dt_obj.strftime("%I:%M %p")
                except Exception:
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
