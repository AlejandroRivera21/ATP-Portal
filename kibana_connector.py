import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class KibanaConnector:
    def __init__(self):
        self.kibana_url = os.getenv("KIBANA_URL")
        self.user = os.getenv("KIBANA_USER")
        self.password = os.getenv("KIBANA_PASSWORD")
        self.index = os.getenv("ES_INDEX")
        self.session = requests.Session()
        self.session.verify = False
        self.session.auth = (self.user, self.password)

    def ping(self):
        try:
            r = self.session.get(f"{self.kibana_url}/api/status", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def get_transacciones(self, operador, rango_inicio, rango_fin):
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"@timestamp": {"gte": rango_inicio, "lte": rango_fin}}},
                    ]
                }
            },
            "size": 500
        }

        if operador and operador != "Ambos":
            query["query"]["bool"]["filter"].append(
                {"term": {"operador.keyword": operador}}
            )

        url = f"{self.kibana_url}/elasticsearch/{self.index}/_search"
        r = self.session.post(url, json=query)
        hits = r.json().get("hits", {}).get("hits", [])
        data = [h["_source"] for h in hits]
        return pd.DataFrame(data)