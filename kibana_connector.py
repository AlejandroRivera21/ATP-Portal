"""
kibana_connector.py
Conector oficial Kibana / Elasticsearch (Kibana 7.17 compatible)
ATP Fiber Colombia S.A.S — NOC Portal
"""

import os
import requests
import pandas as pd
import urllib3
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# Configuración inicial
# ─────────────────────────────────────────────────────────────

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

KIBANA_URL = os.getenv("KIBANA_URL")              # https://kibana-atp.tecnotree.com:8445
KIBANA_USER = os.getenv("KIBANA_USER")
KIBANA_PASSWORD = os.getenv("KIBANA_PASSWORD")
ES_INDEX = os.getenv("ES_INDEX", "http-rest-service-*")

TIMEOUT = 20

FIELDS = [
    "@timestamp",
    "request.Request.description",
    "request.Request.relatedParty.name",
    "response.response.finalResponse.response.code",
    "response.response.finalResponse.response.reason",
]

HEADERS = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json",
}

# ─────────────────────────────────────────────────────────────
# Conector
# ─────────────────────────────────────────────────────────────

class KibanaConnector:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.session.auth = (KIBANA_USER, KIBANA_PASSWORD)
        self.session.headers.update(HEADERS)

    def get_transacciones(
        self,
        operador="Ambos",
        rango_inicio="now-7d",
        rango_fin="now",
        size=10000,
    ) -> pd.DataFrame:
        """
        Usa el endpoint interno oficial de Kibana 7.17:
        /internal/search/es
        """

        url = f"{KIBANA_URL}/internal/search/es"

        # Query ES real
        es_query = {
            "size": size,
            "_source": FIELDS,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": rango_inicio,
                                    "lte": rango_fin,
                                }
                            }
                        }
                    ]
                }
            },
        }

        if operador != "Ambos":
            es_query["query"]["bool"]["filter"].append(
                {
                    "term": {
                        "request.Request.relatedParty.name.keyword": operador
                    }
                }
            )

        # Body que espera Kibana internamente
        payload = {
            "params": {
                "index": ES_INDEX,
                "body": es_query,
            }
        }

        r = self.session.post(url, json=payload, timeout=TIMEOUT)

        if "application/json" not in r.headers.get("Content-Type", ""):
            raise RuntimeError(
                f"Kibana devolvió respuesta no JSON "
                f"(status={r.status_code}). "
                f"Primeros 200 chars:\n{r.text[:200]}"
            )

        data = r.json()
        hits = data.get("rawResponse", {}).get("hits", {}).get("hits", [])

        if not hits:
            return pd.DataFrame()

        df = pd.json_normalize([h["_source"] for h in hits])

        return self._normalizar(df)

    def _normalizar(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            "@timestamp": "timestamp",
            "request.Request.description": "requestrequestdescription",
            "request.Request.relatedParty.name": "requestrequestrelatedpartyname",
            "response.response.finalResponse.response.code": "responseresponsefinalresponseresponsecode",
            "response.response.finalResponse.response.reason": "responseresponsefinalresponseresponsereason",
        }

        df = df.rename(columns=rename_map)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        return df
