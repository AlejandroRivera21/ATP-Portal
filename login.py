from elasticsearch import Elasticsearch
import os

print("👉 login.py se está ejecutando")

def conectar_elasticsearch():
    """
    Crea una conexión básica a Elasticsearch
    """
    url = os.getenv("ES_URL", "http://localhost:9200")
    print(f"Conectando a Elasticsearch en {url}")

    es = Elasticsearch(
        url,
        verify_certs=False
    )
    return es


def prueba_conexion():
    """
    Prueba simple de conexión
    """
    es = conectar_elasticsearch()

    try:
        if es.ping():
            print("✅ Conexión exitosa a Elasticsearch")
        else:
            print("❌ No se pudo conectar a Elasticsearch (ping false)")
    except Exception as e:
        print("❌ Error al intentar conectar:")
        print(e)


def consulta_basica():
    """
    Ejecuta una búsqueda básica en Elasticsearch
    """
    es = conectar_elasticsearch()

    query = {
        "size": 5,
        "query": {
            "match_all": {}
        }
    }

    try:
        response = es.search(
            index="*",
            body=query
        )

        print("✅ Consulta ejecutada correctamente")
        print("Total de documentos encontrados:", response["hits"]["total"])

        for hit in response["hits"]["hits"]:
            print(hit["_source"])

    except Exception as e:
        print("❌ Error al ejecutar la consulta:")
        print(e)


if __name__ == "__main__":
    prueba_conexion()
    consulta_basica()
