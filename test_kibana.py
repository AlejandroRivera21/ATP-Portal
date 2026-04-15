from kibana_connector import KibanaConnector

def main():
    conn = KibanaConnector()

    df = conn.get_transacciones(
        operador="Ambos",
        rango_inicio="now-7d",
        rango_fin="now"
    )

    print("Registros:", len(df))
    print(df.head())

if __name__ == "__main__":
    main()