import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

BOOTSTRAP_SERVERS = ['iot.redesuvg.cloud:9092']
TOPIC = '22500'

DIRECCIONES_VIENTO = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

def main():
    consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    group_id='grupo_estacion_22500',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    security_protocol='PLAINTEXT',
    request_timeout_ms=15000,
    session_timeout_ms=10000
)


    temps = []
    hums = []
    winds = []

    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 6))
    fig.suptitle('Telemetría – Estación Meteorológica')

    try:
        print(f"Escuchando datos del topic '{TOPIC}'...")
        for msg in consumer:
            data = msg.value
            temp = data["temperatura"]
            hum = data["humedad"]
            wind = data["direccion_viento"]

            temps.append(temp)
            hums.append(hum)
            winds.append(wind)

            max_puntos = 50
            temps_plot = temps[-max_puntos:]
            hums_plot = hums[-max_puntos:]

            ax1.clear()
            ax1.plot(temps_plot, marker='o')
            ax1.set_ylabel('Temperatura (°C)')
            ax1.grid(True)

            ax2.clear()
            ax2.plot(hums_plot, marker='s')
            ax2.set_ylabel('Humedad (%)')
            ax2.set_xlabel('Muestras')
            ax2.grid(True)

            plt.tight_layout()
            plt.pause(0.01)

    except KeyboardInterrupt:
        print("Consumer detenido por el usuario.")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()

if __name__ == "__main__":
    main()
