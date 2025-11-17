from kafka import KafkaConsumer
import matplotlib.pyplot as plt

from util_estacion import (
  BOOTSTRAP_SERVERS, TOPIC,
  decode_medicion_3bytes
)

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: v,
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        group_id='grupo_estacion_22500_payload3bytes',
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    temps = []
    hums = []

    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 6))
    fig.suptitle('Telemetría – Payload de 3 bytes')

    try:
        print(f"Escuchando datos codificados en 3 bytes del topic '{TOPIC}'...")
        for i, msg in enumerate(consumer, start=1):
            medicion = decode_medicion_3bytes(msg.value)

            temp = medicion["temperatura"]
            hum = medicion["humedad"]

            print(f"[{i}] Medición decodificada: {medicion}")

            temps.append(temp)
            hums.append(hum)

            max_puntos = 50
            temps_plot = temps[-max_puntos:]
            hums_plot = hums[-max_puntos:]

            ax1.clear()
            ax1.plot(range(len(temps_plot)), temps_plot, marker='o')
            ax1.set_ylabel('Temperatura (°C)')
            ax1.grid(True)

            ax2.clear()
            ax2.plot(range(len(hums_plot)), hums_plot, marker='s')
            ax2.set_ylabel('Humedad (%)')
            ax2.set_xlabel('Muestras')
            ax2.grid(True)

            plt.tight_layout()
            plt.pause(0.01)

    except KeyboardInterrupt:
        print("Consumer 3.4 detenido por el usuario.")
    finally:
        consumer.close()
        plt.ioff()


if __name__ == "__main__":
    main()
