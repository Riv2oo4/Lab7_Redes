import time
from kafka import KafkaProducer
import random


from util_estacion import (
  BOOTSTRAP_SERVERS, TOPIC,
  generar_medicion, encode_medicion_3bytes
)

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v,  # ya mandamos bytes
        key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k
    )

    print(f"Enviando datos *codificados en 3 bytes* al topic '{TOPIC}'...")
    print("Presiona Ctrl + C para detener.\n")

    try:
        while True:
            medicion = generar_medicion()
            payload = encode_medicion_3bytes(medicion)

            print(f"Medición original: {medicion}")
            print(f"Payload (hex): {payload.hex()}\n")

            producer.send(
                TOPIC,
                key="sensor1",
                value=payload
            )
            producer.flush()

            espera = random.randint(15, 30)
            print(f"Siguiente medición en {espera} segundos...\n")
            time.sleep(espera)

    except KeyboardInterrupt:
        print("Producer 3.4 detenido por el usuario.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
