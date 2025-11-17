import random
import time
import json
from kafka import KafkaProducer

# CONFIGURACIÓN

BOOTSTRAP_SERVERS = ['iot.redesuvg.cloud:9092']
TOPIC = '22500'  

TEMP_MEDIA = 25.0      
TEMP_DESV = 10.0       
HUM_MEDIA = 60.0       
HUM_DESV = 15.0        

DIRECCIONES_VIENTO = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']


def generar_temperatura():
    valor = random.gauss(TEMP_MEDIA, TEMP_DESV)
    valor = max(0.0, min(110.0, valor))   
    return round(valor, 2)


def generar_humedad():
    valor = random.gauss(HUM_MEDIA, HUM_DESV)
    valor = max(0.0, min(100.0, valor))
    return int(round(valor))


def generar_direccion_viento():
    return random.choice(DIRECCIONES_VIENTO)


def generar_medicion():
    return {
        "temperatura": generar_temperatura(),
        "humedad": generar_humedad(),
        "direccion_viento": generar_direccion_viento()
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k
    )

    print(f"Enviando datos al topic '{TOPIC}' en {BOOTSTRAP_SERVERS}...")
    print("Presiona Ctrl + C para detener.\n")

    try:
        while True:
            medicion = generar_medicion()
            print(f"Enviando: {medicion}")

            producer.send(
                TOPIC,
                key="sensor1",
                value=medicion
            )

            producer.flush()

            espera = random.randint(15, 30)
            print(f"Siguiente medición en {espera} segundos...\n")
            time.sleep(espera)

    except KeyboardInterrupt:
        print("Producer detenido por el usuario.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
