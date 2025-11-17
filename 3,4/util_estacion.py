import random

# ==== CONFIGURACIÓN GENERAL ====

BOOTSTRAP_SERVERS = ['iot.redesuvg.cloud:9092']
TOPIC = '22500'

TEMP_MEDIA = 25.0
TEMP_DESV = 10.0
HUM_MEDIA = 60.0
HUM_DESV = 15.0

DIRECCIONES_VIENTO = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

WIND_TO_INT = {
    'N': 0,
    'NO': 1,
    'O': 2,
    'SO': 3,
    'S': 4,
    'SE': 5,
    'E': 6,
    'NE': 7
}

INT_TO_WIND = {v: k for k, v in WIND_TO_INT.items()}


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

def encode_medicion_3bytes(medicion: dict) -> bytes:
    temp = medicion["temperatura"]
    hum = medicion["humedad"]
    wind = medicion["direccion_viento"]

    temp_int = int(round(temp * 100))
    hum_int = int(hum)
    wind_int = WIND_TO_INT[wind]

    temp_int = max(0, min(11000, temp_int))
    hum_int = max(0, min(100, hum_int))
    wind_int = max(0, min(7, wind_int))

    packed = (temp_int << 10) | (hum_int << 3) | wind_int

    return packed.to_bytes(3, byteorder='big')


def decode_medicion_3bytes(payload: bytes) -> dict:
    """
    Toma 3 bytes y regresa el dict:
    {
        "temperatura": float,
        "humedad": int,
        "direccion_viento": str
    }
    """
    packed = int.from_bytes(payload, byteorder='big')

    wind_int = packed & 0b111
    hum_int = (packed >> 3) & 0b1111111
    temp_int = packed >> 10

    temp = temp_int / 100.0
    wind = INT_TO_WIND[wind_int]

    return {
        "temperatura": temp,
        "humedad": hum_int,
        "direccion_viento": wind
    }
