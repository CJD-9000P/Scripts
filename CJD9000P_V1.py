#!/usr/bin/env python3

import json
import csv
import os
import sys
import time
import threading
from datetime import datetime

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusSerialClient


# ============================================================
# CJD-9000P -> MQTT
# SOLO LECTURA DEL HORNO / SIN ESCRITURAS MODBUS
# ============================================================


# ============================================================
# CONFIGURACION MODBUS
# ============================================================

PUERTO = "/dev/ttyUSB0"

MODBUS_ID = 1
BAUDRATE = 9600
BYTESIZE = 8
PARITY = "N"
STOPBITS = 2

MODBUS_TIMEOUT = 2
MODBUS_RETRIES = 1

# Cada cuanto se intenta recuperar una conexion Modbus perdida.
MODBUS_RECONNECT_INTERVAL = 5.0

# Numero de lecturas consecutivas fallidas antes de declarar
# el horno como NO DISPONIBLE.
FALLOS_ANTES_OFFLINE = 3

# PV y SV se leen cada segundo.
INTERVALO_LECTURA = 1.0

# Segmento, estado y tiempo se leen cada 30 segundos.
INTERVALO_ESTADO = 30.0

# Se conserva la misma escala del lector.py.
# No se modifica hasta comprobar el significado de dP.
ESCALA_TEMPERATURA = 1.0


# ============================================================
# REGISTROS CJD-9000P
# ============================================================

REG_SPC = 0xA0
REG_CYC = 0xA1
REG_DP = 0xA3

REG_ADDR = 0xBF
REG_BAUD = 0xC0

REG_STEP = 0xC9
REG_ESTADO = 0xCA
REG_TIEMPO = 0xCC
REG_PV = 0xCD
REG_SV = 0xCE


# ============================================================
# CURVAS
# ============================================================

INICIO_CURVAS = {
    1: 0x00,
    2: 0x28,
    3: 0x50,
    4: 0x78,
}


# ============================================================
# CONFIGURACION MQTT
# ============================================================

MQTT_BROKER = "192.168.1.107"
MQTT_PORT = 1883

MQTT_USER = "horno_mqtt"
MQTT_PASSWORD = "prueba"

# EXACTAMENTE EL MISMO TOPIC QUE UTILIZA EL SIMULADOR.
MQTT_TOPIC = "horno/cjd9000p/state"

# Topic adicional para saber si el horno esta disponible.
# No cambia las entidades existentes: se utiliza internamente
# por Home Assistant mediante MQTT Discovery.
MQTT_AVAILABILITY_TOPIC = "horno/cjd9000p/availability"

# Estado de la conexion MQTT del propio programa.
MQTT_CONNECTION_TOPIC = "horno/cjd9000p/mqtt/availability"

DISCOVERY_PREFIX = "homeassistant"

DEVICE_ID = "cjd9000p"
DEVICE_NAME = "CJD-9000P"
MANUFACTURER = "CJD"
MODEL = "CJD-9000P"


# ============================================================
# VARIABLES DE ESTADO
# ============================================================

mqtt_client = None

estado_horno_disponible = False
estado_horno_lock = threading.Lock()

ultimo_estado_mqtt = None

detener = False


# ============================================================
# FUNCIONES GENERALES
# ============================================================

def a_signed_16(valor):
    """Convierte un registro Modbus de 16 bits a entero con signo."""

    if valor >= 0x8000:
        return valor - 0x10000

    return valor


def texto_estado(valor):
    if valor == 0:
        return "PARADO"

    if valor == 1:
        return "FUNCIONANDO"

    if valor == 2:
        return "PAUSADO"

    return f"DESCONOCIDO ({valor})"


# ============================================================
# MODBUS
# ============================================================

client_modbus = ModbusSerialClient(
    port=PUERTO,
    baudrate=BAUDRATE,
    bytesize=BYTESIZE,
    parity=PARITY,
    stopbits=STOPBITS,
    timeout=MODBUS_TIMEOUT,
    retries=MODBUS_RETRIES
)


def leer_registro(direccion):
    """
    Lee un registro mediante Modbus 03H.
    Esta funcion SOLO lee. No existe ninguna escritura Modbus
    en este programa.
    """

    respuesta = client_modbus.read_holding_registers(
        address=direccion,
        count=1,
        slave=MODBUS_ID
    )

    if respuesta.isError():
        raise RuntimeError(
            f"Error Modbus leyendo 0x{direccion:02X}: {respuesta}"
        )

    if not hasattr(respuesta, "registers"):
        raise RuntimeError(
            f"Respuesta Modbus sin registros para 0x{direccion:02X}"
        )

    if len(respuesta.registers) < 1:
        raise RuntimeError(
            f"Respuesta vacia leyendo 0x{direccion:02X}"
        )

    return a_signed_16(respuesta.registers[0])


def leer_bloque(direccion, cantidad):
    """Lee varios registros consecutivos mediante Modbus 03H."""

    respuesta = client_modbus.read_holding_registers(
        address=direccion,
        count=cantidad,
        slave=MODBUS_ID
    )

    if respuesta.isError():
        raise RuntimeError(
            f"Error Modbus desde 0x{direccion:02X}: {respuesta}"
        )

    if not hasattr(respuesta, "registers"):
        raise RuntimeError(
            f"Respuesta Modbus sin registros desde 0x{direccion:02X}"
        )

    if len(respuesta.registers) < cantidad:
        raise RuntimeError(
            f"Respuesta incompleta desde 0x{direccion:02X}: "
            f"recibidos {len(respuesta.registers)}, "
            f"esperados {cantidad}"
        )

    return [
        a_signed_16(x)
        for x in respuesta.registers
    ]


def cerrar_modbus():
    try:
        client_modbus.close()
    except Exception:
        pass


def conectar_modbus():
    """
    Abre/reabre el puerto serie.
    El hecho de que el puerto serie abra no garantiza que el horno
    responda: eso se comprueba posteriormente leyendo registros.
    """

    try:
        cerrar_modbus()

        if client_modbus.connect():
            print(
                f"[{datetime.now():%H:%M:%S}] "
                "Puerto serie Modbus abierto."
            )
            return True

        print(
            f"[{datetime.now():%H:%M:%S}] "
            "No se puede abrir el puerto Modbus."
        )
        return False

    except Exception as e:
        print(
            f"[{datetime.now():%H:%M:%S}] "
            f"Error abriendo Modbus: {e}"
        )
        return False


def leer_configuracion():
    """
    Lee la misma configuracion que lector.py:
    SPC, CYC y dP.
    """

    return {
        "SPC": leer_registro(REG_SPC),
        "CYC": leer_registro(REG_CYC),
        "dP": leer_registro(REG_DP),
    }


def determinar_curva(spc):
    if 1 <= spc <= 4:
        return spc

    print(
        f"SPC={spc}. No corresponde directamente a una curva 1-4."
    )
    return None


def leer_curva(numero_curva):
    """Lee los 20 segmentos de la curva seleccionada."""

    inicio = INICIO_CURVAS[numero_curva]

    valores = leer_bloque(
        inicio,
        40
    )

    segmentos = []

    for n in range(20):

        segmentos.append(
            {
                "segmento": n + 1,
                "C": valores[n * 2],
                "t": valores[n * 2 + 1]
            }
        )

    return segmentos


def mostrar_configuracion(configuracion, curva, segmentos):
    print()
    print("=" * 60)
    print("CONFIGURACION DEL CJD-9000P")
    print("=" * 60)
    print()

    print(f"SPC - seleccion de curva : {configuracion['SPC']}")
    print(f"CYC - ciclos             : {configuracion['CYC']}")
    print(f"dP  - decimal place      : {configuracion['dP']}")

    if curva is not None:
        print(f"Curva utilizada          : {curva}")

    if segmentos is not None:
        print()
        print(" Segmento             C              t")
        print("--------------------------------------------")

        for s in segmentos:
            print(
                f"   {s['segmento']:02d}"
                f"               {s['C']:8d}"
                f"        {s['t']:8d}"
            )

    print()


# ============================================================
# MQTT - ESTADO DEL HORNO
# ============================================================

def horno_disponible():
    with estado_horno_lock:
        return estado_horno_disponible


def publicar_availability(valor):
    """
    Publica ONLINE/OFFLINE retenido.
    Si MQTT esta caido, simplemente no se publica.
    Esto es intencionado: la lectura del horno no depende de MQTT.
    """

    global ultimo_estado_mqtt

    if mqtt_client is None:
        return

    if not mqtt_client.is_connected():
        return

    if valor == ultimo_estado_mqtt:
        return

    resultado = mqtt_client.publish(
        MQTT_AVAILABILITY_TOPIC,
        valor,
        qos=1,
        retain=True
    )

    if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
        ultimo_estado_mqtt = valor
        print(
            f"[{datetime.now():%H:%M:%S}] "
            f"Disponibilidad horno -> {valor}"
        )
    else:
        print(
            f"[{datetime.now():%H:%M:%S}] "
            f"Error publicando disponibilidad: {resultado.rc}"
        )


def marcar_horno_online():
    global estado_horno_disponible

    cambio = False

    with estado_horno_lock:
        if not estado_horno_disponible:
            estado_horno_disponible = True
            cambio = True

    if cambio:
        publicar_availability("online")


def marcar_horno_offline():
    global estado_horno_disponible

    cambio = False

    with estado_horno_lock:
        if estado_horno_disponible:
            estado_horno_disponible = False
            cambio = True

    if cambio:
        publicar_availability("offline")


# ============================================================
# MQTT DISCOVERY
# ============================================================

device = {
    "identifiers": [
        DEVICE_ID
    ],
    "name": DEVICE_NAME,
    "manufacturer": MANUFACTURER,
    "model": MODEL
}


def publicar_discovery(componente, objeto_id, configuracion):

    if not mqtt_client.is_connected():
        return

    topic = (
        f"{DISCOVERY_PREFIX}/"
        f"{componente}/"
        f"{DEVICE_ID}/"
        f"{objeto_id}/config"
    )

    resultado = mqtt_client.publish(
        topic,
        json.dumps(configuracion),
        qos=1,
        retain=True
    )

    if resultado.rc != mqtt.MQTT_ERR_SUCCESS:
        print(
            f"Error publicando Discovery {objeto_id}: "
            f"{resultado.rc}"
        )


def publicar_todo_discovery():

    # Se mantienen exactamente los mismos IDs, nombres, state_topic
    # y value_template del simulador.

    publicar_discovery(
        "sensor",
        "temperatura",
        {
            "name": "Temperatura",
            "unique_id": f"{DEVICE_ID}_temperatura",
            "state_topic": MQTT_TOPIC,
            "availability": {
                "topic": MQTT_AVAILABILITY_TOPIC,
                "payload_available": "online",
                "payload_not_available": "offline"
            },
            "value_template": "{{ value_json.pv }}",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
            "state_class": "measurement",
            "device": device
        }
    )

    publicar_discovery(
        "sensor",
        "consigna",
        {
            "name": "Consigna",
            "unique_id": f"{DEVICE_ID}_consigna",
            "state_topic": MQTT_TOPIC,
            "availability": {
                "topic": MQTT_AVAILABILITY_TOPIC,
                "payload_available": "online",
                "payload_not_available": "offline"
            },
            "value_template": "{{ value_json.sv }}",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
            "state_class": "measurement",
            "device": device
        }
    )

    publicar_discovery(
        "sensor",
        "segmento",
        {
            "name": "Segmento",
            "unique_id": f"{DEVICE_ID}_segmento",
            "state_topic": MQTT_TOPIC,
            "availability": {
                "topic": MQTT_AVAILABILITY_TOPIC,
                "payload_available": "online",
                "payload_not_available": "offline"
            },
            "value_template": "{{ value_json.segmento }}",
            "state_class": "measurement",
            "device": device
        }
    )

    publicar_discovery(
        "sensor",
        "tiempo_cjd",
        {
            "name": "Tiempo CJD",
            "unique_id": f"{DEVICE_ID}_tiempo_cjd",
            "state_topic": MQTT_TOPIC,
            "availability": {
                "topic": MQTT_AVAILABILITY_TOPIC,
                "payload_available": "online",
                "payload_not_available": "offline"
            },
            "value_template": "{{ value_json.tiempo_cjd }}",
            "unit_of_measurement": "s",
            "state_class": "measurement",
            "device": device
        }
    )

    # --------------------------------------------------------
    # DISPONIBILIDAD REAL DEL HORNO
    #
    # Esta SI es una entidad visible en Home Assistant.
    # OFF = la Raspberry no consigue leer el CJD.
    # ON  = el CJD responde correctamente.
    # --------------------------------------------------------
    publicar_discovery(
        "binary_sensor",
        "horno_disponible",
        {
            "name": "Horno disponible",
            "unique_id": f"{DEVICE_ID}_horno_disponible",
            "state_topic": MQTT_AVAILABILITY_TOPIC,
            "payload_on": "online",
            "payload_off": "offline",
            "device_class": "connectivity",
            "device": device
        }
    )

    # --------------------------------------------------------
    # ESTADO DE LA CONEXION MQTT DEL PROGRAMA
    # --------------------------------------------------------
    publicar_discovery(
        "binary_sensor",
        "mqtt_conectado",
        {
            "name": "MQTT conectado",
            "unique_id": f"{DEVICE_ID}_mqtt_conectado",
            "state_topic": MQTT_CONNECTION_TOPIC,
            "payload_on": "online",
            "payload_off": "offline",
            "device_class": "connectivity",
            "device": device
        }
    )

    publicar_discovery(
        "binary_sensor",
        "funcionando",
        {
            "name": "Funcionando",
            "unique_id": f"{DEVICE_ID}_funcionando",
            "state_topic": MQTT_TOPIC,
            "availability": {
                "topic": MQTT_AVAILABILITY_TOPIC,
                "payload_available": "online",
                "payload_not_available": "offline"
            },
            "value_template": (
                "{% if value_json.estado == 1 %}"
                "ON"
                "{% else %}"
                "OFF"
                "{% endif %}"
            ),
            "payload_on": "ON",
            "payload_off": "OFF",
            "device_class": "running",
            "device": device
        }
    )


# ============================================================
# CALLBACKS MQTT
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):

    print()
    print(
        f"[{datetime.now():%H:%M:%S}] "
        f"MQTT conectado. reason_code={reason_code}"
    )

    if reason_code == 0:

        # Estado de la propia conexion MQTT.
        client.publish(
            MQTT_CONNECTION_TOPIC,
            "online",
            qos=1,
            retain=True
        )

        # Cada reconexion vuelve a publicar Discovery.
        # Como es retain=True, Home Assistant siempre puede recuperar
        # la configuracion.
        publicar_todo_discovery()

        # Reflejar inmediatamente el estado conocido del horno.
        if horno_disponible():
            publicar_availability("online")
        else:
            publicar_availability("offline")


def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties
):

    print(
        f"[{datetime.now():%H:%M:%S}] "
        f"MQTT desconectado. reason_code={reason_code}"
    )

    # NO se detiene el programa.
    # Paho intentara recuperar la conexion mientras el loop este activo.


# ============================================================
# CREAR CLIENTE MQTT
# ============================================================

mqtt_client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)

mqtt_client.username_pw_set(
    MQTT_USER,
    MQTT_PASSWORD
)

mqtt_client.reconnect_delay_set(
    min_delay=2,
    max_delay=30
)

# Si el proceso muere de forma inesperada y el broker detecta
# la desconexion, Home Assistant recibira "offline".
mqtt_client.will_set(
    MQTT_AVAILABILITY_TOPIC,
    payload="offline",
    qos=1,
    retain=True
)

# LWT de la propia conexion MQTT.
# Si este proceso muere inesperadamente, el broker publica offline.
# Esto NO depende de que el programa llegue a ejecutar finally.
# Nota: el LWT es compartido por el cliente MQTT y se establece
# antes de conectar.

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect


# ============================================================
# MQTT - PUBLICAR DATOS
# ============================================================

def publicar_datos(datos):

    if not mqtt_client.is_connected():
        return False

    mensaje = json.dumps(
        datos,
        separators=(",", ":")
    )

    # QoS 0 y retain=False igual que el simulador.
    # Si MQTT esta caido, NO se acumulan datos para enviarlos despues.
    resultado = mqtt_client.publish(
        MQTT_TOPIC,
        mensaje,
        qos=0,
        retain=False
    )

    if resultado.rc != mqtt.MQTT_ERR_SUCCESS:
        print(
            f"[{datetime.now():%H:%M:%S}] "
            f"Error publicando MQTT: {resultado.rc}"
        )
        return False

    return True


# ============================================================
# CSV LOCAL
# ============================================================

def crear_archivo_csv(numero_curva):

    os.makedirs(
        "cocciones",
        exist_ok=True
    )

    fecha = datetime.now().strftime(
        "%Y-%m-%d_%H-%M-%S"
    )

    nombre = (
        f"cocciones/"
        f"{fecha}_curva_{numero_curva}.csv"
    )

    archivo = open(
        nombre,
        "w",
        newline="",
        encoding="utf-8"
    )

    escritor = csv.writer(
        archivo,
        delimiter=";"
    )

    escritor.writerow(
        [
            "fecha",
            "hora",
            "PV",
            "SV",
            "segmento",
            "estado",
            "estado_texto",
            "tiempo_CJD"
        ]
    )

    archivo.flush()

    return archivo, escritor, nombre


# ============================================================
# LECTURA DEL HORNO
# ============================================================

def leer_estado_horno():

    # PV + SV en una sola consulta consecutiva.
    valores = leer_bloque(
        REG_PV,
        2
    )

    pv = valores[0] * ESCALA_TEMPERATURA
    sv = valores[1] * ESCALA_TEMPERATURA

    return pv, sv


def leer_estado_programa():

    # C9 + CA en una consulta.
    valores = leer_bloque(
        REG_STEP,
        2
    )

    segmento = valores[0]
    estado = valores[1]

    tiempo = leer_registro(
        REG_TIEMPO
    )

    return segmento, estado, tiempo


# ============================================================
# PROGRAMA PRINCIPAL
# ============================================================

archivo = None
escritor = None

print()
print("=" * 60)
print("        CJD-9000P -> MQTT / HOME ASSISTANT")
print("=" * 60)
print()

print(f"Puerto Modbus : {PUERTO}")
print(f"Direccion     : {MODBUS_ID}")
print(f"Baudrate      : {BAUDRATE}")
print(f"Formato       : {BYTESIZE}{PARITY}{STOPBITS}")
print(f"MQTT broker   : {MQTT_BROKER}:{MQTT_PORT}")
print(f"MQTT topic    : {MQTT_TOPIC}")
print()
print("MODO MODBUS: SOLO LECTURA")
print("Entidades adicionales: Horno disponible / MQTT conectado")
print("NO se utiliza ninguna funcion Modbus de escritura.")
print()


# ============================================================
# INICIAR MQTT SIN HACERLO DEPENDIENTE DEL HORNO
# ============================================================

print("Iniciando cliente MQTT...")

try:

    # connect_async no bloquea ni hace que el horno dependa de MQTT.
    mqtt_client.connect_async(
        MQTT_BROKER,
        MQTT_PORT,
        60
    )

    mqtt_client.loop_start()

except Exception as e:

    # No detenemos el programa por MQTT.
    print(
        f"Advertencia iniciando MQTT: {e}"
    )


# ============================================================
# CONEXION INICIAL AL HORNO
# ============================================================

print()
print("Conectando con el CJD-9000P...")

ultimo_intento_modbus = 0
modbus_ok = False
configuracion = None
curva = None
segmentos = None

# Hasta conseguir una respuesta real del CJD no comenzamos
# el registro de datos.
while not detener:

    ahora_monotonic = time.monotonic()

    if not modbus_ok:

        if (
            ahora_monotonic - ultimo_intento_modbus
            >= MODBUS_RECONNECT_INTERVAL
        ):

            ultimo_intento_modbus = ahora_monotonic

            if conectar_modbus():

                try:

                    configuracion = leer_configuracion()

                    curva = determinar_curva(
                        configuracion["SPC"]
                    )

                    if curva is not None:

                        segmentos = leer_curva(
                            curva
                        )

                    mostrar_configuracion(
                        configuracion,
                        curva,
                        segmentos
                    )

                    modbus_ok = True
                    marcar_horno_online()

                    print(
                        f"[{datetime.now():%H:%M:%S}] "
                        "Comunicacion con el horno OK."
                    )

                except Exception as e:

                    print(
                        f"[{datetime.now():%H:%M:%S}] "
                        f"El CJD no responde correctamente: {e}"
                    )

                    cerrar_modbus()
                    modbus_ok = False

    if modbus_ok:
        break

    time.sleep(0.2)


# ============================================================
# CREAR CSV
# ============================================================

if curva is None:
    # Si SPC no permite determinar la curva, usamos 0 en el nombre
    # pero NO inventamos una curva.
    curva_para_archivo = 0
else:
    curva_para_archivo = curva

archivo, escritor, nombre_csv = crear_archivo_csv(
    curva_para_archivo
)

print()
print("=" * 60)
print("MONITORIZACION INICIADA")
print("=" * 60)
print()
print(f"CSV local: {nombre_csv}")
print()
print("Ctrl+C para terminar.")
print()


# ============================================================
# VARIABLES DE FUNCIONAMIENTO
# ============================================================

fallos_consecutivos = 0

segmento_actual = None
estado_actual = None
tiempo_actual = None

ultimo_estado = 0

ultimo_mensaje = None


# ============================================================
# BUCLE PRINCIPAL
# ============================================================

try:

    while True:

        inicio_ciclo = time.monotonic()
        ahora = datetime.now()

        # ------------------------------------------------------
        # SI MODBUS ESTA CAIDO, INTENTAR RECUPERARLO
        # ------------------------------------------------------

        if not modbus_ok:

            if (
                time.monotonic() - ultimo_intento_modbus
                >= MODBUS_RECONNECT_INTERVAL
            ):

                ultimo_intento_modbus = time.monotonic()

                if conectar_modbus():

                    try:

                        # Una lectura real determina si el horno
                        # esta respondiendo.
                        pv, sv = leer_estado_horno()

                        modbus_ok = True
                        fallos_consecutivos = 0

                        marcar_horno_online()

                        print()
                        print(
                            f"[{ahora:%H:%M:%S}] "
                            "Comunicacion con CJD recuperada."
                        )

                        # Al recuperar la comunicacion volvemos a
                        # leer la configuracion y la curva.
                        try:

                            configuracion = leer_configuracion()

                            curva_nueva = determinar_curva(
                                configuracion["SPC"]
                            )

                            if curva_nueva is not None:

                                curva = curva_nueva
                                segmentos = leer_curva(
                                    curva
                                )

                                print(
                                    f"[{ahora:%H:%M:%S}] "
                                    f"Curva recuperada: {curva}"
                                )

                        except Exception as e:

                            print(
                                f"[{ahora:%H:%M:%S}] "
                                f"Aviso leyendo configuracion: {e}"
                            )

                    except Exception as e:

                        fallos_consecutivos += 1
                        cerrar_modbus()
                        modbus_ok = False

                        print(
                            f"[{ahora:%H:%M:%S}] "
                            f"El CJD sigue sin responder: {e}"
                        )

            time.sleep(0.2)
            continue

        # ------------------------------------------------------
        # LECTURA CRITICA: PV + SV
        # ------------------------------------------------------

        try:

            pv, sv = leer_estado_horno()

            fallos_consecutivos = 0

            if not horno_disponible():
                marcar_horno_online()

        except Exception as e:

            fallos_consecutivos += 1

            print()
            print(
                f"[{ahora:%H:%M:%S}] "
                f"Fallo Modbus "
                f"{fallos_consecutivos}/{FALLOS_ANTES_OFFLINE}: "
                f"{e}"
            )

            # Cerramos para forzar una reapertura limpia.
            cerrar_modbus()
            modbus_ok = False

            if fallos_consecutivos >= FALLOS_ANTES_OFFLINE:

                marcar_horno_offline()

                print(
                    f"[{ahora:%H:%M:%S}] "
                    "HORNO NO DISPONIBLE."
                )

            continue

        # ------------------------------------------------------
        # ESTADO / SEGMENTO / TIEMPO
        # ------------------------------------------------------

        ahora_monotonic = time.monotonic()

        if (
            segmento_actual is None
            or
            ahora_monotonic - ultimo_estado
            >= INTERVALO_ESTADO
        ):

            try:

                (
                    segmento_actual,
                    estado_actual,
                    tiempo_actual
                ) = leer_estado_programa()

                ultimo_estado = ahora_monotonic

            except Exception as e:

                # Una lectura secundaria fallida NO declara
                # inmediatamente el horno como caido porque
                # PV/SV acaban de responder correctamente.
                print()
                print(
                    f"[{ahora:%H:%M:%S}] "
                    f"Aviso leyendo estado CJD: {e}"
                )

        # ------------------------------------------------------
        # CONSTRUIR EL MENSAJE
        # ------------------------------------------------------

        datos = {
            "timestamp": ahora.isoformat(
                timespec="seconds"
            ),
            "pv": round(
                pv,
                1
            ),
            "sv": round(
                sv,
                1
            ),
            "segmento": segmento_actual,
            "estado": estado_actual,
            "estado_texto": (
                texto_estado(estado_actual)
                if estado_actual is not None
                else "DESCONOCIDO"
            ),
            "tiempo_cjd": tiempo_actual
        }

        # ------------------------------------------------------
        # CSV LOCAL
        #
        # Solo se guarda una muestra cuando existe una lectura
        # valida del horno. Durante una caida NO se escriben
        # valores falsos ni se repite el ultimo dato.
        # ------------------------------------------------------

        escritor.writerow(
            [
                ahora.strftime("%Y-%m-%d"),
                ahora.strftime("%H:%M:%S"),
                pv,
                sv,
                segmento_actual,
                estado_actual,
                (
                    texto_estado(estado_actual)
                    if estado_actual is not None
                    else "DESCONOCIDO"
                ),
                tiempo_actual
            ]
        )

        archivo.flush()

        # ------------------------------------------------------
        # MQTT
        #
        # Si MQTT esta caido, no se bloquea la lectura del horno.
        # El CSV local sigue guardando las lecturas.
        #
        # No se acumulan mensajes atrasados para enviarlos luego.
        # Los paquetes perdidos durante una caida MQTT se pierden
        # intencionadamente.
        # ------------------------------------------------------

        if mqtt_client.is_connected():

            publicar_datos(datos)

        # ------------------------------------------------------
        # CONSOLA
        # ------------------------------------------------------

        estado_txt = (
            texto_estado(estado_actual)
            if estado_actual is not None
            else "SIN ESTADO"
        )

        mqtt_txt = (
            "MQTT OK"
            if mqtt_client.is_connected()
            else "MQTT OFF"
        )

        print(
            f"{ahora:%H:%M:%S} | "
            f"PV {pv:7.1f} °C | "
            f"SV {sv:7.1f} °C | "
            f"Seg {segmento_actual} | "
            f"{estado_txt:12s} | "
            f"{mqtt_txt}"
        )

        # ------------------------------------------------------
        # MANTENER INTERVALO DE 1 SEGUNDO
        # ------------------------------------------------------

        duracion = (
            time.monotonic()
            - inicio_ciclo
        )

        espera = (
            INTERVALO_LECTURA
            - duracion
        )

        if espera > 0:
            time.sleep(espera)


except KeyboardInterrupt:

    print()
    print()
    print("Monitorizacion detenida por el usuario.")


except Exception as e:

    print()
    print()
    print("=" * 60)
    print("ERROR NO CONTROLADO")
    print("=" * 60)
    print()
    print(e)
    print()


finally:

    # ----------------------------------------------------------
    # MARCAR HORNO OFFLINE ANTES DE CERRAR MQTT
    # ----------------------------------------------------------

    marcar_horno_offline()

    # ----------------------------------------------------------
    # CERRAR CSV
    # ----------------------------------------------------------

    if archivo is not None:

        try:
            archivo.flush()
        except Exception:
            pass

        try:
            archivo.close()
        except Exception:
            pass

    # ----------------------------------------------------------
    # CERRAR MODBUS
    # ----------------------------------------------------------

    cerrar_modbus()

    # ----------------------------------------------------------
    # MARCAR LA CONEXION MQTT COMO OFFLINE
    # ----------------------------------------------------------

    try:
        if mqtt_client.is_connected():
            mqtt_client.publish(
                MQTT_CONNECTION_TOPIC,
                "offline",
                qos=1,
                retain=True
            )
    except Exception:
        pass

    # ----------------------------------------------------------
    # CERRAR MQTT
    # ----------------------------------------------------------

    if mqtt_client is not None:

        try:

            if mqtt_client.is_connected():

                mqtt_client.publish(
                    MQTT_AVAILABILITY_TOPIC,
                    "offline",
                    qos=1,
                    retain=True
                )

        except Exception:
            pass

        try:
            mqtt_client.loop_stop()
        except Exception:
            pass

        try:
            mqtt_client.disconnect()
        except Exception:
            pass

    print()
    print("CJD-9000P desconectado.")
    print("MQTT desconectado.")
    print()
