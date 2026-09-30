#!/usr/bin/env python3

import csv
import json
import os
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusSerialClient


# ============================================================
# CJD-9000P -> MQTT / HOME ASSISTANT
# V3.4.2
#
# SOLO LECTURA DEL HORNO / SIN ESCRITURAS MODBUS
#
# V3:
# - Configuracion y curva: se leen UNA VEZ al arrancar.
# - PV (temperatura real) y SV (consigna): cada segundo.
# - STEP / ESTADO / TIEMPO: cada segundo.
# - Se intenta leer C9..CE en un unico bloque Modbus.
# - Si el bloque no es soportado, se usa una lectura de respaldo.
# - Curva y datos dinamicos se publican en topics MQTT separados.
# - Configuracion y curva son retained.
# - Los datos dinamicos NO son retained.
# - Estado MQTT y estado del horno usan un unico topic JSON
#   para poder tener un solo LWT real.
# - Cierre limpio con Ctrl+C: se publica PV=25.0 antes de offline.
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

MODBUS_RECONNECT_INTERVAL = 5.0
FALLOS_ANTES_OFFLINE = 3

INTERVALO_LECTURA = 1.0



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
# MQTT
# ============================================================

MQTT_BROKER = "192.168.1.107"
MQTT_PORT = 1883

MQTT_USER = "horno_mqtt"
MQTT_PASSWORD = "prueba"

# Datos dinamicos segundo a segundo.
MQTT_TOPIC = "horno/cjd9000p/state"

# Configuracion inicial, retained.
MQTT_CONFIG_TOPIC = "horno/cjd9000p/config"

# Curva completa, retained.
MQTT_CURVE_TOPIC = "horno/cjd9000p/curva"

# Topic de estado comun.
#
# Un cliente MQTT solo puede tener UN LWT. Por eso en V3
# se utiliza un JSON comun que contiene:
#   {"mqtt":"online/offline", "horno":"online/offline"}
#
# Home Assistant extrae cada valor con value_template.
MQTT_STATUS_TOPIC = "horno/cjd9000p/status"

# Se conserva el topic antiguo para poder limpiarlo durante
# el cierre y no dejar un "online" retenido de versiones previas.
MQTT_AVAILABILITY_TOPIC = "horno/cjd9000p/availability"
MQTT_CONNECTION_TOPIC = "horno/cjd9000p/mqtt/availability"

DISCOVERY_PREFIX = "homeassistant"

DEVICE_ID = "cjd9000p"
DEVICE_NAME = "CJD-9000P"
MANUFACTURER = "CJD"
MODEL = "CJD-9000P"


# ============================================================
# ESTADO GLOBAL
# ============================================================

mqtt_client = None

estado_horno_disponible = False
estado_horno_lock = threading.Lock()

configuracion = None
numero_curva = None
segmentos_curva = None

ultimo_status_publicado = None

modbus_ok = False
fallos_consecutivos = 0
ultimo_intento_modbus = 0.0

# Una vez que comprobamos el bloque C9..CE:
# True  = el horno lo soporta y se utiliza cada segundo.
# False = se utiliza la lectura de respaldo.
bloque_dinamico_soportado = None

archivo = None
escritor = None
nombre_csv = None


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

def formatear_temperatura(valor):
    """Muestra 24.0 como 24 y conserva los decimales reales."""
    valor = float(valor)
    if valor.is_integer():
        return str(int(valor))
    return f"{valor:.1f}"


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
    """Lee un registro mediante Modbus 03H."""

    respuesta = client_modbus.read_holding_registers(
        address=direccion,
        count=1,
        device_id=MODBUS_ID
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
        device_id=MODBUS_ID
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

    return [a_signed_16(x) for x in respuesta.registers]


def cerrar_modbus():
    try:
        client_modbus.close()
    except Exception:
        pass


def conectar_modbus():
    """
    Abre/reabre el puerto serie.
    Abrir el puerto no garantiza que el horno responda:
    eso se comprueba leyendo registros.
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
    Configuracion que solo se lee al arrancar:
    SPC, CYC y dP.
    """

    return {
        "spc": leer_registro(REG_SPC),
        "cyc": leer_registro(REG_CYC),
        "dp": leer_registro(REG_DP),
    }


def determinar_curva(spc):
    if 1 <= spc <= 4:
        return spc

    print(
        f"SPC={spc}. No corresponde directamente a una curva 1-4."
    )
    return None


def leer_curva(numero):
    """Lee los 20 segmentos de la curva seleccionada."""

    inicio = INICIO_CURVAS[numero]

    valores = leer_bloque(
        inicio,
        40
    )

    segmentos = []

    for n in range(20):
        segmentos.append(
            {
                "segmento": n + 1,
                "temperatura": valores[n * 2],
                "tiempo": valores[n * 2 + 1]
            }
        )

    return segmentos


def leer_dinamicos_bloque():
    """
    Intenta leer C9..CE en una sola consulta:

      C9 = STEP
      CA = ESTADO
      CB = registro intermedio no utilizado
      CC = TIEMPO
      CD = PV
      CE = SV

    Se ignora CB.
    """

    valores = leer_bloque(
        REG_STEP,
        6
    )

    segmento = valores[0]
    estado = valores[1]
    tiempo = valores[3]
    pv = valores[4]
    sv = valores[5]

    return pv, sv, segmento, estado, tiempo


def leer_dinamicos_respaldo():
    """
    Lecturas de respaldo si el CJD no acepta C9..CE como
    bloque consecutivo.
    """

    valores_programa = leer_bloque(
        REG_STEP,
        2
    )

    segmento = valores_programa[0]
    estado = valores_programa[1]

    tiempo = leer_registro(REG_TIEMPO)

    valores_temperatura = leer_bloque(
        REG_PV,
        2
    )

    pv = valores_temperatura[0]
    sv = valores_temperatura[1]

    return pv, sv, segmento, estado, tiempo


def leer_dinamicos():
    """
    Lee todos los valores dinamicos cada segundo.

    La primera vez prueba el bloque unico.
    Si funciona, se utiliza en adelante.
    Si falla, cambia a la lectura de respaldo.
    """

    global bloque_dinamico_soportado

    if bloque_dinamico_soportado is not False:
        try:
            valores = leer_dinamicos_bloque()

            if bloque_dinamico_soportado is None:
                bloque_dinamico_soportado = True
                print(
                    f"[{datetime.now():%H:%M:%S}] "
                    "CJD acepta bloque dinamico C9..CE. "
                    "Se utilizara una sola lectura por segundo."
                )

            return valores

        except Exception as e:

            if bloque_dinamico_soportado is None:
                bloque_dinamico_soportado = False

                print(
                    f"[{datetime.now():%H:%M:%S}] "
                    "El bloque C9..CE no ha funcionado. "
                    f"Se usa lectura de respaldo: {e}"
                )
            else:
                raise

    return leer_dinamicos_respaldo()


def mostrar_configuracion():
    print()
    print("=" * 60)
    print("CONFIGURACION DEL CJD-9000P")
    print("=" * 60)
    print()

    if configuracion is None:
        return

    print(
        f"SPC - seleccion de curva : {configuracion['spc']}"
    )
    print(
        f"CYC - ciclos             : {configuracion['cyc']}"
    )
    print(
        f"dP  - decimal place      : {configuracion['dp']}"
    )

    if numero_curva is not None:
        print(
            f"Curva utilizada          : {numero_curva}"
        )

    if segmentos_curva is not None:
        print()
        print(" Segmento       Temperatura       Tiempo")
        print("--------------------------------------------")

        for s in segmentos_curva:
            print(
                f"   {s['segmento']:02d}"
                f"          {s['temperatura']:8d}"
                f"          {s['tiempo']:8d}"
            )

    print()


# ============================================================
# ESTADO DEL HORNO
# ============================================================

def horno_disponible():
    with estado_horno_lock:
        return estado_horno_disponible


def establecer_horno_disponible(valor):
    global estado_horno_disponible

    with estado_horno_lock:
        estado_horno_disponible = bool(valor)


# ============================================================
# MQTT STATUS
# ============================================================

def publicar_status_mqtt(forzar=False, esperar=False):
    """
    Publica el estado comun:

      {
        "mqtt": "online",
        "horno": "online"
      }

    El mismo topic sirve para las dos entidades de conectividad.
    """

    global ultimo_status_publicado

    if mqtt_client is None:
        return False

    if not mqtt_client.is_connected():
        return False

    estado = {
        "mqtt": "online",
        "horno": (
            "online"
            if horno_disponible()
            else "offline"
        )
    }

    mensaje = json.dumps(
        estado,
        separators=(",", ":")
    )

    if not forzar and mensaje == ultimo_status_publicado:
        return True

    resultado = mqtt_client.publish(
        MQTT_STATUS_TOPIC,
        mensaje,
        qos=1,
        retain=True
    )

    if resultado.rc != mqtt.MQTT_ERR_SUCCESS:
        print(
            f"[{datetime.now():%H:%M:%S}] "
            f"Error publicando status MQTT: {resultado.rc}"
        )
        return False

    if esperar:
        try:
            resultado.wait_for_publish(timeout=3)
        except Exception:
            pass

    ultimo_status_publicado = mensaje

    return True


def marcar_horno_online():
    """Confirma ONLINE tras una lectura Modbus valida."""
    estaba_online = horno_disponible()
    establecer_horno_disponible(True)

    if not estaba_online:
        publicar_status_mqtt(forzar=True, esperar=True)



def marcar_horno_offline():
    """Declara OFFLINE tras alcanzar el umbral de fallos Modbus."""
    estaba_online = horno_disponible()
    establecer_horno_disponible(False)

    if estaba_online:
        publicar_status_mqtt(forzar=True, esperar=True)



# ============================================================
# MQTT DISCOVERY
# ============================================================

device = {
    "identifiers": [DEVICE_ID],
    "name": DEVICE_NAME,
    "manufacturer": MANUFACTURER,
    "model": MODEL
}


def publicar_discovery(componente, objeto_id, datos):
    if mqtt_client is None:
        return

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
        json.dumps(datos),
        qos=1,
        retain=True
    )

    if resultado.rc != mqtt.MQTT_ERR_SUCCESS:
        print(
            f"Error publicando Discovery {objeto_id}: "
            f"{resultado.rc}"
        )


def disponibilidad_config():
    return {
        "topic": MQTT_STATUS_TOPIC,
        "value_template": "{{ value_json.horno }}",
        "payload_available": "online",
        "payload_not_available": "offline"
    }


def publicar_todo_discovery():
    """
    Los unique_id existentes se conservan para que Home Assistant
    no cree entidades nuevas para Temperatura, Consigna, Segmento,
    Tiempo CJD, Horno disponible, MQTT conectado y Funcionando.
    """

    publicar_discovery(
        "sensor",
        "temperatura",
        {
            "name": "Temperatura",
            "unique_id": f"{DEVICE_ID}_temperatura",
            "state_topic": MQTT_TOPIC,
            "availability": disponibilidad_config(),
            "value_template": "{{ (value_json.pv | float) | int if (value_json.pv | float).is_integer() else value_json.pv }}",
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
            "availability": disponibilidad_config(),
            "value_template": "{{ (value_json.sv | float) | int if (value_json.sv | float).is_integer() else value_json.sv }}",
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
            "availability": disponibilidad_config(),
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
            "availability": disponibilidad_config(),
            "value_template": "{{ value_json.tiempo_cjd }}",
            "unit_of_measurement": "s",
            "state_class": "measurement",
            "device": device
        }
    )

    publicar_discovery(
        "binary_sensor",
        "horno_disponible",
        {
            "name": "Horno disponible",
            "unique_id": f"{DEVICE_ID}_horno_disponible",
            "state_topic": MQTT_STATUS_TOPIC,
            "value_template": "{{ value_json.horno }}",
            "payload_on": "online",
            "payload_off": "offline",
            "device_class": "connectivity",
            "device": device
        }
    )

    publicar_discovery(
        "binary_sensor",
        "mqtt_conectado",
        {
            "name": "MQTT conectado",
            "unique_id": f"{DEVICE_ID}_mqtt_conectado",
            "state_topic": MQTT_STATUS_TOPIC,
            "value_template": "{{ value_json.mqtt }}",
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
            "availability": disponibilidad_config(),
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

    # --------------------------------------------------------
    # CONFIGURACION INICIAL
    # --------------------------------------------------------

    publicar_discovery(
        "sensor",
        "curva",
        {
            "name": "Curva seleccionada",
            "unique_id": f"{DEVICE_ID}_curva",
            "state_topic": MQTT_CONFIG_TOPIC,
            "value_template": "{{ value_json.curva }}",
            "state_class": "measurement",
            "device": device
        }
    )

    publicar_discovery(
        "sensor",
        "ciclos",
        {
            "name": "Ciclos programados",
            "unique_id": f"{DEVICE_ID}_ciclos",
            "state_topic": MQTT_CONFIG_TOPIC,
            "value_template": "{{ value_json.cyc }}",
            "state_class": "measurement",
            "device": device
        }
    )

    publicar_discovery(
        "sensor",
        "dp",
        {
            "name": "dP",
            "unique_id": f"{DEVICE_ID}_dp",
            "state_topic": MQTT_CONFIG_TOPIC,
            "value_template": "{{ value_json.dp }}",
            "state_class": "measurement",
            "device": device
        }
    )

    # --------------------------------------------------------
    # CURVA: 20 entidades legibles, una por segmento.
    #
    # Cada entidad muestra:
    #   "100 °C / 10 min"
    #
    # La curva es independiente de la consigna SV.
    # --------------------------------------------------------

    if segmentos_curva is not None:

        for indice in range(20):

            numero = indice + 1

            publicar_discovery(
                "sensor",
                f"curva_segmento_{numero:02d}",
                {
                    "name": f"Curva segmento {numero:02d}",
                    "unique_id": (
                        f"{DEVICE_ID}_curva_segmento_{numero:02d}"
                    ),
                    "state_topic": MQTT_CURVE_TOPIC,
                    "value_template": (
                        "{{ value_json.segmentos["
                        f"{indice}"
                        "].temperatura }} °C / "
                        "{{ value_json.segmentos["
                        f"{indice}"
                        "].tiempo }} min"
                    ),
                    "icon": "mdi:chart-line",
                    "device": device
                }
            )


# ============================================================
# PUBLICACION DE CONFIGURACION Y CURVA
# ============================================================

def publicar_configuracion_mqtt():
    if configuracion is None:
        return False

    if numero_curva is None:
        return False

    if mqtt_client is None or not mqtt_client.is_connected():
        return False

    datos = dict(configuracion)
    datos["curva"] = numero_curva

    resultado = mqtt_client.publish(
        MQTT_CONFIG_TOPIC,
        json.dumps(datos, separators=(",", ":")),
        qos=1,
        retain=True
    )

    return resultado.rc == mqtt.MQTT_ERR_SUCCESS


def publicar_curva_mqtt():
    if segmentos_curva is None:
        return False

    if numero_curva is None:
        return False

    if mqtt_client is None or not mqtt_client.is_connected():
        return False

    datos = {
        "curva": numero_curva,
        "segmentos": segmentos_curva
    }

    resultado = mqtt_client.publish(
        MQTT_CURVE_TOPIC,
        json.dumps(datos, separators=(",", ":")),
        qos=1,
        retain=True
    )

    return resultado.rc == mqtt.MQTT_ERR_SUCCESS


# ============================================================
# CALLBACKS MQTT
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):

    print()
    print(
        f"[{datetime.now():%H:%M:%S}] "
        f"MQTT conectado. reason_code={reason_code}"
    )

    if reason_code != 0:
        return

    # Primero Discovery.
    publicar_todo_discovery()

    # Configuracion y curva ya leidas del horno.
    # Si no estaban disponibles durante una conexion anterior,
    # ahora se publican.
    publicar_configuracion_mqtt()
    publicar_curva_mqtt()

    # El status retained refleja tanto MQTT como el horno.
    publicar_status_mqtt(forzar=True)


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

    # No se detiene el programa.
    # Paho mantiene el loop activo y reintenta la conexion.


# ============================================================
# MQTT
# ============================================================

def iniciar_mqtt():

    global mqtt_client

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

    # UNICO LWT REAL DEL CLIENTE.
    #
    # Si el proceso muere inesperadamente, el broker publica:
    # {"mqtt":"offline","horno":"offline"}
    #
    # Esto resuelve el problema que tenia V2 con la entidad
    # "MQTT conectado".
    lwt = {
        "mqtt": "offline",
        "horno": "offline"
    }

    mqtt_client.will_set(
        MQTT_STATUS_TOPIC,
        payload=json.dumps(lwt, separators=(",", ":")),
        qos=1,
        retain=True
    )

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect

    try:
        mqtt_client.connect_async(
            MQTT_BROKER,
            MQTT_PORT,
            60
        )

        mqtt_client.loop_start()

        print(
            "Cliente MQTT iniciado."
        )

    except Exception as e:
        print(
            f"Advertencia iniciando MQTT: {e}"
        )


def publicar_datos(datos):
    if mqtt_client is None:
        return False

    if not mqtt_client.is_connected():
        return False

    mensaje = json.dumps(
        datos,
        separators=(",", ":")
    )

    # Los datos dinamicos NO son retained.
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


def publicar_estado_final():
    """
    Antes de apagar, publica una ultima muestra valida para que
    Home Assistant reciba PV=25 °C.

    La disponibilidad se marca offline inmediatamente despues.
    """

    if mqtt_client is None:
        return

    if not mqtt_client.is_connected():
        return

    ahora = datetime.now()

    datos = {
        "timestamp": ahora.isoformat(timespec="seconds"),
        "pv": 25,
        "sv": 25,
        "segmento": None,
        "estado": 0,
        "estado_texto": "PARADO",
        "tiempo_cjd": None
    }

    resultado = mqtt_client.publish(
        MQTT_TOPIC,
        json.dumps(datos, separators=(",", ":")),
        qos=1,
        retain=False
    )

    if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
        try:
            resultado.wait_for_publish(timeout=3)
        except Exception:
            pass

        print(
            f"[{ahora:%H:%M:%S}] "
            "Ultimo estado enviado a Home Assistant: PV=25 °C"
        )


# ============================================================
# CSV
# ============================================================

def crear_archivo_csv(numero):

    os.makedirs(
        "cocciones",
        exist_ok=True
    )

    fecha = datetime.now().strftime(
        "%Y-%m-%d_%H-%M-%S"
    )

    nombre = (
        f"cocciones/"
        f"{fecha}_curva_{numero}.csv"
    )

    archivo_local = open(
        nombre,
        "w",
        newline="",
        encoding="utf-8"
    )

    escritor_local = csv.writer(
        archivo_local,
        delimiter=";"
    )

    escritor_local.writerow(
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

    archivo_local.flush()

    return archivo_local, escritor_local, nombre


# ============================================================
# INICIALIZACION DEL HORNO
# ============================================================

def inicializar_horno():

    global configuracion
    global numero_curva
    global segmentos_curva
    global modbus_ok
    global ultimo_intento_modbus

    print()
    print("Conectando con el CJD-9000P...")

    while True:

        ahora_monotonic = time.monotonic()

        if (
            ahora_monotonic - ultimo_intento_modbus
            >= MODBUS_RECONNECT_INTERVAL
        ):

            ultimo_intento_modbus = ahora_monotonic

            if conectar_modbus():

                try:
                    # ------------------------------------------------
                    # IMPORTANTE:
                    # configuracion + curva se leen SOLO AQUI.
                    # No se vuelven a leer durante la coccion.
                    # ------------------------------------------------

                    configuracion = leer_configuracion()

                    numero_curva = determinar_curva(
                        configuracion["spc"]
                    )

                    if numero_curva is not None:
                        segmentos_curva = leer_curva(
                            numero_curva
                        )
                    else:
                        segmentos_curva = None

                    mostrar_configuracion()

                    modbus_ok = True

                    # La configuracion y la curva han respondido correctamente por Modbus.
                    #
                    # En versiones anteriores el Discovery de la curva podia publicarse
                    # antes de leer segmentos_curva. En ese caso Home Assistant recibia
                    # los sensores normales, pero no los 20 sensores de la curva.
                    # Ahora, una vez leida la curva, volvemos a publicar Discovery.
                    # Si MQTT aun no esta conectado, on_connect() lo hara despues;
                    # si ya esta conectado, se publica inmediatamente.
                    if mqtt_client is not None and mqtt_client.is_connected():
                        publicar_todo_discovery()
                        publicar_configuracion_mqtt()
                        publicar_curva_mqtt()

                    marcar_horno_online()

                    print(
                        f"[{datetime.now():%H:%M:%S}] "
                        "Comunicacion con el horno OK."
                    )

                    return True

                except Exception as e:

                    print(
                        f"[{datetime.now():%H:%M:%S}] "
                        f"El CJD no responde correctamente: {e}"
                    )

                    cerrar_modbus()
                    modbus_ok = False

        time.sleep(0.2)


# ============================================================
# BUCLE PRINCIPAL
# ============================================================

def ejecutar_monitorizacion():

    global modbus_ok
    global fallos_consecutivos
    global ultimo_intento_modbus

    if numero_curva is None:
        curva_para_archivo = 0
    else:
        curva_para_archivo = numero_curva

    global archivo
    global escritor
    global nombre_csv

    archivo, escritor, nombre_csv = crear_archivo_csv(
        curva_para_archivo
    )

    print()
    print("=" * 60)
    print("MONITORIZACION V3 INICIADA")
    print("=" * 60)
    print()
    print(f"CSV local: {nombre_csv}")
    print()
    print("Lectura dinamica: 1 segundo")
    print("Curva/configuracion: solo al arrancar")
    print()
    print("Ctrl+C para terminar.")
    print()

    ultimo_segmento = None
    ultimo_estado = None
    ultimo_tiempo = None

    while True:

        inicio_ciclo = time.monotonic()
        ahora = datetime.now()

        # --------------------------------------------------------
        # RECUPERACION MODBUS
        # --------------------------------------------------------

        if not modbus_ok:

            if (
                time.monotonic() - ultimo_intento_modbus
                >= MODBUS_RECONNECT_INTERVAL
            ):

                ultimo_intento_modbus = time.monotonic()

                if conectar_modbus():

                    try:
                        # Solo comprobamos comunicacion dinamica.
                        # NO volvemos a leer configuracion ni curva.
                        (
                            pv,
                            sv,
                            segmento,
                            estado,
                            tiempo_cjd
                        ) = leer_dinamicos()

                        modbus_ok = True
                        fallos_consecutivos = 0

                        ultimo_segmento = segmento
                        ultimo_estado = estado
                        ultimo_tiempo = tiempo_cjd

                        marcar_horno_online()

                        print()
                        print(
                            f"[{ahora:%H:%M:%S}] "
                            "Comunicacion con CJD recuperada."
                        )

                    except Exception as e:

                        cerrar_modbus()
                        modbus_ok = False

                        print(
                            f"[{ahora:%H:%M:%S}] "
                            f"El CJD sigue sin responder: {e}"
                        )

            time.sleep(0.2)
            continue

        # --------------------------------------------------------
        # LECTURA DINAMICA CADA SEGUNDO
        # --------------------------------------------------------

        try:

            (
                pv,
                sv,
                segmento,
                estado,
                tiempo_cjd
            ) = leer_dinamicos()

            fallos_consecutivos = 0

            ultimo_segmento = segmento
            ultimo_estado = estado
            ultimo_tiempo = tiempo_cjd

            if not horno_disponible():
                marcar_horno_online()

        except Exception as e:

            fallos_consecutivos += 1

            print(
                f"[{ahora:%H:%M:%S}] "
                f"Fallo Modbus "
                f"{fallos_consecutivos}/{FALLOS_ANTES_OFFLINE}: "
                f"{e}"
            )

            # Cerramos el puerto para forzar una nueva comprobacion
            # en el siguiente ciclo.
            #
            # IMPORTANTE: NO ponemos modbus_ok=False aqui.
            # Si lo hicieramos tras el primer fallo, el programa
            # entraria directamente en la rutina de recuperacion y
            # fallos_consecutivos nunca llegaria a 3.
            cerrar_modbus()

            if fallos_consecutivos >= FALLOS_ANTES_OFFLINE:

                # A partir de 3 fallos consecutivos el horno deja de
                # considerarse disponible en Home Assistant.
                marcar_horno_offline()
                modbus_ok = False

                print(
                    f"[{ahora:%H:%M:%S}] "
                    "HORNO NO DISPONIBLE. "
                    "Home Assistant -> offline."
                )

            continue

        # --------------------------------------------------------
        # MENSAJE DINAMICO
        #
        # PV y SV son valores independientes:
        #
        #   PV = temperatura REAL
        #   SV = consigna actual
        #
        # La curva programada NO se mezcla aqui.
        # --------------------------------------------------------

        datos = {
            "timestamp": ahora.isoformat(
                timespec="seconds"
            ),
            "pv": int(pv) if float(pv).is_integer() else round(pv, 1),
            "sv": int(sv) if float(sv).is_integer() else round(sv, 1),
            "segmento": segmento,
            "estado": estado,
            "estado_texto": texto_estado(estado),
            "tiempo_cjd": tiempo_cjd
        }

        # --------------------------------------------------------
        # CSV
        # --------------------------------------------------------

        escritor.writerow(
            [
                ahora.strftime("%Y-%m-%d"),
                ahora.strftime("%H:%M:%S"),
                int(pv) if float(pv).is_integer() else round(pv, 1),
                int(sv) if float(sv).is_integer() else round(sv, 1),
                segmento,
                estado,
                texto_estado(estado),
                tiempo_cjd
            ]
        )

        archivo.flush()

        # --------------------------------------------------------
        # MQTT
        # --------------------------------------------------------

        publicar_datos(datos)

        # --------------------------------------------------------
        # CONSOLA
        # --------------------------------------------------------

        mqtt_txt = (
            "MQTT OK"
            if mqtt_client is not None
            and mqtt_client.is_connected()
            else "MQTT OFF"
        )

        print(
            f"{ahora:%H:%M:%S} | "
            f"PV {formatear_temperatura(pv):>7s} °C | "
            f"SV {formatear_temperatura(sv):>7s} °C | "
            f"Seg {segmento} | "
            f"{texto_estado(estado):12s} | "
            f"Tiempo {tiempo_cjd!s:>6} | "
            f"{mqtt_txt}"
        )

        # --------------------------------------------------------
        # MANTENER CICLO DE 1 SEGUNDO
        # --------------------------------------------------------

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


# ============================================================
# CIERRE LIMPIO
# ============================================================

def cerrar_programa():

    global archivo

    print()
    print("Cerrando CJD-9000P / MQTT...")

    # --------------------------------------------------------
    # 1. Ultimo dato: PV=25 °C
    # --------------------------------------------------------
    try:
        publicar_estado_final()
    except Exception as e:
        print(f"Aviso publicando estado final: {e}")

    # --------------------------------------------------------
    # 2. Estado final REAL del topic combinado.
    #    NO usamos publicar_status_mqtt(), porque esa funcion
    #    siempre representa la conexion MQTT como online.
    # --------------------------------------------------------
    try:
        establecer_horno_disponible(False)

        if mqtt_client is not None and mqtt_client.is_connected():
            mensaje_offline = json.dumps(
                {"mqtt": "offline", "horno": "offline"},
                separators=(",", ":")
            )

            resultado = mqtt_client.publish(
                MQTT_STATUS_TOPIC,
                mensaje_offline,
                qos=1,
                retain=True
            )

            if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
                try:
                    resultado.wait_for_publish(timeout=5)
                except Exception:
                    pass
                print(f"[{datetime.now():%H:%M:%S}] MQTT -> offline")
            else:
                print(f"Error publicando MQTT offline: {resultado.rc}")
    except Exception as e:
        print(f"Aviso publicando status offline: {e}")

    # --------------------------------------------------------
    # 3. Topics antiguos de V2, por compatibilidad.
    # --------------------------------------------------------
    if mqtt_client is not None and mqtt_client.is_connected():
        for topic in (MQTT_AVAILABILITY_TOPIC, MQTT_CONNECTION_TOPIC):
            try:
                resultado = mqtt_client.publish(
                    topic,
                    "offline",
                    qos=1,
                    retain=True
                )
                if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
                    try:
                        resultado.wait_for_publish(timeout=5)
                    except Exception:
                        pass
            except Exception:
                pass

    # --------------------------------------------------------
    # 4. CSV.
    # --------------------------------------------------------
    if archivo is not None:
        try:
            archivo.flush()
        except Exception:
            pass
        try:
            archivo.close()
        except Exception:
            pass
        archivo = None

    # --------------------------------------------------------
    # 5. Modbus.
    # --------------------------------------------------------
    try:
        cerrar_modbus()
    except Exception:
        pass

    # --------------------------------------------------------
    # 6. MQTT: primero parar el loop y despues desconectar.
    #    Los mensajes offline ya han sido confirmados arriba.
    # --------------------------------------------------------
    if mqtt_client is not None:
        try:
            mqtt_client.loop_stop()
        except Exception:
            pass
        try:
            if mqtt_client.is_connected():
                mqtt_client.disconnect()
        except Exception:
            pass

    print()
    print("CJD-9000P desconectado.")
    print("MQTT desconectado.")
    print()


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 60)
    print("        CJD-9000P -> MQTT / HOME ASSISTANT")
    print("                     VERSION 3.4.2")
    print("=" * 60)
    print()

    print(f"Puerto Modbus : {PUERTO}")
    print(f"Direccion     : {MODBUS_ID}")
    print(f"Baudrate      : {BAUDRATE}")
    print(f"Formato       : {BYTESIZE}{PARITY}{STOPBITS}")
    print(f"MQTT broker   : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"MQTT topic    : {MQTT_TOPIC}")
    print(f"MQTT curva    : {MQTT_CURVE_TOPIC}")
    print()

    print("MODO MODBUS: SOLO LECTURA")
    print("Sin escrituras Modbus.")
    print("PV y SV: cada segundo.")
    print("STEP / ESTADO / TIEMPO: cada segundo.")
    print("Configuracion y curva: solo al arrancar.")
    print()

    iniciar_mqtt()

    # La inicializacion queda dentro del mismo try que el resto
    # para que Ctrl+C tambien funcione si el horno no responde.
    inicializar_horno()

    ejecutar_monitorizacion()


if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:
        print()
        print("Monitorizacion detenida por el usuario.")

    except Exception as e:
        print()
        print("=" * 60)
        print("ERROR NO CONTROLADO")
        print("=" * 60)
        print()
        print(e)
        print()

    finally:
        cerrar_programa()
