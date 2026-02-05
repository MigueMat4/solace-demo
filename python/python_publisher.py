'''
Solace Systems Python API
Demo de un publicador de mensajes directos a un tópico especificado
'''
import os
import platform
import time

# Importación de modulos de Solace Python API desde el paquete de solace.messaging
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Desactivar el buffer de salida estándar si es Windows

MSG_COUNT = 3
TOPIC_PREFIX = "guatemaltek/training/solace"

# Clases internas para manejo de errores
class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: "ServiceEvent"):
        print("\nReconectado al broker")
        print(f"Cause de error: {e.get_cause()}")
        print(f"Mensaje: {e.get_message()}")
    
    def on_reconnecting(self, e: "ServiceEvent"):
        print("\nReconectado al broker")
        print(f"Cause de error: {e.get_cause()}")
        print(f"Mensaje: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\nServicio interrumpido")
        print(f"Cause de error: {e.get_cause()}")
        print(f"Mensaje: {e.get_message()}")

 
class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("Fallo al publicar el mensaje")

# Configuración de las propiedades del broker desde variables de entorno o valores por defecto
broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "tcp://localhost:55555,tcp://localhost:55554",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "default",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "default",
    # No recomendable para un entorno de producción:
    "solace.messaging.tls.cert-validated": False,
    "solace.messaging.tls.cert-validated-date": False,
    }

# Construir un servicio de mensajería con una estrategia de reconexión de 20 reintentos en un intervalo de 3 segundos
messaging_service = MessagingService.builder().from_properties(broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()

# Hilo de conexión al servicio de mensajería 
messaging_service.connect()
print(f'Servicio de mensajería conectado? {messaging_service.is_connected}')

# Gestión de eventos para el servicio de mensajería
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

# Crear un publicador de mensajes directos y asignar el manejador de errores
direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
direct_publisher.set_publish_failure_listener(PublisherErrorHandling())

# Iniciar hilo del publicador de mensajes directos
direct_publisher.start()
print(f'Publicador listo? {direct_publisher.is_ready()}')

# Preparar el mensaje de salida con propiedades dinámicas
message_body = "Hola desde Python"
outbound_msg_builder = messaging_service.message_builder() \
                .with_application_message_id("sample_id") \
                .with_property("application", "samples") \
                .with_property("language", "Python") \

count = 1
print("\nPresione Ctrl+C para detener al publicador\n")
try: 
    while True:
        while count <= MSG_COUNT:
            topic = Topic.of(TOPIC_PREFIX + f'/direct/pub/{count}')
            # Publicar el mensaje con encabezados dinámicos y payload
            outbound_msg = outbound_msg_builder \
                            .with_application_message_id(f'NEW {count}')\
                            .build(f'{message_body} + {count}')
            direct_publisher.publish(destination=topic, message=outbound_msg)

            print(f'Mensaje publicado en {topic}')
            count += 1
            time.sleep(1)
        print("\n")
        count = 1
        time.sleep(10)

except KeyboardInterrupt:
    print('\nFinalizando con el publicador')
    direct_publisher.terminate()
    print('\nDesconectando del servicio de mensajería')
    messaging_service.disconnect()
