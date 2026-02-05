/**
 * Solace Systems Node.js API
 * Demo de un consumidor de mensajes
 */

/*jslint es6 devel:true node:true*/

var QueueConsumer = function (solaceModule, queueName) {
    'use strict';
    var solace = solaceModule;
    var consumer = {};
    consumer.session = null;
    consumer.queueName = queueName;
    consumer.consuming = false;

    // Logger
    consumer.log = function (line) {
        var now = new Date();
        var time = [('0' + now.getHours()).slice(-2), ('0' + now.getMinutes()).slice(-2), ('0' + now.getSeconds()).slice(-2)];
        var timestamp = '[' + time.join(':') + '] ';
        console.log(timestamp + line);
    };

    consumer.log('\n>>> Consumidor listo para conectarse al broker <<<');

    // Función principal
    consumer.run = function (argv) {
        consumer.connect(argv);
    };

    // Establecer conexión a Solace PubSub+ Event Broker
    consumer.connect = function (argv) {
        if (consumer.session !== null) {
            consumer.log('Consumidor conectado y listo para recibir mensajes de la cola: ' + consumer.queueName);
            return;
        }
        // extract params
        var hosturl = argv.slice(2)[0];
        consumer.log('Conectando a Solace PubSub+ Event Broker usando la url: ' + hosturl);
        var usernamevpn = argv.slice(3)[0];
        var username = usernamevpn.split('@')[0];
        consumer.log('Usuario: ' + username);
        var vpn = usernamevpn.split('@')[1];
        consumer.log('Message-VPN: ' + vpn);
        var pass = argv.slice(4)[0];
        // Crear sesión
        try {
            consumer.session = solace.SolclientFactory.createSession({
                url:      hosturl,
                vpnName:  vpn,
                userName: username,
                password: pass,
            });
        } catch (error) {
            consumer.log(error.toString());
        }
        // Definir listeners de eventos de sesión
        consumer.session.on(solace.SessionEventCode.UP_NOTICE, function (sessionEvent) {
            consumer.log('>>> Conexión éxitosa al broker <<<');
            consumer.startConsume();
        });
        consumer.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, function (sessionEvent) {
            consumer.log('Error de conexión al broker: ' + sessionEvent.infoStr +
                ' - verifique que los valores de los parámetros y conectividad sean correctos');
        });
        consumer.session.on(solace.SessionEventCode.DISCONNECTED, function (sessionEvent) {
            consumer.log('Desconectado');
            consumer.consuming = false;
            if (consumer.session !== null) {
                consumer.session.dispose();
                consumer.session = null;
            }
        });
        // Conectar la sesión
        try {
            consumer.session.connect();
        } catch (error) {
            consumer.log(error.toString());
        }
    };

    // Suscribirse a una cola en Solace PubSub+ Event Broker
    consumer.startConsume = function () {
        if (consumer.session !== null) {
            if (consumer.consuming) {
                consumer.log('El consumidor está conectado a la cola y listo para recibir mensajes');
            } else {
                try {
                    consumer.messageConsumer = consumer.session.createMessageConsumer({
                        queueDescriptor: { name: consumer.queueName, type: solace.QueueType.QUEUE },
                        acknowledgeMode: solace.MessageConsumerAcknowledgeMode.CLIENT,
                        createIfMissing: true // Crea la cola si no existe
                    });
                    consumer.messageConsumer.on(solace.MessageConsumerEventName.UP, function () {
                        consumer.consuming = true;
                        consumer.log('El consumidor está activo');
                    });
                    consumer.messageConsumer.on(solace.MessageConsumerEventName.CONNECT_FAILED_ERROR, function () {
                        consumer.consuming = false;
                        consumer.log('>>> Error: el consumidor no se pudo conectar a la cola "' + consumer.queueName +
                            '" <<<\n   Asegurese de que esta cola exista en la Message-VPN');
                            consumer.exit();
                    });
                    consumer.messageConsumer.on(solace.MessageConsumerEventName.DOWN, function () {
                        consumer.consuming = false;
                        consumer.log('>>> El consumidor ahora está inactivo <<<');
                    });
                    consumer.messageConsumer.on(solace.MessageConsumerEventName.DOWN_ERROR, function () {
                        consumer.consuming = false;
                        consumer.log('>>> Un error ocurrió, el consumidor no está activo <<<');
                    });
                    consumer.messageConsumer.on(solace.MessageConsumerEventName.MESSAGE,
                                               function onMessage(message) {
                        consumer.log('Mensaje recibido: "' + message.getBinaryAttachment() + '",' +
                            ' detalles:\n' + message.dump());
                        // Se necesita reconocer explícitamente el ACK del mensaje, de lo contrario no se eliminará del enrutador de mensajes
                        message.acknowledge();
                    });
                    consumer.messageConsumer.connect();
                } catch (error) {
                    consumer.log(error.toString());
                }
            }
        } else {
            consumer.log('No se puede iniciar el Consumidor porque no está conectado a Solace PubSub+ Event Broker');
        }
    };

    consumer.exit = function () {
        consumer.stopConsume();
        consumer.disconnect();
        setTimeout(function () {
            process.exit();
        }, 2000); // Esperar 2 segundos para finalizar
    };

    // Detener correctamente el servicio del Consumer en Solace PubSub+ Event Broker
    consumer.stopConsume = function () {
        if (consumer.session !== null) {
            if (consumer.consuming) {
                consumer.consuming = false;
                consumer.log('Desconectando de la cola...');
                try {
                    consumer.messageConsumer.disconnect();
                    consumer.messageConsumer.dispose();
                } catch (error) {
                    consumer.log(error.toString());
                }
            } else {
                consumer.log('No se puede detener el Consumidor porque no está conectado a la cola "' +
                    consumer.queueName + '"');
            }
        } else {
            consumer.log('No se puede detener el Consumidor porque no está conectado a Solace PubSub+ Event Broker');
        }
    };

    // Desconectarse correctamente de Solace PubSub+ Event Broker
    consumer.disconnect = function () {
        consumer.log('Desconectando de Solace PubSub+ Event Broker...');
        if (consumer.session !== null) {
            try {
                consumer.session.disconnect();
            } catch (error) {
                consumer.log(error.toString());
            }
        } else {
            consumer.log('No está conectado a Solace PubSub+ Event Broker');
        }
    };

    return consumer;
};

var solace = require('solclientjs').debug;

// Inicializar factory con los valores predeterminados de API más recientes
var factoryProps = new solace.SolclientFactoryProperties();
factoryProps.profile = solace.SolclientFactoryProfiles.version10;
solace.SolclientFactory.init(factoryProps);

// Activar el log en la consola de JavaScript al nivel WARN.
// AVISO: funciona solo con ('solclientjs').debug
solace.SolclientFactory.setLogLevel(solace.LogLevel.WARN);

// Crear el consumidor, especificando el nombre de la cola
var consumer = new QueueConsumer(solace, 'Q.TEST');

// Verificar que estén todos los datos de conexión del broker
const config = require('./config.json');
if (!config.host || !config.username || !config.messagevpn || !config.password) {
    consumer.log('No se puede conectar: se esperan todos los argumentos en config.json ->' +
        ' <protocol://host[:port]> <message-vpn> <client-username> <client-password>.\n' +
        'Los protocolos disponibles son ws://, wss://, http://, https://, tcp://, tcps://');
    process.exit(1);
}

// Iniciar hilo del consumidor
consumer.run([
    null,
    null,
    config.host,
    `${config.username}@${config.messagevpn}`,
    config.password
]);

// Esperar a que se envié un KeyBoardInterrupt para finalizar
consumer.log("Presione Ctrl+C para detener al consumidor");
process.stdin.resume();

process.on('SIGINT', function () {
    'use strict';
    consumer.exit();
});
