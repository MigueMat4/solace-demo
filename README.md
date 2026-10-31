# Solace Demo

Repositorio educativo para comprender los fundamentos del **middleware de mensajería** utilizando [Solace Platform](https://solace.com/products/event-broker/).

El proyecto contiene dos aplicaciones sencillas escritas en diferentes lenguajes:

- 🐍 **Python:** publica mensajes.
- 🟢 **Node.js:** consume mensajes.
- 📨 **Solace Platform:** recibe, enruta y entrega los mensajes.

El objetivo es mostrar de la manera más sencilla posible **cómo dos aplicaciones independientes pueden comunicarse entre sí mediante un broker de mensajería**.

---

# 🔄 ¿Cómo funciona este proyecto?

El flujo básico es:

```text
┌──────────────────┐
│                  │
│  Python          │
│  Publicador      │
│                  │
└────────┬─────────┘
         │
         │ Publica mensaje
         ▼
┌─────────────────────────┐
│                         │
│     Solace Platform     │
│     Event Broker        │
│                         │
└────────────┬────────────┘
             │
             │ Entrega mensaje
             ▼
┌──────────────────┐
│                  │
│  Node.js         │
│  Consumidor      │
│                  │
└──────────────────┘
```

---

# 🏗️ Arquitectura del proyecto

La estructura del repositorio es:

```text
solace-demo/
│
├── python/
│   └── ...
│
├── nodejs/
│   ├── package.json
│   └── ...
│
├── .gitignore
├── LICENSE
└── README.md
```

## Python

La carpeta `python/` contiene el publicador.

Su responsabilidad es:

1. Conectarse al broker.
2. Establecer una sesión de mensajería.
3. Crear un mensaje.
4. Publicarlo hacia un destino.
5. Finalizar la conexión.

La aplicación utiliza la API oficial de Solace para Python.

## Node.js

La carpeta `nodejs/` contiene el consumidor.

Su responsabilidad es:

1. Conectarse al broker.
2. Establecer una sesión de mensajería.
3. Suscribirse al destino correspondiente.
4. Esperar mensajes.
5. Procesarlos cuando llegan.

La aplicación utiliza `solclientjs`, la API de Solace para Node.js.

---

# 🔌 Requisitos

Para ejecutar el proyecto necesitas:

- Git
- Python 3
- Node.js
- npm
- Acceso a un broker Solace Platform

No necesitas ejecutar Python y Node.js en el mismo sistema operativo.

Por ejemplo:

```text
┌─────────────────────┐
│ Windows / macOS /   │
│ Linux               │
│                     │
│ Python Publisher    │
└──────────┬──────────┘
           │
           │
           ▼
    Solace Platform
           │
           │
           ▼
┌─────────────────────┐
│ Windows / macOS /   │
│ Linux               │
│                     │
│ Node.js Consumer    │
└─────────────────────┘
```

La clave en esta demo es que ambos puedan conectarse al broker.

---

# 🚀 Instalación

Primero clona el repositorio:

```bash
git clone https://github.com/MigueMat4/solace-demo.git
cd solace-demo
```

---

# 🟢 Consumidor Node.js

Abre una terminal y entra a:

```bash
cd nodejs
```

Instala las dependencias:

```bash
npm install
```

Esto instalará la API de Solace para Node.js utilizada por el proyecto.

La dependencia principal es:

```text
solclientjs
```

---

## Configuración

El consumidor necesita conocer por medio de `config.json`:

```text
Host
Message VPN
Username
Password
Queue Name
```

---

## Ejecutar el consumidor

Una vez instaladas las dependencias:

```bash
node QueueConsumer.js
```

El consumidor se conectará al broker y estará en escucha activa esperando mensajes.

---

# 🐍 Publicador Python

En otra terminal entra a la carpeta:

```bash
cd python
```

## Crear un entorno virtual

Es recomendable utilizar un entorno virtual para mantener las dependencias del proyecto aisladas del resto de Python.

### Windows

```powershell
python -m venv venv
venv\Scripts\activate
```

### macOS / Linux

```bash
python3 -m venv venv
source venv/bin/activate
```

Después instala las dependencias:

```bash
pip install -r requirements.txt
```

El archivo `requirements.txt` incluye la API de Solace para Python (`solace-pubsubplus`) y las dependencias necesarias para ejecutar el ejemplo.

---

## Configuración

El publicador al igual que el consumidor necesita conocer los datos necesarios para conectarse al broker. Lo hace por medio de `config.json`:

```text
Host
Message VPN
Username
Password
Topic
```

Además, debe conocer el destino al que publicará los mensajes.

> ⚠️ No coloques contraseñas, tokens u otras credenciales reales directamente en el código si vas a compartir el proyecto públicamente.

---

## Ejecutar el publicador

Una vez configurado el acceso al broker:

```bash
python python_publisher.py
```

El programa se conectará a Solace y comenzará a publicar mensajes.

---

# 🧪 Probando el ejemplo

Para comprobar que todo funciona, primero inicia el consumidor:

```text
Node.js
    │
    │ esperando mensajes...
    ▼
Solace Platform
```

Después ejecuta el publicador:

```text
Python
   │
   │ "Hola desde Python"
   ▼
Solace Platform
```

El broker entregará el mensaje al consumidor:

```text
Solace Platform
       │
       │ "Hola desde Python"
       ▼
Node.js
```

De esta manera se puede observar el flujo completo:

```text
PUBLICADOR
    │
    │ publish
    ▼
  BROKER
    │
    │ deliver
    ▼
CONSUMIDOR
```

---

# 📚 Recursos

Para continuar aprendiendo:

- [Solace Developer Portal](https://solace.dev/)
- [Documentación de Solace Platform](https://docs.solace.com/)
- [Solace Community](https://community.solace.com/)
- [Solace Python API](https://docs.solace.com/API-Developer-Online-Ref-Documentation/python/)
- [Solace Node.js API](https://docs.solace.com/API-Developer-Online-Ref-Documentation/nodejs/)

También puedes consultar los repositorios oficiales de ejemplos de Solace:

- [SolaceSamples](https://github.com/SolaceSamples)
- [Ejemplos de Solace para Python](https://github.com/SolaceSamples/solace-samples-python)
- [Ejemplos de Solace para Node.js](https://github.com/SolaceSamples/solace-samples-nodejs)
