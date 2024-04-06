# Amazon Books Analyzer
Sistemas Distribuidos I (75.74): TP Escalabilidad: Middleware y Coordinación de Procesos

### Cómo configurar el sistema
[comment]: <> (IMPORTANTE: No se subió el archivo de 2M de líneas. En caso de querer utilizarlo descargarlo y configurar el cliente para enviarlo por defecto envía el de 2 millones de líneas.)

#### Configuración - Arquitectura 
Para cambiar la configuración del sistema (cantidad de workers, logging) se tiene un archivo de configuración config.py con el siguiente formato:

```md
# Configuration
...
AMOUNT_OF_QUERY4_WORKERS = 3
LOGGING_LEVEL = 'ERROR'
```

Una vez realizado un cambio en el archivo de configuración, es necesario correr el siguiente script para crearlo:
```console
python3 set_up_docker_compose.py
```

#### Configuración - Cliente
Para elegir el archivo que lee el cliente, dónde guardará los resultados y otros parámetros, se provee el archivo client/config.ini, con la siguiente información
```md
[DEFAULT]
SERVER_PORT = 12345
SERVER_IP = ClientHandler
RESULT_PORT = 12345
RESULT_IP = ResultHandler
LOGGING_LEVEL = INFO
...
RESULTS_PATH = results.csv
```

### Cómo correr el programa
#### Middleware
Antes de levantar los contenedores es necesario crear la network del middleware. Para ello se provee la siguiente regla de Makefile::
```console
make middleware-run
```

Y esperar que aparezca la siguiente información en la consola:
```console
Server startup complete; 4 plugins started.
  * rabbitmq_prometheus
  * rabbitmq_management
  * rabbitmq_web_dispatch
  * rabbitmq_management_agent
```

#### Levantar el sistema
Para levantar toda la infraestructura del sistema se provee la siguiente regla de Makefile:
```console
make system-run
```

#### Levantar el cliente
Para levantar al cliente se provee la siguiente regla de Makefile:
```console
make client-run
```

### Cómo terminar el programa
Si bien los programas se cierran de manera automática una vez finalizado el procesamiento, el sistema cerrará por completo una vez ejecutado el comando:
```console
make shutdown
```
