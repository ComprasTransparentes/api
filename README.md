# API Compras Transparentes

La API de Compras Transparentes maneja tres tipo de objetos:
- Licitaciones
- Organismos
- Proveedores

y ofrece 4 rutas para acceder a ellos:
- /licitacion/\<id\> | \<codigo de licitacion\>
- /organismo/\<id\>
- /proveedor/\<id\>
- /buscar/?q=\<terminos de busqueda\>&pagina=\<numero de pagina\>

## ¿Cómo ejecutar ComprasTransparentes-API?

ComprasTransparentes-API es una aplicación Python. Requiere Python 2.7+.

Las dependencias del programa se encuentran en el archivo *req* en la raíz del proyecto.
Estas se pueden instalar fácilmente con el comando *pip*
```
pip install -r req
```

ComprasTransparentes-API es una aplicacion UWSGI, por lo que necesita de un servidor UWSGI HTTP para
funcionar. Una de las dependencias en *req* es el servidor UWSGI HTTP Gunicorn, pero este puede ser 
reemplazado con cualquier otro servidor UWSGI. En este caso se usará Gunicorn como ejemplo.

Antes de ejecutar Gunicorn es necesario cargar las configuracion de la base de datos en el entorno 
de ejecucion. Las cuatro variables de configuración son:
```
DB_HOST=<hostname>
DB_NAME=<nombre de la base de datos>
DB_USER=<usuario de la base de datos>
DB_PASS=<contraseña del usuario>
```

ComprasTransparentes-API solo funciona con Postgres 9.1+.

Para ejecutar la aplicación:
```
gunicorn hackslab:app
```



