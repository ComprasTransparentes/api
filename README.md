# API Compras Transparentes

La API de Compras Transparentes maneja tres tipo de objetos:
- Licitaciones
- Organismos
- Proveedores

Estos objetos se pueden consultar mediante consultas GET a diferentes rutas.

## Listar objetos
El listado de los objetos se obtiene de las siguientes rutas:
- /licitacion/
- /organismo/
- /proveedor/

Los resultados se entregan paginados, con 10 elementos por página. Para pedir una página en particular,
se debe agregar el parámetro *pagina*. e.g.:
```
GET /licitacion?pagina=5
```

## Filtrar objetos
Se puede filtrar los resultados con los siguientes parámetros:
- q=\<términos de búsqueda\>

e.g.
```
GET /licitacion?q=Ministerio+del+Interior
```

## Objetos por ID
Para acceder a los objetos directamente se utilizan las rutas:
- /licitacion/\<id\> | \<codigo de licitacion\>
- /organismo/\<id\>
- /proveedor/\<id\>

e.g.
```
# Estas rutas son equivalentes
GET /licitacion/336704
GET /licitacion/5461-45-L114
```


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



