.. Compras Transparentes documentation master file, created by
   sphinx-quickstart on Thu Dec 17 16:34:29 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


.. toctree::
   :maxdepth: 2

   index

Documentación de Compras Transparentes API
##########################################

`Compras Transparentes <http://www.comprastransparentes.cl>`_ es un repositorio de datos de
`Mercadopublico <https://www.mercadopublico.cl>`_. La  API REST de Compras Transparentes permite explorar y extraer
información de este repositorio.

Licitación
==========

.. http:get:: /licitacion

   .. automethoddoc:: endpoints.licitacion.Licitacion.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /licitacion/
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

      {
          "licitaciones": [
              {
                "id": 353559,
                "codigo": "2216-2-LP14",

                "nombre": "AIT EXPLOTACION CENTRO METROPOLITANO DE VEHICULOS ",
                "descripcion": "El presente contrato de asesoría está diseñado para prestar el apoyo necesario con que debe contar el Inspector Fiscal del contrato de concesión, para dar cumplimiento a las pertinentes labores de supervisión y control del contrato de concesión Asesoría de Inspección Técnica a la Explotación de la Obra Centro Metropolitano de Vehículos Retirados de Circulación.",

                "organismo": {
                  "categoria": "Ministerio de Obras Públicas",
                  "id": 1,
                  "nombre": "Administración Sistema de Concesiones"
                }

                "fecha_publicacion": "2014-05-13T17:02:00",
                "fecha_cierre": "2014-07-21T15:00:00",
                "fecha_adjudicacion": "2014-09-25T10:51:40.603",

                "estado": 8,
                "fecha_cambio_estado": "2014-09-25",

                "n_items": 1,

                "adjudicacion": {
                  "acta": "http://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?qs=HsgAuqRzknYHyGc0FNmZdu2Wah4RoSQqKlwvCxeZv7c=",
                  "monto": 1503000000,
                  "n_items": 1
                },
              },
              [...]
          ],
          "n_licitaciones": 104
      }

.. http:get:: /licitacion/<licitacion_id>

   .. automethoddoc:: endpoints.licitacion.LicitacionId.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /licitacion/353559
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

      {
          "id": 353559,
          "codigo": "2216-2-LP14",

          "nombre": "AIT EXPLOTACION CENTRO METROPOLITANO DE VEHICULOS ",
          "descripcion": "El presente contrato de asesoría está diseñado para prestar el apoyo necesario con que debe contar el Inspector Fiscal del contrato de concesión, para dar cumplimiento a las pertinentes labores de supervisión y control del contrato de concesión Asesoría de Inspección Técnica a la Explotación de la Obra Centro Metropolitano de Vehículos Retirados de Circulación.",

          "organismo": {
            "id": 1,
            "categoria": "Ministerio de Obras Públicas",
            "nombre": "Administración Sistema de Concesiones"
          },

          "unidad": {
            "nombre": "MOP Administración Sistema de Concesiones",
            "rut": "61.202.000-0",
            "region": "Región Metropolitana de Santiago",
            "comuna": "Santiago Centro",
            "direccion": "Morande 71, Piso 1, Ventanilla Unica DCyF"
          },

          "usuario": {
            "cargo": "Jefe Unidad de Abastecimiento",
            "nombre": "Pablo Ubeda Alarcón",
            "rut": "13.555.350-6"
          },

          "responsable_contrato": {
            "nombre": "Juan Carlos Gonzalez Vidaurrazaga",
            "telefono": "56-02-4496860-6860",
            "email": "juan.gonzalez.v@mop.gov.cl"
          },

          "responsable_pago": {
            "nombre": "Juan Carlos Gonzalez Vidaurrazaga",
            "email": "juan.gonzalez.v@mop.gov.cl",
          },

          "estado": 8,
          "fecha_cambio_estado": "2014-09-25",

          "fecha_creacion": "2014-04-22T12:53:27.463",
          "fecha_publicacion": "2014-05-13T17:02:00",
          "fecha_inicio": "2014-05-13T18:00:00",
          "fecha_final": "2014-06-20T16:00:00",
          "fecha_cierre": "2014-07-21T15:00:00",
          "fecha_adjudicacion": "2014-09-25T10:51:40.603",

          "n_items": 1,

          "adjudicacion": {
            "n_items": 1,
            "monto": 1503000000,
            "acta": "http://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?qs=HsgAuqRzknYHyGc0FNmZdu2Wah4RoSQqKlwvCxeZv7c=",
          },

          "categorias": [
            {
              "id": 53,
              "nombre": "Servicios profesionales, administrativos y consultorías de gestión empresarial"
            }
          ],
      }

.. http:get:: /licitacion/<licitacion_id>/item

   .. automethoddoc:: endpoints.licitacion.LicitacionIdItem.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /licitacion/353559/item
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

      {
          "n_items": 1,
          "items": [
            {
              "adjudicacion": {
                "cantidad": 1,
                "monto_unitario": 1503000000,
                "monto_total": 1503000000,

                "fecha": "2014-09-25T10:51:40.603",

                "proveedor": {
                  "id": 4074,
                  "nombre": "Zañartu Ingenieros Consultores Limitada",
                  "rut": "79.511.210-3"
                }
              },

              "codigo_categoria": 80101600,
              "nombre_categoria": "Servicios profesionales, administrativos y consultorías de gestión empresarial / Consultorías o asesorías en gestión empresarial / Gestión de proyectos",

              "codigo_producto": 80101604,
              "nombre_producto": "Asesorías en gestión de proyectos",

              "descripcion": "AIT Explotación de la Obra Centro Metropolitano de Vehículos Retirados de Circulación.",

              "unidad": "Unidad"
              "cantidad": 1,
            },
            [...]
          ]
      }

Organismo
=========

.. http:get:: /organismo

   .. automethoddoc:: endpoints.organismo.Organismo.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /organismo
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

      {
        "n_organismos": 5,
        "organismos": [
          {
            "id": 5,
            "categoria": "Ministerio de Defensa Nacional",
            "nombre": "Armada de Chile"
            "monto_adjudicado": 45539582,
          },
          [...]
        ]
      }

.. http:get:: /organismo/<organismo_id>

   .. automethoddoc:: endpoints.organismo.OrganismoId.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /organismo/5
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

       {
         "id": 5,
         "categoria": "Ministerio de Defensa Nacional",
         "nombre": "Armada de Chile"
       }

.. http:get:: /organismo/<organismo_id>/licitacion

   .. automethoddoc:: endpoints.organismo.OrganismoIdLicitacion.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /organismo/5/licitacion
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

     {
         "n_licitaciones": 13603,
         "licitaciones": [
             {
                 "id": 154586,
                 "codigo": "3084-15-L115",

                 "nombre": "ADQUISICION PLANTA AIRE ACONDICIONADO",
                 "descripcion": "NECESIDADES DE ESTA DIRECCION TECNICA.",

                 "fecha_publicacion": "2015-07-09T16:17:06.497",
                 "fecha_cierre": "2015-07-24T17:00:00",
                 "fecha_adjudicacion": "2015-07-31T11:04:42.137",

                 "estado": 8,
                 "fecha_cambio_estado": "2015-07-31",

                 "n_items": 1,

                 "adjudicacion": {
                     "acta": "http://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?qs=YgiADKwHPbuZUJQe+lyvls8XcOhFocIUA1RDXdbkWY0=",
                     "monto": 588235,
                     "n_items": 1
                 }
             },
            [...]
         ]
     }

Proveedor
=========

.. http:get:: /proveedor

   .. automethoddoc:: endpoints.proveedor.Proveedor.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /proveedor
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

      {
        "n_proveedores": 120,
        "proveedores": [
          {
            "id": 5,
            "nombre": "TITO DEL CARMEN NAVARRO NAVARRO",
            "rut": "5.280.062-5",
            "monto_adjudicado": 55843146
          },
          [...]
        ]
      }

.. http:get:: /proveedor/<proveedor_id>

   .. automethoddoc:: endpoints.proveedor.ProveedorId.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /proveedor/5
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

      {
        "id": 5,
        "nombre": "TITO DEL CARMEN NAVARRO NAVARRO",
        "rut": "5.280.062-5"
      }

.. http:get:: /proveedor/<proveedor_id>/licitacion

   .. automethoddoc:: endpoints.proveedor.ProveedorIdLicitacion.on_get

   **Example Request**:

   .. sourcecode:: http

      GET /proveedor/5/licitacion
      Host: example.com

   **Example Response**:

   .. sourcecode:: javascript

     {
       "n_licitaciones": 26,
       "licitaciones": [
         {
           "id": 11,
           "codigo": "1037-24-L115",

           "nombre": "Servicio de instalación de portón metálico de corr",
           "descripcion": "Servicio de instalación de portón metálico de corredera en Centro de Acopio de Plantas Chin Chin, CONAF  Región de Los Lagos",

           "organismo": {
             "categoria": "Ministerio de Agricultura",
             "id": 58,
             "nombre": "Corporación Nacional Forestal"
           },

           "fecha_adjudicacion": "2015-08-10T16:39:48.993",
           "fecha_cierre": "2015-08-03T15:30:00",
           "fecha_publicacion": "2015-07-27T14:48:53.663",

           "estado": 8,
           "fecha_cambio_estado": "2015-08-10",

           "n_items": 1,

           "adjudicacion": {
             "n_items": 1
             "monto": 898336,
             "acta": "http://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?qs=777qvIf9crWklR5ZKx61vChI9x6UqF8mhgoiWY+N3uE=",
           }
         },
         [...]
       ]
     }
