import numpy as np
import pandas as pd
from neo4j import GraphDatabase
import os



##---------------------------------------------FUNCION CREACION DE NODOS VENTAS -------------------------------------------------##
def createOrUpdateVentas(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()

        Query = """
        UNWIND $batch as row
        MERGE (n:Venta {ID_VENTA_SISTEMABRILLA:row.ID_VENTA_SISTEMABRILLA} )
        ON CREATE SET n.VALOR_VENTA = row.VALOR_VENTA , n.VALOR_FINANCIADO = row.VALOR_FINANCIADO, n.VALOR_CUOTA = row.VALOR_CUOTA, n.VALOR_SIN_IVA = row.VALOR_SIN_IVA, n.VALOR_EXTRACUPO = row.VALOR_EXTRACUPO, n.CUOTA_INICIAL = row.CUOTA_INICIAL, n.DEPARTAMENTO = row.DEPARTAMENTO, n.LOCALIDAD = row.LOCALIDAD,n.ID_VENTA_SISTEMABRILLA = row.ID_VENTA_SISTEMABRILLA,n.FECHA_REGISTRO = row.FECHA_REGISTRO,n.FECHA_ENTREGA = row.FECHA_ENTREGA,n.FECHA_REVISION = row.FECHA_REVISION
        ON MATCH SET n.VALOR_VENTA = row.VALOR_VENTA, n.VALOR_FINANCIADO = row.VALOR_FINANCIADO, n.VALOR_CUOTA = row.VALOR_CUOTA, n.VALOR_SIN_IVA = row.VALOR_SIN_IVA, n.VALOR_EXTRACUPO = row.VALOR_EXTRACUPO, n.CUOTA_INICIAL = row.CUOTA_INICIAL, n.DEPARTAMENTO = row.DEPARTAMENTO, n.LOCALIDAD = row.LOCALIDAD,n.ID_VENTA_SISTEMABRILLA = row.ID_VENTA_SISTEMABRILLA,n.FECHA_REGISTRO = row.FECHA_REGISTRO,n.FECHA_ENTREGA = row.FECHA_ENTREGA,n.FECHA_REVISION = row.FECHA_REVISION
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch =  batch
        )
#--------------------------------------------------------FUNCION CREACION DE NODOS DE ARTICULOS --------------------------------------------------#
def createOrUpdateArticulo(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        Query = """
        UNWIND $batch as row
        MERGE (n:Articulo {ID_PRODUCTO:row.ID_PRODUCTO} )
        ON CREATE SET n.DESCRIPCION_ARTICULO = row.DESCRIPCION_ARTICULO, n.MARCA = row.MARCA, n.ARTICULO = row.ARTICULO,n.LINEA = row.LINEA,n.SUBLINEA = row.SUBLINEA,n.LINEA_NEGOCIO = row.LINEA_NEGOCIO
        ON MATCH SET n.DESCRIPCION_ARTICULO = row.DESCRIPCION_ARTICULO, n.MARCA = row.MARCA, n.ARTICULO = row.ARTICULO,n.LINEA = row.LINEA,n.SUBLINEA = row.SUBLINEA,n.LINEA_NEGOCIO = row.LINEA_NEGOCIO
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch =  batch
            
        )
#--------------------------------------------------------FUNCION CREACION DE NODOS DE PROVEDOR --------------------------------------------------#
def createOrUpdateProvedor(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()      
        Query = """
        UNWIND $batch as row
        MERGE (n:Proveedor {ID_PROVEEDOR_VENTA:row.ID_PROVEEDOR_VENTA} )
        ON CREATE SET n.PROVEEDOR_VENTA = row.PROVEEDOR_VENTA, n.TIPO_PROVEDOR = row.TIPO_PROVEDOR,n.UNIDAD_OPERATIVA = row.UNIDAD_OPERATIVA
        ON MATCH SET  n.PROVEEDOR_VENTA = row.PROVEEDOR_VENTA, n.TIPO_PROVEDOR = row.TIPO_PROVEDOR,n.UNIDAD_OPERATIVA = row.UNIDAD_OPERATIVA
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch =  batch

        )

#---------------------------------------------------relacion CLIENTE ------------------------------------#
def generateLinksCliente(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()

        Query = """
        UNWIND $batch as row
        MERGE (n:Cliente {IDENTIFICATION:row.IDENTIFICACION} )
        MERGE (c:Contrato {BK_SUSCCODI: row.CONTRATO})
        MERGE (v:Venta {ID_VENTA_SISTEMABRILLA: row.ID_VENTA_SISTEMABRILLA})
        MERGE (n)<-[r:DEUDOR]-(v)<-[l:CONTIENE_VENTA]-(c)
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch = batch

        )


def generateLinksTitular(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        Query = """
        UNWIND $batch as row
        MERGE (n:Cliente {IDENTIFICATION:row.IDENTIFICACION_TITULAR} )
        MERGE (v:Venta {ID_VENTA_SISTEMABRILLA: row.ID_VENTA_SISTEMABRILLA})
        MERGE (n)<-[r:TITULAR]-(v)
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch =  batch
        )
def generateLinksCodeudor(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()

        Query = """
        UNWIND $batch as row
        MERGE (n:Cliente {IDENTIFICATION:row.IDENTIFICACION_CODEUDOR} )
        MERGE (v:Venta {ID_VENTA_SISTEMABRILLA:row.ID_VENTA_SISTEMABRILLA})
        MERGE (n)<-[r:CODEUDOR]-(v)
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch =  batch
        )

def generateLinksAsesor(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        
        Query = """
        UNWIND $batch as row
        MERGE (n:Cliente {IDENTIFICATION:row.IDENTIFICACION_ASESOR} )
        MERGE (v:Venta {ID_VENTA_SISTEMABRILLA: row.ID_VENTA_SISTEMABRILLA})
        MERGE (n)<-[r:ASESOR]-(v)
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch=batch

        )
#---------------------------------------------------relacion articulo ------------------------------------#
def generateLinksArticulos(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()

        Query = """
        UNWIND $batch as row
        MATCH (v:Venta {ID_VENTA_SISTEMABRILLA: row.ID_VENTA_SISTEMABRILLA})
        MATCH (a:Articulo {ID_PRODUCTO: row.ID_PRODUCTO})
        MERGE (v)-[r:COMPRÓ {cantidad: row.CANTIDAD_ARTICULO}]->(a)
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch =  batch

        )


#---------------------------------------------------relacion provedor ------------------------------------#
def generateLinksArticulo(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        

        Query = """
        UNWIND $batch as row
        MATCH (v:Venta {ID_VENTA_SISTEMABRILLA: row.ID_VENTA_SISTEMABRILLA})
        MATCH (a:articulo {ID_PRODUCTO: row.ID_PRODUCTO})
        MERGE (a)<-[k:SE_COMPRO{CANTIDAD:row.CANTIDAD_ARTICULO}]-(v)
        """

        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch = batch
        )


def generateLinksProvedor(database,batch):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
    
        Query = """
        UNWIND $batch as row
        MATCH (v:Venta {ID_VENTA_SISTEMABRILLA: row.ID_VENTA_SISTEMABRILLA})
        MATCH (p:Proveedor {ID_PROVEEDOR_VENTA: row.ID_PROVEEDOR_VENTA})
        MERGE (v)-[r:VENTA_REALIZADA_POR]->(p)
        """

        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch = batch

        )
##----------------------------------------------------------------------------------------------------------------------------------##






















def createOrUpdateContract(batch,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J = os.getenv('PASSWORD_NEO4J')
    print(PASSWORD_NEO4J,URI_NEO4J,USER_NEO4J)
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J, auth=(USER_NEO4J,PASSWORD_NEO4J)) as driver:
        driver.verify_connectivity()
        print("CREANDO CONTRATOS")
        Query = """
        UNWIND $batch as row
        MERGE (n:Contrato {BK_SUSCCODI: row.BK_SUSCCODI})
        WITH n, row  
        MATCH (f:Fraude {id:1})  
        MERGE (n)-[r:Presento_Fraude]->(f)
        """
        records, summary, keys = driver.execute_query(
            Query,
            batch=batch,
            database_=database
        )

def createOrUpdateDepartment(batch,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("CREANDO DEPARTAMENTOS")
        # Get the name of all 42 year-olds
        Query = """
        UNWIND $batch as row
        WITH row
        WHERE row <> 'No registra'
        MERGE (n:Departamento {name: row})
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch = batch
        )

def createOrUpdateLocation(batch,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("creando localidades")
        # Get the name of all 42 year-olds

        Query = """
        UNWIND $batch as row

        WITH row
        WHERE row <> 'No registra'
        MERGE (n:Localidad {name: row})
        """
        records, summary, keys = driver.execute_query(
            Query,
            batch = batch,
            database_=database,
        )


def createOrUpdateProducts(dataframe,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("Connection estabilished.")
        # Get the name of all 42 year-olds
        for index, row in dataframe.iterrows():
            BK_PRODUCT_ID = row['BK_PRODUCT_ID']
            PRODUCT_DATE = row['PRODUCT_DATE']
            PRODUCT_TYPE = row['DescripcionTipoProducto'].replace(' ','_')
            Query = f"""
            MERGE (n:{PRODUCT_TYPE} {{BK_PRODUCT_ID: $BK_PRODUCT_ID}})
            ON CREATE SET  n.PRODUCT_DATE = $PRODUCT_DATE
            ON MATCH SET   n.PRODUCT_DATE = $PRODUCT_DATE
            """
            records, summary, keys = driver.execute_query(
                Query,
                BK_PRODUCT_ID = BK_PRODUCT_ID,
                PRODUCT_DATE = PRODUCT_DATE,
                database_=database,
            )

def generateLinksProduct(dataframe,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("Connection estabilished.")
        # Get the name of all 42 year-olds
        for index, row in dataframe.iterrows():
            SELECT_CONTRACT = row['BK_SUSCCODI']
            SELECT_PRODUCT = row['BK_PRODUCT_ID']
            SELECT_TYPE = row['DescripcionTipoProducto'].replace(' ','_')
            RELATION = 'contiene_producto'

            Query = f"""
            MATCH (contrato:Contrato {{BK_SUSCCODI: $SELECT_CONTRACT}})
            MATCH (producto:{SELECT_TYPE} {{BK_PRODUCT_ID: $SELECT_PRODUCT}})
            MERGE (producto)<-[r:{RELATION}]-(contrato)
            """
            records, summary, keys = driver.execute_query(
                Query,
                SELECT_CONTRACT = SELECT_CONTRACT,
                SELECT_PRODUCT = SELECT_PRODUCT,
                database_=database,
            )


def generateLinksLocations(batch,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("GENERANDO LINKS LOCALIDADES")
        # Get the name of all 42 year-olds
        Query = """
        UNWIND $batch as row
        WITH row
        
        WHERE row.Departamento <> 'No registra' AND row.NombreLocalidad <> 'No registra'
        MATCH (contrato:Contrato {BK_SUSCCODI: row.BK_SUSCCODI})
        MATCH (departamento:Departamento {name: row.Departamento})
        MATCH (localidad:Localidad {name: row.NombreLocalidad})
        MERGE (departamento)<-[r:pertenece_a_departamento]-(contrato)-[a:pertenece_a_localidad]->(localidad)
        """
        records, summary, keys = driver.execute_query(
            Query,
            batch=batch,
            database_=database,
        )


def generateLinks(batch, database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 = os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')

    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2, PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("LINKS CLIENTES CONTRATOS")
        
        # Cypher query utilizando apoc.create.relationship para crear la relación dinámicamente
        Query = """
        UNWIND $batch AS row
        MATCH (contrato:Contrato {BK_SUSCCODI: row.BK_SUSCCODI})
        MATCH (cliente:Cliente {IDENTIFICATION: row.IDENTIFICATION})
        OPTIONAL MATCH (cliente)-[r]->(contrato)
        WHERE type(r) = row.RELATION AND r.CONTRACT_DATE = row.CONTRACT_DATE
        WITH row, cliente, contrato, r
        WHERE r IS NULL
        CALL apoc.create.relationship(cliente, row.RELATION, {CONTRACT_DATE: row.CONTRACT_DATE}, contrato) YIELD rel
        RETURN rel
        """
        
        # Ejecutar la consulta
        records, summary, keys = driver.execute_query(
            Query,
            batch=batch,
            database_=database,
        )


### RELACIONES PARA ELEMENTOS DEL PRODUCTO

def generateCategories(lista,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("Connection estabilished.")
        # Get the name of all 42 year-olds
        for  category in lista:
    
            Query = """
            MERGE (n:Categoria {key: $category})
            """
            records, summary, keys = driver.execute_query(
                Query,
                category = category,
                database_=database,
            )

def generateSubCategories(lista,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("Connection estabilished.")
        # Get the name of all 42 year-olds
        for  subcategory in lista:
    
            Query = """
            MERGE (n:Subcategoria {key: $subcategory})
            """
            records, summary, keys = driver.execute_query(
                Query,
                subcategory = subcategory,
                database_=database,
            )


def generateTypeProduct(lista,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("Connection estabilished.")
        # Get the name of all 42 year-olds
        for  tipo_producto in lista:
    
            Query = """
            MERGE (n:tipo_producto {key: $tipo_producto})
            """
            records, summary, keys = driver.execute_query(
                Query,
                tipo_producto = tipo_producto,
                database_=database,
            )


#### FUNCIONES
def createOrUpdateClients(batch,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("CREANDO CLIENTES")
        # Get the name of all 42 year-olds
        Query = """
        UNWIND $batch as row
        MERGE (n:Cliente {IDENTIFICATION: row.IDENTIFICATION})
        ON CREATE SET n.CORREO = row.CORREO, n.TELEFONO = row.TELEFONO, n.BK_CLIENT_ID = row.BK_CLIENT_ID, n.IDENTIFICATION_TYPE = row.IDENTIFICATION_TYPE, n.CLIENT_TYPE = row.CLIENT_TYPE, n.FIRST_NAME = row.FIRST_NAME, n.LAST_NAME = row.LAST_NAME, n.GENDER = row.GENDER, n.VINCULATE_DATE = row.VINCULATE_DATE, n.BIRTHDATE = row.BIRTHDATE
        ON MATCH SET n.CORREO = row.CORREO, n.TELEFONO = row.TELEFONO, n.IDENTIFICATION = row.IDENTIFICATION, n.IDENTIFICATION_TYPE = row.IDENTIFICATION_TYPE, n.CLIENT_TYPE = row.CLIENT_TYPE, n.FIRST_NAME = row.FIRST_NAME, n.LAST_NAME = row.LAST_NAME, n.GENDER = row.GENDER, n.VINCULATE_DATE = row.VINCULATE_DATE, n.BIRTHDATE = row.BIRTHDATE
        """
        records, summary, keys = driver.execute_query(
            Query,
            database_=database,
            batch=batch
        )
            
            

def generateLinksCategoryAndSubCategory(dataframe,database):
    ### LEEMOS LAS CREDENCIALES DE LA BASE DE DATOS.
    URI_NEO4J_V22 =  os.getenv('URI_NEO4J_V2')
    USER_NEO4J_V2 = os.getenv('USER_NEO4J_V2')
    PASSWORD_NEO4J_V2 = os.getenv('PASSWORD_NEO4J_V2')
    ### GENERAMOS LA CONEXIÓN A LA BASE DE DATOS
    with GraphDatabase.driver(URI_NEO4J_V22, auth=(USER_NEO4J_V2,PASSWORD_NEO4J_V2)) as driver:
        driver.verify_connectivity()
        print("Connection estabilished.")

        for index, row in dataframe.iterrows():
            SELECT_PRODUCT = row['BK_PRODUCT_ID']
            SELECT_CATEGORY = row['CATEGORY_ID']
            SELECT_SUBCATEGORY = row['SUBCATEGORY_ID']
            SELECT_TYPEPRODUCT = row['DescripcionTipoProducto'].replace(' ','_')
            RELATION_CATEGORY = 'tiene_categoria'
            RELATION_SUBCATEGORY = 'tiene_subCategoria'
            RELATION_TYPEPRODUCT = 'Tipo'
            print(SELECT_PRODUCT,type(SELECT_CATEGORY),type(SELECT_SUBCATEGORY))
            Query = f"""
            MATCH (producto:{SELECT_TYPEPRODUCT} {{BK_PRODUCT_ID: $SELECT_PRODUCT}})
            MATCH (categoria:Categoria {{key: $SELECT_CATEGORY}})
            MATCH (subCategoria:Subcategoria {{key: $SELECT_SUBCATEGORY}})
            MERGE    (subCategoria)<-[b:{RELATION_SUBCATEGORY}]-(producto)-[r:{RELATION_CATEGORY}]->(categoria)
            """
            records, summary, keys = driver.execute_query(
                Query,
                SELECT_PRODUCT = SELECT_PRODUCT,
                SELECT_CATEGORY = SELECT_CATEGORY,
                SELECT_SUBCATEGORY = SELECT_SUBCATEGORY,
                database_=database,
            )
            ### REALIZAMOS LA RELACIÓN CON EL TIPO DE PRODUCTO

            # Query_2 = f"""
            # MATCH (producto:Producto {{BK_PRODUCT_ID: $SELECT_PRODUCT}})
            # MATCH (tipo_producto:tipo_producto {{key: $SELECT_TYPEPRODUCT}})
            # MERGE (producto)-[r:{RELATION_TYPEPRODUCT}]->(tipo_producto)
            # """

            # records, summary, keys = driver.execute_query(
            #     Query_2,
            #     SELECT_PRODUCT = SELECT_PRODUCT,
            #     SELECT_TYPEPRODUCT = SELECT_TYPEPRODUCT,
            #     database_="neo4j",
            # )

