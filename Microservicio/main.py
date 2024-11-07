from fastapi import FastAPI,File,UploadFile
from fastapi.responses import JSONResponse
from typing import Union
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
from neo4j import GraphDatabase
from FunctionsNeo4j import generateLinksArticulo,createOrUpdateClients,createOrUpdateContract,generateLinks,generateLinksProduct,createOrUpdateProducts,createOrUpdateDepartment,createOrUpdateLocation,generateLinksLocations,generateCategories,generateSubCategories,generateTypeProduct,generateLinksCategoryAndSubCategory,generateLinksProvedor,generateLinksArticulos,generateLinksAsesor,generateLinksCodeudor,generateLinksTitular,generateLinksCliente,createOrUpdateProvedor,createOrUpdateArticulo,createOrUpdateVentas
from FunctionsPython import PreprocessData
import pyodbc
import os 

uri = "bolt://localhost:7687"
username = "neo4j"
password = "JosephEfigas"  # replace with your database's password
driver = GraphDatabase.driver(uri, auth=(username, password))


app= FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)
#----------------------------FUNCIONES-------------------------------------#
def conexion_neo4j():
    # Connection to the Neo4j database
    uri = "bolt://localhost:7687"
    username = "joseph"
    password = "Joseph"  # replace with your database's password
    driver = GraphDatabase.driver(uri, auth=(username, password))
def conexion():
    configFile = 'C:/Users/jhosg/Documents/Trabajo/EFIGAS/ConfiguresData.cfg'
    conex1 = 'DRIVER={ODBC Driver 18 for SQL Server};'
    conex2 = 'SERVER={server};DATABASE={database};UID={username};PWD={password}'
    with open(configFile, 'r') as f:
        server = f.readline().replace('\n','')
        database = f.readline().replace('\n','')
        username = f.readline().replace('\n','')
        password = f.readline().replace('\n','')
        conex3 = conex1+conex2.format(server=server,database=database,username=username,password=password)
    print("cadena de conexión : ",conex3)
    cnxn = pyodbc.connect(conex3)
    cnxn.autocommit = True
    return(cnxn)

def consulta_sql_server_contracts_clients(id_list_contratos,cnxn):
    query=f'''
    WITH LatestTelefonos AS (
            SELECT
                IDENTIFICATION,
                DATA_TYPE_ID AS ID,
        
                DATA AS TELEFONO,
                ROW_NUMBER() OVER (PARTITION BY IDENTIFICATION ORDER BY DATA_DATE DESC) AS rn
            FROM [GD].[FACT_ENTITY_DATA] cte
            WHERE cte.[DATA_TYPE_ID] = 2
        
        ),
        PreviousResults AS (
            SELECT DISTINCT
                co.[CONTRACT_ID] AS [CONTRATO],
                co.[BK_SUSCCODI],
                dct.[DESCRIPTION] AS RELATION,
                co.[CONTRACT_DATE],
                co.[CLIENT_ID],
                gl.[NombreLocalidad],
                gl.[Departamento],
                cc.[CORREO] AS CORREO,
                cte.[TELEFONO] AS TELEFONO,
                cl.[BK_CLIENT_ID],
                cl.[IDENTIFICATION],
                it.[DESCRIPTION] AS IDENTIFICATION_TYPE,
                ct.[DESCRIPTION] AS CLIENT_TYPE,
                cl.[FIRST_NAME],
                cl.[LAST_NAME],
                cl.[GENDER],
                cl.[VINCULATE_DATE],
                cl.[BIRTHDATE]
            FROM [GD].[DIM_CONTRACT] co
            LEFT JOIN [GD].[DIM_CLIENT] cl ON co.[CLIENT_ID] = cl.[CLIENT_ID]
            LEFT JOIN [GD].[DimCorreoCliente] cc ON cl.[IDENTIFICATION] = cc.[IDENTIFICATION]
            LEFT JOIN [GD].[DIM_IDENTIFICATION_TYPE] it ON cl.[IDENT_TYPE_ID] = it.[IDENT_TYPE_ID]
            LEFT JOIN [GD].[DIM_CLIENT_TYPE] ct ON cl.[CLIENT_TYPE_ID] = ct.[CLIENT_TYPE_ID]
            LEFT JOIN [GD].[DIM_CONTRACT_TYPE] dct ON co.[CONTRACT_TYPE_ID] = dct.[CONTRACT_TYPE_ID]
            LEFT JOIN LatestTelefonos cte ON cl.[IDENTIFICATION] = cte.[IDENTIFICATION] AND cte.rn = 1
            LEFT JOIN [Comun].[DimContrato] g ON co.[BK_SUSCCODI] = g.[Contrato]
            LEFT JOIN [Comun].[DimLocalidad] gl ON gl.[IdLocalidad] = g.[IdLocalidad]
            AND cl.[FIRST_NAME] NOT IN ('CLIENTE','GENERICO')
            AND cl.[LAST_NAME] !='GENERICO'
            WHERE co.[BK_SUSCCODI] != -1
            AND co.[BK_SUSCCODI] IN {id_list_contratos}
    )
    SELECT * FROM PreviousResults;
    '''
    try:
        df = pd.read_sql(query, cnxn)
    except pyodbc.Error as e:
        print(f'Error al ejecutar la consulta: {e}')
        return None
    return df

def create_and_analyze_graph(tx):
    tx.run('''CALL gds.graph.project(
    "mygraphComponents2",
    ["Contrato","Cliente","Venta","Articulo"],
    {
        type1: {
            type: 'COMPRÓ'

        },
        type2: {
            type: 'DEUDOR'

        },
        type3: {
            type: 'CONTIENE_VENTA'

        },
        type4: {
            type: 'TITULAR'
        },
        type5: {
            type: 'ASESOR'
        },
        type6: {
            type: 'CODEUDOR'
        },
        type7: {
            type: 'Presento_Fraude'
        },
        type8: {
            type: 'Solicitud'

        },
        type9: {
            type: 'Pagaré'
        },
        type10: {
            type: 'Suscriptor'
        },
        type11: {
            type: 'Portal Efigas'
        }
    }
)''')
    # Run a GDS algorithm, e.g., PageRank
    result = tx.run("CALL gds.pageRank.stream('fraudGraph') YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS contract, score ORDER BY score DESC")
    # Print results
    for record in result:
        print(record['contract'], record['score'])

def add_fraudulent_contracts(tx, contracts):
    for contract in contracts:
        tx.run("CREATE (c:Contract {id: $id, name: $name, value: $value})",
               id=contract['id'], name=contract['name'], value=contract['value'])
#----------------------------/FUNCIONES-------------------------------------#


@app.post("/upload")
async def read_root(file: UploadFile = File(...)):
    if file.filename.endswith('.csv'):
        df=pd.read_csv(file.file)
    elif file.filename.endswith('.xlsx'):
        df=pd.read_excel(file.file)
    else:
        return JSONResponse(status_code=400, content={"message": "File format not supported"})
    
    conn=conexion()
    print("informacion del archivo :",df.info())
    id_contratos=df['CONTRATO INICIAL'].dropna().copy().unique()
    id_contratos=tuple(id_contratos.astype(int).astype(str))
    df_info_fraud_contracts=consulta_sql_server_contracts_clients(id_contratos,conn)
    print("informacion del datawarehouse :",df_info_fraud_contracts.info())

    Contracts,clients,Contract_clients,valores_unicos_departamento,valores_unicos_localidad = PreprocessData(df_info_fraud_contracts)

    print("informacion de contratos procesada :",Contracts)

     #### CREAMOS LOS CLIENTES
    createOrUpdateContract(Contracts,'neo4j')

    # # ##### CREAMOS LOS CONTRATOS
    createOrUpdateClients(clients,'neo4j')


    #  ### CREAMOS LOS DEPARTAMENTOS
    #  for start in range(0, len(valores_unicos_departamento)):
    #          batch_df = valores_unicos_departamento[start:start ]
    #          createOrUpdateDepartment(batch_df,database)
    
    #  ### CREAMOS LAS LOCALIDADES
    #  for start in range(0, len(valores_unicos_localidad)):
    #          batch_df = valores_unicos_localidad[start:start ]
    #          createOrUpdateLocation(batch_df,database)
    
    await file.close()  
    return JSONResponse(status_code=200, content={
        "message": "File processed successfully",
    })

