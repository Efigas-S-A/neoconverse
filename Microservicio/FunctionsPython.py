import numpy as np
import pandas as pd


# Función para verificar si un valor es numérico
def es_numerico(valor):
    return valor.isnumeric()

def GetUniqueContracts(Contract_clients):
    ### NOS QUEDAMOS CON LOS CONTRATOS UNICOS
    Contracts = Contract_clients.iloc[:,:7][['BK_SUSCCODI']]
    ### ELIMINAMOS LOS DUPLICADOS 
    Contracts_unicos = Contracts.drop_duplicates(subset='BK_SUSCCODI', keep='first')
    return Contracts_unicos

def GetUniqueClients(Contract_clients):
    ### FUNCIÓN PARA OBTENER LOS CLIENTES UNICOS PARA CREAR O ACTUALIZAR LOS NODOS CORRESPONDIENTES
    CLIENTES = Contract_clients.iloc[:,7:]
    ## ELIMINAMOS LA INFORMACIÓN DEL NÚMERO QUE NO ES RELEVANTE
    CLIENTES['TELEFONO'] = np.where(CLIENTES['TELEFONO'].apply(es_numerico), CLIENTES['TELEFONO'], np.nan)
    ## OBTENEMOS SOLO LOS CLIENTES UNICOS A PARTIR DE LA LLAVE BK_CLIENT_ID
    # Eliminar duplicados en la columna 'columna_clave', manteniendo el primer registro
    CLIENTES_unicos = CLIENTES.drop_duplicates(subset='IDENTIFICATION', keep='first')
    return CLIENTES_unicos

def GetuniqueProducts(productos,Contract_clients,products_id):
    ### HACEMOS EL MERGE CON LOS TIPOS DE PRODUCTOS
    ##  HACEMOS EL MERGE
    # Realizar el merge para agregar el nombre al df1
    productos = pd.merge(productos, products_id, how='left', left_on='PRODUCT_TYPE_ID', right_on='IdTipoProducto')
    ### OBTENEMOS EL BK DEL CONTRATO PARA ASOCIAR MEDIANTE DICHA LLAVE
    # Realizar el merge en base a la columna contract_id
    df_product = pd.merge(productos, Contract_clients[['CONTRACT_ID', 'BK_SUSCCODI']], on='CONTRACT_ID', how='left')
    ### TRANSFORMAMOS LAS VARIABLES NECESARIAS
    df_product['PRODUCT_DATE'] = pd.to_datetime(df_product['PRODUCT_DATE'])
    ### OBTENEMOS LOS PRODUCTOS UNICOS
    unique_products = df_product.drop_duplicates(subset='BK_PRODUCT_ID', keep='first')
    ### OBTENEMOS LOS VALORES UNICOS DE CATEGORIA Y SUBCATEGORIA
    unique_categories = list(df_product['CATEGORY_ID'].unique())
    unique_subcategories = list(df_product['SUBCATEGORY_ID'].unique())
    unique_type_product =  list(df_product['PRODUCT_TYPE_ID'])
    return df_product,unique_products,unique_categories,unique_subcategories,unique_type_product



def PreprocessData(df):
    ### LEEMOS EL DATAFRAME QUE CONTIENE LOS CLIENTES Y LAS RELACIONES
    Contract_clients = df
    Contract_clients['IDENTIFICATION'].fillna(0,inplace=True)
    Contract_clients['BK_CLIENT_ID'].fillna(0,inplace=True)
    Contract_clients['VINCULATE_DATE'] = pd.to_datetime(Contract_clients['VINCULATE_DATE'])
    Contract_clients['BIRTHDATE'] = pd.to_datetime(Contract_clients['BIRTHDATE'])
    Contract_clients['CONTRACT_DATE'] = pd.to_datetime(Contract_clients['CONTRACT_DATE'])
    Contract_clients['GENDER'] = Contract_clients['GENDER'].replace('-1', np.nan)
    Contract_clients['TELEFONO']  = Contract_clients['TELEFONO'].astype(str)
    # Contract_clients['IDENTIFICATION'] = Contract_clients['IDENTIFICATION'].astype(int).astype(str)
    # Contract_clients['BK_CLIENT_ID'] = Contract_clients['BK_CLIENT_ID'].astype(int).astype(str)
    #### SEPARAMOS LO QUE SERIAN LOS NODOS RESPECTIVOS
    Contracts =  GetUniqueContracts(Contract_clients)
    clients   =  GetUniqueClients(Contract_clients)
    #### OBTENEMOS LOS VALORES QUE SI TIENEN DIRECCIÓN ASOCIADA
    Contract_clients['Departamento'] = Contract_clients['Departamento'].fillna("No registra")
    Contract_clients['NombreLocalidad'] = Contract_clients['NombreLocalidad'].fillna("No registra")
    #### OBTENEMOS LOS NODOS DE DEPARTAMENTO Y LOCALIDAD
    valores_unicos_departamento = list(Contract_clients['Departamento'].unique())
    valores_unicos_localidad = list(Contract_clients['NombreLocalidad'].unique())
    #### OBTENEMOS LOS PRODUCTOS
    # products = pd.read_csv(path_excel_products)
    # products_id = pd.read_csv(path_excel_productos_id)
    # df_product,unique_products,unique_categories,unique_subcategories,unique_type_product = GetuniqueProducts(products,Contract_clients,products_id)
    return Contracts,clients,Contract_clients,valores_unicos_departamento,valores_unicos_localidad