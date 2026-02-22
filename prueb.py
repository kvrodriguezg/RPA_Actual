import sys
import os
import json
from datetime import datetime
from FunctionsInsights import FunctionsInsights

# --- PASO 1: MONKEY PATCHING (Interceptar la subida) ---
# Aquí "hackeamos" la función de subida para que NO suba a Azure, sino que guarde localmente.

def MockUploadInformationPortalAzurePais(self, AzurePaisSelect, AzureCompanySelect, AzureClientSelect, AzureOperationSelect, AzureProgramSelect, AzureLenguageInput, AzureDateAudioInput, AzureFilesAudiosInput):
    print("\n🛑 INTERCEPTANDO SUBIDA A AZURE...")
    print(f"   Se procesaron {len(AzureFilesAudiosInput)} audios.")

    for i, AzureFileAudioInput in enumerate(AzureFilesAudiosInput):
        # Reconstrucción idéntica a tu código original
        NumeroLlamada = i + 1
        DateTimeNow = datetime.now()
        FormattedDateTime = DateTimeNow.strftime("%H:%M:%S")
        DateForNameFile = str(DateTimeNow.strftime("%Y%m%d%H%M%S"))
        
        NameFileMp3UploadSpce = str(AzureFileAudioInput[0]).replace(" ", "_")
        # Nota: Usamos una limpieza simple si 're' no está importado, pero tu código usa re.
        # Aquí asumimos que el nombre viene limpio o lo dejamos pasar para el JSON.
        NameFileId = f"{DateForNameFile}_{str(NumeroLlamada)}_{NameFileMp3UploadSpce.replace('.mp3', '')}"
        
        IdAgentAzure = str(AzureFileAudioInput[2])
        AdditionalMetadataAzure = str(AzureFileAudioInput[3]) # <--- AQUÍ ESTÁ LA CLAVE
        
        # Esta es la estructura EXACTA de tu código
        EstructuraJson = '''{
    "company": "''' + str(AzureCompanySelect) + '''",
    "customer": "''' + str(AzureClientSelect) + '''",
    "session": "''' + str(AzureOperationSelect) + '''",
    "sub-session": "''' + str(AzureProgramSelect) + '''",
    "datetime": "''' + str(AzureDateAudioInput) + " " + str(FormattedDateTime) + '''",
    "name": "''' + str(NameFileId) + '''",
    "id_agent": "''' + str(IdAgentAzure) + '''",'''+ str(AdditionalMetadataAzure) + '''
    "audio-language": "''' + str(AzureLenguageInput) + '''"
}'''
        
        # Guardar en disco
        NombreArchivo = f"SIMULACION_{NameFileId}.json"
        with open(NombreArchivo, "w", encoding="utf-8") as f:
            f.write(EstructuraJson)
            
        print(f"✅ JSON Generado: {NombreArchivo}")
        print(f"   Metadata Extra detectada (Variable AdditionalMetadataAzure): [{AdditionalMetadataAzure}]")
        
        if "tenant_id" in AdditionalMetadataAzure:
            print("⚠️ ALERTA: La variable AdditionalMetadataAzure TRAE 'tenant_id'. Viene de la Base de Datos.")
        else:
            print("👍 CORRECTO: La variable AdditionalMetadataAzure está limpia o vacía.")

    return "Exito"

# Aplicamos el parche a la clase
FunctionsInsights.UploadInformationPortalAzurePais = MockUploadInformationPortalAzurePais

# --- PASO 2: EJECUCIÓN CON DATOS REALES DE FACM (ID 19) ---

def EjecutarSimulacion():
    # Configuración extraída de tu AtenaIA.json para el ID 19 (FACM INBOUND)
    # AJUSTA LA FECHA AQUÍ O PÍDELA POR CONSOLA
    FechaProcesoStr = input("Ingrese fecha a procesar (YYYY-MM-DD) [Enter para Hoy]: ")
    if not FechaProcesoStr:
        FechaProceso = datetime.now()
    else:
        FechaProceso = datetime.strptime(FechaProcesoStr, "%Y-%m-%d")

    print(f"🚀 Iniciando simulación para FACM (ID 19) - Fecha: {FechaProceso.strftime('%Y-%m-%d')}")
    print("   Conectando a FTP y Base de Datos reales... Espere...")

    # Instanciamos la clase real
    # "D" es el disco, "JSON" es el tipo (aunque aquí forzamos parámetros manuales)
    RPA = FunctionsInsights("C", "JSON", DateInWorkProcessInsights=FechaProceso)

    # Parámetros HARDCODEADOS del ID 19 (FACM INBOUND) según tu AtenaIA.json
    RPA.ExecuteProcessInsights(
        KeyProcess="19",
        NameConfig="FACM INBOUND",
        ClientProcess="FACM",
        PaisProcess="CHILE",
        TypeConectProcess="SQL_FTP",
        ServerConectProcess="172.29.55.12", # IP FTP
        FoldersConectProcess="/grabaciones/IN_FACM_TELEFONICO/{{DateFolderName}}",
        UserConectProcess="grab_facm_five9_l",
        PassConectProcess="#N^urjc?mu)WWP8,",
        TypeRouteListFolderConnect="", 
        TypeDateProcess="m_d_aaaa",
        
        # Parámetros de Búsqueda SQL (Metadata)
        ProcessSearchParamts={
            "SearchType": "SqlConnectTwo",
            "SearchServer": "172.29.55.125",
            "SearchDatabase": "IASTUDIO",
            "SearchUsername": "Insights",
            "SearchPassword": "(++yeVsHu&*M%ydB",
            "SearchDetalleOne": "vw_metadatos_grabacion_five9",
            "SearchDetalleTwo": "IN_FACM_TELEFONICO"
        },
        
        ConfigSearchMetadataProcess={
            "TypeSearchMetadata": "SqlConnectTwo",
            # IMPORTANTE: Si en tu BD real hay columnas extra configuradas, 
            # el código interno las buscará aunque aquí no las pongamos explícitamente 
            # si la lógica interna consulta la configuración global. 
            # Pero para esta prueba manual, veremos qué trae 'vw_metadatos_grabacion_five9'.
            "ColumnsSearchMetadata": "" # Dejamos vacío para ver si el código lo llena solo o si la vista trae basura.
        },
        
        MaxFilesAudiosProcess=1, # Solo procesamos 1 para probar
        AzureCompany="ATENTO CHILE",
        AzureCustomer="FACM-ATENCIÓN CLIENTES",
        AzureSession="FACM",
        AzureSubSession="INBOUND",
        AzureAudioLanguage="es"
    )

if __name__ == "__main__":
    EjecutarSimulacion()