from Libraries import *
from mutagen.mp3 import MP3
import json
import csv
from datetime import datetime, date, time
from models.Conexion import DatabaseManager
from models.Tablas import TBL_RAZURE_CONFIGURATION

from models.Conexion import DatabaseManager
from models.Tablas import TBL_RAZURE_CONFIGURATION
from sqlalchemy import or_
from time import sleep
from autoit import *

InformationReturnColumnsAdd = ""
ConfigSearchMetadataGlobalProcess = {}
ConfigSearchMetadataGlobalProcess["ColumnsSearchMetadata"] = ""
InsightsSearchInformationAllMetadata = {}
FileSearchCurrent = "0a718ad3-fe10-4866-a24d-b824ae0eff88_3003077999_05118556.mp3"
GlobCsvFiles = glob.glob(f"C:/ProyectosJulio/ATENTO_RPA_PROCESS_AUDIO_INSIGHTS/output/Nueva carpeta/*.csv")
SearchDetalleOne = ""
SearchDetalleTwo = ""
FileSearchModifCurrent = GetUuidFromFileNameGoContactFirst(FileSearchCurrent)
print(f"FileSearchModifCurrent: {FileSearchModifCurrent}")
if GlobCsvFiles:
    for ResFileCsvDownloadMetadata in GlobCsvFiles:
        if os.path.exists(ResFileCsvDownloadMetadata):
            ColumnForReturnProcess = "Agent User Name"

            ColumnForFilterOneProcess = "Campaign Name"
            ColumnForFilterTwoProcess = "Call uuid"
            ColumnForFilterThreeProcess = "Call Outcome Group"
            PandaReadCsv = pd.read_csv(ResFileCsvDownloadMetadata, delimiter=";", dtype=str)
            """ print("PandaReadCsv")
            print(PandaReadCsv)
            print(ColumnForFilterOneProcess in PandaReadCsv.columns)
            print(PandaReadCsv.columns) """
            try:
                if SearchDetalleTwo != "":
                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchModifCurrent, na=False, regex=True) & PandaReadCsv[ColumnForFilterThreeProcess].str.contains(SearchDetalleTwo, na=False, regex=True)]
                elif SearchDetalleOne != "":
                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchModifCurrent, na=False, regex=True)]
                else:
                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchModifCurrent, na=False, regex=True)]
            except:
                ColumnForFilterTwoProcess = "Identificador unico llamada"
                if SearchDetalleTwo != "":
                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchModifCurrent, na=False, regex=True) & PandaReadCsv[ColumnForFilterThreeProcess].str.contains(SearchDetalleTwo, na=False, regex=True)]
                elif SearchDetalleOne != "":
                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchModifCurrent, na=False, regex=True)]
                else:
                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchModifCurrent, na=False, regex=True)]
            print(f"PandaFiltradoReadCsv: {len(PandaFiltradoReadCsv)}")
            print(f"PandaFiltradoReadCsv: {PandaFiltradoReadCsv}")
            PandaCountFiltradoReadCsv = len(PandaFiltradoReadCsv)
            if PandaCountFiltradoReadCsv > 0:
                for PandaIndexReadCsv, PandaRowReadCsv in PandaFiltradoReadCsv.iterrows():
                    try:
                        for ColumnSearchMetadata in ConfigSearchMetadataGlobalProcess["ColumnsSearchMetadata"].split("|"):
                            ReturnColumnAdd = PandaRowReadCsv[ColumnSearchMetadata]
                            ReturnColumnAdd = "" if pd.isna(ReturnColumnAdd) else ReturnColumnAdd
                            if InformationReturnColumnsAdd == '':
                                InformationReturnColumnsAdd = f'"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                            else:
                                InformationReturnColumnsAdd = f'{InformationReturnColumnsAdd}"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                    except:
                        pass
                print(PandaRowReadCsv[ColumnForReturnProcess])
                InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadCsv[ColumnForReturnProcess], InformationReturnColumnsAdd]
    """ InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
else:
    InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd] """

print("InsightsSearchInformationAllMetadata")
print(InsightsSearchInformationAllMetadata)

sys.exit()


WinGetPos = win_get_pos("Kactus 2025 - Actualización en Grupo.")
print(WinGetPos)

""" sys.exit()

win_activate("Contratos")
sleep(5)
Varcontrol_get_text = control_get_text("Contratos", "[CLASS:TDBGridInplaceEdit; INSTANCE:1]")
print(f"Texto obtenido: '{Varcontrol_get_text}'")

sys.exit() """
win_activate("Kactus 2025 - Actualización en Grupo.")
sleep(5)
control_click("Kactus 2025 - Actualización en Grupo.", "", x=225, y=(WinGetPos[3] - WinGetPos[1])-50, button="left", clicks=1)

sys.exit()

def reemplazar_espacios_por_guion():
    """
    Reemplaza los espacios en blanco por guion '-' en las columnas AZU_CCOMPANY y AZU_CCUSTOMER
    de todos los registros de TBL_RAZURE_CONFIGURATION que tengan espacios en esos campos.
    """
    db = DatabaseManager()
    session = db.GetSession()
    try:
        registros = session.query(TBL_RAZURE_CONFIGURATION).filter(
            or_(
                TBL_RAZURE_CONFIGURATION.AZU_CCOMPANY.like('% %'),
                TBL_RAZURE_CONFIGURATION.AZU_CCUSTOMER.like('% %')
            )
        ).all()
        for reg in registros:
            cambiado = False
            if reg.AZU_CCOMPANY and ' ' in reg.AZU_CCOMPANY:
                reg.AZU_CCOMPANY = reg.AZU_CCOMPANY.replace(' ', '-')
                cambiado = True
            if reg.AZU_CCUSTOMER and ' ' in reg.AZU_CCUSTOMER:
                reg.AZU_CCUSTOMER = reg.AZU_CCUSTOMER.replace(' ', '-')
                cambiado = True
            if cambiado:
                print(f"Actualizado ID {reg.AZU_NID}: COMPANY={reg.AZU_CCOMPANY}, CUSTOMER={reg.AZU_CCUSTOMER}")
        session.commit()
        print("Actualización completada.")
    except Exception as e:
        session.rollback()
        print(f"Error actualizando: {e}")
    finally:
        db.CloseSession()

""" if __name__ == "__main__":
    reemplazar_espacios_por_guion() """

# 1. Cargar el CSV a la tabla TBL_RAZURE_CONFIGURATION evitando duplicados
# ----------------------------------------------------------------------
# Lee el archivo CSV generado y sube los datos a la tabla TBL_RAZURE_CONFIGURATION usando SQLAlchemy ORM.
# Solo inserta si no existe ya un registro con la misma combinación de los 5 campos Azure.
# Agrega metadatos de registro requeridos.
def cargar_csv_a_db(ruta_csv):
    db = DatabaseManager()
    session = db.GetSession()
    try:
        with open(ruta_csv, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Verificar si ya existe el registro completo
                existe = session.query(TBL_RAZURE_CONFIGURATION).filter_by(
                    AZU_CCOMPANY=row["AzureCompany"],
                    AZU_CCUSTOMER=row["AzureCustomer"],
                    AZU_CSESSION=row["AzureSession"],
                    AZU_CSUBSESSION=row["AzureSubSession"],
                    AZU_CLANGUAGE=row["AzureAudioLanguage"]
                ).first()
                if existe:
                    continue
                # Buscar registro "vacío" (campos Azure vacíos o nulos)
                vacio = session.query(TBL_RAZURE_CONFIGURATION).filter(
                    (TBL_RAZURE_CONFIGURATION.AZU_CCOMPANY == None) | (TBL_RAZURE_CONFIGURATION.AZU_CCOMPANY == ""),
                    (TBL_RAZURE_CONFIGURATION.AZU_CCUSTOMER == None) | (TBL_RAZURE_CONFIGURATION.AZU_CCUSTOMER == ""),
                    (TBL_RAZURE_CONFIGURATION.AZU_CSESSION == None) | (TBL_RAZURE_CONFIGURATION.AZU_CSESSION == ""),
                    (TBL_RAZURE_CONFIGURATION.AZU_CSUBSESSION == None) | (TBL_RAZURE_CONFIGURATION.AZU_CSUBSESSION == ""),
                    (TBL_RAZURE_CONFIGURATION.AZU_CLANGUAGE == None) | (TBL_RAZURE_CONFIGURATION.AZU_CLANGUAGE == "")
                ).first()
                ahora = datetime.now()
                if vacio:
                    # Actualizar el registro vacío
                    vacio.AZU_CCOMPANY = row["AzureCompany"]
                    vacio.AZU_CCUSTOMER = row["AzureCustomer"]
                    vacio.AZU_CSESSION = row["AzureSession"]
                    vacio.AZU_CSUBSESSION = row["AzureSubSession"]
                    vacio.AZU_CLANGUAGE = row["AzureAudioLanguage"]
                    vacio.AZU_NMAX_AUDIOS_MES = int(row["Suma MaxFilesAudiosPermitted"])
                    vacio.AZU_CESTADO_REGISTRO = 'Activo'
                    vacio.AZU_NCRE_NID_REGISTRO = 1
                    vacio.AZU_DFECHA_REGISTRO = ahora.date()
                    vacio.AZU_DHORA_REGISTRO = ahora.time().replace(microsecond=0)
                else:
                    # Crear nuevo registro
                    nuevo = TBL_RAZURE_CONFIGURATION(
                        AZU_CCOMPANY=row["AzureCompany"],
                        AZU_CCUSTOMER=row["AzureCustomer"],
                        AZU_CSESSION=row["AzureSession"],
                        AZU_CSUBSESSION=row["AzureSubSession"],
                        AZU_CLANGUAGE=row["AzureAudioLanguage"],
                        AZU_NMAX_AUDIOS_MES=int(row["Suma MaxFilesAudiosPermitted"]),
                        AZU_CESTADO_REGISTRO='Activo',
                        AZU_NCRE_NID_REGISTRO=1,
                        AZU_DFECHA_REGISTRO=ahora.date(),
                        AZU_DHORA_REGISTRO=ahora.time().replace(microsecond=0)
                    )
                    session.add(nuevo)
            session.commit()
        print(f"Carga a TBL_RAZURE_CONFIGURATION finalizada.")
    except Exception as e:
        session.rollback()
        print(f"Error al cargar datos en la base: {e}")
    finally:
        db.CloseSession()

# 2. Refactorización: función para generar el CSV resumen desde el JSON
# ---------------------------------------------------------------------
def generar_resumen_azure_csv(ruta_json, ruta_csv):
    """
    Lee la configuración JSON, filtra solo los activos, agrupa por los 5 campos Azure,
    suma MaxFilesAudiosPermitted y exporta el resumen a un CSV compatible con Excel (UTF-8 BOM).
    """
    with open(ruta_json, encoding="utf-8") as f:
        data = json.load(f)
    resumen = {}
    for k, v in data.items():
        if v.get("EstadoConfig") != "Activo":
            continue
        azure = v.get("ConfigJsonAzure", {})
        if not all(x in azure for x in ["AzureCompany", "AzureCustomer", "AzureSession", "AzureSubSession", "AzureAudioLanguage"]):
            continue
        clave = (
            azure["AzureCompany"],
            azure["AzureCustomer"],
            azure["AzureSession"],
            azure["AzureSubSession"],
            azure["AzureAudioLanguage"]
        )
        max_files = v.get("MaxFilesAudiosPermitted", 0)
        resumen[clave] = resumen.get(clave, 0) + (max_files if max_files else 0)
    with open(ruta_csv, "w", newline='', encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "AzureCompany", "AzureCustomer", "AzureSession", "AzureSubSession", "AzureAudioLanguage", "Suma MaxFilesAudiosPermitted"
        ])
        for clave, suma in resumen.items():
            writer.writerow(list(clave) + [suma])
    print(f"Archivo CSV generado en: {ruta_csv}")

def exportar_json_completo_a_csv(ruta_json, ruta_csv):
    """
    Exporta el JSON completo a un CSV/Excel, usando todas las llaves posibles como columnas.
    Si un registro no tiene una llave, el valor queda en blanco.
    """
    with open(ruta_json, encoding="utf-8") as f:
        data = json.load(f)
    # Recolectar todas las llaves posibles (nivel 1 y anidadas)
    all_keys = set()
    registros = []
    for k, v in data.items():
        flat = {"__ID__": k}
        def flatten(d, prefix=""):
            for key, value in d.items():
                if isinstance(value, dict):
                    flatten(value, prefix + key + ".")
                else:
                    flat[prefix + key] = value
        flatten(v)
        registros.append(flat)
        all_keys.update(flat.keys())
    all_keys = ["__ID__"] + sorted([k for k in all_keys if k != "__ID__"])  # __ID__ primero
    with open(ruta_csv, "w", newline='', encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        for reg in registros:
            writer.writerow({k: reg.get(k, "") for k in all_keys})
    print(f"JSON exportado a CSV con todas las llaves en: {ruta_csv}")

# --- USO PRINCIPAL ---
if __name__ == "__main__":
    ruta_json = r"c:\ProyectosJulio\ATENTO_RPA_PROCESS_AUDIO_INSIGHTS\AtenaIA.json"
    ruta_csv = r"c:\ProyectosJulio\ATENTO_RPA_PROCESS_AUDIO_INSIGHTS\ResumenAzure.csv"
    rutaComplet_csv = r"c:\ProyectosJulio\ATENTO_RPA_PROCESS_AUDIO_INSIGHTS\CompletoAzure.csv"
    #generar_resumen_azure_csv(ruta_json, ruta_csv)
    exportar_json_completo_a_csv(ruta_json, rutaComplet_csv)
    #cargar_csv_a_db(ruta_csv)

