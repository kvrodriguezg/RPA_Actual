from Libraries import *
from ApiWiseCX import ApiWiseCXRPA

class FunctionsWiseCX():
    def __init__(self, DiskInsightsRPA):
        self.DiskInsightsPandaRPA = DiskInsightsRPA
        self.ApiUrlProcesslWiseCX = "https://api.wcx.cloud/core/v1"
        self.ApiKeyProcesslWiseCX = "8fdd1af408fb47d288bed38f08e84f39"
        self.ApiUserProcesslWiseCX = "atentochile_api"
        self.ColumnForFiltroProcessWiseCX = "Caso: Resuelta"

    def IniciarProcess(self):
        self.DateForFiltroProcessWiseCX = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        self.RouteFolderDownloadWiseCX = os.path.join(f'{self.DiskInsightsPandaRPA}:\\Insights\\', f'ReportWiseCX')
        os.makedirs(self.RouteFolderDownloadWiseCX, exist_ok=True)

    def ExecuteProcessWiseCX(self, PaisProcess, AzureCompany, AzureCustomer, AzureSession, AzureSubSession, AzureAudioLanguage):
        self.IniciarProcess()

        self.PaisGlobalProcess = PaisProcess
        self.AzureGlobalCompany = AzureCompany
        self.AzureGlobalCustomer = AzureCustomer
        self.AzureGlobalSession = AzureSession
        self.AzureGlobalSubSession = AzureSubSession
        self.AzureGlobalAudioLanguage = AzureAudioLanguage
        self.DateInsightsGlobalProcess = self.DateForFiltroProcessWiseCX

        self.CantidadTranscriptionsEncontradosInsights = 0
        self.CantidadTranscriptionsAzureInsights = 0
        try:
            self.ApiObjectWiseCXRPA = ApiWiseCXRPA(self.ApiUrlProcesslWiseCX, self.ApiKeyProcesslWiseCX, self.ApiUserProcesslWiseCX, self.RouteFolderDownloadWiseCX)
            ResOneInitWiseCXRPA, ResExportInitIdWiseCXRPA = self.ApiObjectWiseCXRPA.GetInitDownloadReportIdInsightsWiseCX("158019")
            #ResOneInitWiseCXRPA = "Exito"
            if ResOneInitWiseCXRPA == "Exito":
                ResThreeStatusWiseCX = None
                print(f"Id Reporte a Generar: {ResExportInitIdWiseCXRPA}")
                """ ResExportInitIdWiseCXRPA = "a919412327494ee282a7335a3a20c26f"
                ResExportInitIdWiseCXRPA = "3d7fbfb631834d028f597ad93e6c0a45"
                print(f"Id Reporte a Generar: {ResExportInitIdWiseCXRPA}") """
                ContadorWhileError = 0
                while 1:
                    ResOneStatusWiseCX, ResTwoStatusWiseCX, ResThreeStatusWiseCX = self.ApiObjectWiseCXRPA.GetStatusDownloadReportIdInsightsWiseCX(ResExportInitIdWiseCXRPA)
                    try:
                        ProgressGetStatusDownload = f"Esperando que Procese el Informe: [{'#' * (ResTwoStatusWiseCX // 2)}{'.' * (50 - ResTwoStatusWiseCX // 2)}] {ResTwoStatusWiseCX}%"
                        sys.stdout.write(f"\r{ProgressGetStatusDownload}")
                        sys.stdout.flush()
                    except:
                        print(f"Esperando que Procese el Informe: {ResTwoStatusWiseCX}")
                    if ResOneStatusWiseCX == "Exito":
                        if ResTwoStatusWiseCX >= 100 and ResThreeStatusWiseCX != None:
                            break
                    else:
                        ContadorWhileError += 1
                        if ContadorWhileError > 60:
                            break
                    time.sleep(3)
                print()
                print(f"Nuevo Reporte Generado: {ResThreeStatusWiseCX}")
                if ResThreeStatusWiseCX != None:
                    FileNameZipWiseCX = os.path.basename(urlparse(ResThreeStatusWiseCX).path)
                    print("Descargado El Comprimido Para Procesar...")
                    ResOneDownloadWiseCX, ResTwoDownloadWiseCX = self.ApiObjectWiseCXRPA.GenerarInformeWiseCX(ResThreeStatusWiseCX, FileNameZipWiseCX)
                    if ResOneDownloadWiseCX == "Exito":
                        print("Descargado Correctamente...")
                        print("Leyendo Archivo CSV...")
                        ResFileCsvDownloadWiseCX = ResTwoDownloadWiseCX.replace(".zip", ".csv")

                        PandaReadCsv = pd.read_csv(ResFileCsvDownloadWiseCX, delimiter=",", dtype=str)
                        PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[self.ColumnForFiltroProcessWiseCX].str.contains(self.DateForFiltroProcessWiseCX, na=False)]
                        PandaCountFiltradoReadCsv = len(PandaFiltradoReadCsv)
                        print(f"Archivo CSV Procesado Con {PandaCountFiltradoReadCsv} Registros...")
                        if PandaCountFiltradoReadCsv > 0:
                            ListUploadPortalAzureInformationComplet = []
                            PandaCountReadCsv = 0
                            for PandaIndexReadCsv, PandaRowReadCsv in PandaFiltradoReadCsv.iterrows():
                                PandaCountReadCsv += 1
                                try:
                                    PandaPercengReadCsv = int((PandaCountReadCsv / PandaCountFiltradoReadCsv) * 100)
                                    ProgressGetGenerarJson = f"Descargando y Generando Json: [{PandaCountReadCsv} de {PandaCountFiltradoReadCsv}] [{'#' * (PandaPercengReadCsv // 2)}{'.' * (50 - PandaPercengReadCsv // 2)}] {PandaPercengReadCsv}%"
                                    sys.stdout.write(f"\r{ProgressGetGenerarJson}")
                                    sys.stdout.flush()
                                except:
                                    pass
                                NumberCaseSearch = PandaRowReadCsv["ID: #"]
                                ResReportsJsonCaseWiseCX = self.ApiObjectWiseCXRPA.GenerarReportJsonCaseWiseCX(NumberCaseSearch)
                                JsonCreateSubDataCaseWiseCX = ""
                                CountSecond = 0
                                for ResReportJsonCaseWiseCX in ResReportsJsonCaseWiseCX:
                                    if ResReportJsonCaseWiseCX["type"] == "contact_message":
                                        JsonCreateSubDataCaseWiseCX = f"""{JsonCreateSubDataCaseWiseCX}{("," if JsonCreateSubDataCaseWiseCX != "" else "")}{{
                "start": {CountSecond},
                "end": {CountSecond},
                "text": "[{ConvertirSegundosFormatoHoraMili(CountSecond)} -> {ConvertirSegundosFormatoHoraMili(CountSecond)}] CUSTOMER: {LimpiarTextForJsonCorrect(ResReportJsonCaseWiseCX["content"])}",
                "speaker": "CUSTOMER"
            }}"""
                                    else:
                                        JsonCreateSubDataCaseWiseCX = f"""{JsonCreateSubDataCaseWiseCX}{("," if JsonCreateSubDataCaseWiseCX != "" else "")}{{
                "start": {CountSecond},
                "end": {CountSecond},
                "text": "[{ConvertirSegundosFormatoHoraMili(CountSecond)} -> {ConvertirSegundosFormatoHoraMili(CountSecond)}] AGENT: {LimpiarTextForJsonCorrect(ResReportJsonCaseWiseCX["content"])}",
                "speaker": "AGENT"
            }}"""
                                    CountSecond += 1
                                JsonWithTranscription = f'''{{
    "audio_info": {{
        "duration_sec": {CountSecond}.00,
        "extraction_metadata": {{
            "country": "{self.PaisGlobalProcess}",
            "company": "{self.AzureGlobalCompany}",
            "customer": "{self.AzureGlobalCustomer}",
            "session": "{self.AzureGlobalSession}",
            "sub-session": "{self.AzureGlobalSubSession}",
            "audio-language": "{self.AzureGlobalAudioLanguage}",
            "datetime": "{PandaRowReadCsv[self.ColumnForFiltroProcessWiseCX]}"
        }}
    }},
    "transcription_items": {{
        "transcription": [
            {JsonCreateSubDataCaseWiseCX}
        ]
    }}
}}
'''
                                ListUploadPortalAzureInformationComplet.append((NumberCaseSearch, JsonWithTranscription))
                            print()
                            self.UploadInformationPortalAzurePais("COLOMBIA", "PRUEBA_TEXTO", self.DateInsightsGlobalProcess, ListUploadPortalAzureInformationComplet)
                        else:
                            print("No se encontraron registros para procesar WiseCX...")
        except Exception as e:
            print(f"Error WiseCX en línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        finally:
            ClearDirectoryMajor(self.RouteFolderDownloadWiseCX)

    def UploadInformationPortalAzurePais(self, AzurePaisSelect, AzureClientSelect, AzureDateAudioInput, AzureInformationsNameJsonInput):
        #Funcion para subir los archivos a Azure
        ContainerName = 'transcriptions'
        FolderName = f'input/{AzureClientSelect}/'#Cambio Por recomentacion de Brasil
        if AzurePaisSelect == "COLOMBIA":
            StorageAccountName = 'sausaistudioco'
            StorageAccountKey = os.environ.get('AZURE_STORAGE_KEY_CO', '')
        elif AzurePaisSelect == "CHILE":
            StorageAccountName = 'sausaistudiocl'
            StorageAccountKey = os.environ.get('AZURE_STORAGE_KEY_CL', '')
        elif AzurePaisSelect == "ARGENTINA":
            StorageAccountName = 'sausaistudioar'
            StorageAccountKey = os.environ.get('AZURE_STORAGE_KEY_AR', '')
        else:
            return "Error"

        BlobResponseServiceClient = BlobServiceClient(
            account_url=f"https://{StorageAccountName}.blob.core.windows.net",
            credential=StorageAccountKey
        )
        ContainerClient = BlobResponseServiceClient.get_container_client(ContainerName)
        BlobFolderClient = ContainerClient.get_blob_client(FolderName)
        
        DateObject = datetime.strptime(AzureDateAudioInput, "%Y-%m-%d")
        AzureDateAudioInput = DateObject.strftime("%d/%m/%Y")

        TotalFilesJson = len(AzureInformationsNameJsonInput)
        NumeroJson = 1
        for AzureInformationNameJsonInput in AzureInformationsNameJsonInput:
            #print(f"Subiendo archivo {NumeroJson} de {TotalFilesJson}...")
            try:
                PandaPercengAzure = int((NumeroJson / TotalFilesJson) * 100)
                ProgressGetGenerarJson = f"Cargando Json A Azure Insights: [{NumeroJson} de {TotalFilesJson}] [{'#' * (PandaPercengAzure // 2)}{'.' * (50 - PandaPercengAzure // 2)}] {PandaPercengAzure}%"
                sys.stdout.write(f"\r{ProgressGetGenerarJson}")
                sys.stdout.flush()
            except:
                pass
            DateTimeNow = datetime.now()
            DateForNameFile = str(DateTimeNow.strftime("%Y%m%d%H%M%S"))
            if AzureInformationNameJsonInput:
                NameFileJsonUploadSpce = str(AzureInformationNameJsonInput[0]).replace(" ", "_")
                NameFileJsonUpload = re.sub(r'[^a-zA-Z0-9]', '', NameFileJsonUploadSpce)
                NameFileId = f"{DateForNameFile}_{str(NumeroJson)}_{str(NameFileJsonUpload)}"
                try:
                    #Aqui Inicia Json
                    FileNameJsonAzureFileAudioInput = f"{NameFileId}.json"
                    BlobNameJson = f"{FolderName}{FileNameJsonAzureFileAudioInput}"
                    BlobFileClientJson = ContainerClient.get_blob_client(BlobNameJson)
                    if not BlobFileClientJson.exists():
                        BlobFileClientJson.upload_blob(AzureInformationNameJsonInput[1], overwrite=True)
                        self.CantidadTranscriptionsAzureInsights += 1
                except:
                    pass
            NumeroJson += 1
        print()
        return "Exito"

    def ValidateDateChange(self, DateType, DateChange):#Fecha en Fomato AAAA-MM-DD
        DateReturn = False
        DateObjeChange = datetime.strptime(DateChange, "%Y-%m-%d") 
        if DateType == "aaaammdd":
            DateReturn = DateObjeChange.strftime("%Y%m%d")
        elif DateType == "d_m_aaaa":
            DateReturn = DateObjeChange.strftime("%#m_%#d_%Y")
        return DateReturn

    def ClearDirectoryMajor(self):
        """Limpia los directorios de trabajo después de procesar los archivos"""
        try:
            shutil.rmtree(self.RouteFolderDownloadWiseCX)
            print("Directorios limpiados correctamente")
        except Exception as e:
            print(f"Error al limpiar directorios: {e}")

    def UpdateInformeGeneralProcessInsights(self, nuevos_datos):
        with open(self.RouteFileReportBaseProcess, 'a', encoding='utf-8') as f:
            f.write(";".join(map(str, nuevos_datos)) + "\n")





