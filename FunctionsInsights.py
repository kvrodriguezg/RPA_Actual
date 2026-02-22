from Libraries import *
from models.Conexion import DynamicDbConnection
from models.Tablas import *
from models.Conexion import DatabaseManager
from datetime import datetime

class FunctionsInsights():

    def __init__(self, DiskInsightsRPA, TypeExecuteInsightsProcess, DateInWorkProcessInsights = None):
        self.DiskInsightsPandaRPA = DiskInsightsRPA
        self.TypeExecutePandaInsightsProcess = TypeExecuteInsightsProcess
        self.DateInWorkProcessInsights = DateInWorkProcessInsights
        self.RouteFolderBaseProcess = None
        self.RouteFolderBaseWavProcess = None
        self.RouteFolderBaseMp3Process = None
        self.ConnectDownloadFTP = None
        self.ConnectDownloadSFTP = None
        self.FilesCsvDownloadMetadata = []

        self.MaxAddFilesAudiosGlobalProcess = 5

    def __UpdateDateInsights__(self, DateInWorkProcessInsights):
        if DateInWorkProcessInsights != None:
            self.DateInWorkProcessInsights = DateInWorkProcessInsights

    def IniciarProcess(self):
        if self.DateInWorkProcessInsights == None:
            self.DateInWorkProcessInsights = (datetime.now() - timedelta(days=1))
        self.DateWorkProcessInsights = self.DateInWorkProcessInsights
        self.RouteFileReportBaseMonthProcess = os.path.join(f'{self.DiskInsightsPandaRPA}:\\InsightsMesReportes\\', f'Informe_RPA_{self.DateWorkProcessInsights.strftime("%Y%m")}.csv')
        self.RouteFileReportBaseProcess = os.path.join(f'{self.DiskInsightsPandaRPA}:\\InsightsReportes\\', f'Informe_RPA_{self.DateWorkProcessInsights.strftime("%Y%m%d")}.csv')
        self.CreateRouteReportInsights(self.RouteFileReportBaseMonthProcess)
        self.CreateRouteReportInsights(self.RouteFileReportBaseProcess)

    def ExecuteProcessInsights(self, KeyProcess, NameConfig, ClientProcess, PaisProcess, TypeConectProcess, ServerConectProcess, FoldersConectProcess, UserConectProcess, PassConectProcess, TypeRouteListFolderConnect, TypeDateProcess, ProcessSearchParamts, ConfigSearchMetadataProcess,  MaxFilesAudiosProcess, AzureCompany, AzureCustomer, AzureSession, AzureSubSession, AzureAudioLanguage, DateInWorkProcessInsights=None, IdInsightsDateWorkProcess=0):
        self.__UpdateDateInsights__(DateInWorkProcessInsights)
        self.IniciarProcess()
        self.RouteFolderBaseProcess = os.path.join(f'{self.DiskInsightsPandaRPA}:\\Insights\\', f'Audios_{KeyProcess}_{self.DateWorkProcessInsights.strftime("%Y%m%d")}_RPA')
        self.RouteFolderBaseWavProcess = os.path.join(self.RouteFolderBaseProcess, 'AudiosWav')
        self.RouteFolderBaseMp3Process = os.path.join(self.RouteFolderBaseProcess, 'AudiosMp3')
        print("self.RouteFolderBaseProcess")
        print(self.RouteFolderBaseProcess)

        self.NameGlobalProcess = NameConfig
        self.ClientGlobalProcess = ClientProcess
        self.PaisGlobalProcess = PaisProcess
        self.TypeConectGlobalProcess = TypeConectProcess
        self.ProcessGlobalSearchParamts = ProcessSearchParamts
        self.ServerConectGlobalProcess = ServerConectProcess
        self.FoldersConectGlobalProcess = FoldersConectProcess
        self.UserConectGlobalProcess = UserConectProcess
        self.PassConectGlobalProcess = PassConectProcess
        self.TypeRouteListFolderConnectGlobalProcess = TypeRouteListFolderConnect
        self.TypeDateGlobalProcess = TypeDateProcess
        self.ConfigSearchMetadataGlobalProcess = ConfigSearchMetadataProcess
        self.TypeSearchMetadataGlobalProcess = self.ConfigSearchMetadataGlobalProcess["TypeSearchMetadata"]
        self.MaxFilesAudiosGlobalProcess = int(MaxFilesAudiosProcess)
        self.AzureGlobalCompany = AzureCompany
        self.AzureGlobalCustomer = AzureCustomer
        self.AzureGlobalSession = AzureSession
        self.AzureGlobalSubSession = AzureSubSession
        self.AzureGlobalAudioLanguage = AzureAudioLanguage
        self.DateInsightsGlobalProcess = self.DateWorkProcessInsights.strftime("%Y-%m-%d") #Fecha Inicio proceso
        
        self.DateHourGestionInicioInsights = str(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        self.DateGestionInicioInsights = datetime.now().strftime("%Y/%m/%d")
        self.HourGestionInicioInsights = datetime.now().strftime("%H:%M:%S")
        self.CantidadAudiosEncontradosInsights = 0
        self.CantidadAudiosConvertidosInsights = 0
        self.CantidadAudiosAzureInsights = 0

        self.InsightsSearchInformationInitMetadata = []
        self.InsightsSearchInformationAllMetadata = {}
        try:
            os.makedirs(self.RouteFolderBaseWavProcess, exist_ok=True)
            os.makedirs(self.RouteFolderBaseMp3Process, exist_ok=True)
            self.FoldersPandaRedConectGlobalProcess = self.FoldersConectGlobalProcess.replace("{{DateFolderName}}", self.ValidateDateChange(self.TypeDateGlobalProcess, self.DateInsightsGlobalProcess))
            for self.FolderPandaRedConectGlobalProcess in self.FoldersPandaRedConectGlobalProcess.split("|"):
                self.FolderPandaConectGlobalProcess = [self.FolderPandaRedConectGlobalProcess]
                self.FolderConectGlobalProcess = self.FolderPandaConectGlobalProcess
                print("self.FolderConectGlobalProcess")
                print(self.FolderConectGlobalProcess)
                if self.TypeConectGlobalProcess == "COMPARTIDA":
                    if "SearchAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                        self.FolderPandaConectGlobalProcess = [os.path.join(self.FolderPandaRedConectGlobalProcess, FolderPandaConectGlobalProcess.name) for FolderPandaConectGlobalProcess in SafeScandir(self.FolderPandaRedConectGlobalProcess) if FolderPandaConectGlobalProcess.is_dir()]
                    elif "SearchSkillsAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                        self.FolderPandaConectGlobalProcess = [os.path.join(self.FolderPandaRedConectGlobalProcess, FolderPandaConectGlobalProcess.name) for FolderPandaConectGlobalProcess in SafeScandir(self.FolderPandaRedConectGlobalProcess) if FolderPandaConectGlobalProcess.is_dir() and FolderPandaConectGlobalProcess.name in self.TypeRouteListFolderConnectGlobalProcess]
                    random.shuffle(self.FolderPandaConectGlobalProcess)
                    for self.FolderConectGlobalProcess in self.FolderPandaConectGlobalProcess:
                        print("Iniciando Proceso Compartida...")
                        print(self.FolderConectGlobalProcess)
                        ArchivosValidatedFilesCompatida = self.ValidatedFilesServer()
                        if not ArchivosValidatedFilesCompatida:
                            print("No se encontraron archivos en la carpeta compartida.")
                            continue
                        print("Inicio Copiado de Audios...")
                        self.DownloadFilesProcess(ArchivosValidatedFilesCompatida)
                elif self.TypeConectGlobalProcess == "FTP":
                    print("Iniciando Proceso FTP...")
                    self.ConexionFTP()
                    self.ConnectDownloadFTP.cwd('/')
                    if "SearchAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                        self.ConnectDownloadFTP.cwd(self.FolderPandaRedConectGlobalProcess)
                        for FoldersPandaConectGlobalProcess in self.ConnectDownloadFTP.nlst():
                            try:
                                self.ConnectDownloadFTP.cwd(FoldersPandaConectGlobalProcess)
                                self.FolderPandaConectGlobalProcess.append(f"{self.FolderPandaRedConectGlobalProcess}/{FoldersPandaConectGlobalProcess}")
                                self.ConnectDownloadFTP.cwd('..')
                            except:
                                pass
                        self.ConnectDownloadFTP.cwd('/')
                    elif "SearchSkillsAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                        self.ConnectDownloadFTP.cwd(self.FolderPandaRedConectGlobalProcess)
                        for FoldersPandaConectGlobalProcess in self.ConnectDownloadFTP.nlst():
                            try:
                                self.ConnectDownloadFTP.cwd(FoldersPandaConectGlobalProcess)
                                if FoldersPandaConectGlobalProcess not in self.TypeRouteListFolderConnectGlobalProcess:
                                    continue
                                self.FolderPandaConectGlobalProcess.append(f"{self.FolderPandaRedConectGlobalProcess}/{FoldersPandaConectGlobalProcess}")
                                self.ConnectDownloadFTP.cwd('..')
                            except:
                                pass
                        self.ConnectDownloadFTP.cwd('/')
                    random.shuffle(self.FolderPandaConectGlobalProcess)
                    for self.FolderConectGlobalProcess in self.FolderPandaConectGlobalProcess:
                        try:
                            self.ConnectDownloadFTP.cwd(self.FolderConectGlobalProcess)
                            ArchivosValidatedFilesFTP = self.ConnectDownloadFTP.nlst()
                            if not ArchivosValidatedFilesFTP:
                                print("No se encontraron archivos en la carpeta FTP.")
                                continue
                            self.DownloadFilesProcess(ArchivosValidatedFilesFTP)
                        except:
                            pass
                elif self.TypeConectGlobalProcess == "SFTP":
                    print("Iniciando Proceso SFTP...")
                    self.ConexionSFTP()
                    if "SearchAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                        self.ConnectDownloadSFTP.chdir(self.FolderPandaRedConectGlobalProcess)
                        for FoldersPandaConectGlobalProcess in self.ConnectDownloadSFTP.listdir_attr():
                            if stat.S_ISDIR(FoldersPandaConectGlobalProcess.st_mode):
                                self.FolderPandaConectGlobalProcess.append(f"{self.FolderPandaRedConectGlobalProcess}/{FoldersPandaConectGlobalProcess.filename}")
                    elif "SearchSkillsAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                        self.ConnectDownloadSFTP.chdir(self.FolderPandaRedConectGlobalProcess)
                        for FoldersPandaConectGlobalProcess in self.ConnectDownloadSFTP.listdir_attr():
                            if stat.S_ISDIR(FoldersPandaConectGlobalProcess.st_mode):
                                if FoldersPandaConectGlobalProcess not in self.TypeRouteListFolderConnectGlobalProcess:
                                    continue
                                self.FolderPandaConectGlobalProcess.append(f"{self.FolderPandaRedConectGlobalProcess}/{FoldersPandaConectGlobalProcess.filename}")
                    random.shuffle(self.FolderPandaConectGlobalProcess)
                    for self.FolderConectGlobalProcess in self.FolderPandaConectGlobalProcess:
                        try:
                            self.ConnectDownloadSFTP.chdir(self.FolderConectGlobalProcess)
                            ArchivosValidatedFilesFTP = self.ConnectDownloadSFTP.listdir()
                            if not ArchivosValidatedFilesFTP:
                                print("No se encontraron archivos en la carpeta SFTP.")
                                continue
                            self.DownloadFilesProcess(ArchivosValidatedFilesFTP)
                        except:
                            pass
                elif self.TypeConectGlobalProcess == "SQL_FTP":
                    print("Iniciando Proceso SQL_FTP...")
                    self.ConexionFTP()
                    self.ConnectDownloadFTP.cwd('/')
                    if "SearchAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                        self.ConnectDownloadFTP.cwd(self.FolderPandaRedConectGlobalProcess)
                        for FoldersPandaConectGlobalProcess in self.ConnectDownloadFTP.nlst():
                            try:
                                self.ConnectDownloadFTP.cwd(FoldersPandaConectGlobalProcess)
                                if FoldersPandaConectGlobalProcess not in self.TypeRouteListFolderConnectGlobalProcess:
                                    continue
                                self.FolderPandaConectGlobalProcess.append(f"{self.FolderPandaRedConectGlobalProcess}/{FoldersPandaConectGlobalProcess}")
                                self.ConnectDownloadFTP.cwd('..')
                            except:
                                pass
                        self.ConnectDownloadFTP.cwd('/')
                    elif "SearchSkillsAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                        self.ConnectDownloadFTP.cwd(self.FolderPandaRedConectGlobalProcess)
                        for FoldersPandaConectGlobalProcess in self.ConnectDownloadFTP.nlst():
                            try:
                                self.ConnectDownloadFTP.cwd(FoldersPandaConectGlobalProcess)
                                self.FolderPandaConectGlobalProcess.append(f"{self.FolderPandaRedConectGlobalProcess}/{FoldersPandaConectGlobalProcess}")
                                self.ConnectDownloadFTP.cwd('..')
                            except:
                                pass
                    random.shuffle(self.FolderPandaConectGlobalProcess)
                    self.SearchInformationInitMetadata()
                    print("self.InsightsSearchInformationInitMetadata-----------------------")
                    print(len(self.InsightsSearchInformationInitMetadata))
                    for self.FolderConectGlobalProcess in self.FolderPandaConectGlobalProcess:
                        try:
                            self.ConnectDownloadFTP.cwd(self.FolderConectGlobalProcess)
                            self.DownloadFilesProcess(self.InsightsSearchInformationInitMetadata)
                        except:
                            pass
                elif self.TypeConectGlobalProcess == "COMPARTIDA_AVAYA":
                    print("Iniciando Proceso Compartida Avaya...")
                    FoldersSearchsConnectProcess = glob.glob(f"{self.FolderConectGlobalProcess[0]}")
                    for FolderSearchConnectProcess in FoldersSearchsConnectProcess:
                        self.FolderConectGlobalProcess = FolderSearchConnectProcess
                        if "SearchAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                            self.FolderPandaConectGlobalProcess = [os.path.join(self.FolderConectGlobalProcess, FolderPandaConectGlobalProcess.name) for FolderPandaConectGlobalProcess in SafeScandir(self.FolderConectGlobalProcess) if FolderPandaConectGlobalProcess.is_dir()]
                        elif "SearchSkillsAllSubFolders" in self.TypeRouteListFolderConnectGlobalProcess:
                            self.FolderPandaConectGlobalProcess = [os.path.join(self.FolderConectGlobalProcess, FolderPandaConectGlobalProcess.name) for FolderPandaConectGlobalProcess in SafeScandir(self.FolderConectGlobalProcess) if FolderPandaConectGlobalProcess.is_dir() and FolderPandaConectGlobalProcess.name in self.TypeRouteListFolderConnectGlobalProcess]
                        else:
                            self.FolderPandaConectGlobalProcess = [self.FolderConectGlobalProcess]
                        random.shuffle(self.FolderPandaConectGlobalProcess)
                        for self.FolderConectGlobalProcess in self.FolderPandaConectGlobalProcess:
                            #self.CantidadAudiosEncontradosInsights = 0
                            print("self.FolderConectGlobalProcess----------------------")
                            print(self.FolderConectGlobalProcess)
                            ArchivosValidatedFilesCompatida = self.ValidatedFilesServer()
                            if not ArchivosValidatedFilesCompatida:
                                print("No se encontraron archivos en la carpeta compartida Avaya...")
                                continue
                            print("Inicio Copiado de Audios...")
                            self.DownloadFilesProcess(ArchivosValidatedFilesCompatida)
                else:
                    return False

            print("Inicio Conversion de Audios...")
            self.ConversorWavToMp3()
            print("Proceso de Conversion a MP3 Completado.....")
            DataForAzureInsights = self.PrepareDataForAzureInsights()
            if DataForAzureInsights:
                print("\n. SUBIENDO ARCHIVOS A AZURE...")
                ResultUploadPortalAzurePais = self.UploadInformationPortalAzurePais(**DataForAzureInsights)
                if ResultUploadPortalAzurePais =="Exito":
                    print("\n=== PROCESO COMPLETADO CON ÉXITO ==================")
                else:
                    print("\n=== ERROR EN LA SUBIDA A AZURE ===")
            else:
                print("\n=== ERROR AL PREPARAR DATOS PARA AZURE ===")
            return True
            pass
        except Exception as e:
            print(f"Error procesando registro en línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        finally:
            ClearDirectoryMajor(self.RouteFolderBaseProcess)
            self.DateHourGestionFinInsights = datetime.now()
            self.UpdateInformeGeneralProcessInsights([self.DateHourGestionInicioInsights, str(self.DateHourGestionFinInsights.strftime("%d/%m/%Y %H:%M:%S")), self.NameGlobalProcess, self.ClientGlobalProcess, f"{self.AzureGlobalCompany}-{self.AzureGlobalCustomer}-{self.AzureGlobalSession}-{self.AzureGlobalSubSession}", self.CantidadAudiosEncontradosInsights, self.CantidadAudiosConvertidosInsights, self.CantidadAudiosAzureInsights, self.DateInWorkProcessInsights])

            if self.TypeExecutePandaInsightsProcess == "BD":
                try:
                    self.DatabaseManager = DatabaseManager()
                    self.LogFechaFin = self.DateHourGestionFinInsights.strftime("%Y-%m-%d")
                    self.LogHoraFin = self.DateHourGestionFinInsights.strftime("%H:%M:%S")
                    MonthNumProcess = int(self.DateInsightsGlobalProcess.split('-')[1])
                    TableNameProcessInsights = f"TBL_LLOG_INSIGHTS_PROCESS_{MonthNumProcess}"
                    TableTargetNameProcessInsights = globals()[TableNameProcessInsights]
                    # UPDATE primero, en una transacción corta
                    with self.DatabaseManager.Engine.begin() as ConnectionBD:
                        UpdateStmt = update(TBL_TINSIGHTS_PROCESS).where(TBL_TINSIGHTS_PROCESS.INS_NID == KeyProcess).values(INS_CESTADO_PROCESO='Finalizado')
                        ConnectionBD.execute(UpdateStmt)

                    # INSERT después, en otra transacción
                    with self.DatabaseManager.Engine.begin() as ConnectionBD:
                        InsertStmt = insert(TableTargetNameProcessInsights).values(
                            LOG_NINS_NID = KeyProcess,
                            LOG_DFECHA_PROCESO = self.DateInsightsGlobalProcess,
                            LOG_DFECHA_INICIO = self.DateGestionInicioInsights,
                            LOG_DHORA_INICIO = self.HourGestionInicioInsights,
                            LOG_NAUDIOS_PROCESAR = self.CantidadAudiosEncontradosInsights,
                            LOG_NAUDIOS_CONVERTIR = self.CantidadAudiosConvertidosInsights,
                            LOG_NAUDIOS_AZURE = self.CantidadAudiosAzureInsights,
                            LOG_DFECHA_FIN = self.LogFechaFin,
                            LOG_DHORA_FIN = self.LogHoraFin,
                            LOG_CESTADO_REGISTRO = 'Finalizado'
                        )
                        ConnectionBD.execute(InsertStmt)

                    if IdInsightsDateWorkProcess > 0:
                        with self.DatabaseManager.Engine.begin() as ConnectionBD:
                            UpdateStmt = update(TBL_TDATE_INSIGHTS_PROCESS).where(TBL_TDATE_INSIGHTS_PROCESS.DAT_NID == IdInsightsDateWorkProcess).values(
                                DAT_CESTADO_REGISTRO="Finalizado",
                                DAT_CCRE_NID_UPDATE="0",
                                DAT_DFECHA_UPDATE=self.LogFechaFin,
                                DAT_DHORA_UPDATE=self.LogHoraFin,
                            )
                            ConnectionBD.execute(UpdateStmt)
                    print(f" Envío completo a la base del mes {MonthNumProcess}")
                except Exception as e:
                    print(f" Error al insertar en TBL_LLOG_INSIGHTS_PROCESS_{MonthNumProcess}: {e}")

    def PrepareDataForAzureInsights(self):
        try:
            AzureFilesAudiosInput = []
            # Procesar archivos MP3
            for FileBaseMp3Process in os.listdir(self.RouteFolderBaseMp3Process):
                if FileBaseMp3Process.endswith('.mp3'):
                    try:
                        RouteCompletProcess = os.path.join(self.RouteFolderBaseMp3Process, FileBaseMp3Process)
                        with open(RouteCompletProcess, 'rb') as file:
                            contenido = file.read()
                            SearchInformationMetadataAgentAudio, SearchInformationMetadataAddAudio = self.SearchInformationMetadataAudio(self.TypeSearchMetadataGlobalProcess, self.FolderConectGlobalProcess, FileBaseMp3Process)
                            AzureFilesAudiosInput.append((FileBaseMp3Process, contenido, SearchInformationMetadataAgentAudio, SearchInformationMetadataAddAudio))
                    except Exception as e:
                        print(f"Error preparando para Azure linea {sys.exc_info()[-1].tb_lineno}: {str(e)}")

            print(f"- Archivos MP3 encontrados: {len(AzureFilesAudiosInput)}")
            return {
                "AzurePaisSelect": self.PaisGlobalProcess,
                "AzureCompanySelect": self.AzureGlobalCompany,
                "AzureClientSelect": self.AzureGlobalCustomer,
                "AzureOperationSelect": self.AzureGlobalSession,
                "AzureProgramSelect": self.AzureGlobalSubSession,
                "AzureLenguageInput": self.AzureGlobalAudioLanguage,
                "AzureDateAudioInput": self.DateInsightsGlobalProcess,
                "AzureFilesAudiosInput": AzureFilesAudiosInput
            }
            
        except Exception as e:
            print(f"Error al preparar datos para Azure linea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            return None

    def UploadInformationPortalAzurePais(self, AzurePaisSelect, AzureCompanySelect, AzureClientSelect, AzureOperationSelect, AzureProgramSelect, AzureLenguageInput, AzureDateAudioInput, AzureFilesAudiosInput):
        #Funcion para subir los archivos a Azure
        ContainerName = 'audios'
        FolderName = f'input/{QuitarTildes(ReemplazarEspacios(AzureClientSelect))}/'#Cambio Por recomendacion de Brasil
        if AzurePaisSelect == "COLOMBIA":
            StorageAccountName = 'sausaistudioco'
            StorageAccountKey = os.environ.get('AZURE_STORAGE_KEY_CO', '')
        elif AzurePaisSelect == "CHILE":
            StorageAccountName = 'sausaistudiocl'
            StorageAccountKey = os.environ.get('AZURE_STORAGE_KEY_CL', '')
        elif AzurePaisSelect == "ARGENTINA":
            StorageAccountName = 'sausaistudioar'
            StorageAccountKey = os.environ.get('AZURE_STORAGE_KEY_AR', '')
        elif AzurePaisSelect == "PERU":
            StorageAccountName = 'sausaistudiope'
            StorageAccountKey = os.environ.get('AZURE_STORAGE_KEY_PE', '')
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

        TotalAudiosProcesos = len(AzureFilesAudiosInput)

        NumeroLlamada = 1
        for AzureFileAudioInput in AzureFilesAudiosInput:
            print(f"Subiendo archivo {NumeroLlamada} de {TotalAudiosProcesos}...")
            """ if NumeroLlamada <= 30:
                NumeroLlamada += 1
                continue """

            DateTimeNow = datetime.now()
            #FormattedDateTime = DateTimeNow.strftime("%d/%m/%Y %H:%M:%S")
            FormattedDateTime = DateTimeNow.strftime("%H:%M:%S")
            DateForNameFile = str(DateTimeNow.strftime("%Y%m%d%H%M%S"))
            
            if AzureFileAudioInput:
                NameFileMp3UploadSpce = str(AzureFileAudioInput[0]).replace(" ", "_")
                NameFileMp3Upload = re.sub(r'[^a-zA-Z0-9]', '', NameFileMp3UploadSpce.replace(".mp3", ""))
                NameFileId = f"{DateForNameFile}_{str(NumeroLlamada)}_{str(NameFileMp3Upload)}"
                IdAgentAzure = str(AzureFileAudioInput[2])
                AdditionalMetadataAzure = str(AzureFileAudioInput[3])
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

                try:
                    #Aqui Inicia MP3
                    FileNameMp3AzureFileAudioInput = f"{NameFileId}.mp3"#Cambio Por recomendacion de Brasil
                    BlobNameMp3 = f"{FolderName}{FileNameMp3AzureFileAudioInput}"
                    BlobFileClientMp3 = ContainerClient.get_blob_client(BlobNameMp3)
                    if not BlobFileClientMp3.exists():
                        BlobFileClientMp3.upload_blob(AzureFileAudioInput[1], overwrite=True)

                        #Aqui Inicia Json
                        FileNameJsonAzureFileAudioInput = f"{NameFileId}.json"#Cambio Por recomendacion de Brasil
                        BlobNameJson = f"{FolderName}{FileNameJsonAzureFileAudioInput}"
                        BlobFileClientJson = ContainerClient.get_blob_client(BlobNameJson)
                        if not BlobFileClientJson.exists():
                            BlobFileClientJson.upload_blob(EstructuraJson, overwrite=True)
                            self.CantidadAudiosAzureInsights += 1
                            if self.CantidadAudiosAzureInsights >= self.MaxFilesAudiosGlobalProcess and self.MaxFilesAudiosGlobalProcess > 0:
                                return "Exito"
                except:
                    pass
            NumeroLlamada += 1
        return "Exito"

    def ValidateDateChange(self, DateType, DateChange):#Fecha en Fomato AAAA-MM-DD
        DateReturn = ""
        DateObjeChange = datetime.strptime(DateChange, "%Y-%m-%d")
        if DateType == "aaaammdd":
            DateReturn = DateObjeChange.strftime("%Y%m%d")
        elif DateType == "d_m_aaaa":
            DateReturn = DateObjeChange.strftime("%#d_%#m_%Y")
        elif DateType == "m_d_aaaa":
            DateReturn = DateObjeChange.strftime("%#m_%#d_%Y")
        elif DateType == "aaaa\\m\\d":
            DateReturn = DateObjeChange.strftime("%Y\\%#m\\%#d")
        elif DateType == "aaaa/m/d":
            DateReturn = DateObjeChange.strftime("%Y/%#m/%#d")
        elif DateType == "aammdd":
            DateReturn = DateObjeChange.strftime("%y%m%d")
        elif DateType == "dd_mm_aaaa":
            DateReturn = DateObjeChange.strftime("%d_%m_%Y")
        return DateReturn

    def CambiarMinusculaExtensionFile(self, archivo):
        nombre, extension = os.path.splitext(archivo)
        return nombre + extension.lower()

    def ValidatedFilesServer(self):
        try:
            print(f"Comprobando la carpeta: {self.FolderConectGlobalProcess}")
            if not os.path.exists(self.FolderConectGlobalProcess):
                print(f"Error: La carpeta {self.FolderConectGlobalProcess} no existe.")
                return []
            print(f"La carpeta {self.FolderConectGlobalProcess} existe, procediendo a listar archivos...")
            try:
                if self.TypeConectGlobalProcess == "COMPARTIDA":
                    FilesSearchFolder = os.listdir(self.FolderConectGlobalProcess)
                elif self.TypeConectGlobalProcess == "COMPARTIDA_AVAYA":
                    FilesSearchFolder = os.listdir(os.path.join(self.FolderConectGlobalProcess, "WAV"))
                if not FilesSearchFolder:
                    print(f"No se encontraron archivos en la carpeta {self.FolderConectGlobalProcess}.")
                    return []
                print(f"Archivos encontrados: {len(FilesSearchFolder)}")
                return FilesSearchFolder
            except Exception as e:
                print(f"Error al intentar listar archivos en la carpeta {self.FolderConectGlobalProcess} en la línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
                return []
        except Exception as e:
            print(f"Error en la línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            return []

    def ConexionFTP(self):
        try:
            self.ConnectDownloadFTP = ftplib.FTP(self.ServerConectGlobalProcess)
            self.ConnectDownloadFTP.login(user=self.UserConectGlobalProcess, passwd=self.PassConectGlobalProcess)
            print("Conexion al FTP OK")
            return True
        except Exception as e:
            print(f"Error al conectar al FTP en la línea {sys.exc_info()[-1].tb_lineno}: {type(e).__name__} - {e}")
            return False

    def ConexionSFTP(self):
        try:
            ConnectClientSFTP = paramiko.SSHClient()
            ConnectClientSFTP.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                ConnectClientSFTP.connect(hostname=self.ServerConectGlobalProcess, port=10300, username=self.UserConectGlobalProcess, password=self.PassConectGlobalProcess)
            except:
                ConnectClientSFTP.connect(hostname=self.ServerConectGlobalProcess, port=22, username=self.UserConectGlobalProcess, password=self.PassConectGlobalProcess)
            self.ConnectDownloadSFTP = ConnectClientSFTP.open_sftp()
            print("Conexion al SFTP OK")
            return True
        except Exception as e:
            print(f"Error al conectar al SFTP en la línea {sys.exc_info()[-1].tb_lineno}: {type(e).__name__} - {e}")
            return False

    def ValidatedFilesCsvBeforeProcess(self, ArchivosValidatedFiles):
        for ArchiValidatedFile in ArchivosValidatedFiles:
            if ArchiValidatedFile.endswith('.csv'):
                self.FilesCsvDownloadMetadata.append(ArchiValidatedFile)

    def DownloadFilesProcess(self, ArchivosValidatedFiles):
        self.ValidatedFilesCsvBeforeProcess(ArchivosValidatedFiles)
        for ArchiValidatedFile in ArchivosValidatedFiles:
            if self.CantidadAudiosEncontradosInsights >= (self.MaxFilesAudiosGlobalProcess + self.MaxAddFilesAudiosGlobalProcess) and self.MaxFilesAudiosGlobalProcess > 0:
                break
            if self.ProcessGlobalSearchParamts["SearchType"] in ["SqlConnectOne", "SqlConnectTwo", "SqlConnectThree"]:
                if self.ProcessGlobalSearchParamts["SearchType"] == "SqlConnectThree":
                    ArchiValidatedFile = os.path.basename(ArchiValidatedFile.NombreArchivo)
                else:
                    ArchiValidatedFile = os.path.basename(ArchiValidatedFile.FILE_GRABACION)
            ArchivoValidatedFile = self.CambiarMinusculaExtensionFile(ArchiValidatedFile)
            if self.TypeConectGlobalProcess == "COMPARTIDA_AVAYA":
                OrigenFolderConectArchivo = os.path.join(self.FolderConectGlobalProcess, "WAV", ArchivoValidatedFile)
            else:
                OrigenFolderConectArchivo = os.path.join(self.FolderConectGlobalProcess, ArchivoValidatedFile)
            if ArchivoValidatedFile.endswith('.wav'):
                DestinoFolderConectArchivo = os.path.join(self.RouteFolderBaseWavProcess, ArchivoValidatedFile)
            elif ArchivoValidatedFile.endswith('.mp3'):
                DestinoFolderConectArchivo = os.path.join(self.RouteFolderBaseMp3Process, ArchivoValidatedFile)
            elif ArchivoValidatedFile.endswith('.csv'):
                self.FilesCsvDownloadMetadata.append(ArchivoValidatedFile) if ArchivoValidatedFile not in self.FilesCsvDownloadMetadata else None
                continue
            else:
                continue
            if self.SearchInformationMetadataFilterUpload(self.FolderConectGlobalProcess, ArchiValidatedFile) == False:
                continue
            try:
                if self.TypeConectGlobalProcess in ["COMPARTIDA", "COMPARTIDA_AVAYA"]:
                    shutil.copy2(OrigenFolderConectArchivo, DestinoFolderConectArchivo)
                elif self.TypeConectGlobalProcess in ["FTP", "SQL_FTP"]:
                    with open(DestinoFolderConectArchivo, 'wb') as DestinyFile:
                        self.ConnectDownloadFTP.retrbinary(f'RETR {ArchiValidatedFile}', DestinyFile.write)
                elif self.TypeConectGlobalProcess == "SFTP":
                    self.ConnectDownloadSFTP.get(ArchiValidatedFile, DestinoFolderConectArchivo)
                else:
                    continue
                self.CantidadAudiosEncontradosInsights += 1
                print(f"Archivo {ArchiValidatedFile} Copiado {DestinoFolderConectArchivo}")
            except Exception as e:
                print(f"Error al copiar el archivo {ArchiValidatedFile} linea {sys.exc_info()[-1].tb_lineno}: {str(e)}")

    def ConversorWavToMp3(self):
        try:
            for archivo in os.listdir(self.RouteFolderBaseWavProcess):
                if archivo.endswith(".wav"):
                    try:
                        ruta_archivo_wav = os.path.join(self.RouteFolderBaseWavProcess, archivo)
                        AudioWavSegment = AudioSegment.from_wav(ruta_archivo_wav)
                        if AudioWavSegment.channels > 1:
                            AudioWavSegment = AudioWavSegment.set_channels(1)
                        AudioWavSegment = AudioWavSegment.normalize()
                        AudioWavSegment = AudioWavSegment.set_frame_rate(44100).set_sample_width(2)

                        archivo_mp3 = os.path.splitext(archivo)[0] + '.mp3'
                        ruta_archivo_mp3 = os.path.join(self.RouteFolderBaseMp3Process, archivo_mp3)

                        AudioWavSegment.export(ruta_archivo_mp3, format="mp3")
                        self.CantidadAudiosConvertidosInsights += 1
                        print(f"Archivo convertido a mp3: {archivo}")
                    except Exception as e:
                        error_line = sys.exc_info()[-1].tb_lineno
                        print(f"Error al convertir archivo en la línea {error_line}: {type(e).__name__} - {e}")
                else:
                    print(f"El archivo {archivo} no es un archivo wav")
        except Exception as e:
            print(f"Error Conversor WavToMp3 en la línea {sys.exc_info()[-1].tb_lineno}: {type(e).__name__} - {str(e)}")

    def UpdateInformeGeneralProcessInsights(self, NuevaInformacionReport):
        with open(self.RouteFileReportBaseProcess, 'a', encoding='utf-8') as f:
            f.write(";".join(map(str, NuevaInformacionReport)) + "\n")
        with open(self.RouteFileReportBaseMonthProcess, 'a', encoding='utf-8') as f:
            f.write(";".join(map(str, NuevaInformacionReport)) + "\n")

    def SearchInformationMetadataAudio(self, TypeSearchMetadata, RouteSearchCurrent, FileSearchCurrent):
        try:
            if TypeSearchMetadata == "FilenameCurrent":
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "GoContactCurrent":
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "GoContactPruebasCurrent":
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "GoContactUuIdCurrent":
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "SqlConnectOne":
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "SqlConnectTwo":
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", "")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", "")
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "SqlConnectThree":
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", "")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", "")
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "AvayaCurrentXML":
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", ".xml")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", ".xml")
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "SFTP_SpeechMiner":
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", "")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", "")
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "SFTP_Transformacion":
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", "")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", "")
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
            elif TypeSearchMetadata == "SqlConnectAvayaPeru":
                return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
                
            return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]
        except Exception as e:
            print(f"Error procesando registro en línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            return self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][0], self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)][1]

    def SearchInformationMetadataFilterUpload(self, RouteSearchCurrent, FileSearchCurrent):
        try:
            InformationReturnColumnsAdd = ""
            SearchType = self.ProcessGlobalSearchParamts["SearchType"]
            SearchServer = self.ProcessGlobalSearchParamts["SearchServer"]
            SearchDatabase = self.ProcessGlobalSearchParamts["SearchDatabase"]
            SearchUsername = self.ProcessGlobalSearchParamts["SearchUsername"]
            SearchPassword = self.ProcessGlobalSearchParamts["SearchPassword"]
            SearchDetalleOne = self.ProcessGlobalSearchParamts["SearchDetalleOne"]
            SearchDetalleTwo = self.ProcessGlobalSearchParamts["SearchDetalleTwo"]
            if self.TypeSearchMetadataGlobalProcess == "FilenameCurrent":
                SplitNameFileMp3Upload = FileSearchCurrent.split("_")
                if len(SplitNameFileMp3Upload) >= 5:
                    IdAgentAzure = SplitNameFileMp3Upload[4]
                else:
                    IdAgentAzure = FileSearchCurrent
                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [IdAgentAzure, InformationReturnColumnsAdd]
                return True
            elif self.TypeSearchMetadataGlobalProcess == "GoContactCurrent":
                GlobCsvFiles = glob.glob(f"{RouteSearchCurrent}/*.csv")
                SearchDetalleOne = self.ProcessGlobalSearchParamts["SearchDetalleOne"]
                SearchDetalleTwo = self.ProcessGlobalSearchParamts["SearchDetalleTwo"]
                if GlobCsvFiles:
                    for ResFileCsvDownloadMetadata in GlobCsvFiles:
                        if os.path.exists(ResFileCsvDownloadMetadata):
                            ColumnForReturnProcess = "Agent User Name"

                            ColumnForFilterOneProcess = "Campaign Name"
                            ColumnForFilterTwoProcess = "Recording address"
                            ColumnForFilterThreeProcess = "Call Outcome Group"
                            PandaReadCsv = pd.read_csv(ResFileCsvDownloadMetadata, delimiter=";", dtype=str)

                            try:
                                if SearchDetalleTwo != "":
                                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchCurrent, na=False, regex=True) & PandaReadCsv[ColumnForFilterThreeProcess].str.contains(SearchDetalleTwo, na=False, regex=True)]
                                else:
                                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchCurrent, na=False, regex=True)]
                            except:
                                ColumnForFilterTwoProcess = "Ruta grabacion"
                                if SearchDetalleTwo != "":
                                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchCurrent, na=False, regex=True) & PandaReadCsv[ColumnForFilterThreeProcess].str.contains(SearchDetalleTwo, na=False, regex=True)]
                                else:
                                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchCurrent, na=False, regex=True)]

                            PandaCountFiltradoReadCsv = len(PandaFiltradoReadCsv)
                            if PandaCountFiltradoReadCsv > 0:
                                for PandaIndexReadCsv, PandaRowReadCsv in PandaFiltradoReadCsv.iterrows():
                                    try:
                                        for ColumnSearchMetadata in self.ConfigSearchMetadataGlobalProcess["ColumnsSearchMetadata"].split("|"):
                                            ReturnColumnAdd = PandaRowReadCsv[ColumnSearchMetadata]
                                            ReturnColumnAdd = "" if pd.isna(ReturnColumnAdd) else ReturnColumnAdd
                                            if InformationReturnColumnsAdd == '':
                                                InformationReturnColumnsAdd = f'"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                                            else:
                                                InformationReturnColumnsAdd = f'{InformationReturnColumnsAdd}"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                                    except:
                                        pass
                                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadCsv[ColumnForReturnProcess], InformationReturnColumnsAdd]
                                return True
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                else:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
            elif self.TypeSearchMetadataGlobalProcess == "GoContactPruebasCurrent":
                GlobCsvFiles = glob.glob(f'{self.ConfigSearchMetadataGlobalProcess["RouteSearchMetadata"]}/*.csv')
                SearchDetalleOne = self.ProcessGlobalSearchParamts["SearchDetalleOne"]
                SearchDetalleTwo = self.ProcessGlobalSearchParamts["SearchDetalleTwo"]
                if GlobCsvFiles:
                    for ResFileCsvDownloadMetadata in GlobCsvFiles:
                        if os.path.exists(ResFileCsvDownloadMetadata):
                            ColumnForReturnProcess = "Agent User Name"

                            ColumnForFilterOneProcess = "Campaign Name"
                            ColumnForFilterTwoProcess = "Recording address"
                            ColumnForFilterThreeProcess = "Call Outcome Group"
                            PandaReadCsv = pd.read_csv(ResFileCsvDownloadMetadata, delimiter=";", dtype=str)

                            try:
                                if SearchDetalleTwo != "":
                                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchCurrent, na=False, regex=True) & PandaReadCsv[ColumnForFilterThreeProcess].str.contains(SearchDetalleTwo, na=False, regex=True)]
                                else:
                                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchCurrent, na=False, regex=True)]
                            except:
                                ColumnForFilterTwoProcess = "Ruta grabacion"
                                if SearchDetalleTwo != "":
                                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchCurrent, na=False, regex=True) & PandaReadCsv[ColumnForFilterThreeProcess].str.contains(SearchDetalleTwo, na=False, regex=True)]
                                else:
                                    PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFilterOneProcess].str.contains(SearchDetalleOne, na=False, regex=True) & PandaReadCsv[ColumnForFilterTwoProcess].str.contains(FileSearchCurrent, na=False, regex=True)]

                            PandaCountFiltradoReadCsv = len(PandaFiltradoReadCsv)
                            if PandaCountFiltradoReadCsv > 0:
                                for PandaIndexReadCsv, PandaRowReadCsv in PandaFiltradoReadCsv.iterrows():
                                    try:
                                        for ColumnSearchMetadata in self.ConfigSearchMetadataGlobalProcess["ColumnsSearchMetadata"].split("|"):
                                            ReturnColumnAdd = PandaRowReadCsv[ColumnSearchMetadata]
                                            ReturnColumnAdd = "" if pd.isna(ReturnColumnAdd) else ReturnColumnAdd
                                            if InformationReturnColumnsAdd == '':
                                                InformationReturnColumnsAdd = f'"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                                            else:
                                                InformationReturnColumnsAdd = f'{InformationReturnColumnsAdd}"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                                    except:
                                        pass
                                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadCsv[ColumnForReturnProcess], InformationReturnColumnsAdd]
                                return True
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                else:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
            elif self.TypeSearchMetadataGlobalProcess == "GoContactUuIdCurrent":
                GlobCsvFiles = glob.glob(f"{RouteSearchCurrent}/*.csv")
                SearchDetalleOne = self.ProcessGlobalSearchParamts["SearchDetalleOne"]
                SearchDetalleTwo = self.ProcessGlobalSearchParamts["SearchDetalleTwo"]
                FileSearchModifCurrent = GetUuidFromFileNameGoContactFirst(FileSearchCurrent)
                if GlobCsvFiles:
                    for ResFileCsvDownloadMetadata in GlobCsvFiles:
                        if os.path.exists(ResFileCsvDownloadMetadata):
                            ColumnForReturnProcess = "Agent User Name"

                            ColumnForFilterOneProcess = "Recording address"
                            ColumnForFilterTwoProcess = "Call uuid"
                            ColumnForFilterThreeProcess = "Call Outcome Group"
                            PandaReadCsv = pd.read_csv(ResFileCsvDownloadMetadata, delimiter=";", dtype=str)

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
                            PandaCountFiltradoReadCsv = len(PandaFiltradoReadCsv)
                            if PandaCountFiltradoReadCsv > 0:
                                for PandaIndexReadCsv, PandaRowReadCsv in PandaFiltradoReadCsv.iterrows():
                                    try:
                                        for ColumnSearchMetadata in self.ConfigSearchMetadataGlobalProcess["ColumnsSearchMetadata"].split("|"):
                                            ReturnColumnAdd = PandaRowReadCsv[ColumnSearchMetadata]
                                            ReturnColumnAdd = "" if pd.isna(ReturnColumnAdd) else ReturnColumnAdd
                                            if InformationReturnColumnsAdd == '':
                                                InformationReturnColumnsAdd = f'"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                                            else:
                                                InformationReturnColumnsAdd = f'{InformationReturnColumnsAdd}"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                                    except:
                                        pass
                                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadCsv[ColumnForReturnProcess], InformationReturnColumnsAdd]
                                return True
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                else:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
            elif self.TypeSearchMetadataGlobalProcess == "SqlConnectOne":
                ColumnForFiltroProcess = "FILE_GRABACION"
                ColumnForReturnProcess = "RUT_RAC"
                PandaReadSql = pd.DataFrame(self.InsightsSearchInformationInitMetadata)
                PandaFiltradoReadSql = PandaReadSql[PandaReadSql[ColumnForFiltroProcess].str.contains(FileSearchCurrent, na=False, case=False)]
                PandaCountFiltradoReadSql = len(PandaFiltradoReadSql)
                if PandaCountFiltradoReadSql > 0:
                    for PandaIndexReadCsv, PandaRowReadSql in PandaFiltradoReadSql.iterrows():
                        self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadSql[ColumnForReturnProcess], InformationReturnColumnsAdd]
                        return True
                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                return False
            elif self.TypeSearchMetadataGlobalProcess == "SqlConnectTwo":
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", "")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", "")
                else:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                ColumnForFiltroProcess = "FILE_GRABACION"
                ColumnForReturnProcess = "LOGIN_ID"
                PandaReadSql = pd.DataFrame(self.InsightsSearchInformationInitMetadata)
                PandaFiltradoReadSql = PandaReadSql[PandaReadSql[ColumnForFiltroProcess].str.contains(FileSearchModifCurrent, na=False, case=False, regex=False)]
                PandaCountFiltradoReadSql = len(PandaFiltradoReadSql)
                if PandaCountFiltradoReadSql > 0:
                    for PandaIndexReadCsv, PandaRowReadSql in PandaFiltradoReadSql.iterrows():
                        self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadSql[ColumnForReturnProcess], InformationReturnColumnsAdd]
                        return True
                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                return False
            elif self.TypeSearchMetadataGlobalProcess == "SqlConnectThree":
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", "")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", "")
                else:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                ColumnForFiltroProcess = "NombreArchivo"
                ColumnForReturnProcess = "AgentUserName"
                PandaReadSql = pd.DataFrame(self.InsightsSearchInformationInitMetadata)
                PandaFiltradoReadSql = PandaReadSql[PandaReadSql[ColumnForFiltroProcess].str.contains(FileSearchModifCurrent, na=False, case=False, regex=False)]
                PandaCountFiltradoReadSql = len(PandaFiltradoReadSql)
                if PandaCountFiltradoReadSql > 0:
                    for PandaIndexReadCsv, PandaRowReadSql in PandaFiltradoReadSql.iterrows():
                        self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadSql[ColumnForReturnProcess], InformationReturnColumnsAdd]
                        return True
                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                return False
            elif self.TypeSearchMetadataGlobalProcess == "AvayaCurrentXML":
                SearchDetalleOne = self.ProcessGlobalSearchParamts["SearchDetalleOne"]
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", ".xml")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", ".xml")
                else:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                FileSearchKeyCurrent = FileSearchModifCurrent.replace(".xml", "")
                FileSearchXML = os.path.join(RouteSearchCurrent, "IDX", FileSearchModifCurrent)
                with open(FileSearchXML, "r", encoding="utf-8") as FileXML:
                    InformationFileXML = FileXML.read()
                InformationDiccionaryFileXML = xmltodict.parse(InformationFileXML)
                JsonInformationXML = json.loads(json.dumps(InformationDiccionaryFileXML, indent=4))
                for SearchOneDetalle in SearchDetalleOne.split("|"):
                    if SearchOneDetalle in JsonInformationXML["CAudioFile"]["CRI"]["PrivateData"]["PrivateData"]["DictionaryEntry"][0]["Value"]:
                        self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [JsonInformationXML["CAudioFile"]["CRI"]["AgentPBXID"], InformationReturnColumnsAdd]
                        return True
                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                return False
            elif self.TypeSearchMetadataGlobalProcess == "SFTP_SpeechMiner":
                if FileSearchCurrent.endswith('.wav'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".wav", "")
                elif FileSearchCurrent.endswith('.mp3'):
                    FileSearchModifCurrent = FileSearchCurrent.replace(".mp3", "")
                else:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                try:
                    for FileCsvMetadataDownload in self.FilesCsvDownloadMetadata:
                        ColumnForFiltroProcess = "Metadata: GSIP_REC_FN"
                        ColumnForReturnProcess = "Metadata: agentId"
                        with self.ConnectDownloadSFTP.open(FileCsvMetadataDownload, 'r') as RemoteFileReadMetadata:
                            ContentRemoteFileReadMetadata = RemoteFileReadMetadata.read().decode('utf-8')
                            PandaReadCsv = pd.read_csv(io.StringIO(ContentRemoteFileReadMetadata))
                        PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFiltroProcess].str.contains(FileSearchModifCurrent, na=False, regex=True)]
                        PandaCountFiltradoReadCsv = len(PandaFiltradoReadCsv)
                        if PandaCountFiltradoReadCsv > 0:
                            for PandaIndexReadCsv, PandaRowReadCsv in PandaFiltradoReadCsv.iterrows():
                                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadCsv[ColumnForReturnProcess], InformationReturnColumnsAdd]
                                return True
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                except:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
            elif self.TypeSearchMetadataGlobalProcess == "SFTP_Transformacion":
                try:
                    for FileCsvMetadataDownload in self.FilesCsvDownloadMetadata:
                        ColumnForFiltroProcess = "grabacion"
                        ColumnForReturnProcess = "agente"
                        with self.ConnectDownloadSFTP.open(FileCsvMetadataDownload, 'r') as RemoteFileReadMetadata:
                            ContentRemoteFileReadMetadata = RemoteFileReadMetadata.read().decode('utf-8')
                            PandaReadCsv = pd.read_csv(io.StringIO(ContentRemoteFileReadMetadata))
                        PandaFiltradoReadCsv = PandaReadCsv[PandaReadCsv[ColumnForFiltroProcess].str.contains(FileSearchCurrent, na=False, regex=True)]
                        PandaCountFiltradoReadCsv = len(PandaFiltradoReadCsv)
                        if PandaCountFiltradoReadCsv > 0:
                            for PandaIndexReadCsv, PandaRowReadCsv in PandaFiltradoReadCsv.iterrows():
                                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadCsv[ColumnForReturnProcess], InformationReturnColumnsAdd]
                                return True
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
                except:
                    self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                    return False
            elif self.TypeSearchMetadataGlobalProcess == "SqlConnectAvayaPeru":
                DynamicDbConnect = DynamicDbConnection()
                ParamsConnect = f"mssql+pyodbc://{SearchUsername}:{SearchPassword}@{SearchServer}/{SearchDatabase}?driver=ODBC+Driver+17+for+SQL+Server"
                ConsultaBD = f"""
                        SELECT * FROM GRABACION_LOG_MULTIMEDIA_VDN WHERE NOMBRE_ARCHIVO = '{FileSearchCurrent}';
                    """
                self.InsightsSearchInformationInitMetadata = DynamicDbConnect.ExecuteQuery(ConsultaBD, ParamsConnect)
                DynamicDbConnect.CloseConnection()
                ColumnForFiltroProcess = "NOMBRE_ARCHIVO"
                ColumnForReturnProcess = "ID_AGENTE_CENTRAL"
                PandaReadSql = pd.DataFrame(self.InsightsSearchInformationInitMetadata)
                PandaFiltradoReadSql = PandaReadSql[PandaReadSql[ColumnForFiltroProcess].str.contains(FileSearchCurrent, na=False, case=False, regex=False)]
                PandaCountFiltradoReadSql = len(PandaFiltradoReadSql)
                if PandaCountFiltradoReadSql > 0:
                    for PandaIndexReadCsv, PandaRowReadSql in PandaFiltradoReadSql.iterrows():
                        try:
                            for ColumnSearchMetadata in self.ConfigSearchMetadataGlobalProcess["ColumnsSearchMetadata"].split("|"):
                                ReturnColumnAdd = PandaRowReadSql[ColumnSearchMetadata]
                                ReturnColumnAdd = "" if pd.isna(ReturnColumnAdd) else ReturnColumnAdd
                                if InformationReturnColumnsAdd == '':
                                    InformationReturnColumnsAdd = f'"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                                else:
                                    InformationReturnColumnsAdd = f'{InformationReturnColumnsAdd}"{ColumnSearchMetadata}": "{ReturnColumnAdd}",'
                        except:
                            pass
                        self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = [PandaRowReadSql[ColumnForReturnProcess], InformationReturnColumnsAdd]
                        return True
                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                return False
            else:
                self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
                return True

        except Exception as e:
            print(f"Error Metadata en línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            self.InsightsSearchInformationAllMetadata[GetFileNameSingle(FileSearchCurrent)] = ["", InformationReturnColumnsAdd]
            return False

    def SearchInformationInitMetadata(self):
        try:
            SearchType = self.ProcessGlobalSearchParamts["SearchType"]
            SearchServer = self.ProcessGlobalSearchParamts["SearchServer"]
            SearchDatabase = self.ProcessGlobalSearchParamts["SearchDatabase"]
            SearchUsername = self.ProcessGlobalSearchParamts["SearchUsername"]
            SearchPassword = self.ProcessGlobalSearchParamts["SearchPassword"]
            SearchDetalleOne = self.ProcessGlobalSearchParamts["SearchDetalleOne"]
            SearchDetalleTwo = self.ProcessGlobalSearchParamts["SearchDetalleTwo"]
            if SearchType == "SqlConnectOne":
                DynamicDbConnect = DynamicDbConnection()
                ParamsConnect = f"mssql+pyodbc://{SearchUsername}:{SearchPassword}@{SearchServer}/{SearchDatabase}?driver=ODBC+Driver+17+for+SQL+Server"
                if SearchDetalleOne in ["ASP_GET_METADATA_AVAYA_1"]:
                    ConsultaBD = f"""
                            WITH INFORMATION_ALL_VIEW AS (
                                SELECT *,
                                    ROW_NUMBER() OVER (PARTITION BY RUT_RAC ORDER BY FECHA) AS ORDER_NUM_FILA,
                                    DENSE_RANK() OVER (ORDER BY RUT_RAC) AS ORDER_GROUP
                                FROM ASP_GET_METADATA_AVAYA
                                WHERE FECHA BETWEEN '{self.DateInsightsGlobalProcess}' AND '{self.DateInsightsGlobalProcess} 23:59:59'
                                AND DNIS IN (SELECT value FROM STRING_SPLIT('{SearchDetalleTwo}', '|'))
                            )
                            SELECT * FROM INFORMATION_ALL_VIEW ORDER BY ORDER_NUM_FILA, ORDER_GROUP;
                        """
                elif SearchDetalleOne in ["ASP_GET_METADATA_AVAYA_2"]:
                    ConsultaBD = f"""
                            WITH INFORMATION_ALL_VIEW AS (
                                SELECT *,
                                    ROW_NUMBER() OVER (PARTITION BY RUT_RAC ORDER BY FECHA) AS ORDER_NUM_FILA,
                                    DENSE_RANK() OVER (ORDER BY RUT_RAC) AS ORDER_GROUP
                                FROM ASP_GET_METADATA_AVAYA
                                WHERE FECHA BETWEEN '{self.DateInsightsGlobalProcess}' AND '{self.DateInsightsGlobalProcess} 23:59:59'
                                AND SERVICIO IN (SELECT value FROM STRING_SPLIT('{SearchDetalleTwo}', '|'))
                            )
                            SELECT * FROM INFORMATION_ALL_VIEW ORDER BY ORDER_NUM_FILA, ORDER_GROUP;
                        """
                self.InsightsSearchInformationInitMetadata = DynamicDbConnect.ExecuteQuery(ConsultaBD, ParamsConnect)
                DynamicDbConnect.CloseConnection()
                return True
            elif SearchType == "SqlConnectTwo":
                DynamicDbConnect = DynamicDbConnection()
                ParamsConnect = f"mssql+pyodbc://{SearchUsername}:{SearchPassword}@{SearchServer}/{SearchDatabase}?driver=ODBC+Driver+17+for+SQL+Server"
                if SearchDetalleOne in ["vw_metadatos_grabacion_five9"]:
                    ConsultaBD = f"""
                            WITH INFORMATION_ALL_VIEW AS (
                                SELECT *,
                                    ROW_NUMBER() OVER (PARTITION BY LOGIN_ID ORDER BY FECHA) AS ORDER_NUM_FILA,
                                    DENSE_RANK() OVER (ORDER BY LOGIN_ID) AS ORDER_GROUP
                                FROM {SearchDetalleOne}
                                WHERE FECHA BETWEEN '{self.DateInsightsGlobalProcess}' AND '{self.DateInsightsGlobalProcess} 23:59:59'
                                AND EXISTS (
                                    SELECT 1
                                    FROM STRING_SPLIT('{SearchDetalleTwo}', '|') AS s
                                    WHERE FILE_GRABACION LIKE '%' + s.value + '%'
                                )
                            )
                            SELECT * FROM INFORMATION_ALL_VIEW ORDER BY ORDER_NUM_FILA, ORDER_GROUP;
                        """
                self.InsightsSearchInformationInitMetadata = DynamicDbConnect.ExecuteQuery(ConsultaBD, ParamsConnect)
                DynamicDbConnect.CloseConnection()
                return True
            elif SearchType == "SqlConnectThree":
                DynamicDbConnect = DynamicDbConnection()
                ParamsConnect = f"mssql+pyodbc://{SearchUsername}:{SearchPassword}@{SearchServer}/{SearchDatabase}?driver=ODBC+Driver+17+for+SQL+Server"
                if SearchDetalleOne in ["VW_GET_METADATA_GOCONTACT_1"]:
                    ConsultaBD = f"""
                            WITH INFORMATION_ALL_VIEW AS (
                                SELECT *,
                                    ROW_NUMBER() OVER (PARTITION BY AgentUserName ORDER BY Callstart) AS ORDER_NUM_FILA,
                                    DENSE_RANK() OVER (ORDER BY AgentUserName) AS ORDER_GROUP
                                FROM {SearchDetalleOne}
                                WHERE Callstart LIKE '{self.DateInsightsGlobalProcess}%'
                                AND CampaignName IN (SELECT value FROM STRING_SPLIT('{SearchDetalleTwo}', '|'))
                            )
                            SELECT * FROM INFORMATION_ALL_VIEW ORDER BY ORDER_NUM_FILA, ORDER_GROUP;
                        """
                elif SearchDetalleOne in ["VW_GET_METADATA_GOCONTACT_2"]:
                    ConsultaBD = f"""
                            WITH INFORMATION_ALL_VIEW AS (
                                SELECT *,
                                    ROW_NUMBER() OVER (PARTITION BY AgentUserName ORDER BY Callstart) AS ORDER_NUM_FILA,
                                    DENSE_RANK() OVER (ORDER BY AgentUserName) AS ORDER_GROUP
                                FROM {SearchDetalleOne}
                                WHERE Callstart LIKE '{self.DateInsightsGlobalProcess}%'
                                AND QueueName IN (SELECT value FROM STRING_SPLIT('{SearchDetalleTwo}', '|'))
                            )
                            SELECT * FROM INFORMATION_ALL_VIEW ORDER BY ORDER_NUM_FILA, ORDER_GROUP;
                        """
                self.InsightsSearchInformationInitMetadata = DynamicDbConnect.ExecuteQuery(ConsultaBD, ParamsConnect)
                DynamicDbConnect.CloseConnection()
                return True
        except Exception as e:
            print(f"Error Metadata Init en línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            return False

    def CreateRouteReportInsights(self, RouteFileReportProcess):
        os.makedirs(os.path.dirname(RouteFileReportProcess), exist_ok=True)
        if not os.path.exists(RouteFileReportProcess):
            ColumnasFileReportBaseProcess = ["Fecha Inicio Gestión", "Fecha Fin Gestión", "Nombre Configuración", "Cliente", "Detalle Cliente", "Audios", "Convertidos", "Azure", "Fecha Proceso"]
            with open(RouteFileReportProcess, 'w', encoding='utf-8') as f:
                f.write(";".join(ColumnasFileReportBaseProcess) + "\n")








