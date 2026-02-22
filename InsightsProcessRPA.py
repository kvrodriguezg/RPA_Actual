from PandaTimer import *
from Libraries import *
from FunctionsInsights import FunctionsInsights
from FunctionsWiseCX import FunctionsWiseCX
from FunctionsKaizenChat import FunctionsZendeskChat
from controller.ConexionControllerBD import ConexionControllerBD

class InsightsProcessRPA():
    def __init__(self):
        self.FileJsonConfigInsightsProcessRPA = "InsightsProcessRPA.json"
        self.FileJsonConfigInitInsights = "AtenaIA.json"

        self.DiskPandaInsightsRPA = "C"
        self.TypeExecutePandaInsightsProcess = "JSON"
        self.ExecutePandaProcessWiseCX = False
        self.ExecutePandaProcessInsights = False
        self.ExecutePandaProcessZendesk = False
        self.CantidadClientesWorkingThreads = 5

        try:
            InsightsJsonRPA = []
            with open(self.FileJsonConfigInsightsProcessRPA, "r", encoding="utf-8") as InsightsProcessRPA:
                InsightsJsonRPA = json.load(InsightsProcessRPA)
            self.DiskPandaInsightsRPA = InsightsJsonRPA["DiskInsightsRPA"]
            self.TypeExecutePandaInsightsProcess = InsightsJsonRPA["TypeExecuteInsightsProcess"]
            self.ExecutePandaProcessWiseCX = InsightsJsonRPA["ExecuteProcessWiseCX"]
            self.ExecutePandaProcessInsights = InsightsJsonRPA["ExecuteProcessInsights"]
            self.ExecutePandaProcessZendesk = InsightsJsonRPA["ExecuteProcessZendesk"]
        except:
            pass

    def ExecuteProcessInsights(self):
        self.__init__()
        try:
            print("Inicio Proceso Validacion Audios Servidores...")

            FechaInit = input("Ingresar la Fecha Inicio: ")
            FechaEnd = input("Ingresar la Fecha Fin: ")
            GenerarListDateProcessPanda = GenerarListaFechasProcessPanda(FechaInit, FechaEnd)
            for ArrayFechaValidated in GenerarListDateProcessPanda:#Lineas Para Pruebas de Varios Dias
                FechaWorkProcess = datetime.strptime(ArrayFechaValidated, "%Y-%m-%d")#Lineas Para Pruebas de Varios Dias

                ListaMultiprocessingProcessActives = []
                InsightsFunctions = FunctionsInsights(self.DiskPandaInsightsRPA, FechaWorkProcess)
                #InsightsFunctions = FunctionsInsights(self.DiskPandaInsightsRPA, self.TypeExecutePandaInsightsProcess)
                if self.TypeExecutePandaInsightsProcess == "BD":
                    self.ExecutePandaDateProcessInsights = False
                    try:
                        # Recolectamos la información de los campos solicitados
                        InsightsAllInformationProcess = ConexionControllerBD().GetAllMetadataProcessInsights()
                        InsightsDateInformationProcess = ConexionControllerBD().GetDateMetadataProcessInsights()
                        TotalRegistrosEncontradosWorking = len(InsightsAllInformationProcess) + len(InsightsDateInformationProcess)
                        print(f"Datos obtenidos de la base de datos: {TotalRegistrosEncontradosWorking}")
                        for InsightsRowInformationProcess in chain(InsightsAllInformationProcess, InsightsDateInformationProcess):
                            ConfigSearchParamts = {
                                'SearchType': ValidatedIsNone(InsightsRowInformationProcess.FIL_CTYPE_CONFIG),
                                'SearchName': '',
                                'SearchServer': '',
                                'SearchDatabase': '',
                                'SearchUsername': '',
                                'SearchPassword': '',
                                'SearchDetalleOne': ValidatedIsNone(InsightsRowInformationProcess.INS_CMTDT_FILTER_VALOR_ONE),
                                'SearchDetalleTwo': ValidatedIsNone(InsightsRowInformationProcess.INS_CMTDT_FILTER_VALOR_TWO)
                            }
                            ConfigSearchMetadata = {
                                'TypeSearchMetadata': ValidatedIsNone(InsightsRowInformationProcess.FIL_CTYPE_CONFIG),
                                'RouteSearchMetadata': '',
                                'ColumnsSearchMetadata': ValidatedIsNone(InsightsRowInformationProcess.INS_CCOLUMNS_MTDT_RETURN)
                            }

                            JsonKeyIA = ValidatedIsNone(InsightsRowInformationProcess.INS_NID)
                            NameConfig = ValidatedIsNone(InsightsRowInformationProcess.INS_CNAME_INSIGHTS)
                            ClientConfig = ValidatedIsNone(InsightsRowInformationProcess.CLI_CCLIENTE_NAME)
                            PaisConfig = ValidatedIsNone(InsightsRowInformationProcess.INS_CPAIS_INSIGHTS)
                            TypeDateConfig = ValidatedIsNone(InsightsRowInformationProcess.INS_CTYPE_DATE_CONFIG)
                            ConfigSearchParamts = ValidatedIsNone(ConfigSearchParamts)
                            TypeConexion = ValidatedIsNone(InsightsRowInformationProcess.CON_CTIPO_CONNECT)
                            ServerConexionConfig = ValidatedIsNone(InsightsRowInformationProcess.CON_CSERVIDOR_CONNECT)
                            FolderConexionConfig = ValidatedIsNone(InsightsRowInformationProcess.INS_CROUTE_ADD_LIST)
                            UserConexionConfig = ValidatedIsNone(InsightsRowInformationProcess.CON_CUSUARIO_CONNECT)
                            PassConexionConfig = ValidatedIsNone(InsightsRowInformationProcess.CON_CPASSWORD_CONNECT)
                            TypeRouteListFolderConnect = ValidatedIsNone(InsightsRowInformationProcess.INS_CTYPE_ROUTE_LIST)
                            TypeSearchMetadata = ValidatedIsNone(InsightsRowInformationProcess.FIL_CTYPE_CONFIG)
                            ConfigSearchMetadata = ValidatedIsNone(ConfigSearchMetadata)
                            MaxFilesAudiosPermitted = ValidatedIsNone(InsightsRowInformationProcess.INS_NMAX_AUDIOS_DAY)
                            AzureCompany = ValidatedIsNone(InsightsRowInformationProcess.AZU_CCOMPANY)
                            AzureCustomer = ValidatedIsNone(InsightsRowInformationProcess.AZU_CCUSTOMER)
                            AzureSession = ValidatedIsNone(InsightsRowInformationProcess.AZU_CSESSION)
                            AzureSubSession = ValidatedIsNone(InsightsRowInformationProcess.AZU_CSUBSESSION)
                            AzureAudioLanguage = ValidatedIsNone(InsightsRowInformationProcess.AZU_CLANGUAGE)

                            if hasattr(InsightsRowInformationProcess, "DAT_DFECHA_PROCESO"):
                                FechaWorkProcess = ValidatedIsNone(InsightsRowInformationProcess.DAT_DFECHA_PROCESO)
                                IdInsightsDateWorkProcess = ValidatedIsNone(InsightsRowInformationProcess.DAT_NID)
                            else:
                                FechaWorkProcess = None
                                IdInsightsDateWorkProcess = 0
                            print(TypeConexion)
                            print("Configuración ------------------------------------------")
                            print(f"{NameConfig}-{FechaWorkProcess}")
                            HiloMultiprocessing = multiprocessing.Process(target=InsightsFunctions.ExecuteProcessInsights, args=(JsonKeyIA, NameConfig, ClientConfig, PaisConfig, TypeConexion, ServerConexionConfig, FolderConexionConfig, UserConexionConfig, PassConexionConfig, TypeRouteListFolderConnect, TypeDateConfig, ConfigSearchParamts, ConfigSearchMetadata, MaxFilesAudiosPermitted, AzureCompany, AzureCustomer, AzureSession, AzureSubSession, AzureAudioLanguage, FechaWorkProcess, IdInsightsDateWorkProcess))
                            HiloMultiprocessing.start()
                            ListaMultiprocessingProcessActives.append(HiloMultiprocessing)
                            if TotalRegistrosEncontradosWorking >= self.CantidadClientesWorkingThreads:
                                self.SleepTerminatedAllProcess(ListaMultiprocessingProcessActives)
                        self.SleepTerminatedAllProcess(ListaMultiprocessingProcessActives, True)
                    except Exception as e:
                        print("Error Ejecucion...")
                        print(f"Error ejecutando registro en línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")
                    finally:
                        pass
                elif self.TypeExecutePandaInsightsProcess == "JSON":
                    with open(self.FileJsonConfigInitInsights, "r", encoding="utf-8") as AtenaFileIA:
                        AtenaJsonIA = json.load(AtenaFileIA)
                    ItemsAtenaJsonIA = AtenaJsonIA.items()
                    print(len(ItemsAtenaJsonIA))
                    for JsonKeyIA, JsonValueIA in ItemsAtenaJsonIA:
                        print(JsonValueIA["TypeConexionConfig"]["TypeConexion"])
                        EstadoConfig = JsonValueIA["EstadoConfig"]
                        if EstadoConfig != "Activo":
                            continue
                        NameConfig = JsonValueIA["NameConfig"]
                        print("NameConfig------------------------------------------")
                        print(NameConfig)
                        ClientConfig = JsonValueIA["ClientConfig"]
                        PaisConfig = JsonValueIA["PaisConfig"]
                        TypeDateConfig = JsonValueIA["TypeDateConfig"]
                        ConfigSearchParamts = JsonValueIA["ConfigSearchParamts"]
                        TypeConexion = JsonValueIA["TypeConexionConfig"]["TypeConexion"]
                        ServerConexionConfig = JsonValueIA["TypeConexionConfig"]["ServerConexionConfig"]
                        FolderConexionConfig = JsonValueIA["TypeConexionConfig"]["FolderConexionConfig"]
                        UserConexionConfig = JsonValueIA["TypeConexionConfig"]["UserConexionConfig"]
                        PassConexionConfig = JsonValueIA["TypeConexionConfig"]["PassConexionConfig"]
                        TypeRouteListFolderConnect = JsonValueIA["TypeRouteListFolderConnect"]
                        TypeSearchMetadata = JsonValueIA["ConfigSearchMetadata"]["TypeSearchMetadata"]
                        ConfigSearchMetadata = JsonValueIA["ConfigSearchMetadata"]
                        MaxFilesAudiosPermitted = JsonValueIA["MaxFilesAudiosPermitted"]
                        AzureCompany = JsonValueIA["ConfigJsonAzure"]["AzureCompany"]
                        AzureCustomer = JsonValueIA["ConfigJsonAzure"]["AzureCustomer"]
                        AzureSession = JsonValueIA["ConfigJsonAzure"]["AzureSession"]
                        AzureSubSession = JsonValueIA["ConfigJsonAzure"]["AzureSubSession"]
                        AzureAudioLanguage = JsonValueIA["ConfigJsonAzure"]["AzureAudioLanguage"]
                        HiloMultiprocessing = multiprocessing.Process(target=InsightsFunctions.ExecuteProcessInsights, args=(JsonKeyIA, NameConfig, ClientConfig, PaisConfig, TypeConexion, ServerConexionConfig, FolderConexionConfig, UserConexionConfig, PassConexionConfig, TypeRouteListFolderConnect, TypeDateConfig, ConfigSearchParamts, ConfigSearchMetadata, MaxFilesAudiosPermitted, AzureCompany, AzureCustomer, AzureSession, AzureSubSession, AzureAudioLanguage))
                        HiloMultiprocessing.start()
                        ListaMultiprocessingProcessActives.append(HiloMultiprocessing)
                        if len(ItemsAtenaJsonIA) > 4:
                            self.SleepTerminatedAllProcess(ListaMultiprocessingProcessActives)
                    self.SleepTerminatedAllProcess(ListaMultiprocessingProcessActives, True)
        except Exception as e:
            print("Error Proceso...")
            print(f"Error procesando registro en línea {sys.exc_info()[-1].tb_lineno}: {str(e)}")

    def SleepTerminatedAllProcess(self, ListaMultiprocessingProcessActives, SleepAll=False):
        while 1:
            if (len(ListaMultiprocessingProcessActives) < self.CantidadClientesWorkingThreads and SleepAll == False) or (len(ListaMultiprocessingProcessActives) == 0):
                break
            for MultiprocessingProcessActives in ListaMultiprocessingProcessActives:
                if MultiprocessingProcessActives.is_alive():
                    pass
                else:
                    print(f"⏳ El proceso ha terminado {ListaMultiprocessingProcessActives[0]}...")
                    ListaMultiprocessingProcessActives.remove(MultiprocessingProcessActives)
            time.sleep(1)

    def ExecuteProcessWiseCX(self):
        self.__init__()
        try:
            print("Inicio Proceso Validación Text Analytics...")
            WiseCXFunctions = FunctionsWiseCX(self.DiskPandaInsightsRPA)
            WiseCXFunctions.ExecuteProcessWiseCX("COLOMBIA", "ATENTO COLOMBIA", "PRUEBA_TEXTO", "VTR", "VTR", "es")
        except:
            pass

    def ExecuteProcessZendesk(self):
        self.__init__()
        try:
            print("Inicio Proceso Validación Zendesk...")
            # Consulta la BD la tabla TBL_TDOWNLOAD_PROCESS para traer todo lo pendiente en estado Unico y Zendesk
            DescargasZendeskPendientes = ConexionControllerBD().GetDescargasZendeskPendientes()
            CountDescargasZendeskPendientes = len(DescargasZendeskPendientes)
            print(f"Cantidad de descargas pendientes en Zendesk: {CountDescargasZendeskPendientes}")
            if CountDescargasZendeskPendientes > 0:
                for DownloadZendeskPendientes in DescargasZendeskPendientes:
                    AzureCompany = ValidatedIsNone(DownloadZendeskPendientes.AZU_CCOMPANY)
                    AzureCustomer = ValidatedIsNone(DownloadZendeskPendientes.AZU_CCUSTOMER)
                    AzureSession = ValidatedIsNone(DownloadZendeskPendientes.AZU_CSESSION)
                    AzureSubSession = ValidatedIsNone(DownloadZendeskPendientes.AZU_CSUBSESSION)
                    AzureAudioLanguage = ValidatedIsNone(DownloadZendeskPendientes.AZU_CLANGUAGE)
                    TicketsZendeskSearch = DownloadZendeskPendientes.DOW_CDETALLE_DOWNLOAD1
                    print("TicketsZendeskSearch")
                    print(TicketsZendeskSearch)
                    ZendeskFunctions = FunctionsZendeskChat(self.DiskPandaInsightsRPA)
                    ZendeskFunctions.Run("COLOMBIA", AzureCompany, AzureCustomer, AzureSession, AzureSubSession, AzureAudioLanguage, TicketsZendeskSearch)
        except Exception as e:
            print(f"Error en ExecuteProcessZendesk Linea {sys.exc_info()[-1].tb_lineno}: {e}")

    def ProgramacionProcess(self):
        if self.ExecutePandaProcessWiseCX:
            PandaExecuteProcessWiseCXTimer = PandaTimer(HoraInit="00:30:00", DelaySleep=2, FunctionExe=self.ExecuteProcessWiseCX)
            PandaExecuteProcessWiseCXTimer.start()
        if self.ExecutePandaProcessInsights:
            PandaExecuteProcessInsightsTimer = PandaTimer(HoraInit="06:00:00", DelaySleep=2, FunctionExe=self.ExecuteProcessInsights, InitMinuteExecute=2)
            PandaExecuteProcessInsightsTimer.start()
        """ if self.ExecutePandaProcessZendesk:
            PandaExecuteProcessZendeskTimer = PandaTimer(HoraInit="00:01:00", DelaySleep=2, FunctionExe=self.ExecuteProcessZendesk, InitMinuteExecute=3)
            PandaExecuteProcessZendeskTimer.start() """

    def StartProcess(self):
        #self.ExecuteProcessInsights()
        #self.ExecuteProcessWiseCX()
        self.ExecuteProcessZendesk()
        #self.ProgramacionProcess()

if __name__ == "__main__":
    """ GetDescargasZendeskPendientes = ConexionControllerBD().GetDescargasZendeskPendientes()
    print(len(GetDescargasZendeskPendientes))
    for item in GetDescargasZendeskPendientes:
        print(item) """
    multiprocessing.freeze_support()
    InsightsProcess = InsightsProcessRPA()
    InsightsProcess.StartProcess()
