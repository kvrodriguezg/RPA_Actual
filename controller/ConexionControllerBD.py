import sys
from models.Tablas import *
from models.Conexion import DatabaseManager

class ConexionControllerBD:
    def __init__(self):
        pass

    def Initialize(self):
        self.ManagerBD = DatabaseManager()

    def GetConnection(self):
        return self.ManagerBD.GetSession()

    def CloseConnection(self):
        self.ManagerBD.CloseSession()

    def GetAllMetadataProcessInsights(self, PaisConsultProcessInsights="COLOMBIA"):
        try:
            self.Initialize()
            DatabaseSessionManager = self.GetConnection()
            InsightsInformationProcess = DatabaseSessionManager.query(
                TBL_TINSIGHTS_PROCESS.INS_NID,
                TBL_RCONNECT_CONFIGURATION.CON_CTIPO_CONNECT,
                TBL_RCLIENTS_CONFIG.CLI_CCLIENTE_NAME,
                TBL_TINSIGHTS_PROCESS.INS_CPAIS_INSIGHTS,
                TBL_TINSIGHTS_PROCESS.INS_CTYPE_ROUTE_LIST,
                TBL_TINSIGHTS_PROCESS.INS_CTYPE_DATE_CONFIG,
                #ConfigSearchParamts
                TBL_RFILTER_CONFIG_INFORMATION.FIL_CTYPE_CONFIG,
                TBL_RFILTER_CONFIG_INFORMATION.FIL_CNOMBRE_CONFIG,
                TBL_TINSIGHTS_PROCESS.INS_CMTDT_FILTER_VALOR_ONE,
                TBL_TINSIGHTS_PROCESS.INS_CMTDT_FILTER_VALOR_TWO,
                #End ConfigSearchParamts
                #TBL_RCONNECT_CONFIGURATION.CON_CTIPO_CONNECT,
                TBL_RCONNECT_CONFIGURATION.CON_CSERVIDOR_CONNECT,
                TBL_TINSIGHTS_PROCESS.INS_CROUTE_ADD_LIST,
                TBL_RCONNECT_CONFIGURATION.CON_CUSUARIO_CONNECT,
                TBL_RCONNECT_CONFIGURATION.CON_CPASSWORD_CONNECT,
                #ConfigSearchMetadata
                ##TBL_RFILTER_CONFIG_INFORMATION.FIL_CTYPE_CONFIG
                TBL_TINSIGHTS_PROCESS.INS_CCOLUMNS_MTDT_RETURN,
                #End ConfigSearchMetadata
                TBL_TINSIGHTS_PROCESS.INS_NMAX_AUDIOS_DAY,
                TBL_RAZURE_CONFIGURATION.AZU_CCOMPANY,
                TBL_RAZURE_CONFIGURATION.AZU_CCUSTOMER,
                TBL_RAZURE_CONFIGURATION.AZU_CSESSION,
                TBL_RAZURE_CONFIGURATION.AZU_CSUBSESSION,
                TBL_RAZURE_CONFIGURATION.AZU_CLANGUAGE,
                TBL_TINSIGHTS_PROCESS.INS_CNAME_INSIGHTS,
                #Filtrado para los Activos y Joins para sacar información de otras tablas
                TBL_TINSIGHTS_PROCESS.INS_CESTADO_REGISTRO
                ).join(
                    TBL_RCONNECT_CONFIGURATION, TBL_TINSIGHTS_PROCESS.INS_NCON_NID == TBL_RCONNECT_CONFIGURATION.CON_NID
                ).join(
                    TBL_RCLIENTS_CONFIG, TBL_TINSIGHTS_PROCESS.INS_NCLI_NID == TBL_RCLIENTS_CONFIG.CLI_NID
                ).join(
                    TBL_RFILTER_CONFIG_INFORMATION, TBL_TINSIGHTS_PROCESS.INS_NFIL_NID == TBL_RFILTER_CONFIG_INFORMATION.FIL_NID
                ).join(
                    TBL_RAZURE_CONFIGURATION, TBL_TINSIGHTS_PROCESS.INS_NAZU_NID == TBL_RAZURE_CONFIGURATION.AZU_NID
                ).filter(
                    TBL_TINSIGHTS_PROCESS.INS_CPAIS_INSIGHTS == PaisConsultProcessInsights,
                    TBL_TINSIGHTS_PROCESS.INS_CESTADO_REGISTRO == "Activo",
                    TBL_TINSIGHTS_PROCESS.INS_CESTADO_PROCESO == "Pendiente"
                ).all()
            return InsightsInformationProcess
        except Exception as e:
            print(f"Error Consultando en la linea {sys.exc_info()[-1].tb_lineno}: {e}")
        finally:
            try:
                DatabaseSessionManager.close()
                self.CloseConnection()
            except:
                pass
    
    def GetDateMetadataProcessInsights(self, PaisConsultProcessInsights="COLOMBIA"):
        try:
            self.Initialize()
            DatabaseSessionManager = self.GetConnection()
            InsightsInformationProcess = DatabaseSessionManager.query(
                TBL_TINSIGHTS_PROCESS.INS_NID,
                TBL_RCONNECT_CONFIGURATION.CON_CTIPO_CONNECT,
                TBL_RCLIENTS_CONFIG.CLI_CCLIENTE_NAME,
                TBL_TINSIGHTS_PROCESS.INS_CPAIS_INSIGHTS,
                TBL_TINSIGHTS_PROCESS.INS_CTYPE_ROUTE_LIST,
                TBL_TINSIGHTS_PROCESS.INS_CTYPE_DATE_CONFIG,
                #ConfigSearchParamts
                TBL_RFILTER_CONFIG_INFORMATION.FIL_CTYPE_CONFIG,
                TBL_RFILTER_CONFIG_INFORMATION.FIL_CNOMBRE_CONFIG,
                TBL_TINSIGHTS_PROCESS.INS_CMTDT_FILTER_VALOR_ONE,
                TBL_TINSIGHTS_PROCESS.INS_CMTDT_FILTER_VALOR_TWO,
                #End ConfigSearchParamts
                #TBL_RCONNECT_CONFIGURATION.CON_CTIPO_CONNECT,
                TBL_RCONNECT_CONFIGURATION.CON_CSERVIDOR_CONNECT,
                TBL_TINSIGHTS_PROCESS.INS_CROUTE_ADD_LIST,
                TBL_RCONNECT_CONFIGURATION.CON_CUSUARIO_CONNECT,
                TBL_RCONNECT_CONFIGURATION.CON_CPASSWORD_CONNECT,
                #ConfigSearchMetadata
                ##TBL_RFILTER_CONFIG_INFORMATION.FIL_CTYPE_CONFIG
                TBL_TINSIGHTS_PROCESS.INS_CCOLUMNS_MTDT_RETURN,
                #End ConfigSearchMetadata
                TBL_TINSIGHTS_PROCESS.INS_NMAX_AUDIOS_DAY,
                TBL_RAZURE_CONFIGURATION.AZU_CCOMPANY,
                TBL_RAZURE_CONFIGURATION.AZU_CCUSTOMER,
                TBL_RAZURE_CONFIGURATION.AZU_CSESSION,
                TBL_RAZURE_CONFIGURATION.AZU_CSUBSESSION,
                TBL_RAZURE_CONFIGURATION.AZU_CLANGUAGE,
                TBL_TINSIGHTS_PROCESS.INS_CNAME_INSIGHTS,
                #Filtrado para los Activos y Joins para sacar información de otras tablas
                TBL_TINSIGHTS_PROCESS.INS_CESTADO_REGISTRO,
                TBL_TDATE_INSIGHTS_PROCESS.DAT_DFECHA_PROCESO,
                TBL_TDATE_INSIGHTS_PROCESS.DAT_NID
                ).join(
                    TBL_RCONNECT_CONFIGURATION, TBL_TINSIGHTS_PROCESS.INS_NCON_NID == TBL_RCONNECT_CONFIGURATION.CON_NID
                ).join(
                    TBL_RCLIENTS_CONFIG, TBL_TINSIGHTS_PROCESS.INS_NCLI_NID == TBL_RCLIENTS_CONFIG.CLI_NID
                ).join(
                    TBL_RFILTER_CONFIG_INFORMATION, TBL_TINSIGHTS_PROCESS.INS_NFIL_NID == TBL_RFILTER_CONFIG_INFORMATION.FIL_NID
                ).join(
                    TBL_RAZURE_CONFIGURATION, TBL_TINSIGHTS_PROCESS.INS_NAZU_NID == TBL_RAZURE_CONFIGURATION.AZU_NID
                ).join(
                    TBL_TDATE_INSIGHTS_PROCESS, TBL_TINSIGHTS_PROCESS.INS_NID == TBL_TDATE_INSIGHTS_PROCESS.DAT_NINS_NID
                ).filter(
                    TBL_TINSIGHTS_PROCESS.INS_CPAIS_INSIGHTS == PaisConsultProcessInsights,
                    TBL_TDATE_INSIGHTS_PROCESS.DAT_CESTADO_REGISTRO == "Pendiente"
                ).all()
            return InsightsInformationProcess
        except Exception as e:
            print(f"Error Consultando en la linea {sys.exc_info()[-1].tb_lineno}: {e}")
        finally:
            try:
                DatabaseSessionManager.close()
                self.CloseConnection()
            except:
                pass
        
    def GetDescargasZendeskPendientes(self):
        """
        Consulta la tabla TBL_TDOWNLOAD_PROCESS con JOIN a TBL_RACCESOS_DOWNLOAD y TBL_RAZURE_CONFIGURATION
        Filtra por:
        - DOW_CESTADO_PROCESO = 'Pendiente'
        - DOW_CESTADO_REGISTRO = 'Unico'
        - DOW_CGRABADORA = 'Zendesk'
        - ACC_CESTADO_REGISTRO = 'Activo'
        - AZU_CCUSTOMER no vacío
        """
        self.Initialize()
        session = self.GetConnection()
        try:
            Resultados = session.query(
                TBL_TDOWNLOAD_PROCESS.DOW_NID,
                TBL_TDOWNLOAD_PROCESS.DOW_CDETALLE_DOWNLOAD1,
                TBL_TDOWNLOAD_PROCESS.DOW_CTIPO_CANTIDAD,
                TBL_TDOWNLOAD_PROCESS.DOW_NCANTIDAD_AUDIOS,
                TBL_RACCESOS_DOWNLOAD.ACC_CROUTE_URL_GRAB,
                TBL_RACCESOS_DOWNLOAD.ACC_CUSUARIO,
                TBL_RACCESOS_DOWNLOAD.ACC_CPASSWORD,
                TBL_RAZURE_CONFIGURATION.AZU_CCOMPANY,
                TBL_RAZURE_CONFIGURATION.AZU_CCUSTOMER,
                TBL_RAZURE_CONFIGURATION.AZU_CSESSION,
                TBL_RAZURE_CONFIGURATION.AZU_CSUBSESSION,
                TBL_RAZURE_CONFIGURATION.AZU_CLANGUAGE
            ).join(
                TBL_RACCESOS_DOWNLOAD, TBL_RACCESOS_DOWNLOAD.ACC_NID == TBL_TDOWNLOAD_PROCESS.DOW_NACC_NID
            ).join(
                TBL_RAZURE_CONFIGURATION, TBL_TDOWNLOAD_PROCESS.DOW_NAZU_NID == TBL_RAZURE_CONFIGURATION.AZU_NID
            ).filter(
                TBL_TDOWNLOAD_PROCESS.DOW_CESTADO_PROCESO == 'Pendiente',
                TBL_TDOWNLOAD_PROCESS.DOW_CESTADO_REGISTRO == 'Unico',
                TBL_RACCESOS_DOWNLOAD.ACC_CGRABADORA == 'Zendesk',
                TBL_RACCESOS_DOWNLOAD.ACC_CESTADO_REGISTRO == 'Activo',
                TBL_RAZURE_CONFIGURATION.AZU_CCUSTOMER != None,
                TBL_RAZURE_CONFIGURATION.AZU_CCUSTOMER != ''
            ).all()
            return Resultados
        except Exception as e:
            print(f"Error consultando descargas Zendesk pendientes con JOIN: {e}")
            return []
        finally:
            session.close()
            self.CloseConnection()

