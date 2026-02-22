from Libraries import *
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class FunctionsZendeskChat:
    def __init__(self, DiskInsightsRPA):
        self.DiskInsightsPandaRPA = DiskInsightsRPA
        self.RouteFolderWorkingKaizenChat = os.path.join(f'{self.DiskInsightsPandaRPA}:\\Insights\\', f'ZendeskChat')
        self.RutaChromedriver = os.path.join(self.RouteFolderWorkingKaizenChat, "chromedriver.exe")
        self.RutaCache = os.path.join(self.RouteFolderWorkingKaizenChat, "ChromCache")
        os.makedirs(self.RutaCache, exist_ok=True)
        self.ChromeOptions = Options()
        self.ChromeOptions.add_argument(f"--user-data-dir={self.RutaCache}")
        self.ChromeOptions.add_experimental_option("detach", True)
        #self.Service = Service(ChromeDriverManager().install())
        self.Service = Service(executable_path=self.RutaChromedriver)
        self.Driver = webdriver.Chrome(service=self.Service, options=self.ChromeOptions)
        self.OutputFolder = os.path.join(self.RouteFolderWorkingKaizenChat, "UploadJson")
        os.makedirs(self.OutputFolder, exist_ok=True)
        self.TotalListarArchivosGenerados = self.ListJsonFiles(self.OutputFolder)

    def SafeGetText(self, Xpath):
        try:
            return self.Driver.find_element(By.XPATH, Xpath).text.strip()
        except:
            return ""

    def ListJsonFiles(self, Folder):
        JsonFiles = [F for F in os.listdir(Folder) if F.endswith('.json')]
        JsonFiles = [F.split("_")[0].replace("#", "") for F in JsonFiles]
        return JsonFiles

    def Run(self, PaisProcess, AzureCompany, AzureCustomer, AzureSession, AzureSubSession, AzureAudioLanguage, TicketIds):
        try:
            self.PaisProcess = PaisProcess
            self.AzureCompany = AzureCompany
            self.AzureCustomer = AzureCustomer
            self.AzureSession = AzureSession
            self.AzureSubSession = AzureSubSession
            self.AzureAudioLanguage = AzureAudioLanguage
            print(self.TotalListarArchivosGenerados)
            for TicketId in TicketIds:
                self.TotalListarArchivosGenerados = self.ListJsonFiles(self.OutputFolder)
                if str(TicketId) in self.TotalListarArchivosGenerados:
                    print(f" El archivo para el ticket ID {TicketId} ya existe. Se omitirá la creación del JSON.")
                    continue
                Url = f"https://stoiximan.zendesk.com/tickets/{TicketId}/print"
                self.ProcessTicket(TicketId, Url)

            print("------------------Iniciando Proceso Cargue Json Zendesk-------------------------")
            self.SubirArchivosAzure(self.OutputFolder, f"input/{AzureCustomer}")
        except Exception as e:
            print(f"Error Proceso Zendesk Linea {sys.exc_info()[-1].tb_lineno}: {e}")
        finally:
            ClearDirectoryMajor(self.OutputFolder)
            try:
                self.Driver.quit()
            except:
                pass

    def ProcessTicket(self, TicketId, Url):
        try:
            self.Driver.get(Url)
            WebDriverWait(self.Driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="info"]/div[3]/p'))
            )
            Titulo = self.SafeGetText('//*[@id="title"]')
            FileName = f"{Titulo}.json".replace(" ", "_").replace("/", "_").replace(":", "_").replace(";", "_").replace("?", "_").replace("!", "_").replace("¡", "_")
            FilePath = os.path.join(self.OutputFolder, FileName)
            if os.path.exists(FilePath):
                print(f" El archivo '{FileName}' ya existe. Se omitirá la creación del JSON.")
                return
            CustomerNameRaw = self.SafeGetText('//*[@id="info"]/div[3]/p')
            Match = re.match(r"^(.*?)\s*<.*?>$", CustomerNameRaw)
            if Match:
                CustomerName = Match.group(1).strip()
            else:
                CustomerName = CustomerNameRaw.strip()
            DurationStr = self.SafeGetText('//*[@id="custom_fields"]/div[7]/p')
            RawDatetime = self.SafeGetText('//*[@id="info"]/div[1]/p/time')
            try:
                RawDatetime = RawDatetime.strip()
                if "at" in RawDatetime:
                    RawDatetime = RawDatetime.replace(" at ", " ")
                    ParsedDatetime = datetime.strptime(RawDatetime, "%d %b %Y %H:%M")
                    DatetimeVal = ParsedDatetime.strftime("%Y-%m-%d %H:%M:00")
                else:
                    DatetimeVal = RawDatetime
            except Exception as E:
                print("Error al convertir fecha:", E)
                DatetimeVal = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            AgentName = self.SafeGetText('//*[contains(text(),"Assignee")]/parent::*/p')
            try:
                DurationSec = float(DurationStr)
            except ValueError:
                DurationSec = None
            Resultado = {
                "audio_info": {
                    "duration_sec": DurationSec,
                    "extraction_metadata": {
                        "country": self.PaisProcess,
                        "company": self.AzureCompany,
                        "customer": self.AzureCustomer,
                        "session": self.AzureSession,
                        "sub-session": self.AzureSubSession,
                        "audio-language": self.AzureAudioLanguage,
                        "datetime": DatetimeVal,
                        "customer_name": CustomerName,
                        "agent_name": AgentName,
                        "numero_caso": TicketId
                    }
                },
                "transcription_items": {
                    "transcription": []
                }
            }
            CommentsSection = self.Driver.find_element(By.ID, 'comments')
            Comments = CommentsSection.find_elements(By.CLASS_NAME, 'comment')
            for Index, Comment in enumerate(Comments):
                try:
                    Speaker = Comment.find_element(By.CLASS_NAME, 'author').text.strip()
                except:
                    Speaker = ""
                try:
                    BodyElement = Comment.find_element(By.CLASS_NAME, 'body')
                    Text = BodyElement.text.strip()
                except:
                    Text = ""
                SpeakerClean = Speaker.lower().strip()
                CustomerClean = CustomerName.lower().strip()
                if SpeakerClean == CustomerClean:
                    Speaker = "CUSTOMER"
                else:
                    Speaker = "AGENT"
                Resultado["transcription_items"]["transcription"].append({
                    "start": Index,
                    "end": Index,
                    "text": Text,
                    "speaker": Speaker
                })
            with open(FilePath, "w", encoding="utf-8") as F:
                json.dump(Resultado, F, indent=4, ensure_ascii=False)
            print(f" JSON generado correctamente como '{FileName}'")
        except Exception as E:
            print(" Error:", E)
        finally:
            pass
    
    def SubirArchivosAzure(self, Origen, Destino):
        Cuenta = "sausaistudioco"
        TokenSas = "shRnDqJuN+FKJLnawC6NrtLsYycemBycES3hjF/qv83gNfvK0t19mFnjKa78IQyXuBwuDfeWCgIW+AStdUiAPQ=="
        Contenedor = "transcriptions"
        BlobService = BlobServiceClient(account_url=f"https://{Cuenta}.blob.core.windows.net", credential=TokenSas)
        ContainerClient = BlobService.get_container_client(Contenedor)

        ArchivosJson = [Archivo for Archivo in os.listdir(Origen) if Archivo.lower().endswith('.json') and os.path.isfile(os.path.join(Origen, Archivo))]
        Total = len(ArchivosJson)
        for Indice, Archivo in enumerate(ArchivosJson, start=1):
            RutaArchivo = os.path.join(Origen, Archivo)
            BlobPath = f"{Destino}/{Archivo}" if Destino else Archivo
            with open(RutaArchivo, "rb") as Data:
                ContainerClient.upload_blob(name=BlobPath, data=Data, overwrite=True)
            barra = int(40 * Indice / Total)
            sys.stdout.write(f"\r[{'#' * barra}{'.' * (40-barra)}] {Indice}/{Total} Subido: {Archivo}   ")
            sys.stdout.flush()
        print("\nCarga finalizada.")

if __name__ == "__main__":
    KaizenChat = FunctionsZendeskChat()
    KaizenChat.Run()

#Comando para ejecutar chrome desde el cmd: 
#>"C:\Program Files\Google\Chrome\Application\chrome.exe" --user-data-dir="C:/Users/pipe-/Documents/Conversor-JSON/chrome_cache/"