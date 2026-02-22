from Libraries import *

class ApiWiseCXRPA():
    def __init__(self, InBaseURL, InKeyURL, InUserURL, RouteFolderDownloadWiseCX):
        self.ApiWiseURL = InBaseURL
        self.ApiWiseKey = InKeyURL
        self.ApiWiseUser = InUserURL
        self.TokenActiveAPI = None
        self.TokenExpiresAtAPI = 0
        self.RouteFolderDownloadWiseCX = RouteFolderDownloadWiseCX

    def GetNewTokenAPI(self):
        ApiHeader = {
            "x-api-key": f"{self.ApiWiseKey}",
            "Content-Type": "application/json"
        }
        ApiParams = {
            "user": f"{self.ApiWiseUser}"
        }
        ResponseRequests = requests.get(f"{self.ApiWiseURL}/authenticate", headers=ApiHeader, params=ApiParams)
        if ResponseRequests.status_code == 200:
            JsonResponseRequests = ResponseRequests.json()
            self.TokenActiveAPI = JsonResponseRequests['token']
            self.TokenExpiresAtAPI = time.time() + 3540
        else:
            raise Exception(f"Error obteniendo token: {ResponseRequests.status_code} - {ResponseRequests.text}")
    
    def EnsureTokenValid(self):
        if self.TokenActiveAPI is None or time.time() >= self.TokenExpiresAtAPI:
            self.GetNewTokenAPI()

    def GetInitDownloadReportIdInsightsWiseCX(self, IdExport, ContadorError = 0):
        try:
            self.EnsureTokenValid()
            HeaderApiSend = {
                "Authorization": f"Bearer {self.TokenActiveAPI}",
                "x-api-key": f"{self.ApiWiseKey}",
                "Content-Type": "application/json",
                "Accept": "gzip, deflate, br",
                "Connection": "keep-alive",
                "User-Agent": "RPA_Insights_Soluciones_Atento/1.0"
            }
            BodyApiSend = {
                "columns": "all"
            }
            ResponseExport = requests.post(f"{self.ApiWiseURL}/analytics/export/{IdExport}", headers=HeaderApiSend, data=json.dumps(BodyApiSend))
            if ResponseExport.status_code == 200:
                ResponseJsonExport = ResponseExport.json()
                return "Exito", ResponseJsonExport["export_id"]
            else:
                if ResponseExport.status_code == 403:
                    self.GetNewTokenAPI()
                    raise ValueError("Sin token valido. Intentando nuevamente...")
                else:
                    return "ErrorSendApi", None
                    #return "Exito", "512f1759d26d4b4694ffa7680c9b14a9"
        except:
            if ContadorError > 2:
                return "ErrorSendApi", None
            ContadorError += 1
            return self.GetInitDownloadReportIdInsightsWiseCX(IdExport, ContadorError)
    
    def GetStatusDownloadReportIdInsightsWiseCX(self, IdReportExport, ContadorError = 0):
        try:
            self.EnsureTokenValid()
            HeaderApiSend = {
                "Authorization": f"Bearer {self.TokenActiveAPI}",
                "x-api-key": f"{self.ApiWiseKey}",
                "Content-Type": "application/json",
                "Accept": "gzip, deflate, br",
                "Connection": "keep-alive",
                "User-Agent": "RPA_Insights_Soluciones_Atento/1.0"
            }
            ResponseStatus = requests.get(f"{self.ApiWiseURL}/analytics/export/{IdReportExport}/status", headers=HeaderApiSend)
            if ResponseStatus.status_code == 200:
                ResponseJsonStatus = ResponseStatus.json()
                return "Exito", ResponseJsonStatus["progress"], ResponseJsonStatus["report_url"]
            else:
                if ResponseStatus.status_code == 403:
                    self.GetNewTokenAPI()
                    raise ValueError("Sin token valido. Intentando nuevamente...")
                else:
                    return "ErrorSendApi", None, None
        except:
            if ContadorError > 2:
                return "ErrorSendApi", None, None
            ContadorError += 1
            return self.GetStatusDownloadReportIdInsightsWiseCX(IdReportExport, ContadorError)

    def GenerarInformeWiseCX(self, UrlFileZipWiseCX, NameFileZipWiseCX, ContadorError = 0):
        try:
            ResponseFileZip = requests.get(UrlFileZipWiseCX, stream=True)
            if ResponseFileZip.status_code == 200:
                RouteFileDownloadWiseCX = os.path.join(self.RouteFolderDownloadWiseCX, NameFileZipWiseCX)
                with open(RouteFileDownloadWiseCX, "wb") as FileZip:
                    for chunk in ResponseFileZip.iter_content(chunk_size=1024):
                        FileZip.write(chunk)
                with zipfile.ZipFile(RouteFileDownloadWiseCX, "r") as zip_ref:
                    zip_ref.extractall(self.RouteFolderDownloadWiseCX)
                return "Exito", RouteFileDownloadWiseCX
            else:
                return "ErrorSendApi", None
        except:
            if ContadorError > 2:
                return "ErrorSendApi", None
            ContadorError += 1
            return self.GenerarInformeWiseCX(UrlFileZipWiseCX, NameFileZipWiseCX, ContadorError)

    def GenerarReportJsonCaseWiseCX(self, NumberCaseSearch, ContadorError = 0):
        try:
            self.EnsureTokenValid()
            HeaderApiSend = {
                "Authorization": f"Bearer {self.TokenActiveAPI}",
                "x-api-key": f"{self.ApiWiseKey}",
                "Content-Type": "application/json",
                "Accept": "gzip, deflate, br",
                "Connection": "keep-alive",
                "User-Agent": "RPA_Insights_Soluciones_Atento/1.0"
            }
            ApiParams = {
                "fields": "id%2Ctype%2Cuser_id%2Ccontent%2Ccontact_from%2Ccontacts_to%2Cattachments%2Ccreated_at%2Csending_status%2Cchannel"
            }
            ResponseRequestsJson = requests.get(f"{self.ApiWiseURL}/cases/{NumberCaseSearch}/activities?fields=id%2Ctype%2Cuser_id%2Ccontent%2Ccontact_from%2Ccontacts_to%2Cattachments%2Ccreated_at%2Csending_status%2Cchannel", headers=HeaderApiSend)
            if ResponseRequestsJson.status_code == 200:
                #NameFileReportDownload = str(ResponseRequestsJson.text).replace('"', '')
                """ print("ResponseRequestsJson.text")
                print(ResponseRequestsJson.text)
                return str(ResponseRequestsJson.text).replace('"', '')
                return ResponseRequestsJson.text """
                return ResponseRequestsJson.json()
            else:
                print(f"Error en la descarga: Código {ResponseRequestsJson.status_code}")
                print(f"Mensaje del servidor: {ResponseRequestsJson.text}")
                return "ErrorSendApi"
        except:
            if ContadorError > 2:
                return "ErrorSendApi", None
            ContadorError += 1
            return self.GenerarReportJsonCaseWiseCX(NumberCaseSearch)

if __name__ == "__main__":
    GoContactRPA = ApiWiseCXRPA("https://api.wcx.cloud/core/v1", "8fdd1af408fb47d288bed38f08e84f39", "atentochile_api")
    #One, Two = GoContactRPA.GetInitDownloadReportIdInsightsWiseCX("158019")
    #One, Two, Three = GoContactRPA.GetStatusDownloadReportIdInsightsWiseCX("512f1759d26d4b4694ffa7680c9b14a9")
    url = "https://wc-exported-files.s3-sa-east-1.amazonaws.com/at3677/2025-02/512f1759d26d4b4694ffa7680c9b14a9.zip"
    nombre_archivo = os.path.basename(urlparse(url).path)
    One, Two = GoContactRPA.GenerarInformeWiseCX(url, nombre_archivo)
    print("----------------------------One-------------------------------------")
    print(One)
    print(Two)
    nombre_archivoCSV = Two.replace(".zip", ".csv")
    print(nombre_archivoCSV)
    df = pd.read_csv(nombre_archivoCSV, delimiter=",", dtype=str)
    columna_filtro = "Caso: Resuelta"
    fecha_deseada = "2025-02-25"
    df_filtrado = df[df[columna_filtro].str.contains(fecha_deseada, na=False)]
    print("df_filtrado")
    print(len(df_filtrado))
    #for d_filtrado in df_filtrado:
    for index, fila in df_filtrado.iterrows():
        print(fila["ID: #"])
        print(fila[columna_filtro])
        Responses = GoContactRPA.GenerarReportJsonCaseWiseCX(fila["ID: #"])
        print("------------------------------------Responses------------------------------------")
        TotalItems = ""
        CountSecond = 0
        for Response in Responses:
            print(Response["type"])
            if Response["type"] == "contact_message":
                TotalItems = f"""{TotalItems}{("," if TotalItems != "" else "")}{{
                "start": {CountSecond},
                "end": {CountSecond},
                "text": "[{ConvertirSegundosFormatoHoraMili(CountSecond)} -> {ConvertirSegundosFormatoHoraMili(CountSecond)}] CUSTOMER: {Response["content"]}",
                "speaker": "CUSTOMER"
            }}"""
            else:
                TotalItems = f"""{TotalItems}{("," if TotalItems != "" else "")}{{
                "start": {CountSecond},
                "end": {CountSecond},
                "text": "[{ConvertirSegundosFormatoHoraMili(CountSecond)} -> {ConvertirSegundosFormatoHoraMili(CountSecond)}] AGENT: {Response["content"]}",
                "speaker": "AGENT"
            }}"""

            CountSecond += 1
        print("TotalItems")
        """ print(TotalItems) """
        JsonWithTranscription = f'''{{
    "audio_info": {{
        "duration_sec": {CountSecond}.00,
        "extraction_metadata": {{
            "country": "COLOMBIA",
            "company": "ATENTO COLOMBIA",
            "customer": "PRUEBA_TEXTO",
            "session": "TESTES_TEXTO",
            "sub-session": "PRUEBA1",
            "audio-language": "es",
            "datetime": "2025-01-24 17:46"
        }}
    }},
    "transcription_items": {{
        "transcription": [
            {TotalItems}
        ]
    }}
}}
'''
        print("**********************************************JsonWithTranscription**********************************************")
        print(JsonWithTranscription)
        break
    """ for index, fila in df_filtrado.iterrows():
        print(fila.to_dict()) """
    """ print(Two)
    print(Three) """



