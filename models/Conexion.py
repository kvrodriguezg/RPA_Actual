from Libraries import *

class DatabaseManager:
    def __init__(self):
        db_url = "mssql+pyodbc://pro_aud:pro_aud@172.17.122.1\\MSSQLD19/ATENTO_RPA_PROCESS_AUDIO_INSIGNHTS?driver=ODBC+Driver+17+for+SQL+Server"
        db_url = "mssql+pyodbc://pro_aud:pro_aud@COLBOGSQL25\\MSSQL_2019/ATENTO_RPA_PROCESS_AUDIO_INSIGNHTS?driver=ODBC+Driver+17+for+SQL+Server"
        self.Engine = create_engine(db_url, pool_size=10, max_overflow=20)
        self.Session = scoped_session(sessionmaker(bind=self.Engine))

    def GetSession(self):
        return self.Session()
    
    def CloseSession(self):
        self.Session.remove()

class DynamicDbConnection:
    def __init__(self):
        self.Connections = threading.local()

    def Connect(self, dbUrl):
        if not hasattr(self.Connections, "Engine"):
            self.Connections.Engine = create_engine(dbUrl, pool_recycle=3600)
        return self.Connections.Engine.connect()

    def ExecuteQuery(self, query, dbUrl):
        with self.Connect(dbUrl) as connection:
            result = connection.execute(text(query))
            return result.fetchall()

    def CloseConnection(self):
        if hasattr(self.Connections, "Engine"):
            self.Connections.Engine.dispose()
            del self.Connections.Engine

