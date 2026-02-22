from datetime import timedelta, datetime
from threading import Thread
from time import sleep

class PandaTimer(Thread):
    def __init__(self, HoraInit, DelaySleep, FunctionExe, InitMinuteExecute=0):
        super(PandaTimer, self).__init__()
        self.StateProcessInit = True
        self.HoraGapInitGapInit = HoraInit
        self.DelayInitSleep = DelaySleep
        self.FunctionInitExe = FunctionExe
        self.MinutesExecute = InitMinuteExecute

    def run(self):
        AuxHourAct = datetime.strptime(self.HoraGapInitGapInit, '%H:%M:%S')
        HourActual = datetime.today()
        HourActual = HourActual.replace(hour=AuxHourAct.hour, minute=AuxHourAct.minute, second=AuxHourAct.second, microsecond=0)
        while HourActual <= datetime.today():
            if self.MinutesExecute > 0:
                HourActual += timedelta(minutes=self.MinutesExecute)
            else:
                HourActual += timedelta(hours=24)

        if HourActual <= datetime.today():
            if self.MinutesExecute > 0:
                HourActual += timedelta(minutes=self.MinutesExecute)
            else:
                HourActual += timedelta(hours=24)
        print('Proceso de programacion automatico iniciado...', flush=True)
        print(f'Ejecucion automatica programada para el {HourActual.date()} a las {HourActual.time()}', flush=True)

        while self.StateProcessInit:
            if HourActual <= datetime.today():
                print('Ejecución programada ejecutada el {0} a las {1}'.format(HourActual.date(), HourActual.time()), flush=True)
                self.FunctionInitExe()
                while HourActual <= datetime.today():
                    if self.MinutesExecute > 0:
                        HourActual += timedelta(minutes=self.MinutesExecute)
                    else:
                        HourActual += timedelta(hours=24)
                print('Próxima ejecución programada el {0} a las {1}'.format(HourActual.date(),  HourActual.time()), flush=True)
            sleep(self.DelayInitSleep)
        else:
            print('Ejecución automática finalizada', flush=True)