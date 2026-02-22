import requests, json, os, sys, base64, ftplib, io, shutil, time, re, glob, multiprocessing, paramiko, zipfile, stat, random
from datetime import datetime, timedelta
from ftplib import  FTP
from pydub import AudioSegment
from azure.storage.blob import BlobServiceClient
from urllib.parse import urlparse
import pandas as pd
import unicodedata
import xmltodict
import threading
from sqlalchemy import create_engine, text, Column, String, Integer, Boolean, JSON, create_engine, Table, MetaData, DateTime
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.sql import insert, update
from itertools import chain

def ConvertirSegundosFormatoHoraMili(SecondConvert):
    TimeDelta = timedelta(seconds=SecondConvert)
    return f"{TimeDelta.seconds // 3600:02}:{(TimeDelta.seconds % 3600) // 60:02}:{TimeDelta.seconds % 60:02}.000"

def LimpiarTextForJsonCorrect(TextClean):
    TextClean = TextClean.replace("'", "").replace('"', "'")
    return re.sub(r'[\r\n\t]', ' ', TextClean)

def GenerarListaFechasProcessPanda(DateInit, DateEnd):
    FechaInicio = datetime.strptime(DateInit, "%Y-%m-%d")
    FechaFin = datetime.strptime(DateEnd, "%Y-%m-%d")
    return [(FechaInicio + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((FechaFin - FechaInicio).days + 1)]

def QuitarTildes(Texto):
    return ''.join(c for c in unicodedata.normalize('NFKD', Texto) if not unicodedata.combining(c))

def ReemplazarEspacios(Texto):
    return Texto.replace(" ", "_")

def ClearDirectoryMajor(FolderRouteBaseProcess):
    """Limpia los directorios de trabajo después de procesar los archivos"""
    try:
        shutil.rmtree(FolderRouteBaseProcess)
        print("Directorios limpiados correctamente")
    except Exception as e:
        print(f"Error al limpiar directorios: {e}")

def ValidatedIsNone(Value):
    return "" if Value is None else Value

def GetFileNameSingle(FileName: str) -> str:
    return os.path.splitext(FileName)[0]

def SafeScandir(PathValidated):
    try:
        return os.scandir(PathValidated)
    except Exception:
        return []

def GetUuidFromFileNameGoContactFirst(FileName: str) -> str:
    return FileName.split('_')[0]
