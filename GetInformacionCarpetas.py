import os
import csv

def obtener_subcarpetas(ruta, camino=[]):
    estructura = []
    for entrada in os.scandir(ruta):
        if entrada.is_dir():
            nuevo_camino = camino + [entrada.name]
            estructura.append(nuevo_camino)
            estructura.extend(obtener_subcarpetas(entrada.path, nuevo_camino))
    return estructura

def guardar_en_csv(estructura, nombre_archivo):
    max_niveles = max(len(fila) for fila in estructura) if estructura else 0
    with open(nombre_archivo, mode='w', newline='', encoding='utf-8') as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow([f"Nivel {i+1}" for i in range(max_niveles)])
        for fila in estructura:
            escritor.writerow(fila + [""] * (max_niveles - len(fila)))

def imprimir_estructura(estructura):
    for fila in estructura:
        print("  " * (len(fila) - 1) + "|- " + fila[-1])

if __name__ == "__main__":
    ruta_principal = input("Ingrese la ruta de la carpeta: ")
    if os.path.isdir(ruta_principal):
        print("obtener_subcarpetas------")
        estructura = obtener_subcarpetas(ruta_principal)
        imprimir_estructura(estructura)
        nombre_csv = "estructura_carpetas.csv"
        guardar_en_csv(estructura, nombre_csv)
        print(f"Estructura guardada en {nombre_csv}")
    else:
        print("La ruta ingresada no es válida o no es una carpeta.")
