# Estructura para ISAM
#con una estructura fija
import os
import struct
from typing import List, Optional, Tuple

BLOCK_FACTOR = 3      
INDEX_FACTOR = 4      

class Record:
    FORMAT = '<i30sif10s?' 
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, key: int, nombre: str, cantidad: int, precio: float, fecha: str, deleted=False):
        self.key = key
        self.nombre = nombre
        self.cantidad = cantidad
        self.precio = precio
        self.fecha = fecha
        self.deleted = deleted

    def pack(self) -> bytes:
        return struct.pack(
            self.FORMAT,
            self.key,
            self.nombre[:30].ljust(30).encode('utf-8'),
            self.cantidad,
            self.precio,
            self.fecha[:10].ljust(10).encode('utf-8'),
            self.deleted
        )

    @staticmethod
    def unpack(data: bytes) -> "Record":
        key, nombre, cantidad, precio, fecha, deleted = struct.unpack(Record.FORMAT, data)
        return Record(key, nombre.decode('utf-8').strip(), cantidad, precio, fecha.decode('utf-8').strip(), deleted)

    def __repr__(self) -> str:
        flag = " (X)" if self.deleted else ""
        return f"R(key={self.key}, nom={self.nombre!r}, cant={self.cantidad}, precio={self.precio}, fecha={self.fecha}){flag}"

