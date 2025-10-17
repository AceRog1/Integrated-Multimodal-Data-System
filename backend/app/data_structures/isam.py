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

class IndexPage:
    HEADER = '<i'                            
    KEYS = f'<{INDEX_FACTOR}i'             
    PTRS = f'<{INDEX_FACTOR + 1}i'          
    SIZE = struct.calcsize(HEADER) + struct.calcsize(KEYS) + struct.calcsize(PTRS)

    def __init__(self):
        self.n = 0
        self.keys = [0] * INDEX_FACTOR     
        self.ptrs = [-1] * (INDEX_FACTOR + 1) 

    def add_entry_block(self, block: List[Tuple[int, int]]):
        if len(block) == 0:
            self.n = 0
            return
        if len(block) > INDEX_FACTOR:
            raise OverflowError("Excede INDEX_FACTOR")

        # aqui tomamos la primera entrada
        self.ptrs[0] = block[0][1]
        for j, (k, p) in enumerate(block):
            self.keys[j] = k
            self.ptrs[j+1] = p
        self.n = len(block)

    def pack(self) -> bytes:
        return struct.pack(self.HEADER, self.n) + \
               struct.pack(self.KEYS, *self.keys) + \
               struct.pack(self.PTRS, *self.ptrs)
    
    @staticmethod
    def unpack(data: bytes) -> "IndexPage":
        (n,) = struct.unpack(IndexPage.HEADER, data[:struct.calcsize(IndexPage.HEADER)])
        off = struct.calcsize(IndexPage.HEADER)
        keys = list(struct.unpack(IndexPage.KEYS, data[off:off + struct.calcsize(IndexPage.KEYS)]))
        off += struct.calcsize(IndexPage.KEYS)
        ptrs = list(struct.unpack(IndexPage.PTRS, data[off:off + struct.calcsize(IndexPage.PTRS)]))
        ip = IndexPage()
        ip.n, ip.keys, ip.ptrs = n, keys, ptrs
        return ip