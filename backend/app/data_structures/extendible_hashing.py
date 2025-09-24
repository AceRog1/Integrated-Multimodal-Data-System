import os
import struct

BUCKET_FACTOR = 3 # El espacio real sera de 20
MAX_COLLISIONS = 1 # max K, La cantidad maxima de buckets de overflow (El real sera de 5)
MAX_GLOBAL_DEPTH = 3 # max D, Una vez que sobrepasa el overflow, se tiene que empezar a hacer chain de buckets (El real sera de 30 -> 1,073,741,824 de buckets sin contar overflows) 

#####
# Pimero se expande los Buckets
# Segundo se usa la mecanica de Overflow
# Tercero se usa Re-Hashing (cuando se acaban los espacios de Overflow)
#####


# Test record -> Esto debe ser dinamico, segun que tipo de data ponga el usuario y a partir de que lo queira crear
class Record:

    FORMAT = 'i20si'
    SIZE = struct.calcsize(FORMAT)
    def __init__(self, id:int, is_deleted:int, name:str):
        self.id = id
        self.is_deleted = is_deleted
        self.name = name

    def pack(self):
        return struct.pack(
            self.FROMAT,
            int(self.id),
            int(self.is_deleted),
            self.name.encode('utf-8')[:20].ljust(20, b"\x00")
        )
    
    @staticmethod
    def unpack(data):
        id, is_deleted, name = struct.unpack(Record.FORMAT, data)
        return Record(id, is_deleted, name)


# Estructura del Bucket
class Bucket:

    FORMAT_HEADER = 'iiii' # size | next_bucket | overflow_chain | size
    HEADER_SIZE = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_BUCKET = HEADER_SIZE + BUCKET_FACTOR*Record.SIZE

    def __init__(self, records:Record=[], size:int=0, next_bucket:int=-1, overflow_chain:int=0):
        self.records = records
        self.size = size
        self.next_bucket = next_bucket
        self.overflow_chain = overflow_chain
    
    def pack(self)->bytes:
        header_data = struct.pack(self.FORMAT_HEADER, self.size, self.next_bucket, self.overflow_chain)
        records_data = b''
        for record in self.records:
            if record.is_deleted != True:
                records_data += record.pack()
        i = self.size
        while i < BUCKET_FACTOR:
            records_data += b'\x00' * Record.SIZE_OF_RECORD
            i += 1
        return header_data + records_data

    @staticmethod
    def unpack(data:bytes):
        size, next_bucket, overflow_chain = struct.unpack(Bucket.FORMAT_HEADER, data[:Bucket.HEADER_SIZE])
        records = []
        offset = Bucket.HEADER_SIZE
        for i in range(size):
            record_data = data[offset:offset+Record.SIZE]
            records.append(Record.unpack(record_data))
            offset += Record.SIZE_OF_RECORD
        return Bucket(records, size, next_bucket, overflow_chain)


# Estructura del Directory
class Directory:
    def __init__(self):
        pass


# Orquestador pincipal
class ExtendibleHashing:
    def __init__(self):
        pass
