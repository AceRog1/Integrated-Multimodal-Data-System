import os
import struct


BUCKET_FACTOR = 3
MAX_COLLISIONS = 1
MAX_GLOBAL_DEPTH = 3


class Record:
    FORMAT = 'ii20s'
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, id: int, is_deleted: int, name: str):
        self.id = id
        self.is_deleted = is_deleted
        self.name = name

    def pack(self) -> bytes:
        return struct.pack(
            self.FORMAT,
            int(self.id),
            int(self.is_deleted),
            self.name.encode('utf-8')[:20].ljust(20, b"\x00")
        )
    
    @staticmethod
    def unpack(data):
        id, is_deleted, name = struct.unpack(Record.FORMAT, data)
        name_str = name.decode('utf-8').rstrip('\x00')
        return Record(id, is_deleted, name_str)


class Bucket:
    FORMAT_HEADER = 'iii'
    HEADER_SIZE = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_BUCKET = HEADER_SIZE + BUCKET_FACTOR * Record.SIZE

    def __init__(self, records: list = None, size: int = 0, next_bucket: int = -1, local_depth: int = 2):
        self.records = records if records is not None else []
        self.size = size
        self.next_bucket = next_bucket
        self.local_depth = local_depth
    
    def pack(self) -> bytes:
        header_data = struct.pack(self.FORMAT_HEADER, self.size, self.next_bucket, self.local_depth)
        records_data = b''
        
        count = 0
        for record in self.records:
            if count >= BUCKET_FACTOR:
                break
            records_data += record.pack()
            count += 1
        
        while count < BUCKET_FACTOR:
            records_data += b'\x00' * Record.SIZE
            count += 1
        
        return header_data + records_data

    @staticmethod
    def unpack(data: bytes):
        size, next_bucket, local_depth = struct.unpack(Bucket.FORMAT_HEADER, data[:Bucket.HEADER_SIZE])
        records = []
        offset = Bucket.HEADER_SIZE
        
        for i in range(size):
            record_data = data[offset:offset + Record.SIZE]
            if record_data != b'\x00' * Record.SIZE:
                records.append(Record.unpack(record_data))
            offset += Record.SIZE
        
        return Bucket(records, size, next_bucket, local_depth)


class Directory:
    FORMAT_HEADER = 'i'
    HEADER_SIZE = struct.calcsize(FORMAT_HEADER)

    def __init__(self, global_depth: int = 2):
        self.global_depth = global_depth
        self.ptrs: list[int] = [-1] * (2 ** self.global_depth)
        self.bucket_chain: list[int] = [0] * (len(self.ptrs))

    def pack(self) -> bytes:
        header_data = struct.pack(self.FORMAT_HEADER, self.global_depth)
        num_data = len(self.ptrs)
        format_data = f'{num_data}i'
        ptr_data = struct.pack(format_data, *self.ptrs)
        chain_data = struct.pack(format_data, *self.bucket_chain)
        return header_data + ptr_data + chain_data

    @staticmethod
    def unpack(data: bytes):
        global_depth, = struct.unpack(Directory.FORMAT_HEADER, data[:Directory.HEADER_SIZE])
        num_data = 2 ** global_depth
        format_data = f'{num_data}i'
        data_size = struct.calcsize(format_data)

        offset_start = Directory.HEADER_SIZE
        offset_end_1 = offset_start + data_size
        offset_end_2 = offset_end_1 + data_size
        
        ptrs = struct.unpack(format_data, data[offset_start:offset_end_1])
        chains = struct.unpack(format_data, data[offset_end_1:offset_end_2])

        directory = Directory(global_depth)
        directory.ptrs = list(ptrs)
        directory.bucket_chain = list(chains)
        
        return directory


class ExtendibleHashing:
    def __init__(self, dir_file: str = "directory.bin", data_file: str = "datafile.bin"):
        self.directory: Directory
        self.dir_file = dir_file
        self.data_file = data_file
        self.next_bucket_pos = 0
        if not os.path.exists(self.dir_file) or not os.path.exists(self.data_file):
            self._initialize_files()
        else:
            self._load_state()
    
    def _hash_function(self, key:int)->tuple[str, int]:
        hash_val = hash(key) % (2 ** self.directory.global_depth)
        binary = bin(hash_val)[2:].zfill(self.directory.global_depth)
        return binary, hash_val
    
    def _initialize_files(self):
        self.directory = Directory()
        
        b1 = Bucket(local_depth=2)
        b2 = Bucket(local_depth=2)

        with open(self.data_file, 'wb') as f:
            f.write(b1.pack())
            f.write(b2.pack())
        
        self.directory.ptrs[0] = 0
        self.directory.ptrs[1] = 1
        self.directory.ptrs[2] = 0
        self.directory.ptrs[3] = 1
        
        self.next_bucket_pos = 2
        self._write_directory()
    
    def _load_state(self):
        with open(self.dir_file, 'rb') as f:
            data = f.read()
        self.directory = Directory.unpack(data)
        with open(self.data_file, 'rb') as f:
            f.seek(0, 2)
            file_size = f.tell()
            self.next_bucket_pos = file_size // Bucket.SIZE_OF_BUCKET
    
    def _write_directory(self):
        with open(self.dir_file, 'wb') as f:
            f.write(self.directory.pack())
    
    def _read_bucket(self, bucket_pos: int) -> Bucket:
        with open(self.data_file, 'rb') as f:
            f.seek(bucket_pos * Bucket.SIZE_OF_BUCKET)
            data = f.read(Bucket.SIZE_OF_BUCKET)
            return Bucket.unpack(data)
    
    def _write_bucket(self, bucket_pos: int, bucket: Bucket):
        with open(self.data_file, 'r+b') as f:
            f.seek(bucket_pos * Bucket.SIZE_OF_BUCKET)
            f.write(bucket.pack())