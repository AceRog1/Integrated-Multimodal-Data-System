import os
import struct
from typing import List


BUCKET_FACTOR = 3
MAX_COLLISIONS = 1
MAX_GLOBAL_DEPTH = 3


def hashing_funct(key: int, D: int):
    binary = bin(hash(key)%(2 ** D))[2:]
    integer = int(binary, 2)
    return binary, integer


def get_indices_for_bucket(idx: int, local_depth: int, global_depth: int):
    p =idx & ((1 << local_depth) - 1)            
    step = 1<< local_depth                      
    repeat = 1<< (global_depth - local_depth) 
    return [p + k * step for k in range(repeat)]


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
    def unpack(data: bytes) -> "Record":
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

    def active_count(self)->int:
        return sum(1 for r in self.records if r.is_deleted == 0)

    def is_full(self)->bool:
        return self.active_count()>= BUCKET_FACTOR

    def add_record(self, record: Record)->bool:
        if not self.is_full():
            self.records.append(record)
            self.size = len(self.records)
            return True
        return False

    def iter_active(self):
        for r in self.records:
            if r.is_deleted == 0:
                yield r

    def clear(self):
        self.records = []
        self.size = 0
        self.next_bucket = -1
    
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
        records:List[Record] = []
        offset = Bucket.HEADER_SIZE
        for _ in range(size):
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
        self.ptrs: List[int] = [-1] * (2 ** self.global_depth)

    def pack(self)->bytes:
        header_data = struct.pack(self.FORMAT_HEADER, self.global_depth)
        fmt = f'{len(self.ptrs)}i'
        ptr_data = struct.pack(fmt, *self.ptrs)
        return header_data + ptr_data

    @staticmethod
    def unpack(data:bytes)->"Directory":
        (global_depth,) = struct.unpack(
            Directory.FORMAT_HEADER, data[:Directory.HEADER_SIZE]
        )
        num = 2 ** global_depth
        fmt = f'{num}i'
        sz = struct.calcsize(fmt)
        start = Directory.HEADER_SIZE
        end = start + sz
        ptrs = list(struct.unpack(fmt, data[start:end]))
        d = Directory(global_depth)
        d.ptrs = ptrs
        return d

    def expand(self):
        self.ptrs.extend(self.ptrs)
        self.global_depth += 1


# Orquestador
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
    
    def _read_bucket(self, bucket_pos:int)->Bucket:
        with open(self.data_file, 'rb') as f:
            f.seek(bucket_pos * Bucket.SIZE_OF_BUCKET)
            data = f.read(Bucket.SIZE_OF_BUCKET)
            return Bucket.unpack(data)
    
    def _write_bucket(self, bucket_pos:int, bucket:Bucket):
        with open(self.data_file, 'r+b') as f:
            f.seek(bucket_pos * Bucket.SIZE_OF_BUCKET)
            f.write(bucket.pack())

    def _create_new_bucket(self, local_depth:int)->int:
        new_pos = self.next_bucket_pos
        self.next_bucket_pos += 1
        with open(self.data_file, 'ab') as f:
            bucket = Bucket(local_depth=local_depth)
            f.write(bucket.pack())
        return new_pos
    
    def _hash_idx(self, key:int)->int:
        _, idx = hashing_funct(key, self.directory.global_depth)
        return idx

    def _chain_positions(self, start_pos:int):
        pos = start_pos
        while pos != -1:
            b = self._read_bucket(pos)
            yield pos, b
            pos = b.next_bucket

    def _collect_chain_records(self, start_pos: int) -> List[Record]:
        recs: List[Record] = []
        for _, b in self._chain_positions(start_pos):
            recs.extend(list(b.iter_active()))
        return recs

    def _truncate_chain_to_base(self, start_pos: int):
        base = self._read_bucket(start_pos)
        pos = base.next_bucket
        while pos != -1:
            b = self._read_bucket(pos)
            b.clear()
            self._write_bucket(pos, b)
            pos = b.next_bucket
        base.next_bucket = -1
        self._write_bucket(start_pos, base)

    def _append_overflow(self, start_pos:int, record:Record)->bool:
        chain_len = 0
        last_pos = start_pos
        last_bucket = self._read_bucket(last_pos)
        while last_bucket.next_bucket != -1:
            chain_len += 1
            last_pos = last_bucket.next_bucket
            last_bucket = self._read_bucket(last_pos)

        if last_bucket.is_full():
            if chain_len >= MAX_COLLISIONS:
                return False
            new_pos = self._create_new_bucket(local_depth=last_bucket.local_depth)
            nb = Bucket(local_depth=last_bucket.local_depth)
            nb.add_record(record)
            self._write_bucket(new_pos, nb)

            last_bucket.next_bucket = new_pos
            self._write_bucket(last_pos, last_bucket)
            return True
        else:
            ok = last_bucket.add_record(record)
            self._write_bucket(last_pos, last_bucket)
            return ok

    def _split_bucket_at_index(self, dir_idx:int):
        bucket_pos = self.directory.ptrs[dir_idx]
        base_bucket = self._read_bucket(bucket_pos)
        old_ld = base_bucket.local_depth
        new_ld = old_ld + 1

        all_recs = self._collect_chain_records(bucket_pos)
        self._truncate_chain_to_base(bucket_pos)

        new_bucket_pos = self._create_new_bucket(local_depth=new_ld)

        base_bucket = self._read_bucket(bucket_pos)
        base_bucket.local_depth = new_ld
        self._write_bucket(bucket_pos, base_bucket)

        new_bucket = self._read_bucket(new_bucket_pos)
        new_bucket.local_depth = new_ld
        self._write_bucket(new_bucket_pos, new_bucket)

        indices = get_indices_for_bucket(dir_idx, old_ld, self.directory.global_depth)
        for idx in indices:
            bit_is_one = ((idx >> (new_ld - 1)) & 1) == 1
            self.directory.ptrs[idx] = new_bucket_pos if bit_is_one else bucket_pos
        self._write_directory()

        base_mem = Bucket(local_depth=new_ld)
        bro_mem = Bucket(local_depth=new_ld)
        for r in all_recs:
            _, idx = hashing_funct(r.id, self.directory.global_depth)
            bit_is_one = ((idx >> (new_ld - 1)) & 1) == 1
            target = bro_mem if bit_is_one else base_mem
            target.add_record(r) 

        self._write_bucket(bucket_pos, base_mem)
        self._write_bucket(new_bucket_pos, bro_mem)

    def _expand_directory_and_rehash(self, triggering_idx:int):
        pass
