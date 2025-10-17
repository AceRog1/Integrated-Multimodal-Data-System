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
    p = idx&((1 << local_depth) - 1)            
    step = 1<<local_depth                      
    repeat = 1<<(global_depth - local_depth) 
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
        
        b1 = Bucket(local_depth=1)
        b2 = Bucket(local_depth=1)

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
        self.directory.expand()
        self._write_directory()

        self._split_bucket_at_index(triggering_idx)

        unique_positions = sorted(set(self.directory.ptrs))
        overflow_records: List[Record] = []

        for pos in unique_positions:
            chain_recs = self._collect_chain_records(pos)
            base = self._read_bucket(pos)
            base_set = {(r.id, r.name) for r in base.iter_active()}
            to_reinsert = [r for r in chain_recs if (r.id, r.name) not in base_set]
            if to_reinsert:
                self._truncate_chain_to_base(pos)
                overflow_records.extend(to_reinsert)

        for r in overflow_records:
            self.insert(r)

    def insert(self, record: Record):
        while True:
            idx = self._hash_idx(record.id)
            bucket_pos = self.directory.ptrs[idx]
            bucket = self._read_bucket(bucket_pos)

            if not bucket.is_full():
                bucket.add_record(record)
                self._write_bucket(bucket_pos, bucket)
                return

            if bucket.local_depth < self.directory.global_depth:
                self._split_bucket_at_index(idx)
                continue

            if self._append_overflow(bucket_pos, record):
                return

            if self.directory.global_depth < MAX_GLOBAL_DEPTH:
                self._expand_directory_and_rehash(idx)
                continue

            raise RuntimeError(
                f"No hay espacio g={self.directory.global_depth}==MAX y overflow agotado en el idx={idx}"
            )
        
    def find(self, key: int):
        idx = self._hash_idx(key)
        start_pos = self.directory.ptrs[idx]
        chain_offset = 0
        for pos, b in self._chain_positions(start_pos):
            for r in b.iter_active():
                if r.id == key:
                    return pos, chain_offset, r
            chain_offset += 1
        return None
    
    # Funcion para verificar si es que tiene o no overflow
    def _bucket_has_overflow(self, start_pos:int)->bool:
        b = self._read_bucket(start_pos)
        return b.next_bucket != -1

    
    def _repack_chain_records(self, base_pos: int, records: List[Record]): #
        # llenar base
        base = self._read_bucket(base_pos)
        base.records = []
        base.size = 0
        i = 0
        while i < len(records) and not base.is_full():
            base.add_record(records[i])
            i += 1
        self._write_bucket(base_pos, base)

        self._truncate_chain_to_base(base_pos)

        tail_pos = base_pos
        overflows = 0
        while i < len(records) and overflows < MAX_COLLISIONS:
            new_pos = self._create_new_bucket(local_depth=base.local_depth)
            nb = Bucket(local_depth=base.local_depth)
            while i < len(records) and not nb.is_full():
                nb.add_record(records[i])
                i += 1
            self._write_bucket(new_pos, nb)

            # enlazar
            tail_b = self._read_bucket(tail_pos)
            tail_b.next_bucket = new_pos
            self._write_bucket(tail_pos, tail_b)
            tail_pos = new_pos
            overflows += 1

        if i < len(records):
            raise RuntimeError("Repack excede MAX_COLLISIONS ")

    # compactar los registros activos de la cadena
    def _compact_chain(self, base_pos: int):
        all_active = self._collect_chain_records(base_pos)
        self._repack_chain_records(base_pos, all_active)

    # Nos hayuda a saber el LSB para saber con cual mergear 
    def _buddy_index(self, dir_idx: int, local_depth: int) -> int:
        if local_depth <= 0:
            return dir_idx
        return dir_idx ^ (1 << (local_depth - 1))

    # Intatnar fucionar el bucket apuntado por dir_idx con su hermano (para reducir la cantidad de buckets en la estructura)
    def _try_merge_once(self, dir_idx:int)->bool:
        pos_a = self.directory.ptrs[dir_idx]
        a = self._read_bucket(pos_a)
        ld = a.local_depth
        if ld == 0:
            return False

        buddy_idx = self._buddy_index(dir_idx, ld)
        pos_b = self.directory.ptrs[buddy_idx]
        if pos_b == pos_a:
            return False

        b = self._read_bucket(pos_b)
        if a.local_depth != b.local_depth:
            return False
        if self._bucket_has_overflow(pos_a) or self._bucket_has_overflow(pos_b):
            return False

        a_cnt = sum(1 for _ in a.iter_active())
        b_cnt = sum(1 for _ in b.iter_active())
        if a_cnt + b_cnt > BUCKET_FACTOR:
            return False

        recs = list(a.iter_active()) + list(b.iter_active())
        a.clear(); b.clear()
        self._write_bucket(pos_a, a)
        self._write_bucket(pos_b, b)

        a.local_depth = ld - 1
        self._write_bucket(pos_a, a) 
        self._repack_chain_records(pos_a, recs)

        new_ld = ld - 1
        indices = get_indices_for_bucket(dir_idx, new_ld, self.directory.global_depth)
        for i in indices:
            self.directory.ptrs[i] = pos_a
        self._write_directory()
        return True

    # El encogimiento del directory para evitar ocupar espacio en RAM inecesario
    def _maybe_shrink_directory(self):
        g = self.directory.global_depth
        if g == 0:
            return
        half = 1 << (g - 1)

        # Verificar mitades identicas
        for i in range(half): 
            if self.directory.ptrs[i] != self.directory.ptrs[i + half]:
                return

        # Verificar si es que los buckets tienen local_depth < g-1
        seen = set(self.directory.ptrs)
        for pos in seen:
            b = self._read_bucket(pos)
            if b.local_depth > g - 1:
                return
            
        # Reduccion
        self.directory.ptrs = self.directory.ptrs[:half]
        self.directory.global_depth = g - 1
        self._write_directory()

    def delete(self, key: int) -> bool:
        idx = self._hash_idx(key)
        start_pos = self.directory.ptrs[idx]

        #marcar
        found = False
        for pos, b in self._chain_positions(start_pos):
            modified = False
            for r in b.records:
                if r.is_deleted == 0 and r.id == key:
                    r.is_deleted = 1
                    modified = True
                    found = True
                    break
            if modified:
                self._write_bucket(pos, b)
                break

        if not found:
            return False

        # compactador
        self._compact_chain(start_pos)

        #merges encadenados
        merged = True
        while merged:
            merged = self._try_merge_once(idx)

        #reduccion
        self._maybe_shrink_directory()
        return True
        
    def print_buckets(self, show_deleted:bool = False)->str:
        unique_positions = sorted(set(self.directory.ptrs))
        lines = []
        lines.append("Buckets (chains):")
        for pos in unique_positions:
            chain_str_parts = []
            for cpos, b in self._chain_positions(pos):
                items = []
                for r in b.records:
                    if show_deleted:
                        items.append(f"({r.id},{r.name},{r.is_deleted})")
                    else:
                        if r.is_deleted == 0:
                            items.append(f"({r.id})")
                items_str = ", ".join(items)
                chain_str_parts.append(f"pos {cpos} (l={b.local_depth}) [ {items_str} ]")
            lines.append("  " + "  ->  ".join(chain_str_parts))
        return "\n".join(lines)
        

if __name__ == "__main__":
    for fp in ("directory.bin", "datafile.bin"):
        if os.path.exists(fp):
            os.remove(fp)

    eh = ExtendibleHashing()

    seq = [2, 3, 5, 7, 11, 17, 8, 19, 23, 28, 29, 31, 32, 36, 41, 43]
    for k in seq:
        eh.insert(Record(k, 0, f"name{k}"))

    print(eh.print_buckets())
    print(eh.find(3))
    print(eh.find(23))
    print(eh.find(43))
    print(eh.find(0))
    print(eh.find(28))

    eh.delete(3)
    print(eh.find(3))
    print(eh.print_buckets())

