# Estructura para ISAM
# con una estructura genérica
import os
import struct
from typing import List, Optional, Tuple, Any, Union
from enum import Enum

class KeyType(Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"

BLOCK_FACTOR = 3      
INDEX_FACTOR = 4      

class ISAMRecord:
    def __init__(self, key: Any, record_position: int, deleted: bool = False, key_type: KeyType = KeyType.INT, max_key_length: int = 50):
        self.key = key
        self.record_position = record_position
        self.deleted = deleted
        self.key_type = key_type
        self.max_key_length = max_key_length
        
        # Verificar formato segun tipo de key
        if key_type == KeyType.INT:
            self.KEY_FMT = "i"
        elif key_type == KeyType.FLOAT:
            self.KEY_FMT = "f"
        elif key_type == KeyType.STRING:
            self.KEY_FMT = f"{max_key_length}s"
        else:
            raise ValueError(f"Tipo de key no soportado: {key_type}")
        
        # Formato: key + record_position + deleted
        self.FORMAT = self.KEY_FMT + "i?"
        self.SIZE = struct.calcsize(self.FORMAT)

    def pack(self) -> bytes:
        if self.key_type == KeyType.STRING:
            key_bytes = str(self.key).encode('utf-8')[:self.max_key_length].ljust(self.max_key_length, b'\x00')
            return struct.pack(self.FORMAT, key_bytes, self.record_position, self.deleted)
        else:
            return struct.pack(self.FORMAT, self.key, self.record_position, self.deleted)

    @staticmethod
    def unpack(data: bytes, key_type: KeyType = KeyType.INT, max_key_length: int = 50) -> "ISAMRecord":
        if key_type == KeyType.STRING:
            key_bytes, record_position, deleted = struct.unpack(f"{max_key_length}si?", data)
            key = key_bytes.decode('utf-8').rstrip('\x00')
        else:
            key, record_position, deleted = struct.unpack("ii?" if key_type == KeyType.INT else "fi?", data)
        
        return ISAMRecord(key, record_position, deleted, key_type, max_key_length)

    def __repr__(self) -> str:
        flag = " (X)" if self.deleted else ""
        return f"ISAMRecord(key={self.key}, pos={self.record_position}){flag}"

class IndexPage:
    def __init__(self, key_type: KeyType = KeyType.INT, max_key_length: int = 50):
        self.key_type = key_type
        self.max_key_length = max_key_length
        self.n = 0
        
        # Determinar formato de keys segun tipo
        if key_type == KeyType.INT:
            self.KEY_FMT = "i"
        elif key_type == KeyType.FLOAT:
            self.KEY_FMT = "f"
        elif key_type == KeyType.STRING:
            self.KEY_FMT = f"{max_key_length}s"
        else:
            raise ValueError(f"Tipo de key no soportado: {key_type}")
        
        self.HEADER = '<i'
        self.KEYS = f'<{INDEX_FACTOR}{self.KEY_FMT}'
        self.PTRS = f'<{INDEX_FACTOR + 1}i'
        self.SIZE = struct.calcsize(self.HEADER) + struct.calcsize(self.KEYS) + struct.calcsize(self.PTRS)
        
        # Inicializar arrays
        if key_type == KeyType.STRING:
            self.keys = [b'\x00' * max_key_length] * INDEX_FACTOR
        else:
            self.keys = [0] * INDEX_FACTOR
        self.ptrs = [-1] * (INDEX_FACTOR + 1) 

    def add_entry_block(self, block: List[Tuple[Any, int]]):
        if len(block) == 0:
            self.n = 0
            return
        if len(block) > INDEX_FACTOR:
            raise OverflowError("Excede INDEX_FACTOR")

        # aqui tomamos la primera entrada
        self.ptrs[0] = block[0][1]
        for j, (k, p) in enumerate(block):
            if self.key_type == KeyType.STRING:
                self.keys[j] = str(k).encode('utf-8')[:self.max_key_length].ljust(self.max_key_length, b'\x00')
            else:
                self.keys[j] = k
            self.ptrs[j+1] = p
        self.n = len(block)

    def pack(self) -> bytes:
        return struct.pack(self.HEADER, self.n) + \
               struct.pack(self.KEYS, *self.keys) + \
               struct.pack(self.PTRS, *self.ptrs)
    
    @staticmethod
    def unpack(data: bytes, key_type: KeyType = KeyType.INT, max_key_length: int = 50) -> "IndexPage":
        HEADER = '<i'
        header_size = struct.calcsize(HEADER)
        (n,) = struct.unpack(HEADER, data[:header_size])
        
        ip = IndexPage(key_type, max_key_length)
        ip.n = n
        
        off = header_size
        keys_data = data[off:off + struct.calcsize(ip.KEYS)]
        keys = list(struct.unpack(ip.KEYS, keys_data))
        off += struct.calcsize(ip.KEYS)
        
        ptrs_data = data[off:off + struct.calcsize(ip.PTRS)]
        ptrs = list(struct.unpack(ip.PTRS, ptrs_data))
        
        ip.keys = keys
        ip.ptrs = ptrs
        return ip
    
    def choose_ptr(self, key: Any) -> int:
        if self.n == 0:
            return -1
        i = -1
        for j in range(self.n):
            current_key = self.keys[j]
            if self.key_type == KeyType.STRING:
                current_key = current_key.decode('utf-8').rstrip('\x00')
            
            if current_key <= key:
                i = j
            else:
                break
        return self.ptrs[0] if i == -1 else self.ptrs[i+1]

    def __repr__(self) -> str:
        pairs = []
        pairs.append(f"P0={self.ptrs[0]}")
        for i in range(self.n):
            pairs.append(f"K{i+1}={self.keys[i]}")
            pairs.append(f"P{i+1}={self.ptrs[i+1]}")
        return f"[IndexPage n={self.n} {' '.join(pairs)}]"
 
class DataPage:
    def __init__(self, records: Optional[List[ISAMRecord]] = None, next_page: int = -1, key_type: KeyType = KeyType.INT, max_key_length: int = 50):
        self.records: List[ISAMRecord] = records if records else []
        self.next_page = next_page
        self.key_type = key_type
        self.max_key_length = max_key_length
        
        # Calcular tamaño dinamicamente
        self.HEADER = '<ii'
        record_size = ISAMRecord(0, 0, key_type=key_type, max_key_length=max_key_length).SIZE
        self.SIZE = struct.calcsize(self.HEADER) + BLOCK_FACTOR * record_size

    def insert_sorted(self, record: ISAMRecord) -> bool:
        if len(self.records) < BLOCK_FACTOR:
            self.records.append(record)
            self.records.sort(key=lambda r: r.key)
            return True
        return False

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER, len(self.records), self.next_page)
        body = b''.join(r.pack() for r in self.records)
        record_size = ISAMRecord(0, 0, key_type=self.key_type, max_key_length=self.max_key_length).SIZE
        padding = b'\x00'* ((BLOCK_FACTOR - len(self.records)) * record_size)
        return header + body + padding

    @staticmethod
    def unpack(data: bytes, key_type: KeyType = KeyType.INT, max_key_length: int = 50) -> "DataPage":
        if not data:
            return DataPage([], -1, key_type, max_key_length)
        HEADER = '<ii'
        header_size = struct.calcsize(HEADER)
        n, next_page = struct.unpack(HEADER, data[:header_size])
        records: List[ISAMRecord] = []
        off = header_size
        record_size = ISAMRecord(0, 0, key_type=key_type, max_key_length=max_key_length).SIZE
        for _ in range(n):
            rec_bytes = data[off:off + record_size]
            records.append(ISAMRecord.unpack(rec_bytes, key_type, max_key_length))
            off += record_size
        return DataPage(records, next_page, key_type, max_key_length)

    def __repr__(self) -> str:
        return f"<DataPage n={len(self.records)} next={self.next_page} recs={self.records}>"
    
class ISAM2Index:
    def __init__(self, base="empresa", key_type: KeyType = KeyType.INT, max_key_length: int = 50):
        self.index_root = base + "_index1.dat"  
        self.index_mid  = base + "_index2.dat" 
        self.datafile   = base + "_data.dat"
        self.key_type = key_type
        self.max_key_length = max_key_length

        for f in [self.index_root, self.index_mid, self.datafile]:
            if not os.path.exists(f):
                open(f, 'wb').close()

    def _read_index_page(self, path: str, offset: int = 0) -> IndexPage:
        with open(path, 'rb') as f:
            f.seek(offset)
            page_size = IndexPage(self.key_type, self.max_key_length).SIZE
            return IndexPage.unpack(f.read(page_size), self.key_type, self.max_key_length)


    def build_index(self, records: List[Tuple[Any, int]]):
        records = sorted(records, key=lambda r: r[0])

        mid_entries: List[Tuple[Any, int]] = [] 
        with open(self.datafile, 'wb') as df:
            for i in range(0, len(records), BLOCK_FACTOR):
                block = records[i:i + BLOCK_FACTOR]
                # Convertir a ISAMRecord
                isam_records = [ISAMRecord(key, pos, key_type=self.key_type, max_key_length=self.max_key_length) for key, pos in block]
                page = DataPage(isam_records, next_page=-1, key_type=self.key_type, max_key_length=self.max_key_length)
                ptr = df.tell()
                df.write(page.pack())
                mid_entries.append((block[0][0], ptr))  

        root_entries: List[Tuple[Any, int]] = [] 
        with open(self.index_mid, 'wb') as fmid:
            for i in range(0, len(mid_entries), INDEX_FACTOR):
                block = mid_entries[i:i + INDEX_FACTOR]  
                idx_page = IndexPage(key_type=self.key_type, max_key_length=self.max_key_length)
                idx_page.add_entry_block(block)
                ptr_mid_page = fmid.tell()
                fmid.write(idx_page.pack())
                root_entries.append((block[0][0], ptr_mid_page))

        with open(self.index_root, 'wb') as froot:
            root_page = IndexPage(key_type=self.key_type, max_key_length=self.max_key_length)
            root_page.add_entry_block(root_entries)
            froot.write(root_page.pack())

        print("indice de dos niveles listo")

    #busqueda 
    def _locate_data_page_offset(self, key: Any) -> int:
        with open(self.index_root, 'rb') as froot:
            root = self._read_index_page(self.index_root)
            mid_ptr = root.choose_ptr(key)

        with open(self.index_mid, 'rb') as fmid:
            fmid.seek(mid_ptr)
            mid = self._read_index_page(self.index_mid, mid_ptr)
            data_ptr = mid.choose_ptr(key)

        return data_ptr

    def search(self, key: Any) -> Optional[int]:
        data_ptr = self._locate_data_page_offset(key)

        with open(self.datafile, 'rb') as df:
            df.seek(data_ptr)
            page_size = DataPage(key_type=self.key_type, max_key_length=self.max_key_length).SIZE
            page = DataPage.unpack(df.read(page_size), self.key_type, self.max_key_length)
            for r in page.records:
                if r.key == key and not r.deleted:
                    return r.record_position

            nxt = page.next_page
            while nxt != -1:
                df.seek(nxt)
                page = DataPage.unpack(df.read(page_size), self.key_type, self.max_key_length)
                for r in page.records:
                    if r.key == key and not r.deleted:
                        return r.record_position
                nxt = page.next_page
        return None
    
    
    # insercion, usa overflow
    def insert(self, key: Any, record_position: int):
        if self.search(key):
            print(f"Registro {key} ya existe.")
            return

        data_ptr = self._locate_data_page_offset(key)
        record = ISAMRecord(key, record_position, key_type=self.key_type, max_key_length=self.max_key_length)

        with open(self.datafile, 'r+b') as df:
            df.seek(data_ptr)
            page_size = DataPage(key_type=self.key_type, max_key_length=self.max_key_length).SIZE
            base = DataPage.unpack(df.read(page_size), self.key_type, self.max_key_length)

            if base.insert_sorted(record):
                df.seek(data_ptr); df.write(base.pack())
                print(f" Insertado {key} en pagina base, @ {data_ptr}")
                return
            
            #recorremos overflow 
            prev_off = data_ptr
            prev_page = base
            while prev_page.next_page != -1:
                prev_off = prev_page.next_page
                df.seek(prev_off)
                curr = DataPage.unpack(df.read(page_size), self.key_type, self.max_key_length)
                if curr.insert_sorted(record):
                    df.seek(prev_off); df.write(curr.pack())
                    print(f"Insertado {key} en overflow existente en @ {prev_off}")
                    return
                prev_page = curr

            df.seek(0, os.SEEK_END)
            new_off = df.tell()
            new_page = DataPage([record], next_page=-1, key_type=self.key_type, max_key_length=self.max_key_length)
            df.write(new_page.pack())

            prev_page.next_page = new_off
            df.seek(prev_off); df.write(prev_page.pack())
            where = "base" if prev_off == data_ptr else "overflow"
            print(f"Insertado {key} en nuevo overflow @ {new_off}")


    def remove(self, key: Any):
        data_ptr = self._locate_data_page_offset(key)
        with open(self.datafile, 'r+b') as df:
            # base
            df.seek(data_ptr)
            page_off = data_ptr
            page_size = DataPage(key_type=self.key_type, max_key_length=self.max_key_length).SIZE
            page = DataPage.unpack(df.read(page_size), self.key_type, self.max_key_length)

            def try_mark(off: int, p: DataPage) -> bool:
                for r in p.records:
                    if r.key == key and not r.deleted:
                        r.deleted = True
                        df.seek(off); df.write(p.pack())
                        return True
                return False

            if try_mark(page_off, page):
                print(f"Eliminado {key} en base @ {page_off}")
                return

            #overflow
            nxt = page.next_page
            while nxt != -1:
                df.seek(nxt)
                op = DataPage.unpack(df.read(page_size), self.key_type, self.max_key_length)
                if try_mark(nxt, op):
                    print(f"Eliminado {key} en overflow @ {nxt}")
                    return
                nxt = op.next_page

            print(f"{key} no encontrado para eliminar")

    def range_search(self, low: Any, high: Any) -> List[int]:
        if low > high:
            low, high = high, low

        results: List[int] = []
        with open(self.datafile, 'rb') as df:
            with open(self.index_mid, 'rb') as fmid:
                root = self._read_index_page(self.index_root)
                #p.i en root
                i_root = -1
                for j in range(root.n):
                    if root.keys[j] <= low:
                        i_root = j
                    else:
                        break
                start_root_pos = 1 if i_root == -1 else i_root + 1

                for root_pos in range(start_root_pos, root.n + 1):
                    mid_off = root.ptrs[root_pos]
                    if mid_off == -1:
                        continue

                    fmid.seek(mid_off)
                    mid = IndexPage.unpack(fmid.read(IndexPage.SIZE))

                    # p.i en mid
                    if root_pos == start_root_pos:
                        i_mid = -1
                        for j in range(mid.n):
                            if mid.keys[j] <= low:
                                i_mid = j
                            else:
                                break
                        start_mid_pos = 1 if i_mid == -1 else i_mid + 1
                    else:
                        start_mid_pos = 1

                    for mid_pos in range(start_mid_pos, mid.n + 1):
                        base_off = mid.ptrs[mid_pos]
                        if base_off == -1:
                            continue

                        cur_off = base_off
                        chain_records = []
                        while cur_off != -1:
                            df.seek(cur_off)
                            page_size = DataPage(key_type=self.key_type, max_key_length=self.max_key_length).SIZE
                            page = DataPage.unpack(df.read(page_size), self.key_type, self.max_key_length)
                            for r in page.records:
                                if not r.deleted and low <= r.key <= high:
                                    chain_records.append(r.record_position)
                            cur_off = page.next_page

                        chain_records.sort()
                        results.extend(chain_records)

                        if mid_pos < mid.n:
                            if mid.keys[mid_pos] > high:
                                break

                    if root_pos < root.n:
                        if root.keys[root_pos] > high:
                            break

        return results
