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
        ip.n, ip.keys, ip.ptrs= n, keys, ptrs
        return ip
    
    def choose_ptr(self, key: int) -> int:
        if self.n == 0:
            return -1
        i = -1
        for j in range(self.n):
            if self.keys[j] <= key:
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
    HEADER = '<ii'
    SIZE = struct.calcsize(HEADER) + BLOCK_FACTOR * Record.SIZE

    def __init__(self, records: Optional[List[Record]] = None, next_page: int = -1):
        self.records: List[Record] = records if records else []
        self.next_page = next_page

    def insert_sorted(self, record: Record) -> bool:
        if len(self.records) < BLOCK_FACTOR:
            self.records.append(record)
            self.records.sort(key=lambda r: r.key)
            return True
        return False

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER, len(self.records), self.next_page)
        body = b''.join(r.pack() for r in self.records)
        padding = b'\x00'* ((BLOCK_FACTOR - len(self.records)) * Record.SIZE)
        return header + body + padding

    @staticmethod
    def unpack(data: bytes) -> "DataPage":
        if not data:
            return DataPage([], -1)
        n, next_page = struct.unpack(DataPage.HEADER, data[:struct.calcsize(DataPage.HEADER)])
        records: List[Record] = []
        off = struct.calcsize(DataPage.HEADER)
        for _ in range(n):
            rec_bytes = data[off:off + Record.SIZE]
            records.append(Record.unpack(rec_bytes))
            off += Record.SIZE
        return DataPage(records, next_page)

    def __repr__(self) -> str:
        return f"<DataPage n={len(self.records)} next={self.next_page} recs={self.records}>"
    
class ISAM2Index:
    def __init__(self, base="empresa"):
        self.index_root = base + "_index1.dat"  
        self.index_mid  = base + "_index2.dat" 
        self.datafile   = base + "_data.dat"    

        for f in [self.index_root, self.index_mid, self.datafile]:
            if not os.path.exists(f):
                open(f, 'wb').close()

    def _read_index_page(self, path: str, offset: int = 0) -> IndexPage:
        with open(path, 'rb') as f:
            f.seek(offset)
            return IndexPage.unpack(f.read(IndexPage.SIZE))


    def build_index(self, records: List[Record]):
        records = sorted(records, key=lambda r: r.key)

        mid_entries: List[Tuple[int, int]] = [] 
        with open(self.datafile, 'wb') as df:
            for i in range(0, len(records), BLOCK_FACTOR):
                block = records[i:i + BLOCK_FACTOR]
                page = DataPage(block, next_page=-1)
                ptr = df.tell()
                df.write(page.pack())
                mid_entries.append((block[0].key, ptr))  

        root_entries: List[Tuple[int, int]] = [] 
        with open(self.index_mid, 'wb') as fmid:
            for i in range(0, len(mid_entries), INDEX_FACTOR):
                block = mid_entries[i:i + INDEX_FACTOR]  
                idx_page = IndexPage()
                idx_page.add_entry_block(block)
                ptr_mid_page = fmid.tell()
                fmid.write(idx_page.pack())
                root_entries.append((block[0][0], ptr_mid_page))

        with open(self.index_root, 'wb') as froot:
            root_page = IndexPage()
            root_page.add_entry_block(root_entries)
            froot.write(root_page.pack())

        print("indice de dos niveles listo")

    #busqueda 
    def _locate_data_page_offset(self, key: int) -> int:
        with open(self.index_root, 'rb') as froot:
            root = IndexPage.unpack(froot.read(IndexPage.SIZE))
            mid_ptr = root.choose_ptr(key)

        with open(self.index_mid, 'rb') as fmid:
            fmid.seek(mid_ptr)
            mid = IndexPage.unpack(fmid.read(IndexPage.SIZE))
            data_ptr = mid.choose_ptr(key)

        return data_ptr

    def search(self, key: int) -> Optional[Record]:
        data_ptr = self._locate_data_page_offset(key)

        with open(self.datafile, 'rb') as df:
            df.seek(data_ptr)
            page = DataPage.unpack(df.read(DataPage.SIZE))
            for r in page.records:
                if r.key == key and not r.deleted:
                    return r

            nxt = page.next_page
            while nxt != -1:
                df.seek(nxt)
                page = DataPage.unpack(df.read(DataPage.SIZE))
                for r in page.records:
                    if r.key == key and not r.deleted:
                        return r
                nxt = page.next_page
        return None
    
    
    # insercion, usa overflow
    def insert(self, record: Record):
        if self.search(record.key):
            print(f"Registro {record.key} ya existe.")
            return

        data_ptr = self._locate_data_page_offset(record.key)

        with open(self.datafile, 'r+b') as df:
            df.seek(data_ptr)
            base = DataPage.unpack(df.read(DataPage.SIZE))

            if base.insert_sorted(record):
                df.seek(data_ptr); df.write(base.pack())
                print(f" Insertado {record.key} en pagina base, @ {data_ptr}")
                return
            
            #recorremos overflow 
            prev_off = data_ptr
            prev_page = base
            while prev_page.next_page != -1:
                prev_off = prev_page.next_page
                df.seek(prev_off)
                curr = DataPage.unpack(df.read(DataPage.SIZE))
                if curr.insert_sorted(record):
                    df.seek(prev_off); df.write(curr.pack())
                    print(f"Insertado {record.key} en overflow existente en @ {prev_off}")
                    return
                prev_page = curr

            df.seek(0, os.SEEK_END)
            new_off = df.tell()
            new_page = DataPage([record], next_page=-1)
            df.write(new_page.pack())

            prev_page.next_page = new_off
            df.seek(prev_off); df.write(prev_page.pack())
            where = "base" if prev_off == data_ptr else "overflow"
            print(f"Insertado {record.key} en nuevo overflow @ {new_off}")


    def remove(self, key: int):
        data_ptr = self._locate_data_page_offset(key)
        with open(self.datafile, 'r+b') as df:
            # base
            df.seek(data_ptr)
            page_off = data_ptr
            page = DataPage.unpack(df.read(DataPage.SIZE))

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
                op = DataPage.unpack(df.read(DataPage.SIZE))
                if try_mark(nxt, op):
                    print(f"Eliminado {key} en overflow @ {nxt}")
                    return
                nxt = op.next_page

            print(f"{key} no encontrado para eliminar")

    def range_search(self, low: int, high: int) -> List[Record]:
        if low > high:
            low, high = high, low

        results: List[Record] = []
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
                            page = DataPage.unpack(df.read(DataPage.SIZE))
                            for r in page.records:
                                if not r.deleted and low <= r.key <= high:
                                    chain_records.append(r)
                            cur_off = page.next_page

                        chain_records.sort(key=lambda r: r.key)
                        results.extend(chain_records)

                        if mid_pos < mid.n:
                            if mid.keys[mid_pos] > high:
                                break

                    if root_pos < root.n:
                        if root.keys[root_pos] > high:
                            break

        return results
    

if __name__ == "__main__":
    base = "empresa"
    for f in [f"{base}_index1.dat", f"{base}_index2.dat", f"{base}_data.dat"]:
        if os.path.exists(f):
            os.remove(f)

    isam = ISAM2Index(base)
    registros = [
        Record(10,"leche", 5, 2.5, "2024-09-01"),
        Record(20,"pan", 3, 1.2, "2024-09-02"),
        Record(30,"queso", 8, 5.0, "2024-09-03"),
        Record(40,"yogurt", 2, 1.5, "2024-09-04"),
        Record(50,"mantequilla", 1, 6.5, "2024-09-05"),
        Record(60,"miel", 7, 8.0, "2024-09-06"),
        Record(70,"jamon", 4, 4.0, "2024-09-07"),
        Record(80,"cafe", 9, 3.5, "2024-09-08"),
    ]

    print("\nIndice ISAM")
    isam.build_index(registros)

    print("\nBusqueda")
    for key in [10, 40, 70, 999]:
        r = isam.search(key)
        print(f"Buscando {key}: {'Encontrado: ' + repr(r) if r else 'No esta'}")

    print("\nInserciones")
    isam.insert(Record(85,"te", 2, 1.0, "2024-09-09"))
    isam.insert(Record(86,"sal", 5, 2.0, "2024-09-10"))
    isam.insert(Record(87,"pimienta", 3, 3.0, "2024-09-11"))
    isam.insert(Record(88,"aceite", 1, 7.0, "2024-09-12"))

    print("\nBusqueda por rango de 30 a 80")
    for r in isam.range_search(30, 80):
        print("- ", r)

    print("\nPrueba remove")
    isam.remove(40)
    isam.remove(87)
    isam.remove(123)

    print("\nBusqueda despues de eliminacion")
    for key in [40, 87, 85]:
        r = isam.search(key)
        print(f"Buscar {key}: {'ENCONTRADO - ' + repr(r) if r else 'No esta'}")

    print("\nRango actualizado 10 a 90")
    for r in isam.range_search(10, 90):
        print("-", r)
