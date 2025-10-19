import os, struct
from typing import List, Optional, Any, Union
from enum import Enum

class KeyType(Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"

class AVLNode:
    __slots__ = ("key", "record_position", "left", "right", "height")
    def __init__(self, key: Any, record_position: int, left: int = -1, right: int = -1, height: int = 1):
        self.key = key
        self.record_position = record_position
        self.left = left
        self.right = right
        self.height = height

class AVLFile:
    def __init__(self, filename: str = "avl.dat", key_type: KeyType = KeyType.INT, max_string_length: int = 50):
        self.filename = filename
        self.key_type = key_type
        self.max_string_length = max_string_length
        
        # Determinar formato según tipo de key
        if key_type == KeyType.INT:
            self.KEY_FMT = "i"
        elif key_type == KeyType.FLOAT:
            self.KEY_FMT = "f"
        elif key_type == KeyType.STRING:
            self.KEY_FMT = f"{max_string_length}s"
        else:
            raise ValueError(f"Tipo de key no soportado: {key_type}")
        
        # Formato: key + record_position + left + right + height
        self.REC_FMT = self.KEY_FMT + "iiii"
        self.REC_SIZE = struct.calcsize(self.REC_FMT)
        self.HEADER_SIZE = struct.calcsize("i")
        
        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                f.write(struct.pack("i", -1))

    def get_root(self) -> int:
        with open(self.filename, "rb") as f:
            return struct.unpack("i", f.read(self.HEADER_SIZE))[0]

    def set_root(self, idx: int) -> None:
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(struct.pack("i", idx))

    def size(self) -> int:
        return (os.path.getsize(self.filename) - self.HEADER_SIZE) // self.REC_SIZE

    def read(self, index: int) -> Optional[AVLNode]:
        if index == -1:
            return None
        with open(self.filename, "rb") as f:
            f.seek(self.HEADER_SIZE + index * self.REC_SIZE)
            data = f.read(self.REC_SIZE)
            if self.key_type == KeyType.STRING:
                key_bytes, record_position, left, right, height = struct.unpack(self.REC_FMT, data)
                key = key_bytes.decode('utf-8').rstrip('\x00')
            else:
                key, record_position, left, right, height = struct.unpack(self.REC_FMT, data)
            return AVLNode(key, record_position, left, right, height)

    def write(self, index: int, node: AVLNode) -> None:
        with open(self.filename, "r+b") as f:
            f.seek(self.HEADER_SIZE + index * self.REC_SIZE)
            if self.key_type == KeyType.STRING:
                key_bytes = node.key.encode('utf-8')[:self.max_string_length].ljust(self.max_string_length, b'\x00')
                f.write(struct.pack(self.REC_FMT, key_bytes, node.record_position, node.left, node.right, node.height))
            else:
                f.write(struct.pack(self.REC_FMT, node.key, node.record_position, node.left, node.right, node.height))

    def append(self, node: AVLNode) -> int:
        with open(self.filename, "ab") as f:
            if self.key_type == KeyType.STRING:
                key_bytes = node.key.encode('utf-8')[:self.max_string_length].ljust(self.max_string_length, b'\x00')
                f.write(struct.pack(self.REC_FMT, key_bytes, node.record_position, node.left, node.right, node.height))
            else:
                f.write(struct.pack(self.REC_FMT, node.key, node.record_position, node.left, node.right, node.height))
        return self.size() - 1

    def height(self, idx: int) -> int:
        if idx == -1:
            return 0
        n = self.read(idx)
        return n.height if n else 0

    def height_update(self, n: AVLNode) -> AVLNode:
        n.height = 1 + max(self.height(n.left), self.height(n.right))
        return n

    def balance(self, idx: int) -> int:
        if idx == -1:
            return 0
        n = self.read(idx)
        return self.height(n.left) - self.height(n.right)

    def right_rotate(self, y_idx: int) -> int:
        y = self.read(y_idx)
        x_idx = y.left
        if x_idx == -1:
            return y_idx
        x = self.read(x_idx)
        y.left = x.right
        x.right = y_idx
        self.write(y_idx, self.height_update(y))
        self.write(x_idx, self.height_update(x))
        return x_idx

    def left_rotate(self, x_idx: int) -> int:
        x = self.read(x_idx)
        y_idx = x.right
        if y_idx == -1:
            return x_idx
        y = self.read(y_idx)
        x.right = y.left
        y.left = x_idx
        self.write(x_idx, self.height_update(x))
        self.write(y_idx, self.height_update(y))
        return y_idx

    def insert(self, key: Any, record_position: int) -> None:
        root = self.get_root()
        if root == -1:
            root = self.append(AVLNode(key, record_position))
            self.set_root(root)
            return
        self.set_root(self.recursive_insert(root, key, record_position))

    def recursive_insert(self, idx: int, key: Any, record_position: int) -> int:
        if idx == -1:
            return self.append(AVLNode(key, record_position))
        n = self.read(idx)
        if key < n.key:
            n.left = self.recursive_insert(n.left, key, record_position)
        elif key > n.key:
            n.right = self.recursive_insert(n.right, key, record_position)
        else:
            # Key ya existe, actualizar record_position
            n.record_position = record_position
            self.write(idx, n)
            return idx
        self.write(idx, self.height_update(n))
        b = self.balance(idx)
        n = self.read(idx)
        if b > 1 and key < self.read(n.left).key:
            return self.right_rotate(idx)
        if b < -1 and key > self.read(n.right).key:
            return self.left_rotate(idx)
        if b > 1 and key > self.read(n.left).key:
            n.left = self.left_rotate(n.left); self.write(idx, n)
            return self.right_rotate(idx)
        if b < -1 and key < self.read(n.right).key:
            n.right = self.right_rotate(n.right); self.write(idx, n)
            return self.left_rotate(idx)
        return idx  

    def find(self, key: Any) -> Optional[int]:
        return self.recursive_find(self.get_root(), key)

    def recursive_find(self, idx: int, key: Any) -> Optional[int]:
        if idx == -1:
            return None
        n = self.read(idx)
        if key == n.key:
            return n.record_position
        if key < n.key:
            return self.recursive_find(n.left, key)
        return self.recursive_find(n.right, key)

    def remove(self, key: Any) -> None:
        root = self.get_root()
        if root == -1:
            return
        self.set_root(self.recursive_remove(root, key))

    def recursive_remove(self, idx: int, key: Any) -> int:
        if idx == -1:
            return -1
        n = self.read(idx)
        if key < n.key:
            n.left = self.recursive_remove(n.left, key)
        elif key > n.key:
            n.right = self.recursive_remove(n.right, key)
        else:
            if n.left == -1 and n.right == -1:
                return -1
            if n.left == -1:
                return n.right
            if n.right == -1:
                return n.left
            succ_idx = self.min_index(n.right)
            succ = self.read(succ_idx)
            n.key = succ.key
            n.right = self.recursive_remove(n.right, succ.key)
        self.write(idx, self.height_update(n))
        b = self.balance(idx)
        n = self.read(idx)
        if b > 1:
            if self.balance(n.left) >= 0:
                return self.right_rotate(idx)
            else:
                n.left = self.left_rotate(n.left); self.write(idx, n)
                return self.right_rotate(idx)
        if b < -1:
            if self.balance(n.right) <= 0:
                return self.left_rotate(idx)
            else:
                n.right = self.right_rotate(n.right); self.write(idx, n)
                return self.left_rotate(idx)
        return idx

    def min_index(self, idx: int) -> int:
        cur = self.read(idx)
        while cur.left != -1:
            idx = cur.left
            cur = self.read(idx)
        return idx

    def range_search(self, min_key: Any, max_key: Any) -> List[int]:
        out: List[int] = []
        self.recursive_range(self.get_root(), min_key, max_key, out)
        return out

    def recursive_range(self, idx: int, lo: Any, hi: Any, out: List[int]) -> None:
        if idx == -1:
            return
        n = self.read(idx)
        if lo < n.key:
            self.recursive_range(n.left, lo, hi, out)
        if lo <= n.key <= hi:
            out.append(n.record_position) 
        if hi > n.key:
            self.recursive_range(n.right, lo, hi, out)
