# Estructura para el AVL File

import struct, os

class AVLNode:
    def __init__(self, key, left=-1, right=-1, height=1):
        self.key = key
        self.left = left
        self.right = right
        self.height = height


class AVLFile:
    def __init__(self, filename="avl.dat", format_str="i"):
        self.filename = filename
        self.FORMAT = format_str
        self.SIZE = struct.calcsize(self.FORMAT + "iii")
        self.HEADER_SIZE = struct.calcsize("i")

        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                f.write(struct.pack("i", -1)) 
    def size(self):
        return (os.path.getsize(self.filename) - self.HEADER_SIZE) // self.SIZE
    
    def get_root(self):
        with open(self.filename, "rb") as f:
            return struct.unpack("i", f.read(self.HEADER_SIZE))[0]

    def set_root(self, idx):
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(struct.pack("i", idx))

    def read(self, index):
        if index == -1:
            return None
        with open(self.filename, "rb") as f:
            f.seek(self.HEADER_SIZE + index * self.SIZE)
            data = f.read(self.SIZE)
            key, left, right, height = struct.unpack(self.FORMAT + "iii", data)
            return AVLNode(key, left, right, height)

    def write(self, index, node):
        with open(self.filename, "r+b") as f:
            f.seek(self.HEADER_SIZE + index * self.SIZE)
            f.write(struct.pack(self.FORMAT + "iii", node.key, node.left, node.right, node.height))

    def append(self, node):
        with open(self.filename, "ab") as f:
            f.write(struct.pack(self.FORMAT + "iii", node.key, node.left, node.right, node.height))
        return self.size() - 1

    def height(self, idx):
        if idx == -1:
            return 0
        return self.read(idx).height

    def balance(self, idx):
        if idx == -1:
            return 0
        node = self.read(idx)
        return self.height(node.left) - self.height(node.right)

    def height_update(self, node):
        node.height = 1 + max(self.height(node.left), self.height(node.right))
        return node

    def rotate_right(self, y_idx):
        y = self.read(y_idx)
        x_idx = y.left
        if x_idx == -1: 
            return y_idx
        x = self.read(x_idx)

        y.left = x.right
        x.right = y_idx

        y = self.height_update(y)
        x = self.height_update(x)

        self.write(y_idx, y)
        self.write(x_idx, x)
        return x_idx

    def rotate_left(self, x_idx):
        x = self.read(x_idx)
        y_idx = x.right
        if y_idx == -1: 
            return x_idx
        y = self.read(y_idx)

        x.right = y.left
        y.left = x_idx

        x = self.height_update(x)
        y = self.height_update(y)

        self.write(x_idx, x)
        self.write(y_idx, y)
        return y_idx

    def insert(self, key):
        root = self.get_root()
        if root == -1:
            root = self.append(AVLNode(key))
            self.set_root(root)
        else:
            new_root = self.recursive_insert(root, AVLNode(key))
            self.set_root(new_root)

    def recursive_insert(self, idx, node):
        if idx == -1:
            return self.append(node)

        current = self.read(idx)
        if node.key < current.key:
            current.left = self.recursive_insert(current.left, node)
        elif node.key > current.key:
            current.right = self.recursive_insert(current.right, node)
        else:
            return idx  # clave duplicada

        current = self.height_update(current)
        self.write(idx, current)

        balance = self.balance(idx)

        if balance > 1 and node.key < self.read(current.left).key:
            return self.rotate_right(idx)
        if balance < -1 and node.key > self.read(current.right).key:
            return self.rotate_left(idx)
        if balance > 1 and node.key > self.read(current.left).key:
            current.left = self.rotate_left(current.left)
            return self.rotate_right(idx)
        if balance < -1 and node.key < self.read(current.right).key:
            current.right = self.rotate_right(current.right)
            return self.rotate_left(idx)

        return idx

    def find(self, key):
        return self._find_recursive(self.get_root(), key)

    def recursive_find(self, idx, key):
        if idx == -1: 
            return None
        node = self.read(idx)
        if key == node.key: 
            return node
        if key < node.key: 
            return self.recursive_find(node.left, key)
        return self.recursive_find(node.right, key)

    def remove(self, key):
        root = self.get_root()
        if root == -1:
            return  # arbol vacio
        new_root = self.remove_recursive(root, key)
        self.set_root(new_root)

    def remove_recursive(self, idx, key):
        if idx == -1:
            return -1  # key no enconrtrada

        node = self.read(idx)

        if key < node.key:
            node.left = self.remove_recursive(node.left, key)
        elif key > node.key:
            node.right = self.remove_recursive(node.right, key)
        else:
            # Caso 1: nodo sin hijos
            if node.left == -1 and node.right == -1:
                return -1
            # Caso 2: un solo hijo
            elif node.left == -1:
                return node.right
            elif node.right == -1:
                return node.left
            # Caso 3: dos hijos 
            else:
                succ_idx = self.get_min(node.right)
                succ = self.read(succ_idx)
                node.key = succ.key
                node.right = self.remove_recursive(node.right, succ.key)

        if idx == -1:
            return -1

        node = self.height_update(node)
        self.write(idx, node)

        balance = self.balance(idx)

        if balance > 1:
            if self.balance(node.left) >= 0:
                return self.rotate_right(idx)
            else:
                node.left = self.rotate_left(node.left)
                self.write(idx, node)
                return self.rotate_right(idx)

        if balance < -1:
            if self.balance(node.right) <= 0:
                return self.rotate_left(idx)
            else:
                node.right = self.rotate_right(node.right)
                self.write(idx, node)
                return self.rotate_left(idx)

        return idx

    def get_min(self, idx):
        current = self.read(idx)
        while current.left != -1:
            idx = current.left
            current = self.read(idx)
        return idx
    

    def range_search(self, min_key, max_key):
        result = []
        self.range_search(self.get_root(), min_key, max_key, result)
        return result

    def range_search(self, idx, min_key, max_key, result):
        if idx == -1: 
            return 
        node = self.read(idx)
        if min_key < node.key:
            self.range_search(node.left, min_key, max_key, result)
        if min_key <= node.key <= max_key:
            result.append(node.key)
        if max_key > node.key:
            self.range_search(node.right, min_key, max_key, result)
