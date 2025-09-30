# bplus_tree.py
import os, struct

class BPLUSNode:
    def __init__(self, is_leaf=True, keys=None, children=None, next_leaf=-1):
        self.is_leaf = is_leaf
        self.keys = keys if keys else []
        self.children = children if children else []
        self.next_leaf = next_leaf # Para la siguiente hoja solo si es hoja

    def __repr__(self):
        tipo = "Leaf" if self.is_leaf else "Internal"
        return f"{tipo}(keys={self.keys}, children={self.children}, next={self.next_leaf})"


class BPLUSTreeBase:
    def __init__(self, filename, order=4, clustered=True):
        self.filename = filename
        self.order = order
        self.clustered = clustered

        self.HEADER_SIZE = struct.calcsize("i") # Indice de la raíz

        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                f.write(struct.pack("i", -1))  
            self.root = -1
        else:
            self.root = self._load_root()


    def _load_root(self):
        with open(self.filename, "rb") as f:
            return struct.unpack("i", f.read(self.HEADER_SIZE))[0]

    def _save_root(self, idx):
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(struct.pack("i", idx))


    def search(self, key):
        if self.root == -1:
            return None
        return self._search_node(self.root, key)

    def _search_node(self, node, key):
        if node.is_leaf:
            for i, k in enumerate(node.keys):
                if k == key:
                    return node.children[i]
            return None
        else:
            for i, k in enumerate(node.keys):
                if key < k:
                    return self._search_node(node.children[i], key)
            return self._search_node(node.children[-1], key)

    def range_search(self, min_key, max_key):
        results = []
        if self.root == -1:
            return results

        node = self.root
        while not node.is_leaf:
            node = node.children[0]

        while node:
            for i, k in enumerate(node.keys):
                if min_key <= k <= max_key:
                    results.append((k, node.children[i]))
            if node.next_leaf == -1:
                break
            node = node.next_leaf
        return results

    def insert(self, key, value):
        if self.root == -1:
            # Si el árbol está vacío, crea la raíz
            root = BPLUSNode(is_leaf=True, keys=[key], children=[value])
            self.root = root
            self._save_root(0)
        else:
            root, promoted = self.insert_node(self.root, key, value)
            if promoted:
                new_root = BPLUSNode(is_leaf=False,
                                     keys=[promoted[0]],
                                     children=[root, promoted[1]])
                self.root = new_root

    def insert_node(self, node, key, value):
        if node.is_leaf:
            if key in node.keys:
                return node, None # No insertar duplicados
            pos = 0
            while pos < len(node.keys) and node.keys[pos] < key:
                pos += 1
            node.keys.insert(pos, key)
            node.children.insert(pos, value)

            # Si el nodo excede el orden, dividir
            if len(node.keys) > self.order:
                return self.split_leaf(node)
            return node, None

        else:
            pos = 0
            while pos < len(node.keys) and key >= node.keys[pos]:
                pos += 1
            child, promoted = self.insert_node(node.children[pos], key, value)
            node.children[pos] = child

            if promoted:
                node.keys.insert(pos, promoted[0])
                node.children.insert(pos + 1, promoted[1])

                if len(node.keys) > self.order:
                    return self.split_internal(node)
            return node, None

    def split_leaf(self, node):
        mid = len(node.keys) // 2
        new_leaf = BPLUSNode(is_leaf=True,
                             keys=node.keys[mid:],
                             children=node.children[mid:],
                             next_leaf=node.next_leaf)
        node.keys = node.keys[:mid]
        node.children = node.children[:mid]
        node.next_leaf = new_leaf
        return node, (new_leaf.keys[0], new_leaf)

    def split_internal(self, node):
        mid = len(node.keys) // 2
        promoted_key = node.keys[mid]
        new_internal = BPLUSNode(is_leaf=False,
                                 keys=node.keys[mid + 1:],
                                 children=node.children[mid + 1:])
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        return node, (promoted_key, new_internal)


    def remove(self, key):
        if self.root == -1:
            return
        self.root = self.remove_node(self.root, key)

        if not self.root.keys and not self.root.is_leaf:
            self.root = self.root.children[0]

    def remove_node(self, node, key):
        if node.is_leaf:
            if key in node.keys:
                i = node.keys.index(key)
                node.keys.pop(i)
                node.children.pop(i)
            return node
        else:
            pos = 0
            while pos < len(node.keys) and key >= node.keys[pos]:
                pos += 1
            node.children[pos] = self.remove_node(node.children[pos], key)
            return node

class BPLUSClustered(BPLUSTreeBase):
    def __init__(self, filename="bplus_clustered.dat", order=4):
        super().__init__(filename, order, clustered=True)


class BPLUSUnclustered(BPLUSTreeBase):
    def __init__(self, filename="bplus_unclustered.dat", order=4):
        super().__init__(filename, order, clustered=False)
