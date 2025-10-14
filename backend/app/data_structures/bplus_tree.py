# bplus_tree.py
from __future__ import annotations
from typing import Any, List, Optional, Tuple, Dict

class BPlusNode:
    __slots__ = ("is_leaf", "keys", "children", "next_leaf")
    def __init__(self, is_leaf: bool):
        self.is_leaf = is_leaf
        self.keys: List[Any] = []
        self.children: List[Any] = []
        self.next_leaf: Optional[BPlusNode] = None  

class BPlusTree:

    def __init__(self, order: int = 8):
        if order < 3:
            raise ValueError("order must be >= 3")
        self.order = order              
        self.root: Optional[BPlusNode] = None

    def clear(self) -> None:
        self.root = None

    def build(self, records: List[Tuple[Any, Any]]) -> None:
        self.clear()
        for k, v in sorted(records, key=lambda t: t[0]):
            self.add(k, v)

    def add(self, key: Any, value: Any) -> bool:
        return self.insert(key, value)

    def search(self, key: Any) -> List[Any]:
        node = self.root
        if node is None:
            return []
        while not node.is_leaf:
            i = self._upper_bound(node.keys, key)
            node = node.children[i]
        out: List[Any] = []
        for i, k in enumerate(node.keys):
            if k == key:
                out.append(node.children[i])
        return out

    def range_search(self, min_key: Any, max_key: Any) -> List[Tuple[Any, Any]]:
        res: List[Tuple[Any, Any]] = []
        node = self.root
        if node is None:
            return res
        while not node.is_leaf:
            i = self._upper_bound(node.keys, min_key)
            node = node.children[i]
        while node:
            for i, k in enumerate(node.keys):
                if k > max_key:
                    return res
                if k >= min_key:
                    res.append((k, node.children[i]))
            node = node.next_leaf
        return res

    def insert(self, key: Any, value: Any) -> bool:
        if self.root is None:
            leaf = BPlusNode(is_leaf=True)
            leaf.keys = [key]
            leaf.children = [value]
            self.root = leaf
            return True

        promoted = self.inner_insert(self.root, key, value)
        if promoted is None:
            return True
        mid_key, right_node = promoted
        new_root = BPlusNode(is_leaf=False)
        new_root.keys = [mid_key]
        new_root.children = [self.root, right_node]
        self.root = new_root
        return True

    def inner_insert(self, node: BPlusNode, key: Any, value: Any):
        if node.is_leaf:
            i = self._upper_bound(node.keys, key)
            node.keys.insert(i, key)
            node.children.insert(i, value)
            if len(node.keys) > self.order:
                return self.leaf_split(node)
            return None

        i = self._upper_bound(node.keys, key)
        promoted = self.inner_insert(node.children[i], key, value)
        if promoted is None:
            return None
        mid_key, right_node = promoted
        node.keys.insert(i, mid_key)
        node.children.insert(i + 1, right_node)
        if len(node.keys) > self.order:
            return self.internal_split(node)
        return None

    def leaf_split(self, node: BPlusNode):
        mid = len(node.keys) // 2
        right = BPlusNode(is_leaf=True)
        right.keys = node.keys[mid:]
        right.children = node.children[mid:]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid]
        right.next_leaf = node.next_leaf
        node.next_leaf = right
        return (right.keys[0], right)

    def internal_split(self, node: BPlusNode):
        mid = len(node.keys) // 2
        mid_key = node.keys[mid]
        right = BPlusNode(is_leaf=False)
        right.keys = node.keys[mid + 1:]
        right.children = node.children[mid + 1:]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        return (mid_key, right)

    @staticmethod
    def _upper_bound(arr: List[Any], key: Any) -> int:
        i, n = 0, len(arr)
        while i < n and key >= arr[i]:
            i += 1
        return i
    
class RID:
    __slots__ = ("page", "slot")
    def __init__(self, page: int, slot: int):
        self.page = page
        self.slot = slot
    def __repr__(self) -> str:
        return f"RID({self.page},{self.slot})"

class HeapFile:
    def __init__(self, page_size: int = 4096):
        self.rows: List[Any] = []
        self.page_size = page_size

    def insert(self, row: Any) -> RID:
        slot = len(self.rows)
        self.rows.append(row)
        page = slot // (self.page_size // 64 + 1)
        return RID(page=page, slot=slot)

    def get_by_rid(self, rid: RID) -> Any:
        idx = rid.page * (self.page_size // 64 + 1) + rid.slot
        return self.rows[idx]

class DataFile:
    def __init__(self):
        self.data: List[Tuple[Any, Any]] = []

    def build(self, pairs: List[Tuple[Any, Any]]) -> None:
        self.data = sorted(pairs, key=lambda kv: kv[0])

    def insert(self, key: Any, row: Any) -> int:
        i = 0
        n = len(self.data)
        while i < n and self.data[i][0] <= key:
            i += 1
        self.data.insert(i, (key, row))
        return i  

    def get(self, idx: int) -> Tuple[Any, Any]:
        return self.data[idx]

    def __len__(self) -> int:
        return len(self.data)


class BPlusUnclustered:
    def __init__(self, order: int = 8):
        self.idx = BPlusTree(order=order)
        self.heap = HeapFile()

    def clear(self) -> None:
        self.idx.clear()
        self.heap = HeapFile()

    def build(self, records: List[Dict[str, Any]], key_field: str) -> None:
        self.clear()
        for row in records:
            k = row[key_field]
            rid = self.heap.insert(row)
            self.idx.add(k, rid)

    def add(self, key: Any, row: Dict[str, Any]) -> None:
        rid = self.heap.insert(row)
        self.idx.add(key, rid)

    def search(self, key: Any) -> List[Dict[str, Any]]:
        rids = self.idx.search(key)
        return [self.heap.get_by_rid(rid) for rid in rids]

    def range_search(self, lo: Any, hi: Any) -> List[Dict[str, Any]]:
        pairs = self.idx.range_search(lo, hi)
        return [self.heap.get_by_rid(rid) for _, rid in pairs]


class BPlusClustered:

    def __init__(self, order: int = 8):
        self.idx = BPlusTree(order=order)
        self.data = DataFile()

    def clear(self) -> None:
        self.idx.clear()
        self.data = DataFile()

    def build(self, records: List[Dict[str, Any]], key_field: str) -> None:
        self.clear()
        pairs = [(r[key_field], r) for r in records]
        self.data.build(pairs)
        for pos, (k, _row) in enumerate(self.data.data):
            self.idx.add(k, pos)  

    def add(self, key: Any, row: Dict[str, Any]) -> None:
        pos = self.data.insert(key, row)  
        self.idx.add(key, pos)

    def _fetch_by_offsets(self, offsets: List[int]) -> List[Dict[str, Any]]:
        return [self.data.get(i)[1] for i in offsets]

    def search(self, key: Any) -> List[Dict[str, Any]]:
        offsets = self.idx.search(key)
        return self._fetch_by_offsets(offsets)

    def range_search(self, lo: Any, hi: Any) -> List[Dict[str, Any]]:
        pairs = self.idx.range_search(lo, hi)  # [(key, pos)]
        return [self.data.get(pos)[1] for _, pos in pairs]
