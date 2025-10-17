from rtree import index

class RTreeIndex:
    def __init__(self):
        p = index.Property()
        self.idx = index.Index(properties=p)
        self.data = {}

    def add(self, id, coords, record):
        x, y = coords
        self.idx.insert(id, (x, y, x, y))
        self.data[id] = record

    def rangeSearch(self, point, radius):
        x, y = point
        box = (x - radius, y - radius, x + radius, y + radius)
        ids = list(self.idx.intersection(box))
        return [self.data[i] for i in ids]

    def knnSearch(self, point, k):
        x, y = point
        ids = list(self.idx.nearest((x, y, x, y), k))
        return [self.data[i] for i in ids]
    

if __name__ == "__main__":
    rtree = RTreeIndex()

    rtree.add(1,(-12.06, -77.03),{"nombre": "Parque Kennedy", "tipo": "Parque"})
    rtree.add(2,(-12.08, -77.04),{"nombre": "Hospital Casimiro Ulloa", "tipo": "Hospital"})
    rtree.add(3,(-12.05, -77.06),{"nombre": "Parque del Amor", "tipo": "Parque"})
    rtree.add(4,(-12.09, -77.05),{"nombre": "Hospital de Emergencias", "tipo": "Hospital"})
    rtree.add(5,(-12.07, -77.07),{"nombre": "Parque Reducto", "tipo": "Parque"})


    print("\nLugares dentro de 0.03 grados de :")
    results_range = rtree.rangeSearch((-12.07, -77.05), 0.03)
    for i, r in enumerate(results_range, start=1):
        print(f"{i}. {r['nombre']} ({r['tipo']})")

    print("\nTres lugares mas cercanos a (-12.07, -77.05):")
    results_knn = rtree.knnSearch((-12.07, -77.05), 3)
    for i, r in enumerate(results_knn, start=1):
        print(f"{i}. {r['nombre']} ({r['tipo']})")
