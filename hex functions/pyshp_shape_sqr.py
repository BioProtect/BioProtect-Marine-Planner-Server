import shapefile as shp
import math
import geopandas as gpd
df = gpd.read_file('data/shapefiles/case_study/12/12.shp')
minx, miny, maxx, maxy = df.geometry.total_bounds
dx = 100000
dy = 100000

nx = int(math.ceil(abs(maxx - minx)/dx))
ny = int(math.ceil(abs(maxy - miny)/dy))
print('ny: ', ny)

with shp.Writer('data/test/polygon_grid.shp') as w:
    w.autoBalance = 1
    w.field("ID")
    id = 0
    print('id: ', id)

    for i in range(ny):
        for j in range(nx):
            id += 1
            vertices = []
            parts = []
            vertices.append([min(minx+dx*j, maxx), max(maxy-dy*i, miny)])
            vertices.append([min(minx+dx*(j+1), maxx), max(maxy-dy*i, miny)])
            vertices.append([min(minx+dx*(j+1), maxx),
                             max(maxy-dy*(i+1), miny)])
            vertices.append([min(minx+dx*j, maxx), max(maxy-dy*(i+1), miny)])
            parts.append(vertices)
            w.poly(parts)
            w.record(id)
