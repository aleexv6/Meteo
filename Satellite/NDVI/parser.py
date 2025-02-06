from pyhdf.SD import SD, SDC
import numpy as np
import pyproj

def parser(fileUrl):
    #Static values from MODIS documentation https://lpdaac.usgs.gov/products/mod13q1v061/ (Layers / Variables dropdown)
    VALID_RANGE = [-2000, 10000]
    FILL_VALUE = -3000
    SCALE_FACTOR = 0.0001
    DATA_NAME = "250m 16 days EVI"

    file = SD(fileUrl, SDC.READ) #pyhdf read file

    #Check for metadata in the file and make it a dict
    gridmeta = file.attributes()["StructMetadata.0"]
    gridmeta = dict([x.split("=") for x in gridmeta.split() if "=" in x])
    for key, val in gridmeta.items(): #make strings code
        try:
            gridmeta[key] = eval(val)
        except:
            pass

    sds_obj = file.select(DATA_NAME) # select sds
    data = sds_obj.get() # get sds data
    data = data.astype(float) #convert data to float for masking with nans

    #apply the attributes to the data for masking
    invalid = np.logical_or(data < VALID_RANGE[0], data > VALID_RANGE[1]) #logical or (invalid is true or false with shape data)
    invalid = np.logical_or(invalid, data == FILL_VALUE) #same but we had fill_value to filter
    data[invalid] = np.nan #mask invalid values with nan 
    data = data * SCALE_FACTOR #compute data with scale
    data = np.ma.masked_array(data, np.isnan(data)) #make masked_array of data

    #grid from sinusoidal to cyl (latitude longitude)
    x0 = gridmeta["UpperLeftPointMtrs"][0] #gridmeta is in meters
    y0 = gridmeta["UpperLeftPointMtrs"][1]
    x1 = gridmeta["LowerRightMtrs"][0]
    y1 = gridmeta["LowerRightMtrs"][1]
    nx, ny = data.shape
    #linspace data then meshgrid to coordinates
    x = np.linspace(x0, x1, nx, endpoint=False)
    y = np.linspace(y0, y1, ny, endpoint=False)
    xv, yv = np.meshgrid(x, y)

    #NASA and HDFEOS code to transform sinu coords to WGS84 (lon, lat)
    sinu = pyproj.Proj(f"+proj=sinu +R={gridmeta["ProjParams"][0]} +nadgrids=@null +wktext") 
    wgs84 = pyproj.Proj("+init=EPSG:4326") 
    lon, lat= pyproj.transform(sinu, wgs84, xv, yv)
    return dict(lons = lon, lats = lat, datas = data)

