echo "install requirements..."
pip3 install -r requirements.txt

echo "set gdal path..."
export CPLUS_INCLUDE_PATH=/usr/include/gdal && export C_INCLUDE_PATH=/usr/include/gdal

echo "install gdal..."
pip3 install gdal==$(gdal-config --version)

