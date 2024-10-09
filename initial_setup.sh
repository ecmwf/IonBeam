git clone https://github.com/ecmwf/IonBeam
git clone https://github.com/ecmwf/IonBeam-Deployment
git clone https://github.com/ecmwf/IonBeam-Config

# Setup the cmake bundle
mkdir cmake_bundle
cp CMakeLists.txt cmake_bundle/
cd cmake_bundle
mkdir build
git clone git clone https://github.com/ecmwf/ecbuild
export PATH=$PWD/ecbuild/bin:$PATH

## detect presence of conda or mamba commands
if command -v conda &> /dev/null
then
    CONDA_CMD="conda"
elif command -v mamba &> /dev/null
then
    CONDA_CMD="mamba"
elif command -v microconda &> /dev/null
then
    CONDA_CMD="microconda"
elif command -v micromamba &> /dev/null
then
    CONDA_CMD="micromamba"
else
    echo "No conda, mamba, microconda, or micromamba command found."
    exit 1
fi

$CONDA_CMD create -n ionbeam python=3.12 ipykernel
CONDA_PREFIX=$($CONDA_CMD env list | grep "ionbeam" | awk '{print $2}')


ecbuild --prefix=$CONDA_PREFIX \
    -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
    -DCMAKE_CXX_COMPILER=g++-13 \
    -DCMAKE_BUILD_TYPE=Debug \
    -S . -B build

cd build
make -j12 && make install

cd IonBeam
$CONDA_CMD run -n ionbeam pip install -e .





