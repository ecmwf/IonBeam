export FLASK_APP=rest_api.py

# Tell odc to use long strings
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1

# Where to find the IonBeam config and which env to use from it
export IONBEAM_CONFIG=../../../IonBeam/config
export IONBEAM_ENVIRONMENT=local
fastapi dev ./rest_api.py --port 5002 --reload