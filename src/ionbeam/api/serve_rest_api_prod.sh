# Tell odc to use long strings
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1

# Where to find the IonBeam config and which env to use from it
export IONBEAM_CONFIG=../../../IonBeam/config
export IONBEAM_ENVIRONMENT=ewc
sudo -E /home/math/.venv/bin/fastapi run rest_api.py --port=80 --host=0.0.0.0 --reload