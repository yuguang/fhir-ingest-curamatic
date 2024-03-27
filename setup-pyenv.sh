export PYTHON_VERSION=3.10
pyenv virtualenv $PYTHON_VERSION "curamatic-fhir-ingest"
pyenv activate "curamatic-fhir-ingest"
pip install -r ./src/requirements.txt
