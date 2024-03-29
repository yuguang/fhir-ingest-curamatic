export PYTHON_VERSION=3.10
pyenv virtualenv $PYTHON_VERSION "fhir-ingest"
pyenv activate "fhir-ingest"
pip install -r ./src/requirements.txt
