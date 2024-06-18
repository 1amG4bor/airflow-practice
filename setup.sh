printf "%s " "Creating and activate a virtual environment.... (Press enter to continue)"
read ans
python3 -m venv .env
source .env/bin/activate

printf "%s " "Installing the dependencies.. (Press enter to continue)"
read ans
pip3 install wheel
export AIRFLOW_HOME=./airflow_home
AIRFLOW_VERSION=2.9.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install "pandas<2.2.0"
pip install pyarrow
pip install SQLAlchemy
pip install sqlite3
pip install psycopg2-binary
pip install apache-airflow-providers-postgres

printf "%s " "Go to project folder and configuring Airflow.. (Press enter to continue)"
read ans
cd airflow_home
airflow version

printf "%s " "Get info of Airflow environment.. (Press enter to continue)"
read ans
airflow info

echo '\n\nREADY TO WORK'

