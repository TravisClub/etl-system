Simple ETL system
=======================

# Setup

1 - Install dev prereqs (use equivalent linux or windows pkg mgmt)
----

    brew install python3.6
    brew install virtualenv


2 - Set up a Python virtual environment (from project root directory)
----

    mkdir etl && cd $_
    unzip etl-system.zip


3 - Set up a Python virtual environment (from project root directory)
----

    virtualenv -p python3.6 venv 
    source venv/bin/activate


4 - Install required python packages into the virtual env
----
    
    pip install -r requirements.txt


5 - Run the tests from project root directory
----

    sh scripts/code-coverage.sh


6 - Run the code
----
    
    python src/etl.py --help



