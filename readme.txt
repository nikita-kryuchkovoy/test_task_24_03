1) Before running the code you should add environment variables
or change default connection values in config.py according to your local db attrs
for: 
    - password
    - user
    - database
    - port
    - host_name

2) The order for running an ELT scripts:
    - main_stg.py
    - main_dds.py