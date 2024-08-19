# Snowflake Procedure Execution Tracer

## Description

This Streamlit application allows users to execute Snowflake stored procedures and trace their execution. It provides detailed insights into the operations performed by the procedure, lists all objects modified during execution, and displays the contents of these modified objects.

## Features

- Execute Snowflake stored procedures
- Display all operations performed within the procedure
- List all objects modified during procedure execution
- Query and display the contents of modified objects

## Prerequisites

- Python 3.7+
- Snowflake account and credentials stored in sf_conf.ini file
- Required Python packages: 
    streamlit, 
    snowflake.connector, 
    configparser, 
    warnings
