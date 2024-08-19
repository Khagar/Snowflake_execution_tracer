import streamlit as st
import snowflake.connector
import configparser
import warnings



config = configparser.ConfigParser()
config.read('sf_conf.ini')

# Snowflake connection parameters
snowflake_config = {
    "account": config["snowflake"]["sfUrl"],
    "user": config["snowflake"]["sfUser"],
    "password": config["snowflake"]["sfPassword"],
    "warehouse": config["snowflake"]["sfWarehouse"],
    "database": config["snowflake"]["sfDatabase"],
    "schema": config["snowflake"]["sfSchema"]

}

warnings.filterwarnings("ignore", category=UserWarning, message="This pattern is interpreted as a regular expression, and has match groups. To actually get the groups, use str.extract.")

# Function to execute Snowflake query and return results as a pandas DataFrame
def run_snowflake_query(query):
    with snowflake.connector.connect(**snowflake_config) as conn:
        cur = conn.cursor()
        cur.execute(query)
        if (query.upper().startswith("CALL")):
            results = cur.fetchone()[0]
        else:
            results = cur.fetch_pandas_all()
        cur.execute("SELECT LAST_QUERY_ID()")
        query_id = cur.fetchone()[0]
    return results, query_id


object_regex = r"((CREATE( OR REPLACE)?) (TABLE|VIEW)( IF (NOT)? EXISTS)?|UPDATE|DELETE FROM|INSERT INTO) ([^\s\/*;]+)"
replace_regex = r"((CREATE( OR REPLACE)?) (TABLE|VIEW)( IF (NOT)? EXISTS)?|UPDATE|DELETE FROM|INSERT INTO) "

# Streamlit app
st.title("Procedure Execution Tracer :mag:")
st.write(
    """This application traces the execution of a procedure based on the Query ID.
    It shows all operations performed within the procedure and allows you to
    inspect objects that were created or modified.
    """
)

# Create a form for user input
with st.form("Trace procedure execution"):
    input_sp_call = st.text_input("Procedure call")
    submitted = st.form_submit_button("Output execution")
    
    if submitted:
        sp_result, sp_query_id = run_snowflake_query(input_sp_call)
        # Query for df_one
        query = f"""
            SELECT 
                QHIST_V.* 
            FROM 
                (SELECT 
                    TV.SESSION_ID, 
                    TV.WAREHOUSE_NAME, 
                    TV.START_TIME, 
                    TV.END_TIME, 
                    TV.CLUSTER_NUMBER 
                FROM 
                    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) TV 
                WHERE 
                    TV.QUERY_ID = '{sp_query_id}'
                ) T 
            INNER JOIN 
                TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) QHIST_V 
                ON QHIST_V.WAREHOUSE_NAME=T.WAREHOUSE_NAME
                AND QHIST_V.SESSION_ID = T.SESSION_ID
                AND 
                ( 
                    QHIST_V.START_TIME between T.START_TIME and T.END_TIME 
                    OR QHIST_V.END_TIME between T.START_TIME and T.END_TIME 
                )
            ORDER BY   
                QHIST_V.START_TIME ASC
        """
        df, _ = run_snowflake_query(query)


        st.subheader("Underlying data")
        st.dataframe(df, use_container_width=True)

        # Apply the extraction to the DataFrame
        df_filtered = df["QUERY_TEXT"][df["QUERY_TEXT"].str.contains(object_regex)]
        df_filtered = df_filtered.str.replace(replace_regex,'',regex = True).str.replace(" .*", '', regex = True).rename("OBJECT_NAME")

        # Display the results        
        st.subheader("Objects modified")
        st.dataframe(df_filtered, use_container_width=True)

        for index, value in df_filtered.items():
            st.subheader(value)
            st.dataframe(run_snowflake_query(f"SELECT * FROM {value}")[0], use_container_width=True)