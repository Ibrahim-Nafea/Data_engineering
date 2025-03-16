
from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def create_db_engine(db_path):
    """This function is to create a db engine from SQLite database in the same work directory, engine will be used in the next function
    to create a pandas dataframe

    Args:
        db_path ("str"): refers to db directory name

    Raises:
        e: Import error, need to install SQLAlchemy
        e: Any other error that lead to fail to create the engine

    Returns:
        var: sqlalchemy.engine.base.Engine
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """This function is to create a pandas DataFrame by importing from SQL db using SQL query and the engine created at create_db_engine()

    Args:
        engine (var): sqlalchemy.engine.base.Engine
        sql_query (text): an SQL query to join multiple tables in Maji_Ndogo DB

    Raises:
        ValueError: Describes that SQL queried an Empty Database
        e: Describes an error in the SQL query itself to be revised
        e: Any other type of error

    Returns:
        DataFrame: Returns a Pandas DataFrame after successful connection with database using SQL query and engine
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """_summary_

    Args:
        URL (_type_): _description_

    Raises:
        e: _description_
        e: _description_

    Returns:
        _type_: _description_
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e