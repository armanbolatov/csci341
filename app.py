import streamlit as st
import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dateutil.parser import parse

# tables listed in the database
all_tables = (
    'DiseaseType', 'Country', 'Disease',
    'Discover', 'Users', 'PublicServant',
    'Doctor', 'Specialize', 'Record', 
)

# lists containing primary keys for each table
primary_keys = {
    'DiseaseType':      ['id'],
    'Country':          ['cname'],
    'Disease':          ['disease_code'],
    'Discover':         ['cname', 'disease_code'],
    'Users':            ['email'],
    'PublicServant':    ['email'],
    'Doctor':           ['email'],
    'Specialize':       ['id', 'email'],
    'Record':           ['email', 'cname', 'disease_code'],
}

# lists containing foreign keys for each table
foreign_keys = {
    'Disease':       {'id': 'DiseaseType'},
    'Discover':      {'cname': 'Country',
                      'disease_code': 'Disease'},
    'Users':         {'cname': 'Country'},
    'PublicServant': {'email': 'Users'},
    'Doctor':        {'email': 'Users'},
    'Specialize':    {'id': 'DiseaseType',
                       'email': 'Doctor'},
    'Record':        {'email': 'PublicServant',
                      'cname': 'Country',
                      'disease_code': 'Disease'},
}


def create_record(engine, table, record):
    """
    Create a new record in the table.

    :param engine: SQLAlchemy engine
    :param table: SQLAlchemy table
    :param record: str, record to create
    """
    try:
        engine.execute(f'INSERT INTO "{table}" VALUES ({record})')
        return True
    except SQLAlchemyError as e:
        st.error(f"Error: {e.__dict__['orig']}")
        return False


def read_table_df(engine, table):
    """
    Read a table from the database into a pandas DataFrame.

    :param engine: SQLAlchemy engine
    :param table: SQLAlchemy table
    """
    try:
        df = pd.read_sql_table(table, con=engine, index_col=False)
    except:
        df = pd.DataFrame([])
    return df


def update_record(engine, table, keys, non_keys):
    """
    Update a record in the table.

    :param engine: SQLAlchemy engine
    :param table: SQLAlchemy table
    :param keys: str, concatenated keys of the record
    :param non_keys: str, concatenated non-keys of the record
    """
    try:
        engine.execute(f'UPDATE "{table}" SET {non_keys} WHERE {keys}')
        return True
    except SQLAlchemyError as e:
        st.error(f"Error: {e.__dict__['orig']}")
        return False
    

def delete_record(engine, table, keys):
    """
    Delete a record from the table.

    :param engine: SQLAlchemy engine
    :param table: SQLAlchemy table
    :param keys_str: str, concatenated keys of the record
    """
    try:
        engine.execute(f'DELETE FROM "{table}" WHERE {keys}')
        return True
    except SQLAlchemyError as e:
        st.error(f"Error: {e.__dict__['orig']}")
        return False


def select_widget(table, column):
    """
    Select either selectbox or text_input for the given column,
    depending on the column type.

    :param table: SQLAlchemy table
    :param column: str, column name
    """
    if table in foreign_keys and column in foreign_keys[table]:
        return st.selectbox(
            f"{column}:",
            set(read_table_df(engine, foreign_keys[table][column])[column])
        )
    else:
        return st.text_input(f"{column}:")


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        return False


def format_columns(columns):
    """
    Format columns to be used in SQL query
    """
    columns = {k: v for k, v in columns.items() if v}
    for key in columns:
        columns[key] = str(columns[key])
        if not is_date(columns[key]) and not columns[key].isdigit():
            columns[key] = f"'{columns[key]}'"
    return columns


if __name__ == '__main__':

    page_title = "CSCI 341 Assignment 2"
    page_icon = ":underage:"
    layout = "centered"

    st.set_page_config(page_title=page_title, page_icon=page_icon, layout=layout)
    st.title(page_title + " " + page_icon)

    # creating PostgreSQL client
    db_schema = "assignment"
    db_username = st.secrets['db_username']
    db_password = st.secrets['db_password']
    engine = create_engine(
        f"postgresql://{db_username}:{db_password}@127.0.0.1:5432/postgres",
        connect_args={'options': f'-csearch_path={db_schema}'}
    )
    
    st.sidebar.write(
        """
        This is a simple database management system that allows you to perform
        basic CRUD operations on the tables in the database. The database
        contains information about diseases, doctors, public servants, and
        countries. First, select a table from the sidebar.
        """
    )
    st.sidebar.markdown("---")
    table = st.sidebar.selectbox('Select the table:', primary_keys.keys())
    st.subheader(f'{table}:')
    df = read_table_df(engine, table)

    st.dataframe(df, use_container_width=True)

    with st.form("update_form"):
        st.write("Enter the primary keys of the record to update.")
        num_keys = len(primary_keys[table])
        key_columns = st.columns(num_keys)
        key_values = {}
        for i, key in enumerate(primary_keys[table]):
            with key_columns[i]:
                key_values[key] = st.selectbox(f"{key}:", set(df[key]))

        st.write("Choose the attributes to update. You may leave some blank.")
        non_keys = set(df.columns) - set(primary_keys[table])
        non_key_values = {}
        for non_key in non_keys:
            non_key_values[non_key] = select_widget(table, non_key)

        submitted = st.form_submit_button("Update")
        if submitted:
            key_values = format_columns(key_values)
            key_str = " AND ".join(
                [f"{key}={key_values[key]}" for key in primary_keys[table]]
            )
            non_key_values = format_columns(non_key_values)
            non_key_str = ", ".join(
                [f"{key}={non_key_values[key]}" for key in non_key_values]
            )
            if not non_key_str:
                st.error("No non-key values specified")
            elif update_record(engine, table, key_str, non_key_str):
                st.success("Record updated successfully!", icon="✅")
                time.sleep(3)
                st.experimental_rerun()

    with st.form("create_form"):
        st.write("Create a new record")
        n_columns = len(df.columns)
        columns = {}
        for column_name in df.columns:
            columns[column_name] = select_widget(table, column_name)
        submitted = st.form_submit_button("Create")
        if submitted:
            columns = format_columns(columns)
            record = ", ".join(columns.values())
            if not record:
                st.error("No values specified")
            elif create_record(engine, table, record):
                st.success("Record created successfully!", icon="✅")
                time.sleep(3)
                st.experimental_rerun()

    with st.form("delete_form"):
        st.write("In this table you can delete a record")
        n_keys = len(primary_keys[table])
        key_values = {}
        key_columns = st.columns(n_keys)
        for i, key in enumerate(primary_keys[table]):
            with key_columns[i]:
                key_values[key] = st.selectbox(f"{key}:", df[key])
        submitted = st.form_submit_button("Delete")
        if submitted:
            key_values = format_columns(key_values)
            keys_str = " AND ".join([f"{key}={key_values[key]}" for key in primary_keys[table]])
            if delete_record(engine, table, keys_str):
                st.success("Record deleted successfully!", icon="✅")
                time.sleep(3)
                st.experimental_rerun()