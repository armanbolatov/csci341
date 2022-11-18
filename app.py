import streamlit as st
import pandas as pd
import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from PIL import Image
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode


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


def aggrid_interactive_table(df: pd.DataFrame):
    """
    Creates an st-aggrid interactive table based on a dataframe.

    :param df: pandas DataFrame
    """
    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )
    options.configure_side_bar()
    options.configure_selection("single")
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        theme='streamlit',
        fit_columns_on_grid_load=True,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )
    return selection


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
    :param keys: str, concatenated keys of the record
    """
    try:
        engine.execute(f'DELETE FROM "{table}" WHERE {keys}')
        return True
    except SQLAlchemyError as e:
        st.error(f"Error: {e.__dict__['orig']}")
        return False


def select_widget(table, column, dtypes):
    """
    Select either selectbox or text_input for the given column,
    depending on the column type.

    :param table: SQLAlchemy table
    :param column: str, column name
    :param dtypes: dict, column data types
    """
    if table in foreign_keys and column in foreign_keys[table]:
        return st.selectbox(
            f"{column}:",
            set(read_table_df(engine, foreign_keys[table][column])[column])
        )
    else:
        if dtypes[column] == 'datetime64[ns]':
            return st.date_input(
                f"{column}:",
                min_value=datetime.date(1800, 1, 1),
                max_value=datetime.date(2200, 1, 1)
                )
        elif dtypes[column] == 'object':
            return st.text_input(f"{column}:")
        elif dtypes[column] == 'int64':
            return st.number_input(f"{column}:", value=0)


def format_columns(columns, dtypes):
    """
    Format columns to be used in SQL query

    :param columns: dict, columns to format
    :param dtypes: dict, column data types
    """
    # remove items with empty values
    columns = {k: v for k, v in columns.items() if v}
    for key in columns:
        if dtypes[key] == 'datetime64[ns]':
            columns[key] = f"'{columns[key]}'"
        elif dtypes[key] == 'object':
            columns[key] = f"'{columns[key]}'"
        elif dtypes[key] == 'int64':
            columns[key] = f"{columns[key]}"
    return columns


if __name__ == '__main__':

    # connect to database
    engine = create_engine(
        st.secrets['db_uri'],
        connect_args={'options': f'-csearch_path=assignment'}
    )

    # set up style for the app
    page_title = "CSCI 341 Assignment 2"
    page_icon = ":underage:"
    layout = "centered"
    st.set_page_config(page_title=page_title, page_icon=page_icon, layout=layout)
    st.title(page_title + " " + page_icon)
  
    # set up the sidebar
    table = st.sidebar.selectbox("Select a table:", primary_keys.keys())
    image = Image.open('media/crying_nigga.jpg')
    st.sidebar.image(image, caption="my face when i finished the bonus part")

    st.write(
        """
        This is a simple database management system that allows you to perform
        basic CRUD operations on the tables in the database. The database
        contains information about diseases, doctors, public servants, and
        countries. First, select a table from the sidebar. Then, you can
        see the table's contents in the main panel.

        The table is interactive, meaning that you can select a record and
        the __update__ and __delete__ forms' primary key fields will update
        accordingly.
        """
    )
    st.subheader(f'{table}:')

    # show the table as a dataframe and save its data types
    df = read_table_df(engine, table) 
    dtypes = df.dtypes
    selection = aggrid_interactive_table(df=df)
    # st.dataframe(df, use_container_width=True)

    with st.form("update_form"):
        st.write("Select the primary keys of the record to update.")
        num_keys = len(primary_keys[table])
        key_columns = st.columns(num_keys)
        key_values, non_key_values = {}, {}
        non_keys = set(df.columns) - set(primary_keys[table])
        if not non_keys:
            disabled = True
            st.warning("Ooops... there are no non-key attributes to update!")
        else:
            disabled = False
        if not disabled:
            for i, key in enumerate(primary_keys[table]):
                with key_columns[i]:
                    selected_index = 0
                    options = list(set(df[key]))
                    if selection["selected_rows"]:
                        selected_key = selection["selected_rows"][0][key]
                        selected_index = options.index(selected_key)
                    key_values[key] = st.selectbox(f"{key}:", options, index=selected_index)
            st.write("""
            Fill the non-key fields of the record to update.
            You may leave some fields empty.""")
            for non_key in non_keys:
                non_key_values[non_key] = select_widget(table, non_key, dtypes)
        submitted = st.form_submit_button("Update", disabled=disabled)
        if submitted:
            key_values = format_columns(key_values, dtypes)
            key_str = " AND ".join(
                [f"{key}={key_values[key]}" for key in primary_keys[table]]
            )
            non_key_values = format_columns(non_key_values, dtypes)
            non_key_str = ", ".join(
                [f"{key}={non_key_values[key]}" for key in non_key_values]
            )
            if not non_key_str:
                st.error("No non-key values specified")
            elif update_record(engine, table, key_str, non_key_str):
                st.success("Record updated successfully!", icon="✅")

    with st.form("create_form"):
        st.write("Fill the fields of the record to create.")
        n_columns = len(df.columns)
        columns = {}
        for column_name in df.columns:
            columns[column_name] = select_widget(table, column_name, dtypes)
        submitted = st.form_submit_button("Create")
        if submitted:
            columns = format_columns(columns, dtypes)
            record = ", ".join(columns.values())
            if len(columns) != n_columns:
                st.error("Not all fields are filled")
            elif create_record(engine, table, record):
                st.success("Record created successfully!", icon="✅")

    with st.form("delete_form"):
        st.write("Select the primary keys of the record to delete.")
        n_keys = len(primary_keys[table])
        key_values = {}
        key_columns = st.columns(n_keys)
        for i, key in enumerate(primary_keys[table]):
            with key_columns[i]:
                selected_index = 0
                options = list(set(df[key]))
                if selection["selected_rows"]:
                    selected_key = selection["selected_rows"][0][key]
                    selected_index = options.index(selected_key)
                key_values[key] = st.selectbox(f"{key}:", options, index=selected_index)
        submitted = st.form_submit_button("Delete")
        if submitted:
            key_values = format_columns(key_values, dtypes)
            keys_str = " AND ".join([f"{key}={key_values[key]}" for key in primary_keys[table]])
            if delete_record(engine, table, keys_str):
                st.success("Record deleted successfully!", icon="✅")