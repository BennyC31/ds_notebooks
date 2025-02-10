import warnings
warnings.filterwarnings('ignore')
import os
from datetime import datetime as dt
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine,text
import oracledb
from dotenv import load_dotenv
load_dotenv()

# env_path = os.path.join('.env')
# load_dotenv(env_path)
data_path = '../data'
xls_data_path ='/Users/benca/OneDrive/Documents/'
log_path = './logs'
# orcl_db_schema = os.getenv('orcl_user')
# mysql_db_schema = os.getenv('database')
orcl_db_schema = os.environ.get('orcl_user')
mysql_db_schema = os.environ.get('database')
def stop_notebook():
    error = RuntimeError('Stop Here')
    raise(error)

def mysql_engine():
    # con_str = f"mysql+pymysql://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}/{os.getenv('database')}"
    con_str = f"mysql+pymysql://{os.environ.get('user')}:{os.environ.get('password')}@{os.environ.get('host')}/{os.environ.get('database')}"
    mysql_engine = create_engine(con_str)
    return mysql_engine

def orcl_engine():
    # return oracledb.connect(user=os.getenv('orcl_user'), password=os.getenv('orcl_password'), dsn=os.getenv('orcl_host'))
    return oracledb.connect(user=os.environ.get('orcl_user'), password=os.environ.get('orcl_password'), dsn=os.environ.get('orcl_host'))

def create_log_file(file_name):
    curr_time = dt.now()
    output_file = f'{log_path}/{file_name}-{dt(curr_time.year,curr_time.month,curr_time.day,curr_time.hour,curr_time.minute)}.log'.replace(':','').replace(' ','')

    return output_file

def write_output(output_file,txt_out):
    try:
        with open(output_file,'a') as out_file:
            out_file.write(f'{txt_out}\n')
    except Exception as e:
        print(f'Error:{e}')
        
def get_data(log_file,sql_stmt,engine,msg):
    start_time = dt.now()
    write_output(log_file,f'{start_time}|Starting get_data...')
    try:
        df = pd.read_sql_query(sql=sql_stmt,con=engine)
        df = df.rename(columns=str.lower)
        write_output(log_file,f'Table {msg}|Rows={len(df)}')
        print(f'Table {msg}|Rows={len(df)}')
        return df
    except Exception as e:
        # error, = e.args
        print(f"An error occurred: {e}")
        write_output(log_file,f'An error occurred: {e}')
    finally:
        end_time = dt.now()
        processing_time_str = f'Total Time: {end_time - start_time}'
        write_output(log_file,processing_time_str)
        write_output(log_file,f'{end_time}|Finished get_data!')
        
def get_xlsx_data(sheet_name,xlsx_file):
    xlsx_df = pd.read_excel(f'{xls_data_path}/{xlsx_file}.xlsx',sheet_name=sheet_name,index_col=None)
    xlsx_df.fillna(0)
    print(f'{sheet_name} Rows={len(xlsx_df)}')
    return xlsx_df

def get_xlsx_results_data(sheet_name,xlsx_file):
    xlsx_df = pd.read_excel(f'{xls_data_path}/{xlsx_file}.xlsx',sheet_name=sheet_name,index_col=None)
    xlsx_df.fillna(0)
    xlsx_df = xlsx_df.query(f'dbstatus in (0,1,3)')
    int_dict = {'gameid':int,'leagid':int,'leagyear':int,'weeknum':int,'wkgameid':int,
            'awayid':int,'homeid':int,'favid':int,'dogid':int,'winteamid':int,'loseteamid':int,'dbstatus':int,'siteloc':int,'gametypeid':int,'yeargmid':int}
    xlsx_df = xlsx_df.astype(int_dict)
    print(f'{sheet_name} All Rows={len(xlsx_df)}')
    return xlsx_df

def execute_sa_sql(log_file,engine,sql_stmt,msg):
    start_time = dt.now()
    write_output(log_file,f'{start_time}|Starting execute_sql...')
    try:
        stmt = text(sql_stmt)
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        print(f"Successfully {msg}!")
        write_output(log_file,f'Successfully {msg}!')
    except Exception as e:
        print(f"An error occurred: {e}")
        write_output(log_file,f'An error occurred: {e}')
    finally:
        end_time = dt.now()
        processing_time_str = f'Total Time: {end_time - start_time}'
        write_output(log_file,processing_time_str)
        write_output(log_file,f'{end_time}|Finished execute_sql!')
def execute_orcl_sql(log_file,orcl_engine,sql_stmt,msg):
    start_time = dt.now()
    write_output(log_file,f'{start_time}|Starting execute_orcl_sql...')
    try:
        orcl_cursor = orcl_engine.cursor()
        orcl_cursor.execute(sql_stmt)
        # orcl_conn.commit()
        orcl_engine.commit()
        print(f"Successfully {msg}!")
        write_output(log_file,f'Successfully {msg}!')
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"An error occurred: {error.message}")
        write_output(log_file,f'An error occurred: {error.message}')
    finally:
        end_time = dt.now()
        processing_time_str = f'Total Time: {end_time - start_time}'
        write_output(log_file,processing_time_str)
        write_output(log_file,f'{end_time}|Finished execute_orcl_sql!')
def insert_orcl_rows(log_file,orcl_engine,ins_df,db_schema,table_name):
    start_time = dt.now()
    write_output(log_file,f'{start_time}|Starting insert_orcl_rows...')

    data_to_insert = [tuple(row) for row in ins_df.itertuples(index=False)]
    columns = ', '.join(ins_df.columns)
    placeholders = ', '.join([':' + str(i+1) for i in range(len(ins_df.columns))])
    sql = f"INSERT INTO {db_schema}.{table_name} ({columns}) VALUES ({placeholders})"

    try:
        orcl_cursor = orcl_engine.cursor()
        orcl_cursor.executemany(sql, data_to_insert)
        # orcl_conn.commit()
        orcl_engine.commit()
        print(f"{table_name} inserted {orcl_cursor.rowcount} rows.")
        write_output(log_file,f'{table_name} Rows Inserted={orcl_cursor.rowcount}')
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"An error occurred {table_name}: {error.message}")
        write_output(log_file,f'An error occurred {table_name}: {error.message}')
    finally:
        end_time = dt.now()
        processing_time_str = f'Total Time: {end_time - start_time}'
        write_output(log_file,processing_time_str)
        write_output(log_file,f'{end_time}|Finished insert_orcl_rows!')
def insert_rows(log_file,df,table_name,engine,db_schema,operation):
    start_time = dt.now()
    write_output(log_file,f'{start_time}|Starting insert_rows...')
    try:
        df.to_sql(name=table_name, con=engine,schema=db_schema, if_exists=operation, index=False)
        print(f"{table_name} inserted {len(df)} rows.")
        write_output(log_file,f'{table_name} Rows Inserted={len(df)}')
    except Exception as e:
        # error, = e.args
        print(f"An error occurred {table_name}: {e}")
        write_output(log_file,f'An error occurred {table_name}: {e}')
    finally:
        end_time = dt.now()
        processing_time_str = f'Total Time: {end_time - start_time}'
        write_output(log_file,processing_time_str)
        write_output(log_file,f'{end_time}|Finished insert_rows!')
def delete_rows_orcl(log_file,orcl_engine,del_df,db_schema_del,table_name):
    start_time = dt.now()
    write_output(log_file,f'{start_time}|Starting delete_rows_orcl...')
    del_df.drop(columns=['dbstatus'],errors='ignore',inplace=True)
    try:
        orcl_cursor = orcl_engine.cursor()
        for unique_id in del_df['gameid']:
            orcl_cursor.execute(f"DELETE FROM {db_schema_del}.{table_name} WHERE gameid = :gameid", gameid=unique_id)
        
        # orcl_conn.commit()
        orcl_engine.commit()
        print(f"{table_name}: Deleted {len(del_df)} row(s).")
        write_output(log_file,f'{table_name}: Deleted {len(del_df)} row(s).')
    except oracledb.Error as e:
        error, = e.args
        print(f"An error occurred {table_name}: {error.message}")
        write_output(log_file,f'An error occurred {table_name}: {error.message}')
        # orcl_conn.rollback()
        orcl_engine.rollback()
    finally:
        orcl_engine.rollback()
        end_time = dt.now()
        processing_time_str = f'Total Time: {end_time - start_time}'
        write_output(log_file,processing_time_str)
        write_output(log_file,f'{end_time}|Finished delete_rows_orcl!')
def delete_rows(log_file,engine,del_df,db_schema_del,table_name):
    start_time = dt.now()
    write_output(log_file,f'{start_time}|Starting delete_rows...')
    with engine.connect() as connection:
        try:
            # Delete existing rows
            for unique_id in del_df['gameid']:
                connection.execute(text(f"DELETE FROM {db_schema_del}.{table_name} WHERE gameid = :gameid"), {'gameid': unique_id})
            
            connection.commit()
            print(f"{table_name}: Deleted {len(del_df)} row(s).")
            write_output(log_file,f'{table_name}: Deleted {len(del_df)} row(s).')
        except Exception as e:
            print(f"An error occurred {table_name}: {e}")
            write_output(log_file,f'An error occurred {table_name}: {e}')
            connection.rollback()
        finally:
            connection.rollback()
            end_time = dt.now()
            processing_time_str = f'Total Time: {end_time - start_time}'
            write_output(log_file,processing_time_str)
            write_output(log_file,f'{end_time}|Finished delete_rows!')
def close_orcl_resources(log_file,orcl_engine):
    start_time = dt.now()
    write_output(log_file,f'{start_time}|Starting close_orcl_resources...')
    try:
        orcl_cursor = orcl_engine.cursor()
        orcl_cursor.close()
        # orcl_conn.close()
        orcl_engine.close()
        print('Oracle Database Resources Closed.')
        write_output(log_file,f'Oracle Database Resources Closed.')
    except oracledb.Error as e:
        error, = e.args
        print(f"An error occurred: {error.message}")
        write_output(log_file,f'An error occurred: {error.message}')
    finally:
        end_time = dt.now()
        processing_time_str = f'Total Time: {end_time - start_time}'
        write_output(log_file,processing_time_str)
        write_output(log_file,f'{end_time}|Finished close_orcl_resources!')
        
def concat_dfs(dfs:list,drop_cols:list):
    concat_df = pd.concat(dfs)
    concat_df.drop(columns=drop_cols,errors='ignore',inplace=True)
    return concat_df
def bundle_data(user_lst,drop_cols):
    df_list = []
    for i in range(len(user_lst)):
        print(user_lst[i])
        df_list.append(get_xlsx_results_data(sheet_name=user_lst[i].lower(),xlsx_file='wager_info'))
        
    df = concat_dfs(df_list,drop_cols)
    return df