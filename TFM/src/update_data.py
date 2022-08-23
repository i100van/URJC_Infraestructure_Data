import datetime
import json
import time
import pandas as pd
import logging
from data_processers.tables_processers import Data_Processer

logging.basicConfig(level=logging.INFO)
from data_managers import Sql_Functions as sql

FIRST_TIME = 2
USE_SSH = 1


def launch_update_only_clean(postgre_conn, reservations_conn, from_date='01-01-2000'):
    try:
        logging.info('Beggining update full clean tables...')
        # bulk_reservs(postgre_conn, reservations_conn)
        postgre_conn.execute_query('DELETE FROM clean.myapps_classreservation')
        postgre_conn.execute_query('DELETE FROM clean.myapps_appusage')
        postgre_conn.execute_query('DELETE FROM clean.myapps_usersession')
        # Raw to clean
        bulk_toClean(from_date, postgre_conn)

    except Exception as err:
        logging.error('Something went wrong updating tables:', err)


def launch_update(myapps_conn, postgre_conn, reservations_conn, from_date=None):
    try:
        logging.info('Beggining update tables process...')

        bulk_reservs(postgre_conn, reservations_conn)

        # Get short time data
        bulk_shortTimeData(from_date, myapps_conn, postgre_conn)

        if from_date != None:
            # Get historical data Archive
            bulk_historical_data(from_date, myapps_conn, postgre_conn)

        # Raw to clean
        bulk_toClean(from_date, postgre_conn)

    except Exception as err:
        logging.error('Something went wrong updating tables:', err)


def bulk_toClean(from_date, postgre_conn):
    processer = Data_Processer()
    # Update AppUsage to clean
    update_table(date_colum_source='timecreated', date_colum_target='timecreated',
                 schema_source='raw.myapps_appusage', schema_target='clean.myapps_appusage',
                 conn_source=postgre_conn, conn_target=postgre_conn,
                 proceser=processer.aplications, from_date=from_date)
    # Update UserSesion to clean
    update_table(date_colum_source='startdate', date_colum_target='startdate',
                 schema_source='raw.myapps_usersession', schema_target='clean.myapps_usersession',
                 conn_source=postgre_conn, conn_target=postgre_conn,
                 proceser=processer.users, from_date=from_date)


def bulk_shortTimeData(from_date, myapps_conn, postgre_conn):
    processer = Data_Processer()
    update_table(date_colum_source='timecreated', date_colum_target='timecreated',
                 schema_source='dbo.ApplicationUsage', schema_target='raw.myapps_appusage',
                 conn_source=myapps_conn, conn_target=postgre_conn, from_date=from_date)
    update_table(date_colum_source='TimeCreated', date_colum_target='startdate',
                 schema_source='dbo.UserSession', schema_target='raw.myapps_usersession',
                 conn_source=myapps_conn, conn_target=postgre_conn,
                 proceser=processer.usersesion_format, from_date=from_date)


def bulk_reservs(postgre_conn, reservations_conn):
    processer = Data_Processer()
    # Update Reservations table
    postgre_conn.execute_query('DELETE FROM raw.myapps_classreservation')
    postgre_conn.execute_query('DELETE FROM clean.myapps_classreservation')
    reservs = reservations_conn.get_df_by_query('SELECT * FROM UXXIAC.VMYAPPS_SOLICITUD_RESERVAS')
    raw = reservs.copy()
    raw.columns = ["CursoAcademico", "CodAlf", "Plan", "Asignatura", "CodAsignatura",
                   "Grupo", "Matriculados", "Software", "Observaciones", "Caracter", "TipoAula", "CodSol",
                   "FechaPuntual", "HoraInicioPuntual", "HoraFinPuntual", "FechaExamen", "HoraInicioExamen",
                   "HoraFinExamen", "FechaInicioPeriodica", "FechaFinPeriodica", "DiaSemanaPeriodica",
                   "HoraInicioPeriodica", "HoraFinPeriodica", "Campus", "Edificio", "Aula", "CapacidadDisponible"]
    postgre_conn.add_df_to_table_to_sql(raw, 'raw', 'myapps_classreservation')
    reservs_p = processer.reservations(reservs)
    postgre_conn.add_df_to_table_to_sql(reservs_p, 'clean', 'myapps_classreservation')


def bulk_historical_data(from_date, myapps_conn, postgre_conn):
    myapps_conn.change_bbdd('flReportingArchive')
    update_table(date_colum_source='TimeCreated', date_colum_target='timecreated',
                 schema_source='dbo.AppUsageArchive', schema_target='raw.myapps_appusage',
                 conn_source=myapps_conn, conn_target=postgre_conn, from_date=from_date)
    update_table(date_colum_source='StartDate', date_colum_target='startdate',
                 schema_source='dbo.UserSessionArchive', schema_target='raw.myapps_usersession',
                 conn_source=myapps_conn, conn_target=postgre_conn, from_date=from_date)
    # Get historical data LTArchive
    myapps_conn.change_bbdd('flReportingLTArchive')
    update_table(date_colum_source='TimeCreated', date_colum_target='timecreated',
                 schema_source='dbo.ApplicationUsage', schema_target='raw.myapps_appusage',
                 conn_source=myapps_conn, conn_target=postgre_conn, from_date=from_date)
    update_table(date_colum_source='StartDate', date_colum_target='startdate',
                 schema_source='dbo.UserSession', schema_target='raw.myapps_usersession',
                 conn_source=myapps_conn, conn_target=postgre_conn, from_date=from_date)


def update_table(date_colum_source: str, date_colum_target: str, schema_source: str, schema_target: str,
                 conn_source: sql.SQL_Functions, conn_target: sql.SQL_Functions, proceser: Data_Processer = None,
                 from_date=None) -> int:
    try:
        logging.info(
            f'--> Updating: {schema_source} -> {schema_target} with date columns: {date_colum_source}-->{date_colum_target}')
        if from_date == None:
            date_max_target = \
                conn_target.get_df_by_query(f'SELECT MAX("{date_colum_target}") FROM {schema_target}').iloc[0, 0]
            format_date = str(date_max_target).replace('\'', '').split('.')[0]
        else:
            format_date = from_date
        logging.info(f'Max date in source: {schema_target} - {format_date}')
        query = 'SELECT * FROM ' + schema_source + ' WHERE ' + date_colum_source + ' >= \'' + format_date + '\''
        df = conn_source.get_df_by_query(query)
        if proceser != None:
            logging.info(f'Processing...')
            df = proceser(df)
        conn_target.add_df_to_table_to_sql(df, schema_target.split('.')[0], schema_target.split('.')[1])
        return 0
    except Exception as err:
        logging.error(f'Something went wrong updating {schema_source} --> {schema_target}: {err}')
        return -1


def create_conexions(credentials: json, ssh: int):
    myapps_conn = sql.SQL_Functions(server=credentials['myapps_ussage']['bbdd_server'],
                                    user=credentials['myapps_ussage']['bbdd_username'],
                                    pss=credentials['myapps_ussage']['bbdd_password'])
    myapps_conn.connect_to_database(type='SQL Server',
                                    bbdd="flReporting")

    reservations_conn = sql.SQL_Functions(server=credentials['reservations']['bbdd_server'],
                                          user=credentials['reservations']['bbdd_username'],
                                          pss=credentials['reservations']['bbdd_password'])
    reservations_conn.connect_to_database(type='Oracle',
                                          port=credentials['reservations']['bbdd_port'],
                                          service=credentials['reservations']['bbdd_service'])
    if ssh == 0:
        postgre_conn = sql.SQL_Functions(server=credentials['postgre']['bbdd_server'],
                                         user=credentials['postgre']['bbdd_username'],
                                         pss=credentials['postgre']['bbdd_password'])
        postgre_conn.connect_to_database(type='Postgre',
                                         bbdd=credentials['postgre']['bbdd_database'],
                                         port=credentials['postgre']['bbdd_port'])
    else:
        postgre_conn = sql.SQL_Functions(server=credentials['postgre_ssh']['bbdd_server'],
                                         user=credentials['postgre_ssh']['bbdd_username'],
                                         pss=credentials['postgre_ssh']['bbdd_password'])
        postgre_conn.connect_to_database(type='Postgre',
                                         bbdd=credentials['postgre_ssh']['bbdd_database'],
                                         port=credentials['postgre_ssh']['bbdd_port'],
                                         ssh=credentials['postgre_ssh']['bbdd_ssh'])

    return myapps_conn, postgre_conn, reservations_conn


def launch_historical_data(date):
    try:
        processer = Data_Processer()
        while date < datetime.datetime(2022, 7, 1):
            start_time = time.time()
            x_ini = date
            x_fin = date + datetime.timedelta(days=15)
            date = x_fin

            print(f'--------------- Period:{x_ini} - {x_fin} ----------------')
            df = postgre_conn.get_df_by_query(
                f'select * from raw.myapps_usersession where startdate>=\'{x_ini}\' and startdate<\'{x_fin}\'')
            print(df.shape)
            if df.shape[0] > 0:
                df = processer.users(df)
                postgre_conn.add_df_to_table_to_sql(df, 'clean', 'myapps_usersession')
                print("Time taken %s seconds" % (time.time() - start_time))

            start_time = time.time()
            df = postgre_conn.get_df_by_query(
                f'select * from raw.myapps_appusage where timecreated>=\'{x_ini}\' and timecreated<\'{x_fin}\'')
            print(df.shape)
            if df.shape[0] > 0:
                df = processer.aplications(df)
                postgre_conn.add_df_to_table_to_sql(df, 'clean', 'myapps_appusage')
                print("Time taken %s seconds" % (time.time() - start_time))
        exit()
    except:
        print(f'********* ME HE ROTO EN {date} *********')
        launch_historical_data(date)


if __name__ == '__main__':
    try:
        f = open('../data/credentials.json')
        credentials = json.load(f)
    except Exception as err:
        logging.error("No detected credentials file:", err)
        exit()
    try:
        if FIRST_TIME == 1:
            myapps_conn, postgre_conn, reservations_conn = create_conexions(credentials, USE_SSH)
            logging.info('** ITS FIRST TIME! Dropping tables...')
            postgre_conn.execute_query('DELETE FROM raw.myapps_classreservation')
            postgre_conn.execute_query('DELETE FROM clean.myapps_classreservation')
            postgre_conn.execute_query('DELETE FROM raw.myapps_appusage')
            postgre_conn.execute_query('DELETE FROM clean.myapps_appusage')
            postgre_conn.execute_query('DELETE FROM raw.myapps_usersession')
            postgre_conn.execute_query('DELETE FROM clean.myapps_usersession')
            launch_update(myapps_conn, postgre_conn, reservations_conn, from_date='01-01-2000')
        elif FIRST_TIME == 2:
            myapps_conn, postgre_conn, reservations_conn = create_conexions(credentials, USE_SSH)
            launch_update(myapps_conn, postgre_conn, reservations_conn)
        elif FIRST_TIME == 3:
            postgre_conn = sql.SQL_Functions(server=credentials['postgre_ssh']['bbdd_server'],
                                             user=credentials['postgre_ssh']['bbdd_username'],
                                             pss=credentials['postgre_ssh']['bbdd_password'])
            postgre_conn.connect_to_database(type='Postgre',
                                         bbdd=credentials['postgre_ssh']['bbdd_database'],
                                         port=credentials['postgre_ssh']['bbdd_port'],
                                         ssh=credentials['postgre_ssh']['bbdd_ssh'])
            df=pd.read_csv('C:/Users/i100v/PycharmProjects/TFM/data/temporary/tfm_raw_myapps_classreservation_p.csv')
            df.rename({'CODSOL':'cod_sol'},axis=1,inplace=True)
            postgre_conn.add_df_to_table_to_sql(df,'clean','myapps_classreservation')
    except Exception as err:
        logging.error(f"Error connecting to database:{err}")
        exit()
