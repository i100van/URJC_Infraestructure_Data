import pandas as pd
import numpy as np
import datetime

def drop_polivalentes(x):
    if 'polivalentes' in str(x).lower():
        return x.replace('Laboratorios Polivalentes', "Laboratorio")
    else:
        return x

def drop_informatica(x):
    if 'informática' in str(x).lower():
        return x.replace('Informática ', "")
    else:
        return x

def rename_software(x, software_lower):
    x = str(x).lower()
    x = x.replace('.exe', '').rstrip()

    list_values = str(x).split(" ")
    digit = str(list_values[-1]).split(".")
    if len(digit)>1 or digit[0].isdigit():
        x = str(x).replace(list_values[-1], "").lower()
    x = x.rstrip()
    if any(x in s for s in software_lower):
        return [i for i in software_lower if x in str(i).lower()][0]
    else:
        return "Otros"

def get_campus(x):
    server_name_list = x.split("-")
    Campus = ['mo', 'qo', 'zo', 'fo', 'vo', 'ao']
    if (len(server_name_list)==4) and (server_name_list[0] in Campus):
        return server_name_list[0]
    else:
        return 'Desconocido'

def rename_device_type(x):
    x = str(x).lower()
    if 'lg' in x:
        return 'LG'
    elif 'mac' in x:
        return 'Mac'
    elif 'ipad' in x:
        return 'iPad'
    elif 'oneplus' in x:
        return 'OnePlus'
    elif 'samsung' in x:
        return 'Samsung'
    elif 'realme' in x:
        return 'Realme'
    elif 'oppo' in x:
        return 'OPPO'
    elif 'lenovo' in x:
        return 'Lenovo'
    elif 'xiaomi' in x:
        return 'Xiaomi'
    elif 'huawei' in x:
        return 'Huawei'
    elif 'iphone' in x:
        return 'iPhone'
    elif 'huawei' in x:
        return 'Huawei'
    elif 'acer' in x:
        return 'Acer'
    elif 'bq' in x:
        return 'bq'
    elif 'nan' in x:
        return 'Desconocido'
    else:
        return x

def get_enrolment_year(x):
    enrolment_year_list = x.split(".")
    if len(enrolment_year_list) == 3:
        try:
            if int(enrolment_year_list[2]) > 2000:
                return int(enrolment_year_list[2])
            else:
                return 0
        except:
            return 0
    else:
        return 0

def get_lab(x):
    server_name_list = x.split("-")
    Campus = ['mo', 'qo', 'zo', 'fo', 'vo', 'ao']
    if (len(server_name_list) == 4) and (server_name_list[0] in Campus):
        return server_name_list[1]
    else:
        return 'Desconocido'


def get_aula(x):
    server_name_list = x.split("-")
    Campus = ['mo', 'qo', 'zo', 'fo', 'vo', 'ao']
    if (len(server_name_list) == 4) and (server_name_list[0] in Campus):
        return server_name_list[2]
    else:
        return 'Desconocido'


def get_computer(x):
    server_name_list = x.split("-")
    Campus = ['mo', 'qo', 'zo', 'fo', 'vo', 'ao']
    if (len(server_name_list) == 4) and (server_name_list[0] in Campus):
        return server_name_list[3].split(".")[0]
    else:
        return 'Particular'


def cast_capacidad(x):
    try:
        return int(x)
    except:
        emp_lis = []
        for z in x.split(' '):
            if z.isdigit():
                emp_lis.append(int(z))
        return np.sum(emp_lis)


def convert_periodical_to_puntual(df: pd.DataFrame):
    table = list()
    for index, row in df.iterrows():
        formats = ["%d-%m-%Y","%d-%m-%Y %H:%M:%S","%d/%m/%Y","%d/%m/%Y %H:%M:%S"]
        try:
            for pattern in formats:
                try:
                    ini = datetime.datetime.strptime(row['Fecha_Inicio'], pattern)
                except:
                    pass
            for pattern in formats:
                try:
                    fin = datetime.datetime.strptime(row['Fecha_Fin'], pattern)
                except:
                    pass
        except:
            print(f'FECHA INICIAL {row["Fecha_Inicio"]} FECHA FIN {row["Fecha_Fin"]}')

        iters = (fin - ini).days // 7
        for i in range(iters):
            row_aux = row.copy()
            aux_ini = ini + datetime.timedelta(days=7 * i)
            aux_fin = ini + datetime.timedelta(days=7 * i)

            if aux_ini.day<10:
                dayi = '0'+str(aux_ini.day)
            else:
                dayi = aux_ini.day
            if aux_ini.month<10:
                monthi = '0'+str(aux_ini.month)
            else:
                monthi = str(aux_ini.month)
            if aux_fin.day<10:
                dayf = '0'+str(aux_fin.day)
            else:
                dayf = str(aux_fin.day)
            if aux_fin.month<10:
                monthf = '0'+str(aux_fin.month)
            else:
                monthf = str(aux_fin.month)

            row_aux['Fecha_Inicio'] = str(dayi) + '-' + str(monthi)+ '-' + str(aux_ini.year)
            row_aux['Fecha_Fin'] = str(dayf) + '-' + str(monthf)+ '-' + str(aux_fin.year)
            row_aux['CARACTER'] = 'Puntual'
            table.append(row_aux)
    return pd.DataFrame(table)


def id_number(id):
    return int(id, 16)


def remove_character(id):
    return id.replace('-', '')


def generate_id(s):
    return abs(hash(s)) % (10 ** 10)


def rename_aulas(aula):
    if aula.startswith('a'):
        return aula.replace('a', 'Aula ')
    elif aula.startswith('d'):
        return aula.replace('d', 'Despacho ')
    elif aula.startswith('sem'):
        return aula.replace('sem', 'Seminario ')
    else:
        return aula


def num_aula(x):
    list_aula = x.split(" ")
    numbers = ""
    for value in list_aula[-1]:
        if value.isdigit():
            numbers += value
    if len(numbers) < 3 and list_aula[-1][0].isdigit():
        return list_aula[-1].upper()
    elif len(numbers) < 3:
        return (list_aula[-1][0].upper()) + "0" + numbers
    else:
        return list_aula[-1].upper()


def aulas_pavia(x):
    return "P" + x


def aulas_lucas(x):
    aula = ""
    for w in x:
        if w.isdigit():
            aula += w
    return "L" + aula


def aulas_maro(x):
    return "M" + x


def aulas_gobernador(x):
    return "G" + x


def aulas_biblioteca_mo(x):
    return "B018"


def aulas_biblioteca_fo(x):
    return "S014"


def aulas_madrid_vicalvaro(x):
    return "Aulario"

def aulas_gestion_vicalvaro(x):
    return "G"+x

class Data_Processer:
    def reservations(self,df):
        # FECHA INCIO
        df['Fecha_Inicio'] = np.nan
        df.FECHA_PUNTUAL = df.FECHA_PUNTUAL.astype(str).replace('None', np.nan)
        df.FECHA_EXAMEN = df.FECHA_EXAMEN.astype(str).replace('None', np.nan)
        df.FECHA_INICIO_PERIODICA = df.FECHA_INICIO_PERIODICA.astype(str).replace('None', np.nan)
        df.Fecha_Inicio.fillna(df.FECHA_PUNTUAL, inplace=True)
        df.Fecha_Inicio = df.Fecha_Inicio.astype(str).replace('nan', np.nan)
        df.Fecha_Inicio.fillna(df.FECHA_EXAMEN, inplace=True)
        df.Fecha_Inicio = df.Fecha_Inicio.astype(str).replace('nan', np.nan)
        df.Fecha_Inicio.fillna(df.FECHA_INICIO_PERIODICA, inplace=True)

        
        # FECHA FIN
        df['Fecha_Fin'] = np.nan
        df.FECHA_FIN_PERIODICA = df.FECHA_FIN_PERIODICA.astype(str).replace('None', np.nan)
        df.Fecha_Fin.fillna(df.FECHA_FIN_PERIODICA, inplace=True)
        df.Fecha_Fin = df.Fecha_Fin.astype(str).replace('nan', np.nan)
        df.Fecha_Fin.fillna(df.FECHA_EXAMEN, inplace=True)
        df.Fecha_Fin = df.Fecha_Fin.astype(str).replace('nan', np.nan)
        df.Fecha_Fin.fillna(df.FECHA_PUNTUAL, inplace=True)
        
        # HORA INICIO
        df['Hora_Inicio'] = np.nan
        df.HORA_INICIO_PUNTUAL = df.HORA_INICIO_PUNTUAL.astype(str).replace('None', np.nan)
        df.HORA_INICIO_EXAMEN = df.HORA_INICIO_EXAMEN.astype(str).replace('None', np.nan)
        df.HORA_INICIO_PERIODICA = df.HORA_INICIO_PERIODICA.astype(str).replace('None', np.nan)
        df.Hora_Inicio.fillna(df.HORA_INICIO_PUNTUAL, inplace=True)
        df.Hora_Inicio = df.Hora_Inicio.astype(str).replace('nan', np.nan)
        df.Hora_Inicio.fillna(df.HORA_INICIO_EXAMEN, inplace=True)
        df.Hora_Inicio = df.Hora_Inicio.astype(str).replace('nan', np.nan)
        df.Hora_Inicio.fillna(df.HORA_INICIO_PERIODICA, inplace=True)
        
        # HORA FIN
        df['Hora_Fin'] = np.nan
        df.HORA_FIN_PUNTUAL = df.HORA_FIN_PUNTUAL.astype(str).replace('None', np.nan)
        df.HORA_FIN_EXAMEN = df.HORA_FIN_EXAMEN.astype(str).replace('None', np.nan)
        df.HORA_FIN_PERIODICA = df.HORA_FIN_PERIODICA.astype(str).replace('None', np.nan)
        df.Hora_Fin.fillna(df.HORA_FIN_PUNTUAL, inplace=True)
        df.Hora_Fin = df.Hora_Fin.astype(str).replace('nan', np.nan)
        df.Hora_Fin.fillna(df.HORA_FIN_EXAMEN, inplace=True)
        df.Hora_Fin = df.Hora_Fin.astype(str).replace('nan', np.nan)
        df.Hora_Fin.fillna(df.HORA_FIN_PERIODICA, inplace=True)
        
        df.drop(['FECHA_PUNTUAL', 'FECHA_EXAMEN', 'FECHA_INICIO_PERIODICA', 'FECHA_FIN_PERIODICA',
                           'HORA_INICIO_PUNTUAL', 'HORA_INICIO_EXAMEN', 'HORA_INICIO_PERIODICA', 'HORA_FIN_PUNTUAL',
                           'HORA_FIN_EXAMEN', 'HORA_FIN_PERIODICA'], axis=1, inplace=True)
        
        df.CODALF = df.CODALF.astype(str)
        df.CARACTER = df.CARACTER.astype('category')
        new_puntuals = convert_periodical_to_puntual(df[df['CARACTER'] == 'Periódica'])
        df = df[~(df['CARACTER'] == 'Periódica')]
        df = df.append(new_puntuals)
        df.Fecha_Inicio = df.Fecha_Inicio.astype(str).str.replace('/','-') + ' ' + df.Hora_Inicio.astype(str)
        df.Fecha_Fin = df.Fecha_Fin.astype(str).str.replace('/','-') + ' ' + df.Hora_Fin.astype(str)
        df.Fecha_Inicio = df.Fecha_Inicio.astype(str).str.replace('/','-')
        df.Fecha_Fin = df.Fecha_Fin.astype(str).str.replace('/','-')
        df.Fecha_Inicio = pd.to_datetime(df.Fecha_Inicio, format="%d-%m-%Y %H:%M:%S",errors='coerce')
        df.Fecha_Fin = pd.to_datetime(df.Fecha_Fin, format="%d-%m-%Y %H:%M:%S" ,errors='coerce')
        df.drop(['Hora_Inicio', 'Hora_Fin'], axis=1, inplace=True)
        df['Year'] = df['Fecha_Inicio'].dt.year
        df['Month'] = df['Fecha_Inicio'].dt.month
        df['Dow'] = df['Fecha_Inicio'].dt.strftime('%A')
        df['Day'] = df['Fecha_Inicio'].dt.day
        df['Hour_Ini'] = df['Fecha_Inicio'].dt.hour
        df['Hour_Fin'] = df['Fecha_Fin'].dt.hour
        df['Duration'] = df.Hour_Fin - df.Hour_Ini
        df.CAMPUS = df.CAMPUS.astype('category')
        df.EDIFICIO = df.EDIFICIO.astype(str)
        df['EDIFICIO'] = df.EDIFICIO.apply(lambda x: drop_polivalentes(x))
        df['EDIFICIO'] = df.EDIFICIO.str.rstrip()
        df.AULA = df.AULA.astype(str)
        df['AULA'] = df.AULA.apply(lambda x: drop_informatica(x))
        df.CAPACIDAD_DISPONIBLE = df.CAPACIDAD_DISPONIBLE.apply(lambda x: cast_capacidad(x))
        df.SOFTWARE = df.SOFTWARE.astype('category')
        df.dropna(subset=['Fecha_Inicio'],inplace=True)
        df.dropna(subset=['Fecha_Fin'],inplace=True)
        return df
        


    def users(self,df):
        df.startdate = pd.to_datetime(df.startdate, infer_datetime_format=True)
        df.enddate = pd.to_datetime(df.enddate, infer_datetime_format=True)
        df.duration = df.duration.astype('int')

        df.username = df.username.astype(str)
        
        df['enrolment_year'] = df.username.apply(lambda x: get_enrolment_year(x))
        df.enrolment_year = df.enrolment_year.astype('int')

        df.domainname = df.domainname.astype(str)
        df.domainname = df.domainname.str.lower()
        df.servername = df.servername.astype(str)
        df.servername = df.servername.str.lower()
        df.clientname = df.clientname.astype(str)
        df.clientname = df.clientname.str.lower()
        
        df['campus'] = df.clientname.apply(lambda x: get_campus(x))
        
        df['edificio'] = df.clientname.apply(lambda x: get_lab(x))
        
        df['aula'] = df.clientname.apply(lambda x: get_aula(x))
        
        df['computer'] = df.clientname.apply(lambda x: get_computer(x))
        df.sessionid = df.sessionid.astype('category')
        df.devicetype = df.devicetype.astype('category')
        df.connectionstatus = df.connectionstatus.astype('category')
        df.connectorbandwidth = df.connectorbandwidth.astype(int)
        df.connectorlatency = df.connectorlatency.astype(int)
        df.connectortype = df.connectortype.astype('category')
        df.drop(['websitename', 'hostname'], inplace=True, axis=1)
        df.operatingsystem = df.operatingsystem.astype('category')
        df.servicepack = df.servicepack.astype('category')
        df.devicetype = df.devicetype.astype(str)
        df['devicetype'] = df.devicetype.apply(lambda x: rename_device_type(x))
        #TODO: Sacar los dicts de aqui
        Campus = {'mo': 'MOSTOLES', 'qo': 'MADRID - CENTRO', 'zo': 'ARANJUEZ', 'fo': 'FUENLABRADA', 'vo': 'MADRID - VICALVARO',
                  'ao': 'ALCORCON'}
        df.replace({'campus': Campus}, inplace=True)
        df.aula = df.aula.astype(str)
        #TODO: Sacar los dicts de aqui
        Edificio = {
            # BIBLIOTECA
            'bib1': 'Biblioteca',  # ¿¿Hay varias bibliotecas??
            'bibl': 'Biblioteca',
            'biib1': 'Biblioteca',  # ??
            'bib': 'Biblioteca',
        
            # AULARIOS
            'aul1': 'Aulario I',
            'aul2': 'Aulario II',
            'aul3': 'Aulario III',
            'aul4': 'Aulario IV',
        
            # DEPARTAMENTALES
            'dep': 'Departamental',
            'dep1': 'Departamental I',
            'dep2': 'Departamental II',
            'dep3': 'Departamental III',
        
            # LABORATORIOS
            'poli': 'Laboratorios Polivalentes',
            'lab1': 'Laboratorios Polivalentes I',
            'lab2': 'Laboratorios Polivalentes II',
            'lab3': 'Laboratorios Polivalentes III',
            'lab4': 'Laboratorios IV',
        
            # GESTION
            'gest': 'Gestión',
        
            # ALCORCON
        
            # ARANJUEZ
            'pavi': 'Antiguo Cuartel De Pavía',
            'maro': 'Maestro Rodrigo',
            'luca': 'Lucas Jordán',
            'gobe': 'Casa Gobernador',
            'farn': 'farn',  # ??
            'infa': 'infa',  # ??
            'joro': 'joro',  # ??
        
            # FUENLABRADA
            'rest': 'rest',  # ???
            'acom': 'acom',  # ??
        
            # MADRID CENTRO
            'quin': 'QUINTANA AULARIO',
            # MADRID VICALVARO
            'cui1': 'cui1',  # ??
        
            # MOSTOLES
            'cat': 'Centro De Apoyo Tecnológico - Cat',
            'rec2': 'Rectorado II',  # ??
            'rct2': 'Rectorado II',  # ??
            'rect': 'Rectorado',  # ??
            'ampl': 'ampl'  # ??
        }
        df.replace({'edificio': Edificio}, inplace=True)
        
        df['num_aula'] = df.aula.apply(lambda x: num_aula(x))
        df.num_aula = df[['edificio', 'num_aula']].apply(
            lambda x: aulas_pavia(x['num_aula']) if x['edificio'] == 'Antiguo Cuartel De Pavía' else x['num_aula'], axis=1)
        df.num_aula = df[['edificio', 'num_aula']].apply(
            lambda x: aulas_maro(x['num_aula']) if x['edificio'] == 'Maestro Rodrigo' else x['num_aula'], axis=1)
        df.num_aula = df[['edificio', 'num_aula']].apply(
            lambda x: aulas_lucas(x['num_aula']) if x['edificio'] == 'Lucas Jordán' else x['num_aula'], axis=1)
        df.num_aula = df[['edificio', 'num_aula']].apply(
            lambda x: aulas_gobernador(x['num_aula']) if x['edificio'] == 'Casa Gobernador' else x['num_aula'], axis=1)
        df.num_aula = df[['edificio', 'num_aula', 'campus']].apply(
            lambda x: aulas_biblioteca_mo(x['num_aula']) if (x['edificio'] == 'Biblioteca' and x['campus'] == 'MOSTOLES') else
            x['num_aula'], axis=1)
        df.num_aula = df[['edificio', 'num_aula', 'campus']].apply(lambda x: aulas_biblioteca_fo(x['num_aula']) if (
                x['edificio'] == 'Biblioteca' and x['campus'] == 'FUENLABRADA') else x['num_aula'], axis=1)
        df.num_aula = df[['edificio', 'num_aula', 'campus']].apply(lambda x: aulas_biblioteca_fo(x['num_aula']) if (
                x['edificio'] == 'Biblioteca' and x['campus'] == 'FUENLABRADA') else x['num_aula'], axis=1)
        df.edificio = df[['edificio', 'campus']].apply(lambda x: aulas_madrid_vicalvaro(x['edificio']) if (
                x['edificio'] == 'Aulario I' and x['campus'] == 'MADRID - VICALVARO') else x['edificio'], axis=1)
        df.num_aula = df[['edificio','num_aula', 'campus']].apply(lambda x: aulas_gestion_vicalvaro(x['num_aula']) if
        (x['edificio']=='Gestión' and x['campus']=='MADRID - VICALVARO') else x['num_aula'], axis=1)
        df.drop('aula', axis=1, inplace=True)
        aulas = pd.read_csv('../data/aulas_name_ref.csv')
        df = pd.merge(df, aulas,  how='left', left_on=['campus','edificio', 'num_aula'],
                         right_on = ['CAMPUS','EDIFICIO', 'num_aula'])
        #Ivan
        df.drop(['EDIFICIO','CAMPUS','Unnamed: 0'],axis=1,inplace=True)
        df.rename({'enrolment_year':'enrolmentyear','num_aula':'numeroaula','AULA':'aula'},axis=1,inplace=True)
        return df
    
    def aplications(self,df):
        df.timecreated = pd.to_datetime(df.timecreated,infer_datetime_format=True)
        df.id = df.id.astype(str)
        df.eventid = df.eventid.astype('category')
        df.computer = df.computer.astype(str)
        df.accountname = df.accountname.astype(str)
        df.subjectdomainname = df.subjectdomainname.astype(str)
        df.logonid = df.logonid.astype('category')
        df.shortcut = df.shortcut.astype(str)
        df['year'] = df['timecreated'].dt.year
        df['month'] = df['timecreated'].dt.month
        df['dow'] = df['timecreated'].dt.strftime('%A')
        df['day'] = df['timecreated'].dt.day
        df['hour'] = df['timecreated'].dt.hour
        #TODO: Cambiar esto para leer de lA VISTA
        ref = pd.read_csv('C:/Users/i100v/PycharmProjects/TFM/data/token_anyarray.csv')
        ref = ref['apps reservadas'].tolist()
        software_lower = []
        for value in ref:
            software_lower.append(str(value).lower())
        df['processname'] = df.processname.apply(lambda x: rename_software(x,software_lower))
        return df

    def usersesion_format(self,df):
        df = df.groupby(by=['UserName', 'DomainName', 'ServerName', 'SessionId', 'ClientName', 'ClientIpAddress',
                                  'DeviceType', 'ConnectionStatus', 'ConnectorBandwidth', 'ConnectorLatency',
                                  'ConnectorType', 'ConnectorVersion', 'WebsiteName', 'OperatingSystem', 'ServicePack'],
                              as_index=False).agg({'TimeCreated': ['min', 'max']})

        df.columns = list(map(''.join, df.columns.values))
        df.columns = ['UserName', 'DomainName', 'ServerName', 'SessionId', 'ClientName', 'ClientIpAddress', 'DeviceType',
                      'ConnectionStatus', 'ConnectorBandwidth', 'ConnectorLatency', 'ConnectorType', 'ConnectorVersion',
                      'WebsiteName', 'OperatingSystem', 'ServicePack', 'StartDate', 'EndDate']
        df.StartDate = pd.to_datetime(df.StartDate,infer_datetime_format=True)
        df.EndDate = pd.to_datetime(df.EndDate,infer_datetime_format=True)
        df['Duration'] = (df.EndDate - df.StartDate)
        df['Duration'] = df.Duration.apply(lambda x: int(x.seconds/60))
        df = df[['StartDate', 'EndDate','Duration','UserName', 'DomainName', 'ServerName', 'SessionId', 'ClientName',
                 'ClientIpAddress', 'DeviceType', 'ConnectionStatus', 'ConnectorBandwidth', 'ConnectorLatency',
                 'ConnectorType', 'ConnectorVersion', 'WebsiteName', 'OperatingSystem', 'ServicePack']]
        return df

    def get_csv_tables(self,df):
        # Get aulas
        aulas = df.groupby(['CAMPUS', 'EDIFICIO', 'AULA'], as_index=False).size()
        aulas.drop(aulas[(aulas['size'] == 0)].index, inplace=True)
        aulas.drop('size', inplace=True, axis=1)
        aulas['num_aula'] = aulas.AULA.apply(lambda x: num_aula(x))
        aulas.to_csv('../data/temporary/aulas.csv')
