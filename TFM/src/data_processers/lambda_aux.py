def get_enrolment_year(x):
    enrolment_year_list = x.split(".")
    if len(enrolment_year_list) == 3:
        try:
            return int(enrolment_year_list[2])
        except:
            return 0
            # return np.NaN
    else:
        return 0
        # return np.NaN


def get_campus(x):
    server_name_list = x.split("-")
    Campus = ['mo', 'qo', 'zo', 'fo', 'vo', 'ao']
    if (len(server_name_list) == 4) and (server_name_list[0] in Campus):
        return server_name_list[0]
    else:
        return np.NaN


def get_lab(x):
    server_name_list = x.split("-")
    Campus = ['mo', 'qo', 'zo', 'fo', 'vo', 'ao']
    if (len(server_name_list) == 4) and (server_name_list[0] in Campus):
        return server_name_list[1]
    else:
        return np.NaN


def get_aula(x):
    server_name_list = x.split("-")
    Campus = ['mo', 'qo', 'zo', 'fo', 'vo', 'ao']
    if (len(server_name_list) == 4) and (server_name_list[0] in Campus):
        return server_name_list[2]
    else:
        return np.NaN


def get_computer(x):
    server_name_list = x.split("-")
    Campus = ['mo', 'qo', 'zo', 'fo', 'vo', 'ao']
    if (len(server_name_list) == 4) and (server_name_list[0] in Campus):
        return server_name_list[3].split(".")[0]
    else:
        return '0'


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
    for index, row in df.iterrows():
        table = list()
        ini = datetime.datetime.strptime(row['Fecha_Inicio'], "%d-%m-%Y")
        fin = datetime.datetime.strptime(row['Fecha_Fin'], "%d-%m-%Y")
        iters = (fin - ini).days // 7
        for i in range(iters):
            row_aux = row.copy()
            row_aux['Fecha_Inicio'] = ini + datetime.timedelta(days=7 * i)
            row_aux['Fecha_Fin'] = ini + datetime.timedelta(days=7 * i)
            row_aux['Tipo'] = 'Puntual'
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


def drop_whitespace(x):
    return x.rstrip()

def aulas_gestion_vicalvaro(x):
    return "G"+x