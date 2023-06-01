import requests
import pandas as pd
import config
from datetime import datetime


def get_user():
    url = "https://public-api.ringover.com/v2/users"

    payload = {}
    headers = {
        'Authorization': config.api_ringo
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    response = (response.json())

    user_list = (len(response["list"]))
    l_name, l_user = [],[]
    for i in range(user_list):
        try:
            l_name.append(response["list"][i]['concat_name'])
            l_user.append(response["list"][i]['user_id'])
        except:
            pass
    df = pd.DataFrame(list(zip(l_name,l_user)), columns = ['Name', 'ID'])
    print(df)

def get_call_api():
    start ="2023-02-13T00:00:00.53Z"
    end ="2023-02-17T00:00:00.53Z"
    limit = 100
    call_type = "OUT"
    groupe = "SDR" #SDR
    filter = "ADVANCED"
    url = f"https://public-api.ringover.com/v2/calls?filter={filter}&start_date={start}&end_date={end}&limit_count={limit}&call_type={call_type}"

    payload = {}
    headers = {
        'Authorization': config.api_ringo
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = (response.json())
    # call_list = (len(response["call_list"]))
    # print(call_list)
    # l_direction,l_name, l_user = [],[],[]
    # for i in range(call_list):
    #     try:
    #         l_direction.append(response['call_list'][i]["direction"])
    #         l_user.append(response['call_list'][i]["user"]['concat_name'])
    #         print(response['call_list'][i]["user"]['concat_name'])
    #     except:
    #         pass
    #
    # df = pd.DataFrame(list(zip(l_name,l_user)), columns = ['Name', 'ID'])
    # print(df)

def merge_rdv_deal():
    df_rdv = pd.read_csv('csv/pipedrive/rdv.csv')
    df_rdv['Activité - Date d’ajout'] = pd.to_datetime(df_rdv['Activité - Date d’ajout'])
    df_rdv = df_rdv.sort_values('Activité - Date d’ajout', ascending=False)
    df_rdv = df_rdv.drop_duplicates('Organisation - Nom', keep='first').reset_index()
    df_rdv.to_csv('csv/pipedrive/rdv-test.csv')
    #
    df_deal = pd.read_csv('csv/pipedrive/deal.csv')
    df_deal['Affaire - Affaire créée'] = pd.to_datetime(df_deal['Affaire - Affaire créée'])
    df_deal = df_deal.sort_values('Affaire - Affaire créée', ascending=False)
    df_deal = df_deal.drop_duplicates('Organisation - ID', keep='first').reset_index()

    l_m_deal_p10, l_m_deal_m10,l_stat_deal = [],[],[]
    l_m_offre_p10, l_m_offre_m10, l_m_offre_nc= [],[],[]
    l_won,l_open,l_lost = [],[],[]
    l_value_deal,l_when_deal,l_o_lost,l_when_won = [],[],[],[]
    l_when_lost = []
    for i in range(len(df_rdv)):
        id_rdv = (df_rdv['Organisation - ID'][i])
        r_many = (df_rdv['Personne - 🚨 RDV obtenu > Volume mensuel estimé'][i])
        try:
            if pd.isna(float(r_many)) is True:
                r_many = "0"
        except:
            pass
        try:
            # Class RDV by value estimate
            if r_many.startswith('0') is True or r_many == "":
                l_m_offre_p10.append(0)
                l_m_offre_m10.append(0)
                l_m_offre_nc.append(1)
            elif r_many.startswith('1') is True or r_many.startswith('2') is True:
                l_m_offre_p10.append(0)
                l_m_offre_m10.append(1)
                l_m_offre_nc.append(0)
            elif r_many.startswith('3') is True or r_many.startswith('4') is True or r_many.startswith('5') == True:
                l_m_offre_p10.append(1)
                l_m_offre_m10.append(0)
                l_m_offre_nc.append(0)
            else:
                l_m_offre_p10.append(0)
                l_m_offre_m10.append(0)
                l_m_offre_nc.append(1)
        except:
            l_m_offre_p10.append("")
            l_m_offre_m10.append("")
            l_m_offre_nc.append("")
        try:
            when_deal = df_deal.loc[df_deal['Organisation - ID'] == id_rdv, "Affaire - Affaire créée"].values[0]
            when_deal = datetime.strptime(when_deal, '%Y-%m-%d %H:%M:%S')
            when_deal = (str(when_deal)[:10])
        except:
            when_deal = ""

        try:
            when_lost = df_deal.loc[df_deal['Organisation - ID'] == id_rdv, "Affaire - Heure de l'échec"].values[0]
            when_lost = datetime.strptime(when_lost, '%Y-%m-%d %H:%M:%S')
            when_lost = (str(when_lost)[:10])
        except:
            when_lost = ""

        try:
            o_many = df_deal.loc[df_deal['Organisation - ID'] == id_rdv, "Affaire - Valeur"].values[0]
            if o_many > 9999:
                m_deal_p10 = 1
                m_deal_m10 = 0
            else:
                m_deal_p10 = 0
                m_deal_m10 = 1

        except:
            o_many = 0
            m_deal_p10 = 0
            m_deal_m10 = 0

        try:
            o_lost = df_deal.loc[df_deal['Organisation - ID'] == id_rdv, "Affaire - Raison de la perte"].values[0]
            print(o_lost)
        except:
            o_lost = ""

        try:
            when_won = df_deal.loc[df_deal['Organisation - ID'] == id_rdv, "Affaire - Date de conclusion"].values[0]
            when_won = datetime.strptime(when_won, '%Y-%m-%d %H:%M:%S')
            when_won = (str(when_won)[:10])
        except:
            when_won = ""

        try:
            is_deal = df_deal.loc[df_deal['Organisation - ID'] == id_rdv, "Affaire - Statut"].values[0]
            if is_deal == "Gagnée" :
                won = 1
                opened = 0
                lost = 0
            elif is_deal == "En cours":
                won = 0
                opened = 1
                lost = 0
            elif is_deal == "Perdue":
                won = 0
                opened = 0
                lost = 1
            else:
                won = 0
                opened = 0
                lost = 0
        except:
            won = 0
            opened = 0
            lost = 0
            is_deal = 0
        l_won.append(won)
        l_open.append(opened)
        l_lost.append(lost)
        l_stat_deal.append(is_deal)
        l_when_deal.append(when_deal)
        l_o_lost.append(o_lost)
        l_m_deal_p10.append(m_deal_p10)
        l_m_deal_m10.append(m_deal_m10)
        l_value_deal.append(o_many)
        l_when_won.append(when_won)
        l_when_lost.append(when_lost)

    df_rdv['class_rdv_nc'] = l_m_offre_nc
    df_rdv['class_rdv_m10'] = l_m_offre_m10
    df_rdv['class_rdv_p10'] = l_m_offre_p10
    df_rdv['when_deal'] = l_when_deal
    df_rdv['when_won'] = l_when_won
    df_rdv['when_lost'] = l_when_lost
    df_rdv['why_lost'] = l_o_lost
    df_rdv['class_deal_m10'] = l_m_deal_m10
    df_rdv['class_deal_p10'] = l_m_deal_p10
    df_rdv['deal_won'] = l_won
    df_rdv['deal_open'] = l_open
    df_rdv['deal_lost'] = l_lost
    df_rdv['stat'] = l_stat_deal
    df_rdv['value'] = l_value_deal
    df_rdv = df_rdv.drop(columns="index")
    df_rdv.to_csv('csv/res/rdv2.csv')


def clean_csv():
    
    # GET STATS FROM CALL.CSV
    
    df_call = pd.read_csv('../stats/csv/ringover/call.csv',delimiter=";")
    df_call['UserName'] = df_call['UserName'].replace('Alisée Delon ','Alisée Delon')

    l_date_call,l_nb_call = [],[]
    for i in range(len(df_call)):
        date_call = (df_call['StartTime'][i][:10])
        l_date_call.append(date_call)
        l_nb_call = 1
    df_call['date'] = l_date_call
    df_call['nb_call'] = l_nb_call
    df_call_sum = df_call.groupby(['date','UserName']).sum(numeric_only = True).reset_index()

    df_call_sum.to_csv("csv/res/df_call_sum.csv")


  # GET STATS FROM RDV.CSV
    df_rdv = pd.read_csv('../stats/csv/res/rdv2.csv')
    df_rdv = df_rdv.rename(columns={'Activité - Créateur': 'UserName'})
    df_rdv['UserName'] = df_rdv['UserName'].replace('Thomas','Thomas  JUILLARD')
    l_sdr = ['Alisée Delon','Mariem Landolsi','Sabrina Methlouthi','Thomas  JUILLARD']

    #### CALL SDR
    df_rdv_call = df_rdv.loc[(df_rdv['UserName'].isin(l_sdr)) & (df_rdv['Personne - 🚨 Source du lead (Obligatoire)'] != "Market (Formulaire)")].reset_index()
    l_date_rdv, l_nb_rdv = [], []
    for i in range(len(df_rdv_call)):
        date_rdv = (df_rdv_call['Activité - Date d’ajout'][i][:10])
        user = (df_rdv_call['UserName'][i])
        date_rdv = datetime.strptime(date_rdv, '%Y-%m-%d')
        date_rdv= (str(date_rdv)[:10])
        l_date_rdv.append(date_rdv)
        l_nb_rdv = 1

    df_rdv_call['date'] = l_date_rdv
    df_rdv_call['nb_rdv'] = l_nb_rdv
    df_rdv_call_sum = df_rdv_call.groupby(['date', 'UserName']).sum(numeric_only = True).reset_index()

    df_rdv_call_sum.to_csv("csv/res/df_rdv_call_sum.csv")
    l_jour = []

    # RECUP RDV OBTENU BY DATE
    l_rdv,l_ratio,l_prez,l_week = [],[],[],[]
    l_class_rdv_nc,l_class_rdv_m10,l_class_rdv_p10,l_class_deal_m10,l_class_deal_p10 = [],[],[],[],[]
    l_won,l_lost,l_opened = [],[],[]

    start = (df_call_sum['date'][0]).replace("-","")
    end = (df_call_sum['date'][len(df_call_sum)-1]).replace("-","")
    for v in range(len(df_call_sum)):

        # comparison btw df_call & df_sum
        date = df_call_sum['date'][v]
        # Tranform date en str puis transform en n° du jour
        date_day = datetime.strptime(date, '%Y-%m-%d')

        n_week_day = (date_day.weekday())
        n_year_day = (date_day.strftime("%Y"))
        week_number_new = date_day.isocalendar().week
        week_year = f'{n_year_day}-W{week_number_new}'
        # Stock des données récoltées dans des listes
        l_week.append(str(week_year))
        l_jour.append(n_week_day)

        user = df_call_sum['UserName'][v]
        prez = 1
        try:
            find_nb_rdv = (df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'nb_rdv'].values[0])

        except:
            find_nb_rdv = 0
        try:
            find_class_rdv_nc = int(df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'class_rdv_nc'].values[0])
        except:
            find_class_rdv_nc = 0
        try:
            find_class_rdv_m10 = int(df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'class_rdv_m10'].values[0])
        except:
            find_class_rdv_m10 = 0
        try:
            find_class_rdv_p10 = int(df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'class_rdv_p10'].values[0])
        except:
            find_class_rdv_p10 = 0
        try:
            find_class_deal_m10 = int(df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'class_deal_m10'].values[0])
        except:
            find_class_deal_m10 = 0
        try:
            find_class_deal_p10 = int(df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'class_deal_p10'].values[0])
        except:
            find_class_deal_p10 = 0
        try:
            find_deal_lost = int(df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'deal_lost'].values[0])
        except:
            find_deal_lost = 0
        try:
            find_deal_won = int(df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'deal_won'].values[0])
        except:
            find_deal_won = 0
        try:
            find_deal_opened = int(df_rdv_call_sum.loc[(df_rdv_call_sum['UserName'] == user)&(df_rdv_call_sum['date'] == date), 'deal_open'].values[0])
        except:
            find_deal_opened = 0


        l_rdv.append(find_nb_rdv)
        l_prez.append(prez)

        l_class_rdv_nc.append(find_class_rdv_nc)
        l_class_rdv_m10.append(find_class_rdv_m10)
        l_class_rdv_p10.append(find_class_rdv_p10)
        l_class_deal_m10.append(find_class_deal_m10)
        l_class_deal_p10.append(find_class_deal_p10)

        l_opened.append(find_deal_opened)
        l_won.append(find_deal_won)
        l_lost.append(find_deal_lost)

    # Ajout des listes dans le DF
    df_call_sum['presence_sdr'] = l_prez
    df_call_sum['rdv_obtenu'] = l_rdv
    df_call_sum['rdv_vol_nc'] = l_class_rdv_nc
    df_call_sum['rdv_vol_m10'] = l_class_rdv_m10

    df_call_sum['rdv_vol_m10'] = pd.to_numeric(l_class_rdv_m10)
    df_call_sum['rdv_vol_m10'] = pd.to_numeric(df_call_sum['rdv_vol_m10'])

    df_call_sum['rdv_vol_p10'] = l_class_rdv_p10
    df_call_sum['rdv_deal_m10'] = l_class_deal_m10
    df_call_sum['rdv_deal_p10'] = l_class_deal_p10
    df_call_sum['jour'] = l_jour
    df_call_sum['week'] = l_week

    df_call_sum['opened'] = l_opened
    df_call_sum['lost'] = l_lost
    df_call_sum['won'] = l_won

    # Transfor n° du jour par jour écrit en toute lettre
    df_call_sum = df_call_sum[["jour","week",'date','UserName',"presence_sdr",'nb_call','rdv_obtenu','rdv_vol_nc','rdv_vol_m10','rdv_vol_p10','rdv_deal_m10','rdv_deal_p10','opened','won','lost']]
    l_day = [('Lundi', 0), ('Mardi', 1), ('Mercredi', 2), ('Jeudi', 3), ('Vendredi', 4)]

    for a,b in l_day:
        df_call_sum["jour"] = df_call_sum["jour"].replace(b,a)

    # creation du csv
    df_call_sum.to_csv(f"csv/res/indiv_call_result.csv")

    # Calcul du global hebdo
    resume = (df_call_sum.groupby(['week','date','jour']).sum(numeric_only=True)).reset_index()

    resume.to_csv(f"csv/res/result_call.csv",index=False)

def demos():
    #### DEMOS
    df_rdv = pd.read_csv('../stats/csv/res/rdv2.csv')
    df_rdv = df_rdv.rename(columns={'Activité - Créateur': 'UserName'})
    df_rdv['UserName'] = df_rdv['UserName'].replace('Thomas', 'Thomas  JUILLARD')
    l_week,l_jour = [],[]
    df_demo = df_rdv
    for i in range(len(df_demo)):
        source = (df_demo['Personne - 🚨 Source du lead (Obligatoire)'][i])
        value = (df_demo['Personne - 🚨 RDV obtenu > Volume mensuel estimé'][i])
        date = df_demo['Activité - Date d’ajout'][i][:10]

        # Tranform date en str puis transform en n° du jour
        date_day = datetime.strptime(date, '%Y-%m-%d')

        n_week_day = (date_day.weekday())
        n_year_day = (date_day.strftime("%Y"))
        week_number_new = date_day.isocalendar().week
        week_year = f'{n_year_day}-W{week_number_new}'
        # Stock des données récoltées dans des listes
        l_week.append(str(week_year))
        l_jour.append(n_week_day)

    df_demo['jour'] = l_jour
    df_demo['week'] = l_week
    l_day = [('Lundi', 0), ('Mardi', 1), ('Mercredi', 2), ('Jeudi', 3), ('Vendredi', 4)]

    for a, b in l_day:
        df_demo["jour"] = df_demo["jour"].replace(b, a)

    df_demo.to_csv(f"csv/res/rdv_demo.csv", index=False)

def for_sales():
    df = pd.read_csv('csv/pipedrive/sales.csv')
    df = df.fillna(0)
    # Convertir la colonne date en datetime si ce n'est pas déjà le cas
    df['Affaire - Affaire créée'] = pd.to_datetime(df['Affaire - Affaire créée'])
    df["Affaire - Heure de l'échec"] = pd.to_datetime(df["Affaire - Heure de l'échec"])
    df["Affaire - Date de conclusion"] = pd.to_datetime(df["Affaire - Date de conclusion"])

    # Trier le DataFrame par date
    df = df.sort_values('Affaire - Affaire créée')

    # Grouper par entreprise et obtenir la dernière affaire pour chaque entreprise
    df_derniere_affaire = df.groupby('Affaire - Organisation').last().reset_index()
    df_derniere_affaire.to_csv(f"csv/pipedrive/deals_sales.csv", index=False)

def update_sheet():
    df_3 = pd.read_csv("csv/res/rdv_demo.csv",index_col=False)
    df_4= pd.read_csv("csv/pipedrive/deals_sales.csv",index_col=False)
    # lem = pd.read_csv("csv/lemlist/campaigns-export.csv",index_col=False)
    from oauth2client.service_account import ServiceAccountCredentials
    import gspread
    from gspread_pandas import Spread
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds/creds_sheet.json", scope)

    client = gspread.authorize(creds)

    today = datetime.now().strftime('%d/%m/%Y - %H:%M')
    today = (today)

    # gspread_pandas pour ajouter le df dans le sheet
    s = Spread('stats SDR 22-23')
    s.df_to_sheet(df_3, sheet='Démos', start='A1',replace=True)
    s.df_to_sheet(df_4, sheet='Deals', start='A1',replace=True)
    # s.df_to_sheet(lem, sheet='Lemlist', start='A1',replace=True)


    # just gspread for update a cell
    s_demo = client.open('stats SDR 22-23').worksheet('Démos')

    s_demo.update_cell(1,1,f'Updated : {today}')

def activities():
    import requests
    import json

    api_token = 'YOUR_API_TOKEN'
    filter_id = 1107
    start = 0
    limit = 1000
    url = f'https://companydomain.pipedrive.com/api/v1/activities?api_token={config.api_pipe}&start={start}&limit={limit}&filter_id={filter_id}'

    # initialise une liste pour stocker toutes les activités
    all_activities = []

    while True:
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200:
            all_activities.extend(data['data'])

            # check if there is additional data
            if data['additional_data']['pagination']['more_items_in_collection']:
                start += limit
                url = f'https://companydomain.pipedrive.com/api/v1/activities?api_token={api_token}&start={start}&limit={limit}&filter_id={filter_id}'
            else:
                break
        else:
            print('Error:', data['error'])
            break

    print('Nombre total d\'activités récupérées:', len(all_activities))



