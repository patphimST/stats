import pandas as pd
import requests
import config
from datetime import datetime, timedelta

def get_deal():
    url = 'https://api.pipedrive.com/v1/deals'

    filter = [(1143,"perdus"),(1116,'gagnés'),(1142,'en cours')]

    for f,fs in filter:
        params = {
            'api_token': config.api_pipe,
            'filter_id': f,
            'limit' : 400
        }

        response = requests.get(url, params=params)
        data = response.json()["data"]

        name_orga = [data[i]['org_id']['name'] for i in range(len(data))]
        id_orga = [data[i]['org_id']['value'] for i in range(len(data))]
        owner = [data[i]['user_id']['name'] for i in range(len(data))]
        value = [data[i]['value'] for i in range(len(data))]
        add_time = [data[i]['add_time'] for i in range(len(data))]
        next_step_time = [data[i]['next_activity_date'] for i in range(len(data))]

        df = pd.DataFrame(list(zip(id_orga,name_orga,value,owner,add_time,next_step_time)), columns=['id_orga','name_orga','value','owner','add_time','next_step_time'])
        print("old",len(df))
        df = df.sort_values('add_time', ascending=False).drop_duplicates(['id_orga'])
        print("new",len(df))

        df.to_csv(f'csv/pipedrive/api/deals_{fs}.csv')
        print(f"Export des deals {fs} en .csv : OK")

def item_search():
    df = pd.read_csv('csv/pipedrive/Port.csv')
    l_id = []
    for i in range(len(df)):
        term = df['NAME'][i]
        url = f"https://api.pipedrive.com/v1/itemSearch/field?term={term}&field_type=organizationField&field_key=name&return_item_ids=true&limit=5&api_token={config.api_pipe}"

        payload = {}
        headers = {
            'Accept': 'application/json',
            'Cookie': '__cf_bm=pS5wO3R9HHjEQce5KbjkC7sk7hUoCx0hDUEQ7ZPQ9kI-1679592359-0-AQRy3ozejAiCN8N56MwmB3uI3ZV8Rgt7B8RJUcJXYxxDRAmBkAVNQJR31kZ3MA/WKQLuPUfsLP3FHJawyksXMEU='
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()["data"]
        try:
            id = (data[0]['id'])
        except:
            id = ""
        l_id.append(id)
    df['id_org'] = l_id
    df.to_csv('csv/pipedrive/Port.csv')

def chiffre_hebdo():
    df_rdv = pd.read_csv('csv/pipedrive/rdv_week.csv')
    df_rdv = df_rdv.fillna('vide')
    l_val = []
    for i in range (len(df_rdv)):
        val = df_rdv['Personne - 🚨 RDV obtenu > Volume mensuel estimé'][i]
        if val.startswith('0') or val.startswith('1') or val.startswith('2'):
            val = "<10"
        elif val.startswith('3') or val.startswith('4') or val.startswith('5'):
            val = ">10"
        else:
            val = "vide"
        l_val.append(val)
    df_rdv['val'] = l_val
    df_rdv = df_rdv.groupby(['Personne - 🚨 Source du lead (Obligatoire)','val','Organisation - Nom']).count()
    df_rdv = (df_rdv['Organisation - ID'])
    df_rdv.to_excel(f'csv/pipedrive/count_df_rdv.xlsx')

    # df_rdv = df_rdv.groupby(['Personne - 🚨 Source du lead (Obligatoire)','val']).count()
    # print(df_rdv['Organisation - ID'])
    print("*****************************")

    df_deal = pd.read_csv('csv/pipedrive/deal_week.csv')
    df_deal = df_deal.fillna('vide')
    l_val = []
    for i in range (len(df_deal)):
        val = df_deal['Affaire - Valeur'][i]
        if val <10000:
            val = "<10"
        elif val >= 10000:
            val = ">10"

        l_val.append(val)
    df_deal['val'] = l_val
    df_deal = df_deal.groupby(['val','Affaire - Organisation','Personne - 🚨 Source du lead (Obligatoire)','Affaire - Valeur']).count().reset_index()
    # print(df_deal['Organisation - ID'])
    # df_deal = df_deal.groupby(['val']).count()
    # df_deal = (df_deal['Organisation - ID'])
    df_deal.to_excel(f'csv/pipedrive/count_df_deal.xlsx')
    l_so = [("Market (Formulaire)","Démo"),("Prospection externe","Prospection Ext."),("Chasse (Sales)","Sales"),("Market (Email)","Email"),("Market (Chasse SDR)","Chasse SDR"),("Market (Call SDR)","Call SDR"),("Market (Publi)","Publi"),("Réseau Perso","Réseau Perso"),]
    sum_deal_m10 = []
    sum_deal_p10 = []
    for i in range(len(df_deal)):
        source = df_deal['Personne - 🚨 Source du lead (Obligatoire)'][i]
        val = df_deal['val'][i]
        valeur = df_deal["Affaire - Valeur"][i]

        for lsa, lsb in l_so:
            if source == lsa:
                source2 = lsb
                if val == ">10" and (source2 != "Sales" and source2 != "Prospection Ext."and source2 != "Réseau Perso" ):
                    sum_deal_p10.append(valeur)
                    print(df_deal['Affaire - Organisation'][i],f'({source2} - {valeur})')
                elif val == "<10" and (source2 != "Sales" and source2 != "Prospection Ext."and source2 != "Réseau Perso" ):
                    sum_deal_m10.append(valeur)
                    print(df_deal['Affaire - Organisation'][i],f'({source2} - {valeur})')
    print("Deal <10 :",sum(sum_deal_m10))
    print("Deal >10 :",sum(sum_deal_p10))
