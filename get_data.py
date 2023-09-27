import numpy as np
import requests
import pandas as pd
import config
from datetime import datetime



# def merge_rdv_deal():
#     df_rdv = pd.read_csv('csv/pipedrive/rdv.csv')
#     df_rdv['Activité - Date d’ajout'] = pd.to_datetime(df_rdv['Activité - Date d’ajout'])
#     df_rdv = df_rdv.sort_values('Activité - Date d’ajout', ascending=False)
#     df_rdv = df_rdv.drop_duplicates('Organisation - Nom', keep='first').reset_index()
#
#     df_deal = pd.read_csv('csv/pipedrive/deal.csv')
#     df_deal['Affaire - Affaire créée'] = pd.to_datetime(df_deal['Affaire - Affaire créée'])
#     df_deal = df_deal.sort_values('Affaire - Affaire créée', ascending=False)
#     df_deal = df_deal.drop_duplicates('Organisation - ID', keep='first').reset_index()
#     df_deal.to_csv(f"csv/pipedrive/deals_clean.csv", index=False)

def demos():
    df_rdv = pd.read_csv('csv/pipedrive/rdv.csv')
    df_rdv['Organisation - Étiquette'] = df_rdv['Organisation - Étiquette'].fillna("-")
    df_rdv['Personne - 🚨 Volume mensuel estimé'] = df_rdv['Personne - 🚨 Volume mensuel estimé'].fillna("-")
    if "Activité - Date de création" in df_rdv.columns:
        # Renommer la colonne en "Date d'ajout"
        df_rdv.rename(columns={"Activité - Date de création": "Activité - Date d’ajout"}, inplace=True)
    df_rdv['year_week'] = pd.to_datetime(df_rdv['Activité - Date d’ajout']).dt.strftime("%yW%U")
    df_rdv = df_rdv.sort_values('Activité - Date d’ajout').drop_duplicates('Organisation - Nom', keep='last')
    df_rdv.to_csv(f"csv/pipedrive/rdv.csv", index=False)

    ahmed_regul = [130039,135482,139624,145550,138339,138353,138375,60063,139463,137700,140792,75961,138384,139562,139566,130601,136081,138608,147081,138575,137811]

    for a in ahmed_regul:
        masque = df_rdv['Organisation - ID'] == a
        df_rdv.loc[masque, 'Activité - Attribué à l’utilisateur'] = 'Ahmed NAFAI'

    df_deal = pd.read_csv("csv/pipedrive/deal.csv")

    df_deal = df_deal.sort_values('Affaire - Affaire créée').drop_duplicates('Affaire - Organisation', keep='last')

    result = df_rdv['Organisation - Nom'].isin(df_deal['Affaire - Organisation'])
    result = df_rdv.loc[result]
    result.to_csv("result.csv")

    df1 = pd.merge(df_rdv, df_deal, left_on='Organisation - Nom', right_on='Affaire - Organisation', how='left')

    # Supprimer la colonne "name" de df1 si nécessaire
    df1 = df1.drop('Affaire - Organisation', axis=1)

    df1["Activité - Date d’ajout"] = pd.to_datetime(df1["Activité - Date d’ajout"])
    df1["Affaire - Affaire créée"] = pd.to_datetime(df1["Affaire - Affaire créée"])

    # Ajout de la colonne "Statut"
    df1["Statut"] = ""
    df1.loc[df1["Affaire - Affaire créée"] < df1["Activité - Date d’ajout"], "Statut"] = "OLD"
    df1.loc[df1["Affaire - Affaire créée"] >= df1["Activité - Date d’ajout"], "Statut"] = "OK"
    df1.loc[df1["Affaire - Affaire créée"].isnull() | (df1["Affaire - Affaire créée"] == ""), "Statut"] = "NO"
    df1.to_csv('csv/pipedrive/rdv2.csv')


def update_sheet():
    df_3 = pd.read_csv("csv/pipedrive/rdv2.csv",index_col=False)
    # df_4= pd.read_csv("csv/pipedrive/deals_clean.csv",index_col=False)
    # df_6= pd.read_csv("csv/pipedrive/rdv&deal.csv",index_col=False)
    # df_5= pd.read_csv("csv/pipedrive/decouverte.csv",index_col=False)
    from oauth2client.service_account import ServiceAccountCredentials

    from gspread_pandas import Spread
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds/creds_sheet.json", scope)

    # client = gspread.authorize(creds)

    # today = datetime.now().strftime('%d/%m/%Y - %H:%M')
    # today = (today)

    # gspread_pandas pour ajouter le df dans le sheet
    s = Spread("stats Acquisition 22-23")
    s.df_to_sheet(df_3, sheet='Demos', start='A1',replace=True)
    # s.df_to_sheet(df_4, sheet='Deals', start='A1',replace=True)
    # s.df_to_sheet(df_5, sheet='Decouverte', start='A1',replace=True)
    # s.df_to_sheet(df_6, sheet='Demos&Deals', start='A1',replace=True)


