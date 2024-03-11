import requests
from pprint import pprint
import pandas as pd
import config

def search_org(x):
    search = requests.get(f'https://supertripper.pipedrive.com/api/v1/itemSearch/?api_token={config.api_pipe}'
                      f'&term={x}&item_types=organization&fields=name')
    search = search.json()
    print(search)
    # l_name=[]
    # l_org_id=[]
    # l_org_name=[]
    # l_org_label=[]
    # l_org_act_count=[]
    # l_org_score=[]
    # l_own_id=[]
    # if len(search['data']['items']) == 0:
    #     l_name=(x)
    #     l_org_score=("pas trouvé")
    #     l_org_act_count=("")
    #     l_org_label=("")
    #     l_org_name=("")
    #     l_org_id=("")
    #     l_own_id=("")
    # elif len(search['data']['items']) == 1:
    #     for i in range (len(search["data"]['items'])):
    #         org_id = search['data']['items'][i]['item']["id"]
    #         org_name = search['data']['items'][i]['item']["name"]
    #         org_score = search['data']['items'][i]["result_score"]
    #         try:
    #             own_id = search['data']['items'][i]["owner"]['id']
    #         except:
    #             own_id = ""
    #         orga = requests.get(f'https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.api_pipe}')
    #         orga = orga.json()
    #
    #         org_label_id = (orga['data']["label"])
    #         listlabel = pd.read_csv('../stats_sdr/csv/keep/label.csv')
    #         try:
    #             org_label = listlabel.loc[listlabel['id'] == org_label_id, 'label'].values[0]
    #         except:
    #             org_label = ""
    #         org_act_count = (orga['data']["activities_count"])
    #
    #     l_name=(x)
    #     l_org_id=(org_id)
    #     l_org_name=(org_name)
    #     l_org_label=(org_label)
    #     l_org_act_count=(org_act_count)
    #     l_org_score=(org_score)
    #     l_own_id=(own_id)
    # result = [l_name,l_org_id,l_org_name,l_org_label,l_org_act_count,l_org_score,l_own_id]
    # return result

    # df = pd.DataFrame(list(zip(l_name, l_org_id, l_org_name,l_org_label,l_org_act_count,l_org_score)),
    #                   columns=[ 'query','org_name', 'org_id', 'org_label', 'org_act_count', 'org_score'])
    # df.to_csv(fr"C:\Users\Patrick\PycharmProjects\pipedrive\csv\res\res_{filename}")

def search_pers(pers_name):
    search = requests.get(f'https://supertripper.pipedrive.com/api/v1/itemSearch/?api_token={config.api_pipe}'
                      f'&term={pers_name}&item_types=person&fields=name')
    search = search.json()
    pprint(search)
    for i in range (len(search["data"]['items'])):
        org_id = search['data']['items'][i]['item']["id"]
        org_name = search['data']['items'][i]['item']["name"]

        orga = requests.get(f'https://supertripper.pipedrive.com/api/v1/persons/{org_id}/?api_token={config.api_pipe}')
        orga = orga.json()

        org_label_id = (orga['data']["label"])
        listlabel = pd.read_csv('../stats_sdr/csv/keep/label.csv')
        try:
            org_label = listlabel.loc[listlabel['id'] == org_label_id, 'label'].values[0]
        except:
            org_label = "no_label"

        org_act_count = (orga['data']["activities_count"])
        print(org_id,org_name,org_label,org_act_count)

def filter():
    search = requests.get(
        f'https://supertripper.pipedrive.com/api/v1/activities/?api_token={config.api_pipe}&filter_id=1055')
    search = search.json()
    print(search)
