# Importer relevante biblioteker
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO


def check_at_smiley(lower, upper):

    errors = []

    # Definer en header, der bliver sendt med GET-request
    header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36"
        }

    # Sti til Arbejdstilsynets smiley-side
    url = "https://websmiley.at.dk/websmiley/advancedsearchform.aspx"

    # Send GET-request og hent Arbejdstilsynets smiley-side

    try:
        r = requests.get(url,headers=header,timeout=8)
        r.raise_for_status()
    except requests.exceptions.HTTPError as error:
        errors.append(error)


    # Hvis status er OK parser scriptet indholdet, henter formData og tilføjer det til et dictionary
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, "html.parser")

        viewstate = soup.select("#__VIEWSTATE")[0]['value']
        eventvalidation = soup.select("#__EVENTVALIDATION")[0]['value']

        formData = {
            '__EVENTVALIDATION': eventvalidation,
            '__VIEWSTATE': viewstate,
            '__EVENTTARGET': '',
            'companyName': 'Virksomhedens navn',
            'cvrNr': 'CVR-nr.',
            'pNr': 'P-nr.',
            'ddlBrancheGrupper':0,
            'ddlRegioner': 0,
            'ddlSmiley':0,
            'btnDownload': 'Download smileydata'   
        }


    # Send POST-request, hvor den netop hentede formdata sendes med
    try:
        r = requests.post(url, data=formData, headers=header)
        r.raise_for_status()
    except requests.exceptions.HTTPError as error:
        errors.append(error)
    try:
        # Konverter responsdata til en string og indlæs i en DataFrame
        # filtrer datasæt efter fynske virksomheder og røde smileyer
        text = r.text
        df = pd.read_csv(StringIO(text),sep=";",keep_default_na=False)
        new = df
        # Rens kolonnenavne for mellemrum og filtrer efter røde smileyer på fyn
        new.columns = new.columns.str.replace(" ","")
        new = new[(new["POSTNR"] > lower) & (new["POSTNR"] < upper) & (new["SMILEY"] == "rød")]

        # Fjern duplikerede rækker, sorter efter navn 
        # Lav ID-kolonne for det nye datasætaml alle IDs i en liste
        new = new.drop_duplicates()
        new = new.sort_values("NAVN")
        new["id"] = new[list(new.columns)].astype(str).sum(axis=1).map(hash)
        new = new.reset_index(drop=True)
        new_smiley_ids = list(dict.fromkeys(list(new["id"])))
    
    except:
        print("There was an error adding IDs to new dataset")

    # Forsøg at indlæse tidligere datasæt i en dataframe
    # og filtrer efter fynske virksomheder
    # Hvis der ikke findes et tomt datasæt, så opret en tom Datafr
    try:
        old = pd.read_csv("csv/at.csv",sep=";",keep_default_na=False)
        old.columns = old.columns.str.replace(" ","")
        old = old[(old["POSTNR"] > lower) & (old["POSTNR"] < upper) & (old["SMILEY"] == "rød")]

        old = old.drop_duplicates()
        old = old.sort_values("NAVN")
        old["id"] = old[list(old.columns)].astype(str).sum(axis=1).map(hash)
        old_smiley_ids = list(dict.fromkeys(list(old["id"])))
        old = old.reset_index(drop=True)

    except:
        old = pd.DataFrame(list())
        old_smiley_ids = list()

    try:
        # Opret tomme dataframes, hvor vi kan lægge henholdsvis tilføjede og fjernede smileyer
        added = pd.DataFrame()
        removed = pd.DataFrame()
        added_pnr = pd.DataFrame()
        removed_pnr = pd.DataFrame()
        

        changes_found = False
        

        # Loop igennem nye smileyer for at se, om der eksisterer noget i de nye, som ikke eksisterede i det gamle
        # Hvis der gør, betyder det, at der er tilføjet en smiley
        for hit in new_smiley_ids:
            if not hit in old_smiley_ids:
                new_row = new[new["id"] == hit]
                added = pd.concat([new_row, added])
                changes_found = True


        # Loop igennem nye smileyer for at se, om der eksisterer noget i de nye, som ikke eksisterede i det gamle
        # Hvis der gør, betyder det, at der er fjernet en smiley. 
        for hit in old_smiley_ids:
            if not hit in new_smiley_ids:
                new_row = old[old["id"] == hit]
                removed = pd.concat([new_row, removed])
                changes_found = True
        
        
        # Lav liste over p-numre i henholdsvis gammelt og nyt datasæt
        # Sammenhold dem - hvis der er tilføjede eller fjernede p-numre
        # samles de i et csv.   
        added_pnr_list = list()
        removed_pnr_list = list()
        
        if len(added) > 0:
            added_pnr_list = list(dict.fromkeys(list(added["PNR"])))
        if len(removed)>0:
            removed_pnr_list = list(dict.fromkeys(list(removed["PNR"])))
        
        for hit in added_pnr_list:
            if not hit in removed_pnr_list:
                new_row = added[added["PNR"] == hit]
                added_pnr = pd.concat([new_row, added_pnr])
                changes_found = True
                
        for hit in removed_pnr_list:
            if not hit in added_pnr_list:
                new_row = removed[removed["PNR"] == hit]
                removed_pnr = pd.concat([new_row, removed_pnr])
                changes_found = True
                
        if changes_found is True:
            df.to_csv("csv/at.csv",sep=";",index=False)

        added.to_csv("csv/added_at.csv",sep=";",index=False)
        removed.to_csv("csv/removed_at.csv",sep=";",index=False)
        
        added_pnr.to_csv("csv/added_pnr.csv", sep=";", index=False)
        removed_pnr.to_csv("csv/removed_pnr.csv", sep=";",index=False)
                        


    except:
        print("There was an error comparing new and old dataset")


# Kald funktionen
check_at_smiley(4999,6000)