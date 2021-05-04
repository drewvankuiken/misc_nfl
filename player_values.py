#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  3 15:44:53 2021

@author: dvankuiken
"""

import pandas as pd
from functools import reduce
import re
import pickle

def clean_table(pos):
    # spaces suck
    pos.columns = pos.columns.str.replace(' ','')
    # drop SF values
    if 'SFValue' in pos.columns:
        pos.drop(["SFValue","SFValue.1"],1, inplace = True)
    elif 'SuperFlex' in pos.columns:
        pos.drop(['SuperFlex','SuperFlex.1'],1,inplace = True)
    # split horizontal tables in 2 and stack  
    pos2 = pos.filter(regex='.1$')
    pos2.columns = pos2.columns.str.replace('.1','')
    # dangerous, but just grab first two columns
    # couldn't crack finding columns that don't end in .1
    pos1 = pos.iloc[:,0:2]
    out = pd.concat([pos1, pos2])
    ### to do: position variable
    out.columns = ['player','value']
    out = out.loc[out.player!='All Others']
    return out

def positions(data,pos):
    data['position'] = pos
    return data

def pull_tables(html):
    # process everything
    indata = pd.read_html(html, header=0)
    if len(indata)==5:
        del indata[0]
    intdata = list(map(clean_table, indata))
    # add positions
    pos = ['qb','rb','wr','te']
    posdata = list(map(positions, intdata, pos))
    outdata = pd.concat(posdata, ignore_index=True)
    return outdata

def date_rename(indata,indate):
    indata.columns = ['player',indate,'position']
    return indata

def constr_url(year,mon,month):
    url = ('https://www.fantasypros.com/'+year+'/'+mon+
           '/fantasy-football-rankings-dynasty-trade-value-chart-'+month+'-'+
           year+'-update/')
    return url

# clean player names to avoid name mismatches
def clean_names(x):
    x = str(x)
    x = re.sub(r'[.]+','',x)
    x = re.sub(r' II$| III$', '', x)
    x = re.sub(r' V$| IV$', '', x)
    x = re.sub(r' Jr.$| Sr.$', '', x)
    x = re.sub(r' Jr$| Sr$', '', x)
    x = re.sub(r'\'\w+', '', x)
    return x

# htmls
may2021url = constr_url('2021','05','may')
apr2021url = constr_url('2021','04','april')
# used a weird format this month, don't wanna deal w this, it's useless anyway
#mar2021url = constr_url('2021','03','march')
feb2021url = constr_url('2021','02','february')
jan2021url = constr_url('2021','01','january')
dec2020url = constr_url('2020','12','december')
nov2020url = constr_url('2020','11','november')
oct2020url = constr_url('2020','10','october')
sep2020url = constr_url('2020','09','september')
# no august update
#aug2020url = constr_url('2020','08','august')
jul2020url = constr_url('2020','07','july')
jun2020url = constr_url('2020','06','june')
may2020url = constr_url('2020','04','may') 
# no apr 2020
mar2020url = constr_url('2020','03','march') 
feb2020url = constr_url('2020','02','february') 
jan2020url = constr_url('2020','01','january') 
dec2019url = constr_url('2019','12','december') 
nov2019url = constr_url('2019','11','november')
# no oct 2019
sep2019url = constr_url('2019','09','september')
aug2019url = constr_url('2019','08','august')
jul2019url = constr_url('2019','07','july')
jun2019url = constr_url('2019','06','june')
may2019url = constr_url('2019','04','may') 

# clean individual data 
months = [may2021url, apr2021url, feb2021url, jan2021url, dec2020url,
          nov2020url, oct2020url, sep2020url, jul2020url, jun2020url,
          may2020url, feb2020url, jan2020url, dec2019url,
          nov2019url, sep2019url, jul2019url, jun2019url,
          may2019url]
monthly_dat = list(map(pull_tables, months))

# need to rename value column to reflect date of valuation
dates = ['may2021', 'apr2021', 'feb2021', 'jan2021', 'dec2020', 
         'nov2020', 'oct2020', 'sep2020', 'jul2020', 'jun2020', 
         'may2020', 'feb2020', 'jan2020', 'dec2019',
         'nov2019', 'sep2019', 'jul2019', 'jun2019',
         'may2019']
merge_data = list(map(date_rename,monthly_dat,dates))
# clean player names pre-merge
for i in merge_data:
    i['player'] = i.player.apply(clean_names)

# merge
historical_data = reduce(lambda x, y: pd.merge(x, y, on = ['player', 'position'], how='outer'), merge_data)
historical_data[['sep2020']] = historical_data[['sep2020']].replace({'\*':''}, regex=True)
historical_data['sep2020'] = pd.to_numeric(historical_data['sep2020'])

hist_data = historical_data.groupby(['player','position']).mean().reset_index()

hist_data.to_csv('fp_player_values.csv', index=True)

# pickle
with open('player_value.pickle','wb') as output:
    pickle.dump(hist_data, output)
    






'''
# turn jul 2020 to numeric

examples
AJ Dillon (delete periods)
Allen Robinson (delete II)
melvin gordon (delete III)
DJ Chark (delete Jr.)
drop 1 of the demarcus robinsons 
Keelan Cole Sr. (delete Sr.)
drop Scott Miller
Will Fuller V
'''





