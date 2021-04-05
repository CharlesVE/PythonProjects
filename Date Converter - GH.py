"""
Created on Thu Nov 12 12:18:58 2020

@author: Charlie
"""

import pandas as pd
#import numpy as np
#import datetime as dt

df_tradeList = pd.read_csv(r'C:\Users\Charlie\Documents\CVE\Python\Useful Functions\DateConverter - Generic\Example TradeList - Generic.csv')
df_tradeList['entry_date'] = pd.to_datetime(df_tradeList['entry_date'].values, format = '%d/%m/%Y')
df_tradeList['exit_date'] = pd.to_datetime(df_tradeList['exit_date'].values, format = '%d/%m/%Y')
df_tradeList.index = pd.to_datetime(df_tradeList.index)

def datimusPrime_entry(df_DP):
    '''
    This function takes in a table of entry and exits dates (dd/mm/YYYY) and converts the entry dates into Tradestation dates.
    For example, 26/04/2020 would become 1200426.

    Parameters
    ----------
    df_DP : pandas.DataFrame
        A table of entry and exit dates.

    Returns
    -------
    df_DP['ts_dates_entry']: pandas.Series
        A column of entry dates in Tradestation format.

    '''
    df_DP['entry_day']=df_DP['entry_date'].dt.day
    df_DP['entry_day_str']=df_DP['entry_day'].apply(lambda x: '{0:0>2}'.format(x)) #put 0's in front of single digit day values and make it a string
    df_DP['entry_month']=df_DP['entry_date'].dt.month
    df_DP['entry_month_str']=df_DP['entry_month'].apply(lambda x: '{0:0>2}'.format(x)) #put 0's in front of single digit month values and make it a string
    df_DP['entry_year']=df_DP['entry_date'].dt.year
    df_DP['ts_year']=df_DP['entry_year']-1900
    df_DP['ts_year_str']=df_DP['ts_year'].apply(str)
    
    df_DP['ts_dates_entry'] = df_DP[['ts_year_str', 'entry_month_str', 'entry_day_str']].apply(
    lambda x: ''.join(x.dropna().astype(str)),
    axis=1)
    
    return df_DP['ts_dates_entry']
    
def datimusPrime_exit(df_DP):
    '''
    This function takes in a table of entry and exits dates (dd/mm/YYYY) and converts the exit dates into Tradestation dates.
    For example, 26/04/2020 would become 1200426.

    Parameters
    ----------
    df_DP : pandas.DataFrame
        A table of entry and exit dates.

    Returns
    -------
    df_DP['ts_dates_exit']: pandas.Series
        A column exit dates in Tradestation format.

    '''
    df_DP['exit_day']=df_DP['exit_date'].dt.day
    df_DP['exit_day_str']=df_DP['exit_day'].apply(lambda x: '{0:0>2}'.format(x)) #put 0's in front of single digit day values and make it a string
    df_DP['exit_month']=df_DP['exit_date'].dt.month
    df_DP['exit_month_str']=df_DP['exit_month'].apply(lambda x: '{0:0>2}'.format(x)) #put 0's in front of single digit month values and make it a string
    df_DP['exit_year']=df_DP['exit_date'].dt.year
    df_DP['ts_year']=df_DP['exit_year']-1900
    df_DP['ts_year_str']=df_DP['ts_year'].apply(str)
    
    df_DP['ts_dates_exit'] = df_DP[['ts_year_str', 'exit_month_str', 'exit_day_str']].apply(
    lambda x: ''.join(x.dropna().astype(str)),
    axis=1)
    
    return df_DP['ts_dates_exit']    
    
    
ts_dates_entry = datimusPrime_entry(df_tradeList)
ts_dates_exit  = datimusPrime_exit(df_tradeList)

def to_tradeStation(entryvector, exitvector):
    '''
    This function takes in the two reformatted dates from the outputs of the functions `datimusPrime_entry` and `datimusPrime_exit`.
    It uses these to create output, a file with a Tradestation strategy that enters and exits based on the dates given, using 100k.

    Parameters
    ----------
    entryvector : pandas.Series
        This is a column of entry dates in Tradestation format that has been outputted from datimusPrime_entry. 
    exitvector : pandas.Series
        This is a column of exit dates in Tradestation format that has been outputted from datimusPrime_exit. 

    Returns
    -------
    output : str
    	A string of Tradestation code ready to be exported into Tradestation.
    	This code will enter on the entry dates given and exit on the exit dates given using 100k.

    '''
    output = pd.Series(dtype=str)
    print(r'''
{
Inputs:
// Entries
;}
var:
PS(0),
atr(0),
SL_ON(0)
;

PS = position_size("Other", "Fixed", 100000, 0, atr, SL_ON * 0.00);
          ''')
    output.loc[len(output)] = r'''
{
Inputs:
// Entries
;}
var:
PS(0),
atr(0),
SL_ON(0)
;

PS = position_size("Other", "Fixed", 100000, 0, atr, SL_ON * 0.00);
          '''
          
    print("if")
    output.loc[len(output)] = "if"
    for i in entryvector:
        if i!= entryvector[-1]:
            print(f"date= {i} or")
            output.loc[len(output)] = f"date= {i} or"
        else:
            print(f"date= {i}")
            output.loc[len(output)] = f"date= {i}"
    print("THEN BUY PS contract THIS BAR CLOSE;")
    output.loc[len(output)] = "THEN BUY PS contract THIS BAR CLOSE;"
    print("if")
    output.loc[len(output)] = "if"
    for i in exitvector:
        if i!=exitvector[-1]:    
            print(f"date= {i} or")
            output.loc[len(output)] = f"date= {i} or"
        else:
            print(f"date= {i}")
            output.loc[len(output)] = f"date= {i}"
    print("THEN SELL all shares THIS BAR CLOSE;")
    output.loc[len(output)] = "THEN SELL all shares THIS BAR CLOSE;"
    print(r'''
{
// RSI crosses above
if rsi(close,rsilength) crosses above rsilimit then Begin
sell ("RSI") this bar close;
end;
            
//Trailing Profit - added 03/03/2020 then removed
If Maxpositionprofit >= PctTrailingFloorAMT$ And 
Openpositionprofit <= (maxpositionprofit*PctTrailingPct)
Then Sell ("PCTTRAIL") This Bar close;

//Setstopposition;
Setstoploss(SL);
}
    ''')
    output.loc[len(output)] = r'''
{
// RSI crosses above
if rsi(close,rsilength) crosses above rsilimit then Begin
sell ("RSI") this bar close;
end;
            
//Trailing Profit - added 03/03/2020 then removed
If Maxpositionprofit >= PctTrailingFloorAMT$ And 
Openpositionprofit <= (maxpositionprofit*PctTrailingPct)
Then Sell ("PCTTRAIL") This Bar close;

//Setstopposition;
Setstoploss(SL);
}
    '''
    return output

output = to_tradeStation(ts_dates_entry, ts_dates_exit) 

#output.to_csv('output.txt', index=False, header='col')
