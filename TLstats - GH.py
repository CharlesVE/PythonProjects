from os import listdir
import pandas as pd

stock_data = pd.read_csv(r'C:\Users\Charlie\Documents\CVE\Python\Python Data\SPY\SPY93 Data.csv',
                         parse_dates=['Date'], index_col=0)

def reformat(source):
    '''
    `reformat` takes a trade list from Tradestation and reformats it into something usable in python.
    
    Parameters
    ----------
    source : str
        Files Location

    Returns
    -------
    strategy_dict : dict
        A dictionary of reformated tradelists in the DataFrame format, as well as an aggregate tradelist.

    '''
    filenames = listdir(source)
    strategy_dict = {}
    total_tl = pd.DataFrame(columns=['#',
                                     'Entry Date',
                                     'Exit Date',
                                     'Long/Short',
                                     'Entry Price',
                                     'Exit Price',
                                     'Shares',
                                     'Profit',
                                     'Cumulative Profit',
                                     'Entry Timing',
                                     'Exit Timing'])
    for file in filenames:
        tl = pd.read_csv(f'{source}\\{file}')
        # tl.fillna(method='ffill', inplace=True)
        for col in tl.columns:
            tl[col] = tl[col].str[1:]
        new_tl = pd.DataFrame(columns=['#',
                                       'Entry Date',
                                       'Exit Date',
                                       'Long/Short',
                                       'Entry Price',
                                       'Exit Price',
                                       'Shares'])
        # Purpose of code below is the change 2 rows into 1
        for i in range(0, len(tl), 2):
            trade = tl.iloc[i:i+2]
            row = {}
            row['#'] = trade.iloc[0]['#']
            row['Entry Date'] =  trade.iloc[0]['Date/Time']
            row['Exit Date'] =  trade.iloc[1]['Date/Time']
            row['Long/Short'] = 1 if trade.iloc[0]['Type'] == 'Buy' else -1
            row['Entry Price'] = float(trade.iloc[0]['Price'][1:])
            row['Exit Price'] = float(trade.iloc[1]['Price'][1:])
            row['Shares'] = float(trade.iloc[0]['Shares/Ctrts'])
            new_tl = new_tl.append(row, ignore_index=True)
        
        new_tl['Entry Date'] = pd.to_datetime(new_tl['Entry Date'], format='%d/%m/%Y')
        new_tl['Exit Date'] = pd.to_datetime(new_tl['Exit Date'], format='%d/%m/%Y')
        new_tl['Profit'] = (new_tl['Exit Price'] - new_tl['Entry Price']) * new_tl['Long/Short'] * new_tl['Shares']
        new_tl['Cumulative Profit'] = new_tl['Profit'].cumsum()
        
        entry_prices = new_tl.set_index('Entry Date')['Entry Price']
        exit_prices = new_tl.set_index('Exit Date')['Exit Price']
        
        new_tl['Entry Timing'] = (~(entry_prices == stock_data.loc[entry_prices.index]['Open'])).astype(int).values
        new_tl['Exit Timing'] = (~(exit_prices == stock_data.loc[exit_prices.index]['Open'])).astype(int).values
        
        strategy_dict[file[:-4]] = new_tl #gets rid of .csv
        
        total_tl = total_tl.append(new_tl)
    
    strategy_dict['Total'] = total_tl
        # print(new_tl)   
    
    return strategy_dict


def summary(tl_dict):
    '''
    This function takes in a dictionary of tradelists and creates the summary of statistics 
    for those tradelists as well the statistics for them as a whole.

    Parameters
    ----------
    tl_dict : dict
        This must be the output of the `reformat` function.

    Returns
    -------
    summary_table : pandas.DataFrame
        A summary table containing all relevant statistics to each strategy, as well as the portolio as a whole.

    '''

    summary_table = pd.DataFrame(columns=tl_dict.keys())
    for strategy, trade_list in tl_dict.items():
        avg_trade_net_profit = trade_list['Profit'].mean()
        summary_table.at['Average Trade Net Profit', strategy] =  avg_trade_net_profit
        
        winning_trades = trade_list[trade_list['Profit'] > 0]
        avg_winning_trade_profit = winning_trades['Profit'].mean()
        summary_table.at['Average Winning Trade Profit', strategy] = avg_winning_trade_profit
        
        losing_trades = trade_list[trade_list['Profit'] < 0]
        avg_losing_trade_profit = losing_trades['Profit'].mean()
        summary_table.at['Average Losing Trade Profit', strategy] = avg_losing_trade_profit
        
        equity = create_equity(trade_list)
        
        drawdown = equity - equity.cummax()
        max_drawdown = drawdown.min()
        summary_table.at['Max Drawdown', strategy] = max_drawdown
        
        util = utility(trade_list)
        
        if strategy == 'Total':
            allocation = 300000
        else:
            allocation = round(util.abs().max(),-3)
        market_returns = stock_data['Close'].pct_change()
        strategy_returns = equity / allocation
        strategy_market_cov = market_returns.cov(strategy_returns)
        market_var = market_returns.var()
        beta = strategy_market_cov/market_var
        summary_table.at['Beta', strategy] = beta
        
        
    
    return summary_table


def create_equity(trade_list):
    '''
    Takes in a trade list and creates a series of cumulative returns.

    Parameters
    ----------
    trade_list : pandas.DataFrame
        This must be the output of the `reformat` function.

    Returns
    -------
    equity : pandas.Series
        This is a series of the cumulative returns for a given `trade_list`.
        This starts from 0, the day before the first trade, until the final exit date.

    '''
    
    equity = pd.Series(index=stock_data.index, dtype=float, name='cumulative_profits').fillna(0)
    
    for row in trade_list.itertuples():
        entry_date = row[2]
        exit_date = row[3]
        entry_price = row[5]
        exit_price = row[6]
        shares = row[7]
        long_or_short = row[4]
        
        close_prices = stock_data['Close'].loc[entry_date:exit_date]
        close_prices[-1] = exit_price
        
        profits = (close_prices - entry_price) * long_or_short * shares
        first_profit = profits[:1]
        remaining_profits = profits.diff()[1:]
        equity.loc[first_profit.index] += first_profit
        equity.loc[remaining_profits.index] += remaining_profits

    first_trade = trade_list['Entry Date'].min()
    first_trade = equity.index[equity.index.get_loc(first_trade)-1] #This gives the date before the actual first entry date, to start from an equity value of 0
    last_trade =  trade_list['Exit Date'].max()
    
    
    equity = equity.cumsum()
    equity = equity[first_trade:last_trade]    
    return equity

def utility(trade_list):
    '''
    This function takes in a trade list in order to create a column of the amount invested at a particular date.

    Parameters
    ----------
    trade_list : pandas.DataFrame
        A table of trades by row containing when they began and ended,
        as well as other important information about each trade.

    Returns
    -------
    utility : pandas.Series
        A column of dates and amount invested at that particular date.

    '''
        
    utility = pd.Series(index=stock_data.index, dtype=float, name='utility').fillna(0)
    
    for row in trade_list.itertuples():
        entry_date = row[2]
        exit_date = row[3]
        entry_price = row[5]
        shares = row[7]
        long_or_short = row[4]
        exit_timing = row[11]
        
        trade_dates = stock_data.loc[entry_date:exit_date].index
        if exit_timing == 1:
            trade_dates = trade_dates[:-1]
            
        utility.loc[trade_dates] += entry_price * shares * long_or_short
   
    return utility    


def in_trade(tl_dict):
    '''
    This function takes in a dictionary of tradelists and returns a binary table indicating if we are in a trade.

    Parameters
    ----------
    tl_dict : dict
        This must be the output of the `reformat` function.

    Returns
    -------
    df_intrade: pandas.DataFrame
        Time series DataFrame indicating a 1 if we are in a trade and a 0 if we are not.

    '''

    df_intrade = pd.DataFrame(columns = tl_dict.keys(), index=stock_data.index)
    
    for strategy, trade_list in tl_dict.items():
        for row in trade_list.itertuples():
            entry_date = row[2]
            exit_date = row[3]
            
            in_trade_dates = stock_data.loc[entry_date:exit_date].index
            
            df_intrade[strategy].loc[in_trade_dates] = 1
            
    
  
    return df_intrade.fillna(0)

def overlap_matrix(tl_dict):
    '''
    This function's purpose is to produce a table that shows the overlap percent relative to the strategy that trades
    the least amount.

    Parameters
    ----------
    tl_dict : dict
        This must be the output of the `reformat` function.

    Returns
    -------
    df_overlap : pandas.DataFrame
        A table of overlap percentages, relative to the strategy that trades the least amount,
        between different strategies.

    '''
    df_overlap = pd.DataFrame(columns=tl_dict.keys(), index=tl_dict.keys(), dtype=float)
    df_intrade = in_trade(tl_dict)
    for col1 in df_intrade.columns:
        for col2 in df_intrade.columns:
            # cols_intrade = df_intrade[[col1, col2]]
            denominator = min(df_intrade[col1].sum(), df_intrade[col2].sum())
            df_overlap.at[col1, col2]= len(df_intrade[(df_intrade[col1]==1) & (df_intrade[col2]==1)])/denominator
    return df_overlap
    
    

new_tl_dict = reformat(r'C:\Users\Charlie\Documents\CVE\Python\Current Projects\Portfolio Statistics\trade_lists')
summary_table = summary(new_tl_dict)
o_matrix = overlap_matrix(new_tl_dict)




