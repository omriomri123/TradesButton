import streamlit as st
import pandas as pd
import dataframe_utils
import datetime
import yaml
import csv
import os
import io

with open('settings.yaml') as f:
    settings = yaml.safe_load(f)


def process_csv_to_df(csv_file):
    mark_to_market_df, TS_trades_df, active_strategy, symbol = extract_important_data(
        csv_file)
    analyzed_df = analyze_df(
        mark_to_market_df, TS_trades_df, active_strategy, symbol)
    return analyzed_df


def download_dataframe_as_csv(df, name):
    file_name = name.split('.')[0]
    suggested_name = f'{file_name}_weekly.csv'
    home_dir = os.path.expanduser("~")

    # Check the operating system and determine the download folder path
    if os.name == 'posix':  # Linux, macOS
        download_folder_path = os.path.join(home_dir, 'Downloads')
    elif os.name == 'nt':  # Windows
        download_folder_path = os.path.join(home_dir, 'Downloads')

    df.to_csv(f'{download_folder_path}\{suggested_name}', index=False)


def extract_important_data(csv_file):
    content = csv_file.getvalue().decode('utf-8')
    csv_reader = csv.reader(io.StringIO(content))

    lst_of_TS_trades = []
    lst_of_mark_to_market = []
    arrived_TS_trades = False
    arrived_active_strategies = False
    arrived_mark_to_market = False
    arrived_daily = False
    for row in csv_reader:
        if list(set(row)) == [""]:  # ignoring the empty rows
            continue

        #  Finding the rows of the "TradeStation Trades List":
        if "Trades List" in row:
            arrived_TS_trades = False
        elif "TradeStation Trades List" in row:
            arrived_TS_trades = True
        elif row != [] and arrived_TS_trades:
            lst_of_TS_trades.append(row)

        #  Finding the rows of the "Mark-To-Market Period Analysis":
        elif "TradeStation Periodical Returns: Daily" in row:
            arrived_daily = True
        elif "Mark-To-Market Rolling Period Analysis:" in row:
            arrived_mark_to_market = False
            arrived_daily = False
        elif "Mark-To-Market Period Analysis:" in row:
            arrived_mark_to_market = True
        elif arrived_mark_to_market and arrived_daily:
            lst_of_mark_to_market.append(row)

        #  Finding the Symbol:
        elif "Symbol" in row:
            #  symbol row is a row that if we are ignoring the empty cells is in the length of 2
            symbol_row = [i for i in row if i != ""]
            if len(symbol_row) != 2:
                continue
            symbol = symbol_row[1]  # The second cell in the row

        #  Finding the Active Strategy:
        elif "TradeStation Strategies Applied" in row:
            arrived_active_strategies = True
        elif "TradeStation Strategy Inputs" in row:
            arrived_active_strategies = False
        elif arrived_active_strategies:
            if "(On)" in row[0]:
                active_strategy = row[0]

    mark_to_market_df = dataframe_utils.convert_data_to_df(lst_of_mark_to_market)
    mark_to_market_df = mark_to_market_df[::-1].reset_index()
    mark_to_market_df = mark_to_market_df.drop("index", axis=1)

    TS_trades_df = dataframe_utils.convert_data_to_df(lst_of_TS_trades)

    return mark_to_market_df, TS_trades_df, active_strategy, symbol


def df_weekly_profit(df: pd.DataFrame):
    df['IsSaturday'] = (df[settings['DateTab']].dt.dayofweek == 6).astype(int)
    df['Weekly Profit'] = df[settings['ProfitTab']].rolling(
        7, min_periods=1).sum() * df['IsSaturday']

    df['Total Weekly Profit'] = df['Weekly Profit'].cumsum()
    df = df.drop(columns="IsSaturday")
    return df


def df_add_deals(df, TS_df):
    TS_df = TS_df[["Date/Time", settings['DealsFromTS']]]
    TS_df = TS_df[(TS_df[settings['DealsFromTS']] != "1") &
                  (TS_df[settings['DealsFromTS']] != "n/a")]
    df["Period"] = pd.to_datetime(df["Period"])
    TS_df["Date/Time"] = pd.to_datetime(TS_df["Date/Time"])
    df = pd.merge(df, TS_df, "left", left_on="Period", right_on="Date/Time")
    df = df.drop(columns="Date/Time")

    df['Sign'] = df[settings['DealsFromTS']].astype(str).apply(
        lambda x: -1 if '(' in x else 1)
    dataframe_utils.df_dollar_to_float(df, settings['DealsFromTS'])
    df[settings['DealsFromTS']] *= df['Sign']
    df[settings['DealsFromTS']] = df[settings['DealsFromTS']].fillna(0)
    df = df.rename(columns={settings['DealsFromTS']: 'Deals'})
    df = df.drop(columns="Sign")
    df['Total Deals'] = df['Deals'].cumsum()
    return df


def fill_gap_days(df):
    start_date = df.head(1).reset_index().loc[0, settings['DateTab']]
    end_date = datetime.datetime.today()
    date_range = pd.date_range(start_date, end_date)
    missing_dates = set(date_range) - set(settings['DateTab'])
    new_rows = pd.DataFrame(
        {settings['DateTab']: list(missing_dates), settings['ProfitTab']: 0})
    df = df[[settings['DateTab'], settings['ProfitTab']]]
    df = pd.concat([df, new_rows], ignore_index=True)
    return df


def analyze_df(df, TS_df, active_strategy, symbol):
    dataframe_utils.df_parse_date(df)

    dataframe_utils.df_dollar_to_float(df, settings['ProfitTab'])

    df['Sign'] = df['% Profitable'].apply(
        lambda x: -1 if not '100.00%' in x else 1)

    df.loc[df[settings['ProfitTab']] ==
           1, settings['ProfitTab']] = 0

    df[settings['ProfitTab']] *= df['Sign']

    #  Adding gap days with 0 value in profit
    df = fill_gap_days(df)

    #  Adding the cumulative sum column:
    df = df.groupby(
        df[settings['DateTab']].dt.date)[settings['ProfitTab']].sum().reset_index()
    df[settings['CumulativeTab']] = df[settings['ProfitTab']
                                       ].cumsum()
    df = df.rename(columns={settings['CumulativeTab']: 'Total Profit'})

    #  Adding Strategy:
    df.insert(0, 'Strategy', active_strategy.replace("(On)", ""))
    #  Adding Symbol:
    df.insert(1, 'Symbol', symbol)

    #  Adding the Deals:
    df = df_add_deals(df, TS_df)
    # Adding weekly profit
    df = df_weekly_profit(df)

    return df


def process_file(file):
    try:
        st.subheader(f"{file.name}:")

        dataframe = process_csv_to_df(file)
        download_dataframe_as_csv(dataframe, file.name)
        st.write(dataframe)
        st.download_button(
            label="Download data as CSV",
            data=dataframe.to_csv().encode('utf-8'),
            file_name=f'{file.name.split(".")[0]}_weekly.csv',
            mime='text/csv',
        )

        return dataframe

    except Exception as ex:
        st.write(f"Couldn't parse file! Reason - {ex}")


def streamlit_ui():
    st.set_page_config(page_title=settings['title'], layout="wide")

    df_list = []
    with st.sidebar:
        uploaded_file = st.file_uploader(
            "Choose a CSV file", accept_multiple_files=True)
        if st.button(label="Download all CSV's"):
            for df in df_list:
                file_name = list(df.keys())[0]
                download_dataframe_as_csv(df[file_name], file_name)

    if uploaded_file:
        for file in uploaded_file:
            df_list.append({file.name: process_file(file)})


def main():
    streamlit_ui()


if __name__ == "__main__":
    main()
