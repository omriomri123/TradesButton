import os
import streamlit as st
import pandas as pd
import csv
import io

CONSTS = {
    'ProfitTab': "Net Profit",
    'CumulativeTab': "Cumulative Profit",
    'DateTab': "Period",
    'DealsFromTS': "Shares/Ctrts - Profit/Loss",
    'CummulativeDeals': "Total Deals"
}


def process_csv(csv_file):
    mark_to_market_df, TS_trades_df, active_strategy, symbol = extract_important_data(
        csv_file)
    analyzed_df = analyze_df(
        mark_to_market_df, TS_trades_df, active_strategy, symbol)
    return analyzed_df


def extract_important_data(csv_file):
    # content = csv_file.getvalue().decode('utf-8')
    csv_reader = csv.reader(csv_file)
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

    # st.write(lst_of_mark_to_market[:25])
    mark_to_market_df = convert_data_to_df(lst_of_mark_to_market)
    mark_to_market_df = mark_to_market_df[::-1].reset_index()
    mark_to_market_df = mark_to_market_df.drop("index", axis=1)
    TS_trades_df = convert_data_to_df(lst_of_TS_trades)
    return mark_to_market_df, TS_trades_df, active_strategy, symbol


def convert_data_to_df(important_data):
    headers = important_data[0]
    dict_to_df = {i: [] for i in headers}
    for row in important_data[1:]:
        for ind, cell in enumerate(row):
            dict_to_df[headers[ind]].append(cell)

    if "" in dict_to_df:
        dict_to_df.pop("")
    raw_df = pd.DataFrame.from_dict(dict_to_df)
    return raw_df


def df_dollar_to_float(df, col_name):
    df[col_name] = df[col_name].str.extract(
        r'([\d\.\,]+)').replace(",", "")
    df[col_name] = df[col_name
                      ].str.replace(",", "").astype(float)


def df_float_to_dollar(df, col_name):
    df[col_name] = df[col_name].apply(
        lambda x: '0' if x == 0 else f'{x:.2f}$' if x != int(x) else f'{x:.0f}$')


def df_parse_date(df):
    date_formats = ['%m/%d/%Y']

    def try_parsing_date(value):
        for fmt in date_formats:
            try:
                return pd.to_datetime(value, format=fmt)
            except ValueError:
                pass
        return pd.NaT

    df[CONSTS['DateTab']] = df[CONSTS['DateTab']].apply(
        lambda x: try_parsing_date(x))


def df_weekly_profit(df: pd.DataFrame):
    df['IsSaturday'] = (df[CONSTS['DateTab']].dt.dayofweek == 5).astype(int)
    df['Weekly Profit'] = df[CONSTS['ProfitTab']].rolling(
        7, min_periods=1).sum() * df['IsSaturday']

    df['Total Weekly Profit'] = df['Weekly Profit'].cumsum()
    df = df.drop(columns="IsSaturday")
    return df


def df_add_deals(df, TS_df):
    TS_df = TS_df[["Date/Time", CONSTS['DealsFromTS']]]
    TS_df = TS_df[(TS_df[CONSTS['DealsFromTS']] != "1") &
                  (TS_df[CONSTS['DealsFromTS']] != "n/a")]
    df["Period"] = pd.to_datetime(df["Period"])
    TS_df["Date/Time"] = pd.to_datetime(TS_df["Date/Time"])
    df = pd.merge(df, TS_df, "left", left_on="Period", right_on="Date/Time")
    df = df.drop(columns="Date/Time")

    df['Sign'] = df[CONSTS['DealsFromTS']].astype(str).apply(
        lambda x: -1 if '(' in x else 1)
    df_dollar_to_float(df, CONSTS['DealsFromTS'])
    df[CONSTS['DealsFromTS']] *= df['Sign']
    df[CONSTS['DealsFromTS']] = df[CONSTS['DealsFromTS']].fillna(0)
    df = df.rename(columns={CONSTS['DealsFromTS']: 'Deals'})
    df = df.drop(columns="Sign")
    df['Total Deals'] = df['Deals'].cumsum()
    return df


def fill_gap_days(df):
    start_date = df.head(1).reset_index().loc[0, CONSTS['DateTab']]
    end_date = df.tail(1).reset_index().loc[0, CONSTS['DateTab']]
    date_range = pd.date_range(start_date, end_date)
    missing_dates = set(date_range) - set(CONSTS['DateTab'])
    new_rows = pd.DataFrame(
        {CONSTS['DateTab']: list(missing_dates), CONSTS['ProfitTab']: 0})
    df = df[[CONSTS['DateTab'], CONSTS['ProfitTab']]]
    df = pd.concat([df, new_rows], ignore_index=True)


def analyze_df(df, TS_df, active_strategy, symbol):
    df_parse_date(df)
    # df[CONSTS["DateTab"]] = df[CONSTS["DateTab"]].dt.strftime('%m/%d/%Y')

    df_dollar_to_float(df, CONSTS['ProfitTab'])

    df['Sign'] = df['% Profitable'].apply(
        lambda x: -1 if not '100.00%' in x else 1)

    df.loc[df[CONSTS['ProfitTab']] ==
           1, CONSTS['ProfitTab']] = 0

    df[CONSTS['ProfitTab']] *= df['Sign']

    #  Adding gap days with 0 value in profit
    fill_gap_days(df)

    #  Adding the cum sum column:
    df = df.groupby(
        df[CONSTS['DateTab']].dt.date)[CONSTS['ProfitTab']].sum().reset_index()
    df[CONSTS['CumulativeTab']] = df[CONSTS['ProfitTab']
                                     ].cumsum()
    df = df.rename(columns={CONSTS['CumulativeTab']: 'Total Profit'})

    #  Adding Strategy:
    df.insert(0, 'Strategy', active_strategy.replace("(On)", ""))
    #  Adding Symbol:
    df.insert(1, 'Symbol', symbol)

    #  Adding the Deals:
    df = df_add_deals(df, TS_df)
    # Adding weekly profit
    df = df_weekly_profit(df)

    return df


def color_cell(v):
    if "-" in str(v):
        return 'color:red;'
    if "0$" == str(v):
        return 'opacity: 20%;'
    return 'color:green;'


def style_positive(v, props=''):
    return props if int(v) > 0 else None


def style_zero(v, props=''):
    return props if "-" in str(v) else None


def style_df(df):
    df = df.set_index([CONSTS["DateTab"]])
    df = df[[CONSTS["ProfitTab"], "Total Profit"]]
    df = df.style.applymap(color_cell)
    return df


def display(file):
    try:
        st.subheader(f"{file.name}:")

        dataframe = process_csv(file)

        st.write(dataframe)
        st.download_button(
            label="Download data as CSV",
            data=dataframe.to_csv().encode('utf-8'),
            file_name=f'{file.name.split(".")[0]}_weekly.csv',
            mime='text/csv',
        )

    except Exception as ex:
        st.write(f"Couldn't parse file! Reason - {ex}")


def streamlit_ui():
    st.set_page_config(page_title="Csv Convert", layout="wide")

    with st.sidebar:
        uploaded_file = st.file_uploader(
            "Choose a CSV file", accept_multiple_files=True)
    if uploaded_file:
        for file in uploaded_file:
            display(file)


def main():
    streamlit_ui()


if __name__ == "__main__":
    main()
