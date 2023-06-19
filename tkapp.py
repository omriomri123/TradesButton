import csv
import datetime
import os
import pandas as pd
import tkinter as tk
import customtkinter
from tkinter import filedialog

CONSTS = {
    'ProfitTab': "Net Profit",
    'CumulativeTab': "Cumulative Profit",
    'DateTab': "Period",
    'DealsFromTS': "Shares/Ctrts - Profit/Loss",
    'CummulativeDeals': "Total Deals"
}

TEXT_COMPONENT = None


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
    df['IsSaturday'] = (df[CONSTS['DateTab']].dt.dayofweek == 6).astype(int)
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
    end_date = datetime.datetime.today()
    date_range = pd.date_range(start_date, end_date)
    missing_dates = set(date_range) - set(CONSTS['DateTab'])
    new_rows = pd.DataFrame(
        {CONSTS['DateTab']: list(missing_dates), CONSTS['ProfitTab']: 0})
    df = df[[CONSTS['DateTab'], CONSTS['ProfitTab']]]
    df = pd.concat([df, new_rows], ignore_index=True)
    return df


def analyze_df(df, TS_df, active_strategy, symbol):
    df_parse_date(df)

    df_dollar_to_float(df, CONSTS['ProfitTab'])

    df['Sign'] = df['% Profitable'].apply(
        lambda x: -1 if not '100.00%' in x else 1)

    df.loc[df[CONSTS['ProfitTab']] ==
           1, CONSTS['ProfitTab']] = 0

    df[CONSTS['ProfitTab']] *= df['Sign']

    #  Adding gap days with 0 value in profit
    df = fill_gap_days(df)

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


def process_dataframe(app):
    global TEXT_COMPONENT
    file_name = ''
    try:
        # Open file dialog to select CSV file
        file_paths = filedialog.askopenfilenames(
            filetypes=[('CSV Files', '*.csv')])

        for file_path in file_paths:
            file_name = os.path.basename(file_path).split('.')[0]
            # Perform your desired function on the DataFrame
            # Here, we're simply multiplying all values by 2
            with open(file_path, 'r') as f:
                df = process_csv(f)

            # Save the modified DataFrame as a new CSV file
            suggested_name = f'{file_name}_weekly.csv'
            home_dir = os.path.expanduser("~")

            # Check the operating system and determine the download folder path
            if os.name == 'posix':  # Linux, macOS
                download_folder_path = os.path.join(home_dir, 'Downloads')
            elif os.name == 'nt':  # Windows
                download_folder_path = os.path.join(home_dir, 'Downloads')

            # save_path = filedialog.asksaveasfilename(
            #     defaultextension='.csv', filetypes=[('CSV Files', '*.csv')], initialfile=suggested_name)
            df.to_csv(f'{download_folder_path}\{suggested_name}', index=False)
        TEXT_COMPONENT.configure(
            text=f'Files saved \nPath:{download_folder_path}')

    except Exception as ex:
        TEXT_COMPONENT.configure(
            text='One or more files not in the correct format: ' + file_name)


app = customtkinter.CTk()
customtkinter.set_appearance_mode("dark")
app.geometry("400x150")
app.title("CSV Processing")

button = customtkinter.CTkButton(
    app, text="my button", command=lambda: process_dataframe(app))
button.pack(padx=20, pady=20)
TEXT_COMPONENT = customtkinter.CTkLabel(
    app, text='')
TEXT_COMPONENT.pack()

app.mainloop()
