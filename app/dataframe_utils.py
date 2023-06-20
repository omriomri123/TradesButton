import pandas as pd
import yaml

with open(r'settings.yaml') as f:
    settings = yaml.safe_load(f)


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
    df = df.set_index([settings["DateTab"]])
    df = df[[settings["ProfitTab"], "Total Profit"]]
    df = df.style.applymap(color_cell)
    return df


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

    df[settings['DateTab']] = df[settings['DateTab']].apply(
        lambda x: try_parsing_date(x))


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
