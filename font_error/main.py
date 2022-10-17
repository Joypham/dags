from Config import *
import re
import pandas as pd
import json
import codecs
from sqlalchemy import create_engine
from font_error.param import *

URI = f"postgresql+pg8000://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
redshift_connection = create_engine(URI)

ESCAPE_SEQUENCE_RE = re.compile(r"""
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )""", re.UNICODE | re.VERBOSE)


def json_extract(string):
    try:
        return json.loads(string)['content']
    except:
        return 'No rating'


def json_extract3(string):
    rating = json.loads(string)

    if len(rating['tags']) < 1:
        return 'No rating'

    total_content = ''
    for content in rating['tags']:
        if total_content == '':
            total_content = content['content']
        else:
            total_content = '; '.join([total_content, content['content']])
    return total_content


def rating_clean(string):
    return 'No rating' if 'reviewemoji' in string.lower() else string


def decode_escapes(s):
    def decode_match(match):
        return codecs.decode(match.group(0), 'unicode-escape')

    return ESCAPE_SEQUENCE_RE.sub(decode_match, s)


def de_emojify(string):
    regex_pattern = re.compile(pattern="["
                                       u"\U0001F600-\U0001F64F"  # emoticons
                                       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                       u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                       "]+", flags=re.UNICODE)
    return regex_pattern.sub(r'', string)


def mobile_rate_tv():
    # get data to process
    df = pd.read_sql_query(sql=QUERY_GET_DATA, con=redshift_connection)
    if len(df) > 0:
        df['content_tv'] = df['content'].apply(json_extract)
        # Có nhiều dũ liệu ở phần content, cần extract lại
        mask = df['content_tv'].isnull()
        df.loc[mask, 'content_tv'] = df.loc[mask, 'content'].apply(json_extract3)
        df['content_tv'] = df['content_tv'].apply(decode_escapes).apply(rating_clean)
        # Remove emoji from user's comment
        df['content_tv'] = df['content_tv'].apply(de_emojify).str.strip()
        # Insert to redshift
        for i in range(0, len(df), 10):
            batch_df = df.loc[i:i + 9]
            batch_df.to_sql("mobile_rate_tv", con=redshift_connection, if_exists='append', index=False,
                            schema='ub_rawdata')
            print(batch_df.tail(3))


if __name__ == "__main__":
    mobile_rate_tv()
