import requests
from sentry.param import *
from Config import *
import pandas as pd
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import datetime

URI = f"mysql+mysqldb://{SENTRY_CONFIG['user']}:{SENTRY_CONFIG['password']}@{SENTRY_CONFIG['host']}:{SENTRY_CONFIG['port']}/{SENTRY_CONFIG['dbname']}"
sentry_connection = create_engine(URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=sentry_connection))

pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)

'''
    https://docs.sentry.io/api/events/
'''


def generate_report_date(report_date=None, **kwargs):
    if not report_date or report_date == "%Y-%m-%d":
        report_date_object = datetime.now() - timedelta(days=1)
    else:
        report_date_object = datetime.strptime(report_date, '%Y-%m-%d %H:%M:%S')

    task_instance = kwargs['task_instance']
    task_instance.xcom_push(
        key='report_date',
        value=report_date_object.strftime("%Y-%m-%d")
    )


def sync_data_from_tmp_to_final_table():
    raw_query = """INSERT IGNORE INTO sentry_events (
        `id`,`projectID`, `issue_id`, `message`, `title`,
        `user`, `tags`, `tags_reformat` ,`dateCreated`, `info`
    )
    SELECT `id`,`projectID`, `issue_id`, `message`, `title`, `user`, `tags`,`tags_reformat` , `dateCreated`, `info` FROM tmp_sentry_events"""
    try:
        db_session.execute(raw_query)
        raw_query_2 = "TRUNCATE TABLE tmp_sentry_events"
        db_session.execute(raw_query_2)
        db_session.commit()
        return True
    except:
        print("An exception occurred")
        return False


def get_all_project():
    url = SentryUrl.get_all_project
    response = requests.get(
        url,
        headers={
            'Authorization': 'Bearer 26f3513a5a19427a854e1d07b2561132b5e4726ab18d40f8b86a70d74dfeacee',
            'Content-Type': 'application/json'
        }
    )
    cursors = response.json()
    df = pd.DataFrame()
    for cursor in cursors:
        info = cursor.copy()
        remove_keys = [
            'id', 'slug', 'name', 'dateCreated', 'status', 'isPublic', 'isBookmarked',
            'color', 'firstEvent', 'firstTransactionEvent', 'hasSessions'
        ]
        k1 = []
        for key in list(info):
            if key not in remove_keys:
                info.pop(key, None)
        # print(info)
        data = {}
        column_name = ['id', 'slug', 'name', 'dateCreated', 'status']
        for key, value in cursor.items():
            if key in column_name:
                data.update({
                    key: value
                })

        for column in column_name:
            if column not in data.keys():
                data.update({
                    column: ['']
                })

        k = pd.DataFrame(data)
        k['info'] = f"{info}".replace("'", "\"")
        k['dateCreated'] = pd.to_datetime(k['dateCreated'], format='%Y-%m-%d %H:%M:%S')
        df = df.append(k, ignore_index=True)
    df["dateCreated"] = df["dateCreated"].astype("str")
    db_session.execute("TRUNCATE TABLE sentry_projects")
    db_session.commit()
    for i in range(0, len(df), 10):
        batch_df = df.loc[i:i + 9]
        batch_df.to_sql("sentry_projects", con=sentry_connection, if_exists='append', index=False, schema='sentry')
        print(batch_df.tail(3))


def get_all_events_by_project(report_date: str = None, **kwargs):
    # report_date = datetime.strptime(report_date, '%Y-%m-%d')
    report_date = datetime.strptime('2022-09-15', '%Y-%m-%d')
    get_all_project_slugs = pd.read_sql_query(
        sql=QUERY_GET_PROJECT_SLUGS,
        con=sentry_connection)
    project_slugs = get_all_project_slugs['slug'].tolist()
    for project_slug in project_slugs:
        count = 0
        value = 0
        url = f"https://sentry.io/api/0/projects/toqua/{project_slug}/events/?&cursor=0:{value}:0"
        response = requests.get(
            url,
            headers={'Authorization': 'Bearer 26f3513a5a19427a854e1d07b2561132b5e4726ab18d40f8b86a70d74dfeacee',
                     'Content-Type': 'application/json'}
        )
        next_cursors = response.json()
        flag = True
        while next_cursors and flag:
            df = pd.DataFrame()
            url = f"https://sentry.io/api/0/projects/toqua/{project_slug}/events/?&cursor=0:{value}:0"
            response = requests.get(
                url,
                headers={'Authorization': 'Bearer 26f3513a5a19427a854e1d07b2561132b5e4726ab18d40f8b86a70d74dfeacee',
                         'Content-Type': 'application/json'}
            )
            next_cursors = response.json()
            check = next_cursors == {'detail': 'The requested resource does not exist'}
            if check:
                break
            value = value + 100
            count = count + 1
            if next_cursors:
                for cursor in next_cursors:
                    try:
                        cursor['issue_id'] = cursor['groupID']
                        info = cursor.copy()
                        remove_keys = ['id', 'eventID', 'projectID', 'message', 'title', 'user', 'tags',
                                       'dateCreated']
                        for key in list(info):
                            if key in remove_keys:
                                info.pop(key, None)
                        column_names = ['id', 'projectID', 'message', 'title', 'user', 'tags', 'dateCreated',
                                        'issue_id']
                        for key in list(cursor):
                            if key not in column_names:
                                cursor.pop(key, None)
                            if key in column_names and key not in list(cursor):
                                cursor[key] = ''
                        tag_reformat = {}
                        for tag in cursor['tags']:
                            tag_reformat[tag['key']] = tag['value']
                        # print(json.dumps(tag_reformat, ensure_ascii=False))
                        cursor['info'] = f"{json.dumps(info, ensure_ascii=True)}"
                        cursor['user'] = f"{json.dumps(cursor['user'], ensure_ascii=False)}"
                        cursor['tags'] = f"{json.dumps(cursor['tags'], ensure_ascii=False)}"
                        cursor['tags_reformat'] = f"{json.dumps(tag_reformat, ensure_ascii=False)}"
                        k = pd.DataFrame(cursor, index=[0])
                        k['dateCreated'] = pd.to_datetime(k['dateCreated'], format='%Y-%m-%d %H:%M:%S')
                        sentry_date = str(cursor['dateCreated'])[:10]
                        sentry_date = datetime.strptime(sentry_date, '%Y-%m-%d')
                        if sentry_date > report_date:
                            df = pd.concat([df, k])
                            # df = df.append(k, ignore_index=True)
                        else:
                            flag = False
                            break
                    except:
                        print(f"error: {cursor}")
            df.drop_duplicates().reset_index(drop = True)
            # print(df)
            for i in range(0, len(df), 100):
                # TRUNCATE table ub_rawdata.sentry_issues
                batch_df = df.loc[i:i + 99]
                batch_df.to_sql("tmp_sentry_events", con=db_session.connection(), if_exists='append', index=False,
                                schema='sentry')
                db_session.commit()
                print(batch_df.tail(3))
            sync_data_from_tmp_to_final_table()


if __name__ == "__main__":
    get_all_events_by_project()
