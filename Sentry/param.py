from datetime import date, timedelta, datetime


class SentryUrl:
    get_all_project = 'https://sentry.io/api/0/projects/'
    get_all_issues = 'https://sentry.io/api/0/projects/toqua/doi-qua/issues/'


QUERY_GET_PROJECT_SLUGS = """
    SELECT DISTINCT 
        slug 
    FROM sentry_projects
"""
