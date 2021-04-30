import argparse
import yaml
import json
import logging
from logging.config import dictConfig
from google.oauth2 import service_account
from resources import *

if __name__ == '__main__':
    
    with open('logging.yaml', 'r') as src:
        logging_config = yaml.load(src, Loader=yaml.Loader)
        
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger()

    
    with open('config.yaml', 'r') as src:
        config = yaml.load(src, Loader=yaml.Loader)
    
    environment = config['environment']
    filename = environment['serviceAccountKey']
    scopes = environment['scopes']
    subject = environment['adminEmail']
    
    credentials = service_account.Credentials.from_service_account_file(
        filename, scopes=scopes, subject=subject
    )
    
    parser = argparse.ArgumentParser()
    resource_group = parser.add_mutually_exclusive_group(required=True)
    resource_group.add_argument('--users', action='store_true')
    resource_group.add_argument('--meets', action='store_true')
    resource_group.add_argument('--usage', action='store_true')
    resource_group.add_argument('--courses', action='store_true')
    resource_group.add_argument('--calendar', action='store_true')
    resource_group.add_argument('--course_work', action='store_true')
    resource_group.add_argument('--user_accounts', action='store_true')
    resource_group.add_argument('--logins', action='store_true')
    resource_group.add_argument('--chat', action='store_true')
    resource_group.add_argument('--drive', action='store_true')
    resource_group.add_argument('--gmail_user_profiles', action='store_true')
    args = parser.parse_args()
    
    if args.users:
        users = Users(config, credentials=credentials, logger=logger)
        users.list_all()
    
    if args.meets:
        meets = Meets(config, credentials=credentials, logger=logger)
        meets.list_all()
        
    if args.usage:
        usage = UserUsage(config, credentials=credentials, logger=logger)
        usage.list_all_dates()

    if args.courses:
        
        courses = Courses(config, credentials=credentials, logger=logger)
        courses.list_all()

    if args.course_work:
        courses = Courses(config, credentials=credentials, logger=logger)
        courses.cache_local('id', courses.courseIds)
        course_work = CourseWork(config, courses.courseIds, credentials=credentials, logger=logger)
        course_work.list_all_courseWorks()
        
    if args.calendar:
        calendar = Courses(config, credentials=credentials, logger=logger)
        calendar.list_all()
        
    if args.logins:
        login = Login(config, credentials=credentials, logger=logger)
        login.list_all()
        
    if args.user_accounts:
        user_accounts = UserAccounts(config, credentials=credentials, logger=logger)
        user_accounts.list_all()
        
    if args.chat:
        chat = Chat(config, credentials=credentials, logger=logger)
        chat.list_all()
        
    if args.drive:
        drive = Drive(config, credentials=credentials, logger=logger)
        drive.list_all()
        
    if args.gmail_user_profiles:
        users = Users(config, credentials=credentials, logger=logger)
        userIds = []
        users.cache_local('primaryEmail', userIds)
        gmail_user_profiles = GmailUserProfiles(config, userIds=userIds, credentials=credentials, logger=logger)
        gmail_user_profiles.get_all_profiles()
