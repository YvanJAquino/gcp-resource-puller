import argparse
import yaml
import json
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

import logging
from logging.config import dictConfig

from google.oauth2 import service_account
from googleapiclient import discovery
from google.cloud import bigquery

import argparse
import yaml
import json
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

import logging
from logging.config import dictConfig

from google.oauth2 import service_account
from googleapiclient import discovery
from google.cloud import bigquery

class Resource(ABC):
    
    """Defines an abstract base class (a template) for API resources."""
    # required initialization parameters
    def __init__(self, config, logger):
        # Required attributes (from subclasses)
        self.logger            = None
        self.config            = None
        self.resource          = None
        self.endpoint          = None
        self.method            = None
        self.next_method       = None
        self.method_parameters = None
        self.request_key       = None
        self.table_id          = None
        self.records           = None
        
    def list_all(self):
        
        """Paginates (properly) over a given resource using the list_next method.  
        After every page is processed, it checks to see if a the records are 500 or more,
        inserts the records into Bigquery, and then empties the record cache.  """
        
        self.logger.info('Initiating data acquisition and insertion process...')
        
        batch_size = self.config['environment']['services']['bigquery']['bigqueryBatchSize']
        request = self.method(**self.method_parameters)
        while request:
            response = request.execute()
            records = response.get(self.request_key)
            if records:
                self.records += records
                request = self.next_method(request, response)
                if len(self.records) >= batch_size or not request:
                    self.insert_records()
                    self.records = []
            else:
                self.logger.info('Nothing to write: data might not be ready or partially available')
                break
                
        self.logger.info('Data acquisition complete.')
                                
    def insert_records(self):
        table_id = '.'.join((
            self.config['environment']['projectId'], 
            self.config['environment']['services']['bigquery']['datasetId'], 
            self.table_id
        ))
        client = bigquery.Client()
        load_config = dict(
            destination=table_id,
            job_config=bigquery.LoadJobConfig(
                autodetect=True,
                source_format=bigquery.job.SourceFormat.NEWLINE_DELIMITED_JSON,
                create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
                write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND
            )
        )
        return client.load_table_from_json(self.records, **load_config)

    def cache_local(self, key, cache=[]):
        self.logger.info(f'Generating a local cache of {key}')
        request = self.method(**self.method_parameters)
        while request:
            response = request.execute()
            records = response.get(self.request_key)
            if records:
                for record in records:
                    cache.append(record[key])
                request = self.next_method(request, response)
                
        self.logger.info(f'Local {key} cache is ready.')
        return self

    
class Users(Resource):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_directory_v1.users.html"""
    
    def __init__(self, config, credentials=None, serviceName='admin', version='directory_v1', logger=None):
        self.logger            = logger
        self.logger.info('Regenerating Users data...')
        
        self.config            = config
        self.resource          = discovery.build(
            serviceName, version,
            credentials=credentials,
            cache_discovery=False
        )
        self.endpoint          = self.resource.users()
        self.method            = self.endpoint.list
        self.next_method       = self.endpoint.list_next
        self.method_parameters = config['resources']['users']['method_parameters']
        self.request_key       = 'users'
        self.table_id          = config['resources']['users']['tableId']
        self.records           = []


class Activities(Resource):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_reports_v1.html.
    Activities is a parent class to applications (via the applicationName parameter)"""
    
    def __init__(self, config, credentials=None, serviceName='admin', version='reports_v1', logger=None):
        self.logger      = logger
        self.config      = config
        self.resource = discovery.build(
            serviceName, version,
            credentials=credentials,
            cache_discovery=False
        )
        self.endpoint    = self.resource.activities()
        self.method      = self.endpoint.list
        self.next_method = self.endpoint.list_next
        self.request_key = 'items'
        # Included as requirements
        self.table_id    = None
        self.records     = None
        

class Meets(Activities):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_reports_v1.html"""
    
    def __init__(self, config, credentials=None, logger=None):
        self.logger = logger
        self.logger.info('Regenerating Meets data...')
        # Meets is a subset of Activities with applicationName=Meet
        super().__init__(config=config, credentials=credentials, logger=self.logger)
        self.method_parameters = config['resources']['meets']['method_parameters']
        self.table_id = config['resources']['meets']['tableId']
        self.records = []
        

class Calendar(Activities):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_reports_v1.html"""
    
    def __init__(self, config, credentials=None, logger=None):
        self.logger = logger
        self.logger.info('Regenerating Calendar data...')
        # Meets is a subset of Activities with applicationName=Meet
        super().__init__(config=config, credentials=credentials, logger=self.logger)
        self.method_parameters = config['resources']['calendar']['method_parameters']
        self.table_id = config['resources']['calendar']['tableId']
        self.records = []


class UserUsage(Resource):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_reports_v1.userUsageReport.html"""
    
    def __init__(self, config, date=None, days_back=14, credentials=None, serviceName='admin', version='reports_v1', logger=None):
        self.logger = logger
        self.logger.info('Regenerating userUsage data...')
        self.config = config
        self.resource = discovery.build(
            serviceName, version,
            credentials=credentials,
            cache_discovery=False
        )
        
        if date:
            self.config['resources']['userUsage']['method_parameters']['date'] = date
        
        self.days_back = days_back
        self.endpoint = self.resource.userUsageReport()
        self.method = self.endpoint.get
        self.next_method = self.endpoint.get_next
        self.method_parameters = self.config['resources']['userUsage']['method_parameters']
        self.request_key = 'usageReports'
        self.table_id = self.config['resources']['userUsage']['tableId']
        self.records = []
        
    def list_all_dates(self):
        today = datetime.today()
        for days in range(self.days_back):
            date = today - timedelta(days=days)
            date = date.strftime('%Y-%m-%d')
            
            self.config['resources']['userUsage']['method_parameters']['date'] = date
            self.logger.info(f'-- getting data from {date}')
            self.list_all()


class Courses(Resource):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/classroom_v1.courses.html#list
    https://developers.google.com/classroom/reference/rest/v1/courses"""
    
    def __init__(self, config, credentials=None, serviceName='classroom', version='v1', logger=None):
        self.logger            = logger
        self.logger.info('Regenerating courses data...')
        self.config            = config
        self.resource          = discovery.build(
            serviceName, version,
            credentials=credentials,
            cache_discovery=False
        )
        self.endpoint          = self.resource.courses()
        self.method            = self.endpoint.list
        self.next_method       = self.endpoint.list_next
        self.method_parameters = config['resources']['courses']['method_parameters']
        self.request_key       = 'courses'
        self.table_id          = config['resources']['courses']['tableId']
        self.records           = []
        self.courseIds         = []

        
class CourseWork(Resource):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/classroom_v1.courses.courseWork.html
    https://developers.google.com/classroom/reference/rest/v1/courses.courseWork/list"""
    
    def __init__(self, config, courseIds, credentials=None, serviceName='classroom', version='v1', logger=None):
        self.logger            = logger
        self.logger.info('Regenerating course work data...')
        self.config            = config
        self.resource          = discovery.build(
            serviceName, version,
            credentials=credentials,
            cache_discovery=False
        )
        self.endpoint          = self.resource.courses().courseWork()
        self.method            = self.endpoint.list
        self.next_method       = self.endpoint.list_next
        self.method_parameters = config['resources']['courseWork']['method_parameters']
        self.request_key       = 'courseWork'
        self.table_id          = config['resources']['courseWork']['tableId']
        self.records           = []
        self.courseIds         = courseIds
        
    def list_all_courseWorks(self):
        for courseId in self.courseIds:
            errors = []
            self.config['resources']['courseWork']['method_parameters']['courseId'] = courseId
            try:
                self.list_all()
            except:
                errors.append(courseId)
        if errors:
            with open('course_work.err', 'w') as src:
                src.write(json.dumps(errors, indent=2))
            
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
    resource_group.add_argument('--course_work', action='store_true')
    resource_group.add_argument('--calendar', action='store_true')
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
