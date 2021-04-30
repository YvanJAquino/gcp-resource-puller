import yaml
import json
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import logging
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
        
    def list_all(self, iters=0, size=None):
        
        """Paginates (properly) over a given resource using the list_next method.  
        After every page is processed, it checks to see if a the records are 500 or more,
        inserts the records into Bigquery, and then empties the record cache.  """
        
        if not iters:
            self.logger.info('Initiating data acquisition and insertion process...')
        else:
            self.logger.info('Iterating {iters} / {size}'.format(iters=iters, size=size))
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
        if (not iters and not size) or (iters == size - 1):    
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


class Login(Activities):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_reports_v1.html"""
    
    def __init__(self, config, credentials=None, logger=None):
        self.logger = logger
        self.logger.info('Regenerating Login data...')
        # Meets is a subset of Activities with applicationName=Meet
        super().__init__(config=config, credentials=credentials, logger=self.logger)
        self.method_parameters = config['resources']['login']['method_parameters']
        self.table_id = config['resources']['login']['tableId']
        self.records = []
        
        
class Chat(Activities):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_reports_v1.html"""
    
    def __init__(self, config, credentials=None, logger=None):
        self.logger = logger
        self.logger.info('Regenerating Chat data...')
        # Meets is a subset of Activities with applicationName=Meet
        super().__init__(config=config, credentials=credentials, logger=self.logger)
        self.method_parameters = config['resources']['chat']['method_parameters']
        self.table_id = config['resources']['chat']['tableId']
        self.records = []


class Drive(Activities):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_reports_v1.html"""
    
    def __init__(self, config, credentials=None, logger=None):
        self.logger = logger
        self.logger.info('Regenerating Drive data...')
        # Meets is a subset of Activities with applicationName=Meet
        super().__init__(config=config, credentials=credentials, logger=self.logger)
        self.method_parameters = config['resources']['drive']['method_parameters']
        self.table_id = config['resources']['drive']['tableId']
        self.records = []


class UserAccounts(Activities):
    
    """https://googleapis.github.io/google-api-python-client/docs/dyn/admin_reports_v1.html"""
    
    def __init__(self, config, credentials=None, logger=None):
        self.logger = logger
        self.logger.info('Regenerating UserAccounts data...')
        # Meets is a subset of Activities with applicationName=Meet
        super().__init__(config=config, credentials=credentials, logger=self.logger)
        self.method_parameters = config['resources']['user_accounts']['method_parameters']
        self.table_id = config['resources']['user_accounts']['tableId']
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
            self.list_all(iters=days, size=self.days_back)


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
        size = len(self.courseIds)
        for index, courseId in enumerate(self.courseIds):
            errors = []
            self.config['resources']['courseWork']['method_parameters']['courseId'] = courseId
            try:
                self.list_all(iters=index, size=size)
            except:
                errors.append(courseId)
        if errors:
            with open('course_work.err', 'w') as src:
                src.write(json.dumps(errors, indent=2))
                

class GmailUserProfiles(Resource):
    
    """Google Python API Client: https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1.html
    Google Developers REST Reference: https://developers.google.com/gmail/api/reference/rest"""
    
    def __init__(self, config, userIds, credentials=None, serviceName='gmail', version='v1', logger=None):
        self.logger            = logger
        self.logger.info('Regenerating Gmail data...')
        
        self.config            = config
        self.resource          = discovery.build(
            serviceName, version,
            credentials=credentials,
            cache_discovery=False
        )
        self.endpoint          = self.resource.users()
        self.method            = self.endpoint.getProfile
        self.next_method       = None
        self.method_parameters = config['resources']['gmailUserProfiles']['method_parameters']
        self.request_key       = None
        self.table_id          = config['resources']['gmailUserProfiles']['tableId']
        self.records           = []
        self.userIds           = userIds

    def list_all(self):
        print('Use get_one, get_all_profiles')
        pass
    
    def get_one(self):
        request = self.method(**self.method_parameters)
        response = request.execute()
        if response:
            self.records.append(response)

    def get_all_profiles(self):
        size = len(self.userIds)
        for index, userId in enumerate(self.userIds):
            errors = []
            self.config['resources']['gmailUserProfiles']['method_parameters']['userId'] = userId
            try:
                self.get_one()
            except:
                errors.append(errors)
        if errors:
            with open('gmail_user_profiles.err', 'w') as src:
                src.write(json.dumps(errors, indent=2))
        if self.records:
            self.insert_records()
