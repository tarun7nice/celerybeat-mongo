# Copyright 2014 Artyom Topchyan

# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at http://www.apache.org/licenses/LICENSE-2.0

import datetime
from celery.beat import Scheduler, ScheduleEntry
from celery.utils.log import get_logger
from celery import current_app
from pymongo import Connection
import celery.schedules
import pdb

class MongoScheduleEntry(ScheduleEntry):
    
    def __init__(self, task,db):
        self._task = task
        self.db=db
        self.app = current_app._get_current_object()
        self.name = self._task["name"]
        self.task = self._task["task"]
        
        if "interval" in  self._task:
            interval= self._task["interval"]
            if "period" in interval and "every" in interval:
                period=interval["period"]
                every= interval["every"]  
            else:
                self._task["enabled"]=False
                return

            self.schedule =  celery.schedules.schedule(datetime.timedelta(**{period: every}))
           
        if "crontab" in  self._task:
            tab= self._task["crontab"]
            if "minute" in tab and "hour" in tab and "day_of_week" in tab and "day_of_month" in tab and "month_of_year" in tab:
                minute=tab["minute"]
                hour= tab["hour"]  
                day_of_week= tab["day_of_week"]  
                day_of_month= tab["day_of_month"]  
                month_of_year= tab["month_of_year"]  
            else:
                self._task["enabled"]=False
                return
            self.schedule =  celery.schedules.crontab(minute=minute,
                                     hour=hour,
                                     day_of_week=day_of_week,
                                     day_of_month=day_of_month,
                                     month_of_year=month_of_year)
  
            
        self.args = self._task["args"] if "args" in self._task else ""
        self.kwargs = self._task["kwargs"] if "args" in   self._task else ""
        self.options = {
            'queue': self._task["queue"] if "queue" in  self._task else "" ,
            'exchange': self._task["exchange"] if "exchange" in  self._task else "",
            'routing_key': self._task["routing_key"] if "routing_key" in  self._task else "",
            'expires': self._task["expires"] if "expires" in  self._task else None
        }
        if "total_run_count" not in  self._task:
            self._task["total_run_count"] = 0
        self.total_run_count = self._task["total_run_count"]
        
        if "last_run_at" not in self._task:
            self._task["last_run_at"] = self._default_now()
        self.last_run_at = self._task["last_run_at"]

    def _default_now(self):
        return self.app.now()
        
    def next(self):
        self._task["last_run_at"] = self.app.now()
        self._task["total_run_count"] += 1
        return self.__class__(self._task,self.db)
        
    __next__ = next
    
    def is_due(self):
        if not self._task["enabled"]:
            return False, 5.0   # 5 second delay for re-enable.
        return self.schedule.is_due(self.last_run_at)
        
    def __repr__(self):
        return '<MongoScheduleEntry ({0} {1}(*{2}, **{3}) {{4}})>'.format(
            self.name, self.task, self.args,
            self.kwargs, self.schedule,
        )
        
    def reserve(self, entry):
        new_entry = Scheduler.reserve(self, entry)
        return new_entry

    def save(self):
        if self.total_run_count > self._task["total_run_count"]:
            self._task["total_run_count"] = self.total_run_count 
        if self.last_run_at and self._task["last_run_at"] and self.last_run_at > self._task["last_run_at"]:
            self._task["last_run_at"] = self.last_run_at
       
        #if "crontab" in  self._task:
        #         self.db.update({"_id":self._task["_id"]},{"$set":{   "total_run_count":self._task["total_run_count"],"last_run_at":self._task["last_run_at"] ,
        #              "queue":self._task["queue"],
        #              "exchange":self._task["exchange"],
        #              "routing_key":self._task["routing_key"],
        #              "expires":self._task["expires"],
        #              "args":self._task["args"],
        #              "kwargs":self._task["kwargs"],
        #               "crontab":self._task["crontab"],
                                                          
        #                                                  }})
        #if "interval" in  self._task:
        #    self.db.update({"_id":self._task["_id"]},{"$set":{   "total_run_count":self._task["total_run_count"],"last_run_at":self._task["last_run_at"] ,
        #              "queue":self._task["queue"],
        #              "exchange":self._task["exchange"],
        #              "routing_key":self._task["routing_key"],
        #              "expires":self._task["expires"],
        #              "args":self._task["args"],
        #              "kwargs":self._task["kwargs"],
        #               "interval":self._task["interval"],
                                                          
        #                                                  }})
        self.db.update({"_id":self._task["_id"]},{"$set":{   "total_run_count":self._task["total_run_count"],"last_run_at":self._task["last_run_at"] }})
    

class MongoScheduler(Scheduler):

    
    # how often should we sync in schedule information
    # from the backend mongo database
    UPDATE_INTERVAL = datetime.timedelta(seconds=5)
    
    Entry = MongoScheduleEntry
   
    def __init__(self, *args, **kwargs):

        if hasattr(current_app.conf, "CELERY_MONGODB_SCHEDULER_DB"):
            db = current_app.conf.CELERY_MONGODB_SCHEDULER_DB
        else:
            db = "celery"
        if hasattr(current_app.conf, "CELERY_MONGODB_SCHEDULER_COLLECTION") \
            and current_app.conf.CELERY_MONGODB_SCHEDULER_COLLECTION:
            collection=current_app.conf.CELERY_MONGODB_SCHEDULER_COLLECTION
        else:
            collection="schedules"

        if hasattr(current_app.conf, "CELERY_MONGODB_SCHEDULER_URL"):
              
             connection=Connection(current_app.conf.CELERY_MONGODB_SCHEDULER_URL) 
             get_logger(__name__).info("backend scheduler using %s/%s:%s",
                    current_app.conf.CELERY_MONGODB_SCHEDULER_DB,
                    db,collection)
        else:
            connection=Connection() 


        self.db=connection[db][collection]
  
        self._schedule = {}
        self._last_updated = None
        Scheduler.__init__(self, *args, **kwargs)
        self.max_interval = (kwargs.get('max_interval') \
                or self.app.conf.CELERYBEAT_MAX_LOOP_INTERVAL or 300)
      
    def setup_schedule(self):
        pass
    
    def requires_update(self):
        """check whether we should pull an updated schedule
        from the backend database"""
        if not self._last_updated:
            return True
        return self._last_updated + self.UPDATE_INTERVAL < datetime.datetime.now()
        
    def get_from_database(self):
        #get_logger(__name__).info("Checking Schedules")
        self.sync()
        d = {}
        for doc in self.db.find():
            d[doc["name"]] = MongoScheduleEntry(doc,self.db)
        return d
     
    @property
    def schedule(self):
        if self.requires_update():
            self._schedule = self.get_from_database()
            self._last_updated = datetime.datetime.now()
        return self._schedule
    
    def sync(self):
        for entry in self._schedule.values():
            entry.save()

