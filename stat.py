import os 
import json
import re
import ast
import cx_Oracle
import time
import urllib.parse
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from pytz import timezone

from google.cloud import bigquery
from google.cloud.bigquery.job import UnknownJob
from google.cloud.bigquery.job import LoadJob
from google.cloud.bigquery.job import CopyJob
from google.cloud.bigquery.job import ExtractJob

GBQ_CREDS_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/google-creds.json'
GBQ_ENV_VAR = 'GOOGLE_APPLICATION_CREDENTIALS'

os.environ[GBQ_ENV_VAR] = GBQ_CREDS_PATH


class gbq_driver():

    _gclinet = None
    _job_config = None
    _result_obj = {}
    _jobs_len = 0
    _local_timezone = None

    def __init__(self):
        print('Connecting to GBQ ...')
        self._gclient = bigquery.Client()
        print('Established')
        self._local_timezone = timezone('US/Eastern')
        self._job_config = bigquery.QueryJobConfig(dry_run=True, user_query_cache=False)
        print('Retrieve  list_jobs ... ')
        self._get_job_list_size()
        print('Done')

    def close(self):
        if self._gclient:
            self._gclient.close()

    def _get_job_list_size(self):
        self._jobs_len = sum(1 for _ in self._gclient.list_jobs(all_users=True, min_creation_time = datetime.utcnow() - relativedelta(months=1)))

    def get_job_list_size(self):
        return self._jobs_len

    def _get_affected_object(self, reg=''):
        res = ''
        try:
            if reg:
                objs = reg.split('`')
                if objs and len(objs)>0:
                    res = f"{objs[1]}"
        finally:
                 return res

    def _get_obj(self, pattern, sql):
        affected_objs = ''
        try:
            pattern_regs = re.findall(f'(?i){pattern}.*', sql)
            if len(pattern_regs)>1:
                for regs in pattern_regs:
                    affected_objs += f'{self._get_affected_object(regs)},'

            else:
                affected_objs += self._get_affected_object(*pattern_regs)
        except Exception as e:
            print(e)
        finally:
            return affected_objs

    def _parse_obj(self, sql):
        objs = {}
        obj_insert = self._get_obj('into', sql)
        obj_from = self._get_obj('from', sql)  
        obj_upd = self._get_obj('update', sql)  
        
        objs.update({'ins': obj_insert if obj_insert else None})
        objs.update({'upd': obj_upd if obj_upd else None})
        objs.update({'sel': obj_from if re.findall('(?i)select',sql) else None})
        objs.update({'del': obj_from if re.findall('(?i)delete',sql) else None})
        return objs
        
    def _get_price(self, mem):
        return round((mem/1024/1024/1024/1024)*5) if mem is not None else 0



    def process_job_list(self):
        e_bytes = 0
        t_bytes = 0 
        indx = 0

        for job in self._gclient.list_jobs(all_users=True, min_creation_time = datetime.utcnow() - relativedelta(months=1)):
            if job and not isinstance(job, (UnknownJob, LoadJob, CopyJob, ExtractJob)):
                # print(f'{job.job_id} {type(job)} {indx}')
                self._result_obj.update({ f'indx_{indx}' :
                        {
                            'jid' : job.job_id,
                            'prj' : job.project,
                            'bil' : job.billing_tier,
                            'crt' : job.created,
                            'end' : job.ended, #.strftime("%Y-%m-%d %H:%M:%S"),
                            'dur' : round((job.ended -job.created).total_seconds()),
                            'dry' : job.dry_run,
                            'est' : job.estimated_bytes_processed or 0,
                            'mem' : job.total_bytes_processed or 0,
                            'prc' : self._get_price(job.total_bytes_processed),
                            'eml' : job.user_email,
                            'sql' : job.query,
                            'aff' : job.num_dml_affected_rows,
                            'obj' : self._parse_obj(job.query),
                            'err' : job.error_result 
                                }
                            }
                        
                )
                indx += 1
                t_bytes += job.total_bytes_processed or 0
                if self._result_obj:
                    self._result_obj.update({ f'summary': 
                            {
                                'mem': round(t_bytes/1024/1024),
                                'len': self.get_job_list_size(),
                                'prc': self._get_price(t_bytes)

                            }
                        })

    def get_result_as_dict(self):
        return self._result_obj

    def get_result_as_json(self):
        if self._result_obj:
            for upper_key, upper_val in self._result_obj.items():
                for k, v in upper_val.items():
                    if k in 'crt end':
                        self._result_obj[upper_key][k] = v.strftime("%Y-%m-%d %H:%M:%S")
            return json.dumps(self._result_obj) if self._result_obj else None
        else:
            return None

        

class oracle_driver():
    _conn = None
    _insert_sql = """INSERT INTO GBQ_LOG(
                JOB_ID,
                PROJECT,
                BILLING,
                CREATED,
                ENDED,
                DURATION,
                DRY_RUN,
                ESTIMATED_BYTES,
                TOTAL_BYTES,
                PRICE,
                EMAIL,
                QUERY,
                NUM_DML_AFFECTED_ROWS,
                INS_OBJ,
                SEL_OBJ,
                UPD_OBJ,
                DEL_OBJ,
                ERR_MSG,
                ERR_LOC,
                ERR_RES
            ) VALUES (
                        :jid,
                        :prj,
                        :bil,
                        :crt,
                        :end,
                        :dur,
                        :dry,
                        :est,
                        :mem,
                        :prc,
                        :eml,
                        :sql,
                        :aff,
                        :ins,
                        :sel,
                        :upd,
                        :del,
                        :err_msg,
                        :err_loc,
                        :err_res
            )
        """

    def __init__(self):
        self._connect()


    def _connect(self):
        try:
            self._conn = cx_Oracle.connect("user", "pass", "jdbcurl")
        except Error as e:
            self.close()
            

    def close(self):
        if self._conn is not None:
            self._conn.close() 


    def insert(self, params):
        cursor = self._conn.cursor()
        vals =  (
                    params.get('jid', None),
                    params.get('prj', None),
                    params.get('bil', 0),
                    params.get('crt', None),
                    params.get('end', None),
                    params.get('dur', 0),
                    params.get('dry', None),
                    params.get('est', 0),
                    params.get('mem', 0),
                    params.get('prc', 0),
                    params.get('eml', None),
                    params.get('sql').replace("'", '"') if params.get('sql') else None,
                    params.get('aff', 0),
                    params.get('obj', {}).get('ins', None),
                    params.get('obj', {}).get('sel', None),
                    params.get('obj', {}).get('upd', None),
                    params.get('obj', {}).get('del', None),
                    params.get('err').get('message', None) if params.get('err') else None,
                    params.get('err').get('location', None) if params.get('err') else None,
                    params.get('err').get('reason', None) if params.get('err') else None
                )
        try:
            cursor.execute(self._insert_sql.upper(), vals)
        except Exception as e:
            vals = [*vals]
            vals[11] = None
            cursor.execute(self._insert_sql.upper(), tuple(vals))
        finally:
            self._conn.commit()
            if cursor:
                cursor.close()


class file_driver():
    
    def __init__(self):
        print('Start report save action')

    def save(self, obj):
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/gbq_report_{datetime.now().strftime('%Y%m%d')}.json", 'w') as f:
            f.write(obj)

    def close(self):
        print('End report save action')


class statistics():
    _gbq = None
    _storage = None
    _jobs_report = None
    _start_time = None

    def __init__(self):
        self._start_time = datetime.now()
        self._gbq = gbq_driver()
        print(f'Connection to gbq established')
        self._storage = oracle_driver()
        print(f'Connection to oracle db established')


    def _gather(self):
        print(f'Start gathering')
        print(f'Number of jobs {self._gbq.get_job_list_size()}')
        self._gbq.process_job_list()
        self._jobs_report = self._gbq.get_result_as_dict()

    def _process_report(self):
        if self._jobs_report:
            for k, v in self._jobs_report.items():
                if k.lower() == 'summary':
                    continue
                self._storage.insert(v)     

    def _save_report(self):
        fd = file_driver()
        try:
            fd.save(self._gbq.get_result_as_json())
        finally:
            fd.close()


    def run(self):
        try:
            self._gather()
            self._process_report()
            self._save_report()
            if self._jobs_report:
                print(f"Statistics uploaded. {self._jobs_report['summary']} \nTotal time = {round((datetime.now() - self._start_time).total_seconds())}s")           
            else:
                print(f"Statistics does not processed")           
        finally:
            if self._gbq:
                self._gbq.close()
            if self._storage:
                self._storage.close()


if __name__ == '__main__':
    sts = statistics()
    sts.run()

  


 





