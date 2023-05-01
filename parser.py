import re
import csv
import logging
import traceback

class acct_parser():
    '''
    lsb.acc has three types of records
        - JOB_FINISH
        - EVENT_ADRSV_FINISH
        - JOB_RESIZE
    '''
    def __init__(self):
        self.acct_fields = {
                'JOB_RESIZE': ['Event Type', 'Event Time','jobId','tdx','startTime','userId',
                               'userName','resizeType','lastResizeTime','numExecHosts',
                               'execHosts','numResizeHosts','resizeHosts','numAllocSlots',
                               'allocSlots','numResizeSlots','resizeSlots'],
                'JOB_FINISH': ['Event Type', 'Version Number', 'Event Time', 'jobId', 'userId', 
                               'options', 'numProcessors', 'submitTime', 'beginTime', 'termTime', 
                               'startTime', 'userName', 'queue', 'resReq', 'dependCond', 
                               'preExecCmd', 'fromHost', 'cwd', 'inFile', 'outFile', 'errFile', 
                               'jobFile', 'numAskedHosts', 'askedHosts', 'numExHosts', 'execHosts', 
                               'jStatus', 'hostFactor', 'jobName', 'command', 'ru_utime', 'ru_stime', 
                               'ru_maxrss', 'ru_ixrss', 'ru_ismrss', 'ru_idrss', 'ru_isrss', 
                               'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock', 
                               'ru_ioch', 'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 
                               'ru_nivcsw', 'ru_exutime', 'mailUser', 'projectName', 'exitStatus', 
                               'maxNumProcessors', 'loginShell', 'timeEvent', 'idx', 'maxRMem', 
                               'maxRSwap', 'inFileSpool', 'commandSpool', 'rsvId', 'sla', 
                               'exceptMask', 'additionalInfo', 'exitInfo', 'warningAction', 
                               'warningTimePeriod', 'chargedSAAP', 'licenseProject', 'app', 
                               'postExecCmd', 'runtimeEstimation', 'jobGroupName', 'requeueEvalues', 
                               'options2', 'resizeNotifyCmd', 'lastResizeTime', 'rsvId', 
                               'jobDescription','submitEXT_Num', 'submitEXT_key', 'submitEXT_value', 
                               'numHostRusage', 'hostRusage_hostname', 'hostRusage_mem', 
                               'hostRusage_swap', 'hostRusage_utime', 'hostRusage_stime', 
                               'options3', 'runLimit', 'avgMem', 'effectiveResReq', 'srcCluster', 
                               'srcJobId', 'dstCluster', 'dstJobId', 'forwardTime', 'flow_id', 
                               'acJobWaitTime', 'totalProvisionTime', 'outdir', 'runTime', 'subcwd', 
                               'num_network', 'networkID', 'networkAlloc_num_window', 
                               'networkAlloc_affinity', 'serial_job_energy', 'cpi', 'gips', 'gbs', 
                               'gflops', 'numAllocSlots', 'allocSlots', 'ineligiblePendTime', 
                               'indexRangeCnt', 'indexRangeStart1', 'indexRangeEnd1', 
                               'indexRangeStep1', 'indexRangeStartN', 'indexRangeEndN', 
                               'indexRangeStepN', 'requeueTime', 'numGPURusages', 'gRusage_hostname', 
                               'gRusage_numKVP', 'gRusage_key', 'gRusage_value', 'storageInfoC', 
                               'storageInfoV', 'finishKVP_numKVP', 'finishKVP_key', 'finishKVP_value'],
                'EVENT_ADRSV_FINISH': ['Event Type', 'Version Number', 'Event Logging Time', 
                                       'Reservation Creation Time', 'Reservation Type', 'Creator ID', 
                                       'Reservation ID', 'User Name', 'Time Window', 'Creator Name', 
                                       'Duration', 'Number of Resources', 'Host Name', 'Number of CPUs', 
                                       'Version number', 'Event Time', 'jobId', 'tdx', 'startTime', 
                                       'userId', 'userName', 'resizeType', 'lastResizeTime', 'numExecHosts', 
                                       'execHosts', 'numResizeHosts', 'resizeHosts', 'numAllocSlots', 
                                       'allocSlots', 'numResizeSlots', 'resizeSlots'],
                }

        # Flexible fields are fields that has variable lengths.
        # The space that take bu these fields are (number of arrays) * (length of array)
        # The key of self.flexible_fields is the number of arrays.
        # The value of self.flexible_fields is the length of each array.
        self.flexible_fields = {
                'JOB_FINISH': {'numAskedHosts'      :   1, # caution! order of this dict matters!
                               'numExHosts'         :   1, 
                               'submitEXT_Num'      :   2,
                               'numHostRusage'      :   5,
                               'num_network'        :   2, 
                               'numAllocSlots'      :   1,
                               'indexRangeCnt'      :   6,
                               'numGPURusages'      :   4, 
                               'storageInfoC'       :   1,
                               'finishKVP_numKVP'   :   2,
                               },
                'EVENT_ADRSV_FINISH': {'Number of Resources': 2, }, # caution! order of this dict matters!
                'JOB_RESIZE': {'numExecHosts'       : 1, 
                               'numResizeHosts'     : 1,
                               'numAllocSlots'      : 1,
                               'numResizeSlots'     : 1,
                               },
                }

        self.int_fields = {
                'JOB_FINISH': ['Event Time', 'jobId', 'userId', 'numProcessors', 'submitTime',
                               'beginTime', 'termTime', 'startTime', 'numAskedHosts', 'numExHosts',
                               'jStatus', 'exitStatus', 'maxNumProcessors', 'idx', 'maxRMem',
                               'maxRSwap', 'numHostRusage', 'lastResizeTime', 'runLimit', 'avgMem',
                               'num_network', 'numAllocSlots', 'numGPURusages', 'acJobWaitTime',
                               ],
                }
        self.float_fields = {
                'JOB_FINISH': ['hostFactor','serial_job_energy']
                }

    def __call__(self, log_line):
        '''
        input: single line of log text
	    return: dict of parsed log.
        '''
        # special case: broken line: line missing heading double quote
        if log_line[0] != '"':
            log_line = '"' + log_line
        # regex: split by space, while ignore contents enclosed by double quotes "*"
        segments = re.split(r'[ ](?=(?:[^"]*"[^"]*")*[^"]*$)', log_line)

        # special case: empty line
        if len(segments) == 0:
            return {}

        for i, seg in enumerate(segments):
            if len(seg) == 0:
                return {}
            if seg[0] == '"':
                # remove the quotes only if the field start with a quote
                segments[i] = seg.strip('"') 
        log_type = segments[0] # type of record
        log_dict = {}

        if log_type in self.acct_fields:
            ## Important: some empty fields will cause misalign, must modify segs
            ## before further processing 
            template = self.acct_fields[log_type]
            if log_type in self.flexible_fields:
                for field, length in self.flexible_fields[log_type].items():
                    # the flexible fields count and the length of each one
                    idx_field = template.index(field)
                    repeat_field = int(segments[idx_field])
                    field_placeholder = [[] for i in range(length)]
                    for i in range(repeat_field):
                        for j in range(length):
                            field_placeholder[j].append(segments.pop(idx_field + 1))
                
                    for i in range(len(field_placeholder)):
                        segments.insert(idx_field+1, field_placeholder.pop(-1)) 

            if len(segments) == len(self.acct_fields[log_type]):
                log_dict = dict({k: v for k, v in zip(self.acct_fields[log_type], segments) if v})
                # correct the type of values
                for int_field in self.int_fields[log_type]:
                    log_dict[int_field] = int(log_dict[int_field])
                for float_field in self.float_fields[log_type]:
                    log_dict[float_field] = float(log_dict[float_field])
                
                # return log dict

            else:
                print('can not parse record: ')
                print(log_line)
                print('\n')
        else:
            print('record type not recognized: ', log_type)
            print(log_line)
            print('\n')

        return log_dict

    def dict2list(self, log_dict):
        template = self.acct_fields[log_dict['Event Type']]
        log_list = []
        for key in template:
            if key in log_dict:
                log_list.append(log_dict[key])
            else:
                log_list.append(None)
        return log_list

if __name__ == '__main__':
    # init parser
    acct_read = acct_parser()
    success = 0
    failed = 0

    # df = pd.DataFrame(columns=acct_read.acct_fields['JOB_FINISH'])
    # init csv
    output_csv = open('lsf.csv', 'w', newline='')
    csv_writer = csv.DictWriter(output_csv, fieldnames = acct_read.acct_fields['JOB_FINISH'])
    csv_writer.writeheader()

    with open('/hpcf/lsf/lsf_prod/work/hpcf_research_cluster/logdir/lsb.acct', 'r', encoding="utf8", errors="ignore") as f:
        for i, log_text in enumerate(f):
            if i % 100 == 0:
                print(f'parsing line {i}')
            if log_text.startswith('"JOB_FINISH"'):
                try:
                    csv_writer.writerow(acct_read(log_text.strip()))
                    success += 1
                except Exception as e:
                    failed += 1
                    logging.error(traceback.format_exc())
            if i == 10000:
                # for testing, quit execution after 10000 logs
                break


    output_csv.close()
    print(f'successful parsed {success} records')
    print(f'failed {failed} records')
    print(f'csv saved to lsf.csv')




