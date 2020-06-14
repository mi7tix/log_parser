#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import logging
import logging.handlers
import argparse
import re
import os
import sys
from datetime import datetime
import json
from collections import namedtuple, defaultdict
import string
import shutil
import gzip
import statistics
from subscription_manager.managerlib import cfg


config_path = './config.json'


config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
}

log_date = namedtuple('LogDate', ['date', 'path'])

def process_args():
    """
    Handle command line arguments
 
    Args:
       None
    Returns:
        None
    """
    parser = argparse.ArgumentParser(description='Alternative config parser.')
    parser.add_argument('--config',
                        help='Config file path'                    
                        )
    parse_args = parser.parse_args()
    return parse_args


def set_logging(cfg):
    loging_path = cfg.get('LOGGING_FILE')
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        datefmt='%Y.%m.%d %H:%M:%S',
        filename=loging_path
    )

def config_read_values(path):
    """
    Parsing config from file
    Args:
        path: path to config file
    Returns:
        config values
    """    
    try:
        with open(path) as cfg_file:
            read_config = json.loads(cfg_file.read(config_path))
            if path:
                alter_config = json.loads(cfg_file.read(path))
                read_config.update(alter_config)
            return read_config
    except Exception as e:
        logger.info("Cannot read config file,{}".format(e))


def init_report_dir(conf):
    report_dir = conf.get('REPORT_DIR')
    if not os.path.exists(report_dir):
        os.mkdir(report_dir)

def last_log_search(log_dir):
    """
    Find last log info
    Args:
        log_dir: log_dir list
    Returns:
        last find logs to current date
    """
    r = re.compile(r'^nginx-access-ui\.log-(?P<date>\d{8})(\.gz)?$')
    for logs in os.listdir(log_dir):
        ref = r.match(logs)
        if not ref:
            continue
        try:
            date_str = ref.groupdict()['date']
            parsed_date = datetime.strptime(date_str, "%Y%m%d")
        except Exception as e:
            logging.info("Cannot initiate last log datetime {}".format(e))
            if not log_date or parsed_date > log_date.date :
                last_log = log_date(parsed_date, os.path.join(log_dir, logs))
                logging.info('Last log found {}'.format(log_date.path))
                return last_log
               

def process_line(line, line_number):
    """
    line parser for values
    """
    values = line.split(' ')
    url = values[7]
    request_time = float(values[-1])
    return {'url': url, 'request_time': request_time}
    
            
def baselog_parser(path,refer_percent = None):
    """
    line parser to fetch urls from gzip arg
    Args:
        base log
    """
    with gzip.open(path, 'rb') if path.endswith('.gz') else open(path) as base_log:
        total_lines = 0
        handle_lines = 0
        for line in base_log:
            parsed_line = process_line(line, total_lines)
            total_lines += 1
            if parsed_line:
                handle_lines += 1
                yield parsed_line
        
        sucseccful_per = float(handle_lines) / float(total_lines)
        logging.info("sucseccful percent {}".format(sucseccful_per * 100))
        if sucseccful_per < float(refer_percent):
            raise RuntimeError('Log parser limit exceeded ')

                
            
def collect_url_data(dct):
    """
    Get total urls data 
    Args:
       All lines from base_log
    Returns:
        Urls_count
    """
    urls = namedtuple('Urls', ['urls', 'count'])
    urls.urls = defaultdict(list)
    urls.count = 0
    urls.total_time = 0
    for url_data in dct:
        k, v = url_data['url'], url_data['request_time']
        urls.urls[k].append(v)
        urls.count += 1
        urls.total_time += v
    return urls
    
def prepare_stat(cfg, urls):
    "Statistic reporting"
    stat_list = []
    for url, req_tm in urls.urls.items():
        stat = {}
        stat["url"] = url
        stat["count"] = len(req_tm)
        stat["count_perc"] = round(100.0 * (stat["count"] / urls.count), 3)
        stat["time_sum"] = round(sum(req_tm), 3)
        stat["time_perc"] = round(100.0 * (stat["time_sum"] /
                                           urls.total_time), 3
                                  )
        stat["time_avg"] = round(stat["time_sum"] / stat["count"], 3)
        stat["time_max"] = round(max(req_tm), 3)
        stat["time_med"] = round(statistics.median(req_tm), 3)

        stat_list.append(stat)

    sorted_stat = (sorted(stat_list, key=lambda k: k["time_sum"], reverse=True)
                   )[:cfg['REPORT_SIZE']]   
    return json.dumps(sorted_stat)

    
def write_report(cfg, path, stat):
    "rewrite html te,plate"
    report_tmp = path + '.tmp'
    with open(cfg.get('REPORT_TEMPLATE'), encoding='utf-8') as rep_template:
        with open(report_tmp, mode='w', encoding='utf-8') as report:
            tmpl = string.Template(rep_template.read())
            logging.info('Writing temporary report {}'.format(report_tmp))
            report.write(tmpl.safe_substitute(table_json=stat))

    logging.info('Moving {0} to {1}'.format(report_tmp, path))
    shutil.move(report_tmp, path)     


def main(config):
        report_dir = config.get('REPORT_DIR')
        if not os.path.exists(report_dir):
            os.mkdir(report_dir)
        last_log = last_log_search(config.get('LOG_DIR'))
        report_path = os.path.join(config.get('REPORT_DIR'),
                                   'report-{0}.html'.format(datetime.strftime(last_log.date,
                                   '%Y.%m.%d'))
                                   )
        logging.info("report path {}".format(report_path))
        parser = baselog_parser(last_log.path, config.get('SUCSESSFUL_PERCENT')) 
        urls = collect_url_data(parser)
        stat = prepare_stat(config, urls)
        write_report(config, report_path, stat)
      

if __name__ == "__main__":
        alter_config = process_args()
        config = config_read_values(alter_config)
        set_logging(config)
        main(config)

    
