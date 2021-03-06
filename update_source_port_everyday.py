#!usr/bin/python
#coding: utf8

import sys
from source import *
from pandas import DataFrame
from print_r import print_r

reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == '__main__':
    # cs_file = multi_make_data(cs_form, cs_title(), 'cs_output')
    # ipran_file = multi_make_data(ipran_form, ipran_title(), 'ipran_output')
    # insert_ne_db(cs_file, 'cs_output')
    # insert_ne_db(ipran_file, 'ipran_output')
    # cs_port_file = multi_make_data(cs_port_form, cs_port_title(), 'cs_port_output')
    # ipran_port_file = multi_make_data(ipran_port_form, ipran_port_title(), 'ipran_port_output')
    # insert_ne_db(cs_port_file, 'cs_port_output')
    # insert_ne_db(ipran_port_file, 'ipran_port_output')
    # create_index('cs_port_output', {u'端口网管标识': 1})
    # create_index('ipran_port_output', {u'端口网管标识': 1})
    
    front_key_dict = front_key_group(cs_port_group_list(), cs_regular_dict(), 'port')
    DataFrame(get_front_data(front_key_dict, 'port')).to_csv('cs_port_output.csv', encoding='gbk')
    insert_ne_db('cs_port_output.csv', 'cs_port_output')
    create_index('cs_port_output', {'NMS_ORIG_RES_ID': 1})
    front_key_dict = front_key_group(cs_ne_group_list(), cs_regular_dict(), 'ne')
    DataFrame(get_front_data(front_key_dict, 'ne')).to_csv('cs_output.csv', encoding='gbk')
    insert_ne_db('cs_output.csv', 'cs_output')
    create_index('cs_output', {'NMS_ORIG_RES_ID': 1})

    # match_with_id('cs_port_id.csv', 'front_port_id.csv')

    match('port', 'NMS_ORIG_RES_ID', {})
    pass
