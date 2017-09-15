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
    front_key_dict = front_key_group(cs_port_group_list(), cs_port_regular_dict(), 'RES_GUANGXI.RME_PORT')
    DataFrame(get_front_data(front_key_dict, 'RES_GUANGXI.RME_PORT')).to_csv('cs_port_output.csv', encoding='gbk')
    insert_ne_db('cs_port_output.csv', 'cs_port_output')
    create_index('cs_port_output', {'NMS_ORIG_RES_ID': 1})
    pass
