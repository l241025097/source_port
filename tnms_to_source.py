#!usr/bin/python
#coding: utf8

import sys
import DBM
import cx_Oracle
import pymongo
from print_r import print_r
from pandas import DataFrame, read_csv, merge
from multiprocessing import freeze_support, Process
import urllib
reload(sys)
sys.setdefaultencoding('utf8')

def connect_cs():
    connect_tuple = DBM.DBM().dbhr_cs()
    dbh = cx_Oracle.Connection(*connect_tuple)
    sth = dbh.cursor()
    return dbh, sth

def connect_source(host='127.0.0.1', port='61111', user='luoyl25', pwd='S198641cn', db='front_source'):
    '''连接数据库'''
    pwd = urllib.quote_plus(pwd)
    connect_str = 'mongodb://' + user + ':' + pwd + '@' + host + ':' + port + '/' + db
    client = pymongo.MongoClient(connect_str)
    dbh = client[db]
    return dbh

def get_tnms(sign):
    '''
    网元：
    0 设备名称 equip_name 设备名称
    1 设备别名 equip_alias 设备别名
    2 网络级别 NET_LEVEL 网络级别
    3 网络层次 TRS_LEVEL 网络层次
    4 所属网管名称 belong_net_manager 所属网管名称
    5 所属机房设备放置点 belong_room 所属机房/设备放置点
    6 设备网管标识 equip_net_id 设备网管标识
    7 设备厂家 oem 设备厂家
    8 设备型号 type 设备型号
    9 设备类型 kind 设备类型
    10 维护状态 maintain_state 维护状态
    11 维护方式 maintain_type 维护方式
    12 维护单位 maintainer 维护单位
    13 备注 memo 备注
    14 所属地市 所属传输系统
    端口：
    0 设备网管标识
    1 设备名称 设备名称 设备名称
    2 机架名称 机架名称 机架名称
    3 机框名称 机框名称 机框名称
    4 机框序号 机框序号 机框序号
    5 机框下插槽序号 机框下插槽序号 机框下插槽序号
    6 板卡名称 板卡名称 板卡名称
    7 板卡网管标示
    8 板卡类型 板卡类型 板卡类型
    9 板卡型号 板卡型号 板卡型号
    10 主备方式 主备方式 主备方式
    11 板卡序列号 板卡序列号 板卡序列号
    12 端口介质类型 端口介质类型 端口介质类型
    13 端口序号 端口序号 端口序号
    14 端口名称 端口名称 端口名称
    15 端口状态 端口状态 端口状态
    16 端口速率 端口速率 端口速率
    17 端口网管标识 端口网管标识 端口网管标识
    18 所属厂家
    19 设备类型
    '''
    dbh, sth = connect_cs()
    if sign == 'ne':
        sql = u"select \
                te.label_cn \"设备名称\", \
                te.native_ems_name \"设备别名\", \
                decode((select d.data_type from district d,nms_system n where d.cuid=n.related_space_cuid and n.cuid=te.related_ems_cuid),'2','二干','3','本地') \"网络级别\", \
                decode(te.service_level,'3','骨干层','5','汇聚层','2','核心层','4','骨干层','6','接入层','1','核心层') \"网络层次\", \
                (select nm.label_cn from nms_system nm where nm.cuid=te.related_ems_cuid) \"所属网管名称\", \
                nvl((select r.label_cn from room r where r.cuid = te.related_room_cuid),'未知') \"所属机房设备放置点\", \
                te.cuid \"设备网管标识\", \
                (select dv.label_cn from device_vendor dv where dv.cuid=te.related_vendor_cuid) \"设备厂家\", \
                nvl((select nmc.product_model from ne_model_cfg_type nmc where nmc.cuid=te.model),'未知') \"设备型号\", \
                decode(te.signal_type,'1','SDH','2','PDH','3','WDM','11','IPRAN','7','PDH微波') \"设备类型\", \
                decode(te.live_cycle,'1','正常','2','在建','3','废弃') \"维护状态\", \
                decode(te.maint_mode,'1','自维','2','代维') \"维护方式\", \
                '1' \"维护单位\", \
                '1' \"备注\", \
                (select d.label_cn from district d where d.cuid=substr(te.related_district_cuid,0,26)) \"所属地市\" \
                from trans_element te"
    elif sign == 'port':
        sql = u"select \
                (select te.cuid from trans_element te where te.cuid = p.related_ne_cuid) \"设备网管标识\", \
                (select te.label_cn from trans_element te where te.cuid = p.related_ne_cuid) \"设备名称\", \
                '1' \"机架名称\", \
                '1' \"机框名称\", \
                '1' \"机框序号\", \
                '1' \"机框下插槽序号\", \
                (select c.label_cn from card c where c.cuid=p.related_card_cuid) \"板卡名称\", \
                (select c.cuid from card c where c.cuid=p.related_card_cuid) \"板卡网管标示\", \
                nvl((select ck.card_remark from card c,card_kind ck where c.cuid=p.related_card_cuid and c.model=ck.cuid),'未知') \"板卡类型\", \
                (select ck.cardtype_name from card c,card_kind ck where c.cuid=p.related_card_cuid and c.model=ck.cuid) \"板卡型号\", \
                '1' \"主备方式\", \
                (select eh.holder_name from card c,equipment_holder eh where c.cuid=p.related_card_cuid and c.related_upper_component_cuid=eh.cuid) \"板卡序列号\", \
                decode(p.port_type, '1','电端口','2','光端口','3','适配口','4','逻辑端口') \"端口介质类型\", \
                p.port_no \"端口序号\", \
                p.label_cn \"端口名称\", \
                decode(p.port_state, '1', '空闲', '2', '占用','3','预占','4','损坏') \"端口状态\", \
                decode(p.port_rate, '0','未知','1','2M','2','8M','3','10M','4','45M','5','140M','9','155M','13','622M','15','1250M','16','2.5G','17','10G','34','100M','35','GE','36','10GE') \"端口速率\", \
                p.cuid \"端口网管标识\", \
                (select dv.label_cn from device_vendor dv,card c where dv.cuid = c.vendor and c.cuid = p.related_card_cuid) \"所属厂家\", \
                (select decode(te.signal_type,'1','SDH','2','PDH','3','WDM','11','IPRAN','7','PDH微波') from trans_element te where te.cuid = p.related_ne_cuid) \"设备类型\" \
                from ptp p"
    try:
        sth.execute(sql)
        dbh.commit()
    except Exception.__bases__ as err:
        dbh.rollback()
        print err
    count = 0
    while 1:
        print count
        row = sth.fetchone()
        if not row:
            break
        yield tuple(str(col).decode('gbk') for col in row)
        count += 1

def ne_title():
    return [
        {'ipran_output': u'设备名称', 'cs_output': 'equip_name'},
        {'ipran_output': u'设备别名', 'cs_output': 'equip_alias'},
        {'ipran_output': u'网络级别', 'cs_output': 'NET_LEVEL'},
        {'ipran_output': u'网络层次', 'cs_output': 'TRS_LEVEL'},
        {'ipran_output': u'所属网管名称', 'cs_output': 'belong_net_manager'},
        {'ipran_output': u'所属机房/设备放置点', 'cs_output': 'belong_room'},
        {'ipran_output': u'设备网管标识', 'cs_output': 'equip_net_id'},
        {'ipran_output': u'设备厂家', 'cs_output': 'oem'},
        {'ipran_output': u'设备型号', 'cs_output': 'type'},
        {'ipran_output': u'设备类型', 'cs_output': 'kind'},
        {'ipran_output': u'维护状态', 'cs_output': 'maintain_state'},
        {'ipran_output': u'维护方式', 'cs_output': 'maintain_type'},
        {'ipran_output': u'维护单位', 'cs_output': 'maintainer'},
        {'ipran_output': u'备注', 'cs_output': 'memo'},
        {'ipran_output': u'地市', 'cs_output': 'city'},
    ]

def port_title():
    return [
        {'ipran_port_output': u'设备网管标识', 'cs_port_output': u'设备网管标识'},
        {'ipran_port_output': u'设备名称', 'cs_port_output': u'设备名称'},
        {'ipran_port_output': u'机架名称', 'cs_port_output': u'机架名称'},
        {'ipran_port_output': u'机框名称', 'cs_port_output': u'机框名称'},
        {'ipran_port_output': u'机框序号', 'cs_port_output': u'机框序号'},
        {'ipran_port_output': u'机框下插槽序号', 'cs_port_output': u'机框下插槽序号'},
        {'ipran_port_output': u'板卡名称', 'cs_port_output': u'板卡名称'},
        {'ipran_port_output': u'板卡网管标示', 'cs_port_output': u'板卡网管标示'},
        {'ipran_port_output': u'板卡类型', 'cs_port_output': u'板卡类型'},
        {'ipran_port_output': u'板卡型号', 'cs_port_output': u'板卡型号'},
        {'ipran_port_output': u'主备方式', 'cs_port_output': u'主备方式'},
        {'ipran_port_output': u'板卡序列号', 'cs_port_output': u'板卡序列号'},
        {'ipran_port_output': u'端口介质类型', 'cs_port_output': u'端口介质类型'},
        {'ipran_port_output': u'端口序号', 'cs_port_output': u'端口序号'},
        {'ipran_port_output': u'端口名称', 'cs_port_output': u'端口名称'},
        {'ipran_port_output': u'端口状态', 'cs_port_output': u'端口状态'},
        {'ipran_port_output': u'端口速率', 'cs_port_output': u'端口速率'},
        {'ipran_port_output': u'端口网管标识', 'cs_port_output': u'端口网管标识'},
        {'ipran_port_output': u'所属厂家', 'cs_port_output': u'所属厂家'},
        {'ipran_port_output': u'设备类型', 'cs_port_output': u'设备类型'},
    ]

def get_source_ne(collection, id_key):
    dbh = connect_source()
    repeat_list = []
    source_ne_dict = {}
    count = 0
    for cursor in dbh.get_collection(collection).find({}, {'_id': 0}):
        if cursor[id_key] in source_ne_dict:
            repeat_list.append(cursor)
            continue
        source_ne_dict[cursor[id_key]] = cursor
        print count
        count += 1
    DataFrame(repeat_list).to_excel(collection+'_repeat'+'.xlsx')
    return source_ne_dict

def match_ne(collection, id_key, match_list):
    ''' 匹配网元 '''
    source_dict = get_source_ne(collection, id_key)
    ne_gen = get_tnms('ne')
    dbh = connect_source()

    # 剔除PDH和微波
    filter_list = ['IPRAN'] if collection == 'ipran_output' else ['SDH', 'WDM']
    ne_gen = (ne for ne in ne_gen if ne[9] in filter_list)

    update_list = []
    for ne in ne_gen:
        if ne[6] in source_dict:
            sign_list = []
            for i, match_dict in enumerate(match_list):
                if i in (5, 12, 13, 14):
                    continue
                match_key = match_dict[collection]
                source_value = source_dict[ne[6]][match_key]
                if i == 9:
                    if source_value in ('CSG', 'ASG', 'RSG', 'UTN'):
                        match_str = 'IPRAN'
                    elif source_value == 'OTN':
                        match_str = 'WDM'
                    else:
                        match_str = source_value
                    if ne[i] != match_str:
                        sign_list.append(match_key + u'-更新')
                elif ne[i] != source_value:
                    sign_list.append(match_key + u'-更新')
            sign_str = ';'.join(sign_list)
        else:
            sign_str = u'资源需录入'
        ne_dict = {match_list[i][collection]: detail for i, detail in enumerate(ne)}
        ne_dict['update'] = sign_str
        update_list.append(ne_dict)

    DataFrame(update_list).to_csv(collection+'_upsert.csv', encoding='gbk')

def match_port(collection, id_key, match_list):
    port_gen = get_tnms('port')
    dbh = connect_source()

    # 剔除PDH和微波
    filter_list = ['IPRAN'] if collection == 'ipran_port_output' else ['SDH', 'WDM']
    port_gen = (port for port in port_gen if port[19] in filter_list)

    update_list = []
    for port in port_gen:
        source_dict = dbh.get_collection(collection).find_one({id_key: port[17]}, {'_id': 0})
        if source_dict:
            sign_list = []
            for i, match_dict in enumerate(match_list):
                if i not in (1, 6, 14,):
                    continue
                match_key = match_dict[collection]
                source_value = source_dict[match_key]
                if port[i] != source_value:
                    sign_list.append(match_key + u'-更新')
            sign_str = ';'.join(sign_list)
        else:
            sign_str = u'资源需录入'
        port_dict = {match_list[i][collection]: detail for i, detail in enumerate(port)}
        port_dict['update'] = sign_str
        update_list.append(port_dict)

    DataFrame(update_list).to_csv(collection+'_upsert.csv', encoding='gbk')

def match_for_import():
    ipran_port_df = read_csv('ipran_port_output_upsert.csv', encoding='gbk')
    ipran_port_df = ipran_port_df[ipran_port_df['update'] == u'资源需录入']
    ipran_df = read_csv('ipran_output.csv', encoding='gbk')
    ipran_import_df = merge(ipran_port_df, ipran_df, how='inner', on=u'设备网管标识')
    ipran_import_df[[u'设备名称_x', u'机架名称', u'机框名称', u'机框序号', u'机框下插槽序号', u'板卡名称', u'板卡网管标示', u'板卡类型', u'板卡型号', u'主备方式', u'板卡序列号', u'端口介质类型', u'端口序号', u'端口名称', u'端口状态', u'端口速率', u'端口网管标识']].to_csv('ipran_import.csv', encoding='gbk')

    cs_port_df = read_csv('cs_port_output_upsert.csv', encoding='gbk')
    cs_port_df = cs_port_df[cs_port_df['update'] == u'资源需录入']
    cs_df = read_csv('cs_output.csv', encoding='gbk')
    cs_import_df = merge(cs_port_df, cs_df, how='inner', left_on=u'设备网管标识', right_on='equip_net_id')
    cs_import_df[[u'设备名称', u'机架名称', u'机框名称', u'机框序号', u'机框下插槽序号', u'板卡名称', u'板卡网管标示', u'板卡类型', u'板卡型号', u'主备方式', u'板卡序列号', u'端口介质类型', u'端口序号', u'端口名称', u'端口状态', u'端口速率', u'端口网管标识']].to_csv('cs_import.csv', encoding='gbk')

def match_for_card_id():
    port_gen = get_tnms('port')
    key_list = port_title()
    output_list = []
    id_list = {
        'PTP-0580014959efeaf4015a1c0a678f7066': 1
    }
    for row in port_gen:
        if row[17] in id_list:
            output_list.append({key_list[index]['ipran_port_output']: col for index, col in enumerate(row)})
    DataFrame(output_list).to_csv('temp_card_id.csv', encoding='gbk')

if __name__ == '__main__':
    # freeze_support()
    match_ne('cs_output', 'equip_net_id', ne_title())
    match_ne('ipran_output', u'设备网管标识', ne_title())
    match_port('cs_port_output', u'端口网管标识', port_title())
    match_port('ipran_port_output', u'端口网管标识', port_title())
    match_for_import()
    # match_for_card_id()
