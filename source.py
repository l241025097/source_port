#!usr/bin/python
#coding:utf8
'''获取资源清查系统中的数据'''

import cookielib
import csv
import datetime
import json
import os
import re
import sys
import urllib
import urllib2
from multiprocessing.pool import ThreadPool

import lxml.html
import MySQLdb
import cx_Oracle
import xlrd
import xlwt
import pymongo
from print_r import print_r
import DBM
from collections import namedtuple

reload(sys)
sys.setdefaultencoding('utf8')

def login(cookie_file):
    '''登录生成cookie'''
    cookie_file = 'cookie.txt'
    url = "http://10.245.1.50:9082/resweb_check_guangxi/logon.spr?method=custlogon"
    form = {
        "userName":"luoyl25",
        "password":"1"
    }
    form = urllib.urlencode(form)
    cookie = cookielib.MozillaCookieJar()
    handler = urllib2.HTTPCookieProcessor(cookie)
    opener = urllib2.build_opener(handler)
    opener.addheaders = [(
        "User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0"
    )]
    urllib2.install_opener(opener)
    req = urllib2.Request(url)
    urllib2.urlopen(req)
    cookie.save(cookie_file, ignore_discard=True, ignore_expires=True)
    return cookie_file

def get_data(cookie_file, form):
    '''根据表单获取html'''
    cookie = cookielib.MozillaCookieJar()
    cookie.load(cookie_file, ignore_discard=True, ignore_expires=True)
    handler = urllib2.HTTPCookieProcessor(cookie)
    opener = urllib2.build_opener(handler)
    opener.addheaders = [(
        "User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0"
    )]
    urllib2.install_opener(opener)
    form = urllib.urlencode(form)
    url = "http://10.245.1.50:9082/resweb_check_guangxi/sourcedata.spr"
    req = urllib2.Request(url, data=form)
    res = urllib2.urlopen(req)
    html_content = res.read()
    return html_content.decode('utf8')

def make_data(form, title, file_name):
    '''分页获取html'''
    cookie = login('cookie.txt')
    content = get_data(cookie, form("1", "1", "2"))
    all_num = int(lxml.html.fromstring(content).cssselect("rows#0")[0].attrib['rowcount'])
    step = 1000
    excel_list = [title]
    for page in xrange(1, int(all_num/float(step)) + 2):
        content = get_data(cookie, form(str(page), str(step), str(all_num)))
        tree = lxml.html.fromstring(content)
        max_row = len(tree.cssselect("rows#0 row"))
        for row in xrange(max_row):
            row += 1
            selector_str = "rows row#"+str(row)+" cell"
            cell_obj = tree.cssselect(selector_str)
            excel_list.append([cell.text for cell in cell_obj])
        print file_name, page
    if len(excel_list) >= 65535:
        write_csv(excel_list, file_name)
    else:
        write_data(excel_list, file_name)

def multi_make_data(form, title, file_name):
    '''多进程分页获取html'''
    cookie = login('cookie.txt')
    content = get_data(cookie, form("1", "1", "2"))
    all_num = int(lxml.html.fromstring(content).cssselect("rows#0")[0].attrib['rowcount'])
    step = 1000
    excel_list = [title]
    page_list = count_seperate(all_num, step)
    page_gen = (page + [step, all_num, cookie, form] for page in page_list)
    pool_obj = ThreadPool(10)
    multi_res = [pool_obj.apply_async(start_proc, args=(page_detail,)) for page_detail in page_gen]
    for each_res in multi_res:
        try:
            excel_list.extend(each_res.get())
        except Exception.__bases__ as err:
            print err
    if len(excel_list) >= 65535:
        full_file_name = write_csv(excel_list, file_name)
    else:
        full_file_name = write_csv(excel_list, file_name)
    return full_file_name
    # pool_obj.close()
    # pool_obj.join()
    # delete_db(u'资源清查局站')
    # pool_obj_1 = Pool()
    # for each_res in multi_res:
    #     pool_obj_1.apply_async(insert_db, args=(each_res.get(), u'资源清查局站', title))
    # pool_obj_1.close()
    # pool_obj_1.join()

def count_seperate(total, step):
    '''将所有页数平分为10段'''
    all_page = total/step + 1
    bit = all_page / 10 + 1
    page_list = []
    for index in xrange(0, 10):
        begin = index * bit + 1
        end = (index + 1) * bit + 1
        if begin > all_page:
            break
        else:
            if end <= all_page + 1:
                page_list.append([begin, end])
            else:
                page_list.append([begin, all_page + 1])
    return page_list

def start_proc(page_detail):
    '''启动进程处理分段页数'''
    begin, end, step, all_num, cookie, form = page_detail
    process_list = []
    for page in xrange(begin, end):
        content = get_data(cookie, form(str(page), str(step), str(all_num)))
        tree = lxml.html.fromstring(content)
        max_row = len(tree.cssselect("rows#0 row"))
        for row in xrange(max_row):
            row += 1
            selector_str = "rows row#"+str(row)+" cell"
            cell_obj = tree.cssselect(selector_str)
            process_list.append([cell.text for cell in cell_obj])
        print page
    return process_list

def write_data(data_list, file_name):
    '''将数据写入excel'''
    book = xlwt.Workbook()
    sheet = book.add_sheet(u'sheet1')
    for row, row_value in enumerate(data_list):
        for col, col_value in enumerate(row_value):
            sheet.write(row, col, col_value)
        print row
    file_name += datetime.datetime.now().strftime('%Y%m%d%H%M%S')+'.xls'
    book.save(file_name)
    return file_name

def write_csv(data_list, file_name):
    '''将数据写入csv'''
    # file_name += datetime.datetime.now().strftime('%Y%m%d%H%M%S')+'.csv'
    file_name += '.csv'
    with open(file_name, 'wb') as w_fh:
        csv_w_fh = csv.writer(w_fh)
        for row in data_list:
            csv_w_fh.writerow([col.encode('gbk') for col in row])
    return file_name

def delete_db(table_name):
    '''插入前清空数据库表'''
    conn_dict = {
        'host': 'localhost',
        'user': 'root',
        'passwd': 'Weixin@1026',
        'db': 'blink',
        'port': 3306,
        'charset': 'utf8',
    }
    dbh = MySQLdb.connect(**conn_dict)
    sth = dbh.cursor()
    try:
        sth.execute('truncate table %s'% table_name)
        dbh.commit()
    except Exception.__bases__ as err:
        dbh.rollback()
        print err

def insert_db(data_list, table_name, title):
    '''将数据写入数据库'''
    conn_dict = {
        'host': 'localhost',
        'user': 'root',
        'passwd': 'Weixin@1026',
        'db': 'blink',
        'port': 3306,
        'charset': 'utf8',
    }
    dbh = MySQLdb.connect(**conn_dict)
    sth = dbh.cursor()
    key_str = ','.join(title)
    placeholder_str = ','.join(['%s']*len(title))
    sql = 'insert into %s (%s) value (%s)'% (table_name, key_str, placeholder_str, )
    try:
        sth.executemany(sql, data_list)
        dbh.commit()
    except Exception.__bases__ as err:
        dbh.rollback()
        print str(err).decode('utf8').encode('gbk')
        for index, row in enumerate(data_list):
            row_str = ','.join("'"+dbh.escape_string(str(col))+"'" for col in row)
            sql = 'insert into %s (%s) value (%s)'% (table_name, key_str, row_str, )
            try:
                sth.execute(sql)
                dbh.commit()
            except Exception.__bases__ as err:
                dbh.rollback()
                print str(err).decode('utf8').encode('gbk')
            print index

def update_site():
    '''更新site数据库'''
    db_data = []
    site_dict = {}
    csv_file = get_dir_file('site')
    with open(csv_file, 'rb') as r_fh:
        csv_gen = (tuple(cell.decode('gbk') for cell in line) for line in csv.reader(r_fh))
        for index, line_list in enumerate(csv_gen):
            if index == 0:
                continue
            id_str, tt_id_str, site = str(line_list[0]), str(line_list[4]), line_list[2]
            site_dict[site] = {'city': '', 'region': ''}
            try:
                city = re.search(ur'广西壮族自治区(.*?)市', line_list[1]).group(1)
                site_dict[site]['city'] = city
                if re.search(ur'自治县|管理区$', line_list[1]):
                    region = re.search(ur'.*市(.{2})', line_list[1]).group(1)
                else:
                    region = re.search(ur'.*?市(.*?)[区县市]$', line_list[1]).group(1)
                site_dict[site]['region'] = region
                db_data.append((id_str, tt_id_str, site, city, region))
            except Exception.__bases__ as err:
                print site, err
    return db_data, site_dict

def update_room_point(site_dict):
    db_data = []
    for room_type in ('room', 'stay_point'):
        csv_file = get_dir_file(room_type)
        with open(csv_file, 'rb') as r_fh:
            csv_gen = (tuple(cell.decode('gbk') for cell in line) for line in csv.reader(r_fh))
            for index, line_list in enumerate(csv_gen):
                if index == 0:
                    continue
                id_str = room_type + '_' + line_list[0]
                site_str, room_str = (line_list[1], line_list[2]) if room_type == 'room' else (line_list[2], line_list[1])
                try:
                    city = site_dict[site_str]['city']
                    region = site_dict[site_str]['region']
                    db_data.append((id_str, room_str, site_str, city, region))
                except Exception.__bases__ as err:
                    print err
    return db_data

def connect_mongo_source(host='127.0.0.1', port='61111', user='luoyl25', pwd='S198641cn', db='front_source'):
    '''连接数据库'''
    pwd = urllib.quote_plus(pwd)
    connect_str = 'mongodb://' + user + ':' + pwd + '@' + host + ':' + port + '/' + db
    client = pymongo.MongoClient(connect_str)
    dbh = client[db]
    return dbh

def connect_front():
    connect_key = DBM.DBM().dbhr_front()
    dbh = cx_Oracle.Connection(*connect_key)
    sth = dbh.cursor()
    return dbh, sth

def insert_ne_db(csv_file, collection):
    dbh = connect_mongo_source()
    dbh.get_collection(collection).drop()
    with open(csv_file, 'rb') as r_fh:
        for i, line in enumerate(csv.reader(r_fh)):
            print i
            if i == 0:
                key_list = [col.decode('gbk') for col in line]
            else:
                insert_dict = {key_list[j]: col.decode('gbk') for j, col in enumerate(line)}
                dbh.get_collection(collection).insert(insert_dict)

def create_index(collection, index_dict):
    dbh = connect_mongo_source()
    index_list = [
        (index, pymongo.ASCENDING) if up_down == 1 else (index, pymongo.DESCENDING) \
            for index, up_down in index_dict.viewitems() \
    ]
    sign = dbh.get_collection(collection).create_index(index_list)
    return sign

def match_city():
    '''正则表达式匹配机房和设备放置点的地市'''
    with open('source_detail.txt', 'r') as r_fh:
        city_gen = ( \
            (str(index), line.strip().decode('utf8'), regex_line(line.strip())) \
            for index, line in enumerate(r_fh.readlines()) \
        )
    return write_csv(city_gen, 'city_belong')

def regex_line(line):
    '''正则表达式匹配地市'''
    city_index = (
        u'南宁', u'崇左', u'百色', u'梧州', u'贺州',
        u'柳州', u'来宾', u'钦州', u'北海', u'防城港',
        u'桂林', u'玉林', u'贵港', u'河池'
    )
    for city in city_index:
        if re.match('.*'+city+'.*', line.decode('utf8')):
            return city
    return 'na'

def city_belong_db(file_name, db_table):
    '''将city_belong csv表中的数据插入数据库db_table中'''
    connect_dict = {
        'host': 'localhost',
        'user': 'root',
        'passwd': 'Weixin@1026',
        'port': 3306,
        'db': 'blink',
        'charset': 'utf8'
    }
    dbh = MySQLdb.Connect(**connect_dict)
    sth = dbh.cursor()
    try:
        sth.execute('truncate '+ db_table)
        dbh.commit()
    except MySQLdb.Error as err:
        dbh.rollback()
        print err
    sql = 'insert into '+ db_table + '(id, bs, city) values (%s, %s, %s)'
    with open(file_name, 'rb') as r_fh:
        data_list = [ \
            [col.decode('gbk').encode('utf8') for col in line] for line in csv.reader(r_fh) \
        ]
        try:
            sth.executemany(sql, data_list[1:])
            dbh.commit()
        except MySQLdb.Error as err:
            dbh.rollback()
            print err

def get_dir_file(source_type=''):
    '''获取当前目录下最新的cs_output和ipran_output为前缀的文件名'''
    if source_type == 'cs':
        regex_str = 'cs_output'
    elif source_type == 'ipran':
        regex_str = 'ipran_output'
    elif source_type == 'room':
        regex_str = 'room_output'
    elif source_type == 'stay_point':
        regex_str = 'stay_point_output'
    elif source_type == 'site':
        regex_str = 'site_output'
    current_path = os.path.dirname(__file__)
    if not current_path:
        current_path = os.getcwd()
    file_gen = (
        each_file for _, _, files in os.walk(current_path)\
            for each_file in files if re.match(regex_str, each_file)
    )
    suffix_gen = (
        (
            re.search(regex_str+r'(.*)(\.\w+)', each_file).group(1),
            re.search(regex_str+r'(.*)(\.\w+)', each_file).group(2)
        ) for each_file in file_gen
    )
    suffix = sorted(suffix_gen, key=lambda suffix: suffix[0], reverse=True)[0]
    return regex_str+''.join(suffix)

def make_source_dict(file_name, source_type=''):
    '''将网页上获取的传输资源生成字典'''
    id_col = 7 if source_type == 'cs'else 8
    suffix_str = re.search(r'.*\.(.*)', file_name).group(1)
    if suffix_str in ('xls', 'xlsx'):
        book = xlrd.open_workbook(file_name)
        sheet = book.sheet_by_index(0)
        source_dict = {
            sheet.cell(row, id_col).value: {
                'site': sheet.cell(row, 1).value,
                'room': sheet.cell(row, id_col - 1).value
            } for row in xrange(sheet.nrows)
        }
    elif suffix_str == 'csv':
        with open(file_name, 'rb') as r_fh:
            for line in csv.reader(r_fh):
                print line
    return source_dict

def make_base_dict():
    '''将传输综合网管导出的网元生成字典'''
    book = xlrd.open_workbook(u'新-传输综合网管资源.xlsx')
    sheet = book.sheet_by_index(0)
    return {sheet.cell(row, 0).value: sheet.cell(row, 6).value for row in xrange(sheet.nrows)}

def check_room_point(base_file, base_dict, source_dict):
    '''根据大数据匹配输出文件核查机房是否关联'''
    book = xlrd.open_workbook(base_file)
    sheet = book.sheet_by_index(0)
    w_book = xlwt.Workbook()
    w_sheet = w_book.add_sheet('sheet1')
    zero_row = (u'网元id', u'综合网管名称', u'预测机房', u'资源系统名称', u'资源系统机房', u'名称核查', u'机房核查')
    for col, value in enumerate(zero_row):
        w_sheet.write(0, col, value)
    for row in xrange(sheet.nrows):
        site, room = sheet.cell(row, 0).value, sheet.cell(row, 2).value
        try:
            site_id = base_dict[site]
        except Exception.__bases__ as err:
            print site, err
            continue
        write_col = [site_id, site, room, None, None, u'名称有问题', u'机房有问题', u'']
        if site_id in source_dict:
            write_col[3] = source_dict[site_id]['site']
            write_col[4] = source_dict[site_id]['room']
            write_col[5] = u'名称一致'if source_dict[site_id]['site'] == site else u'名称不一致'
            write_col[6] = u'机房一致'if source_dict[site_id]['room'] == room else u'机房不一致'
        if write_col[6] == u'机房不一致':
            classified, _ = pick_one(site, room, source_dict[site_id]['room'])
            write_col[7] = u'建议修改'if classified == 0 else u'维持原机房'
        for col, value in enumerate(write_col):
            w_sheet.write(row + 1, col, value)
    w_book.save('check_'+base_file)

def pick_one(pre_str, phrase_0, phrase_1):
    '''贝叶斯分类从两个机房中选出最相似的一个'''
    # from jieba import cut
    from math import log
    match_tuple = (phrase_0, phrase_1)
    symbol_tuple = (
        '#', '(', ')', '_', '?', '\\', '-', '/', '!', ',', r'\s', r'\t', '',
        u'（', u'）', u'？', u'、', u'！', u'，', u'。',
        u'基', u'站', u'局', u'机', u'房', u'设', u'备', u'放', u'置', u'点', u'传', u'输', u'汇', u'聚'
    )
    vector_set = set([
        word for phrase in match_tuple \
            for word in phrase \
                if word not in symbol_tuple \
    ])
    vector_list = [[1] * len(vector_set), [1] * len(vector_set)]
    for classified, phrase in enumerate(match_tuple):
        for word in phrase:
            if word in vector_set:
                index = list(vector_set).index(word)
                vector_list[classified][index] += 1
    pre_vector = [0] * len(vector_set)
    input_str = pre_str
    cut_prefix = re.search(ur'^\d+(.*)', input_str)
    if cut_prefix:
        input_str = cut_prefix.group(1)
    cut_suffix = re.search(ur'(.*?)\d+$', input_str)
    if cut_suffix:
        input_str = cut_suffix.group(1)
    for word in input_str:
        if word in vector_set:
            index = list(vector_set).index(word)
            pre_vector[index] += 1
    p_list = [
        sum(pre_vector[index] * log(num/float(sum(vector) + 2)) for index, num in enumerate(vector))
        for vector in vector_list
    ]
    if p_list[0] == p_list[1]:
        return 1, match_tuple[1]
    return p_list.index(max(p_list)), match_tuple[p_list.index(max(p_list))]

def make_port_dict(file_name, begin_row, key_num):
    '''将读取的csv整理成为字典'''
    with open(file_name, 'rb') as r_fh:
        return {
            line_list[key_num]: line_list\
                for index, line_list in enumerate(csv.reader(r_fh))\
                    if index >= begin_row
        }

def match_with_cs(file_name, cs_dict, ipran_dict, begin_row, key_num):
    ''''''
    with open(file_name, 'rb') as r_fh:
        no_list = [
            line_list for index, line_list in enumerate(csv.reader(r_fh))\
                if index >= index\
                    if line_list[key_num] not in cs_dict and line_list[key_num] not in ipran_dict
        ]
    now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    with open('no_port_'+now_str+'.csv', 'wb') as w_fh:
        writer = csv.writer(w_fh)
        writer.writerows(no_list)

def match_with_card(file_name, card_dict, begin_row, key_num):
    # 44
    with open(file_name, 'rb') as r_fh:
        no_list = [
            line_list for index, line_list in enumerate(csv.reader(r_fh))\
                if index >= index\
                    if line_list[key_num] not in card_dict
        ]
    now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    with open('no_card_'+now_str+'.csv', 'wb') as w_fh:
        writer = csv.writer(w_fh)
        writer.writerows(no_list)

def check_port_rate(port_dict):
    id_str = '\n'.join(key for key, value in port_dict.viewitems() if value[15] == '2.5G' and value[19].decode('gbk') == u'验证通过')
    with open('port_id.txt', 'w') as w_fh:
        w_fh.write(id_str)
    pass

def cs_title():
    '''传输设备表头'''
    return [
        'ID', 'equip_name', 'equip_alias', 'NET_LEVEL', 'TRS_LEVEL', 'belong_net_manager',
        'belong_room', 'equip_net_id', 'oem', 'type', 'kind', 'maintain_state', 'maintain_type',
        'maintainer', 'memo', 'BATCH_CODE', 'VAIL_DESC', 'VAIL_STATUS', 'TIME_STAMP',
        'IS_UNIQUE', 'IS_CHECK', 'CHECK_RELA_ID', 'EDIT_STATE'
    ]

def cs_port_title():
    '''传输端口表头'''
    return [
        u'ID', u'设备名称', u'机架名称', u'机框名称', u'机框序号', u'机框下插槽序号', u'板卡名称',
        u'板卡类型', u'板卡型号', u'主备方式', u'板卡序列号', u'端口介质类型', u'端口序号', u'端口名称',
        u'端口状态', u'端口速率', u'端口网管标识', u'批次号', u'验证信息', u'状态', u'时间戳', u'重复性校验',
        u'清查审核状态', u'核查relaId', u'数据核查状态'
    ]

def ipran_title():
    '''ipran设备表头'''
    return [
        u'ID', u'设备名称', u'设备别名', u'网络级别', u'网络层次', u'所属网管名称',
        u'所属传输系统', u'所属机房/设备放置点', u'设备网管标识', u'设备厂家', u'设备型号',
        u'设备类型', u'维护状态', u'维护方式', u'维护单位', u'备注', u'批次号', u'验证信息',
        u'状态', u'时间戳', u'重复性校验', u'清查审核状态', u'核查relaId', u'数据核查状态',
    ]

def ipran_port_title():
    '''ipran设备表头'''
    return [
        u'ID', u'设备名称', u'机架名称', u'机框名称', u'机框序号', u'机框下插槽序号', u'板卡名称',
        u'板卡类型', u'板卡型号', u'主备方式', u'板卡序列号', u'所属母卡序号', u'端口介质类型',
        u'端口序号', u'端口名称', u'端口状态', u'端口速率', u'端口网管标识', u'批次号', u'验证信息',
        u'状态', u'时间戳', u'重复性校验', u'清查审核状态', u'核查relaId', u'数据核查状态',
    ]

def site_title():
    '''局站表头'''
    return [
        u'ID', u'所属区域', u'局站名称', u'局站类型', u'铁塔站址编码', u'局站地址',
        u'局站标准地址', u'局站等级', u'经度', u'纬度', u'批次号', u'验证信息',
        u'状态', u'时间戳', u'重复性校验', u'清查审核状态', u'核查relaId', u'数据核查状态',
    ]

def room_title():
    '''机房表头'''
    return [
        u'ID', u'所属局站', u'机房名称', u'机房地址', u'机房标准地址', u'机房常用名称',
        u'机房归属', u'是否共享', u'与那家运营商共享', u'机房等级', u'机房所属专业',
        u'是否基站机房', u'机房类型', u'机房长本属性', u'维护方式', u'维护单位', u'机房负责人',
        u'经度', u'纬度', u'批次号', u'验证信息', u'状态', u'时间戳', u'重复性校验',
        u'清查审核状态', u'核查relaId', u'数据核查状态',
    ]

def stay_point_title():
    '''设备放置点表头'''
    return [
        u'ID', u'设备放置点名称', u'所属局站名称', u'设备放置点地址', u'设备放置点标准地址',
        u'经度', u'纬度', u'批次号', u'验证信息', u'状态', u'时间戳', u'重复性校验',
        u'清查审核状态', u'核查relaId', u'数据核查状态',
    ]

def enodeb_title():
    '''4G基站表头'''
    return [
        'ID', 'eqp_name', 'eqp_type', 'nm_res_id', 'eqp_alias', 'spc_room', 'bts_type',
        'mme', 'SGW', 'station_class', 'pub_mfk', 'mnt_state', 'nmt_mode', 'nmt_unit',
        'cycle', 'software_version', 'hardware_version', 'ems_name', 'BATCH_CODE', 'VAIL_DESC',
        'VAIL_STATUS', 'TIME_STAMP', 'IS_UNIQUE', 'IS_CHECK', 'CHECK_RELA_ID', 'EDIT_STATE'
    ]

def cs_sys_title():
    '''传输系统表头'''
    return [
        u'ID', u'系统名称', u'系统别名', u'系统类型', u'系统级别', u'网络层次', u'备注', u'批次号',
        u'验证信息', u'状态', u'时间戳', u'重复性校验', u'清查审核状态', u'核查relaId', u'数据核查状态'
    ]

def cs_form(current_page, step_page, all_page):
    '''传输设备表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000000637",
        "data":"%5B%5D",
        "type":"1",
        "auditStatus":"-1",
        "batchNumber":"",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def cs_port_form(current_page, step_page, all_page):
    '''传输设备表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000000635",
        "data":"",
        "type":"1",
        "auditStatus":"-1",
        "batchNumber":"",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def ipran_form(current_page, step_page, all_page):
    '''ipran设备表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000001089",
        "data":"",
        "type":"1",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def ipran_port_form(current_page, step_page, all_page):
    '''ipran设备表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000001090",
        "data":"",
        "type":"1",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def site_form(current_page, step_page, all_page):
    '''局站表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000000623",
        "data":"",
        "type":"1",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def room_form(current_page, step_page, all_page):
    '''机房表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000000624",
        "data":"",
        "type":"1",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def stay_point_form(current_page, step_page, all_page):
    '''设备放置点表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000000743",
        "data":"",
        "type":"1",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def enodeb_form(current_page, step_page, all_page):
    '''设备放置点表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000001095",
        "data":"",
        "type":"1",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def cs_sys_form(current_page, step_page, all_page):
    '''传输系统表单'''
    form = {
        "method":"queryResourceData",
        "itemId":"1000001088",
        "data":"",
        "type":"1",
        "curr":current_page,
        "len":step_page,
        "count":all_page,
        "pagingDisplay":{
            "0": "default",
            "1": "default"
        }
    }
    return form

def my_urljoin(*args):
    '''合并url'''
    patern = re.compile(r'(^(\\+|\/+))|((\\+|\/+)$)')
    return r'/'.join(re.sub(patern, '', path) for path in args)

def query(special, service, conditions=[], page_size=20, page_num=1):
    '''http://***.***.***.***/服务类别标识/专业标识/服务标识？province=UN&参数列表'''
    '''服务类别标识：unicom_res_service
    省分代码对照-广西：45'''
    service_type = 'query'
    query_param = {
        'pageSize': page_size,
        'pageNum': page_num,
        'queryConditions': conditions
    }
    query_dict = {
        'province': '45',
        'QueryParam': json.dumps(query_param)
    }
    url = 'http://10.245.3.221:8888/res_guangxi/unicom_res_service'
    query_url = my_urljoin(*(url, service_type, special, service))
    query_str = urllib.urlencode(query_dict)
    req = urllib2.Request(query_url, query_str)
    res = urllib2.urlopen(req)
    res_dict = json.loads(res.read().decode('utf8'))
    return res_dict

def query_value(key, value):
    query_dict = {
        'OPR_STATE_ID': {
            'special': 'pub',
            'service': 'queryDescInfo.out',
            'conditions': [{'SERIAL_NO': value}, {'KEYWORD': 'OPERATE_STATE'}],
            'zh': 'DESC_CHINA'
        },
        'MNT_STATE_ID': {
            'special': 'pub',
            'service': 'queryDescInfo.out',
            'conditions': [{'SERIAL_NO': value}, {'KEYWORD': 'MAINTENANCE_STATE'}],
            'zh': 'DESC_CHINA'
        },
        'PORT_RATE': {
            'special': 'pub',
            'service': 'queryDescInfo.out',
            'conditions': [{'SERIAL_NO': value}],
            'zh': 'DESC_CHINA'
        }
    }
    res_key = query_dict[key]['zh']
    res_dict = query(query_dict[key]['special'], query_dict[key]['service'], query_dict[key]['conditions'])
    if res_dict['datas']:
        return res_dict['datas'][0][res_key]

def make_row_dict(sth):
    Row = namedtuple('Row', (des[0] for des in sth.description))
    return Row

def cs_port_group_list():
    return  [
        # 'POSITION', # 端口编码
        'PORT_TYPE_ID', # 端口光电类型，固定值1：电端口；2：光端口；3：逻辑端口；4：适配口
        'OPR_STATE_ID', # 业务状态，keyword: OPERATE_STATE pub queryDescInfo.out
        'MNT_STATE_ID', # 维护状态，keyword: MAINTENANCE_STATE pub queryDescInfo.out
        'LINK_FLAG', # 关联标志，固定值0
        'DELETE_STATE', # 资源删除状态，固定值0
        'RES_TYPE_ID', # 资源类型固定值310
        'PORT_RATE', # 端口速率，keyword: RME_TRS_LGC_PORT_RATE pub queryDescInfo.out
        'IS_LOG_PORT', # 查不到，固定值0 否
        'SUPER_RES_TYPE', # 所属设备类型，固定值1053 传输网元
        'SPECIALITY_ID' # 专业类型: 固定值50 传输
    ]

def cs_port_regular_dict():
    return {
        'PORT_TYPE_ID': {'zh': u'端口光电类型', '1': u'电端口', '2': u'光端口', '3': u'逻辑端口', '4': u'适配口'},
        'LINK_FLAG': {'zh': u'关联标志', '0': u'未知', '1': u'未知'},
        'DELETE_STATE': {'zh': u'资源删除状态', '1': u'已删除', '0': u'未删除'},
        'RES_TYPE_ID': {'zh': u'资源类型', '310': u'端口'},
        'IS_LOG_PORT': {'zh': u'是否逻辑端口', '0': u'未知', '1': u'未知'},
        'SUPER_RES_TYPE': {'zh': u'所属设备类型', '1053': u'传输网元'},
        'SPECIALITY_ID': {'zh': u'专业类型', '50': u'传输'},
    }

def front_key_group(group_list, regular_dict, front_table):
    group_dict = {}
    dbh, sth = connect_front()
    for group_key in group_list:
        sql = 'SELECT %s from %s WHERE DELETE_STATE=0 GROUP BY %s' % (group_key, front_table, group_key)
        try:
            sth.execute(sql)
            sth.rowfactory = make_row_dict(sth)
            dbh.commit()
        except Exception.__bases__ as err:
            dbh.rollback()
            print err
        while 1:
            row = sth.fetchone()
            if not row:
                break
            row_dict = row._asdict()
            value = row_dict[group_key]
            if value:
                value = str(value)
                if group_key in regular_dict:
                    continue
                zh_str = query_value(group_key, value)
                if group_key not in group_dict:
                    group_dict[group_key] = {}
                if value not in group_dict[group_key]:
                    group_dict[group_key][value] = zh_str
    group_dict.update(regular_dict)
    return group_dict

def decode_row(row_dict):
    for key, value in row_dict.viewitems():
        if value and not isinstance(value, datetime.datetime):
            value = str(value).decode('gbk')
        yield (str(key).decode('gbk'), value)

def change_row_dict(front_key_dict, row_gen):
    for key, value in row_gen:
        if value and key in front_key_dict:
            value = front_key_dict[key][value]
        yield (key, value)

def get_front_data(front_key_dict, front_table):
    dbh, sth = connect_front()
    sql = 'select * from %s a where a.DELETE_STATE=0' % front_table
    try:
        sth.execute(sql)
        sth.rowfactory = make_row_dict(sth)
        dbh.commit()
    except Exception.__bases__ as err:
        dbh.rollback()
        print err
    count = 0
    while 1:
        # if count > 100:
        #     break
        print count
        count += 1
        row = sth.fetchone()
        if not row:
            break
        row_dict = row._asdict()
        row_gen = decode_row(row_dict)
        yield dict(change_row_dict(front_key_dict, row_gen))
