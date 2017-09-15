#!usr/bin/python
#coding:utf8

import chardet

class DBM:

    def dbhl_blink(self):
        return self._key_for_mysql('localhost', 'luoyl25', 'S198641cn', 'blink', 3306, 'utf8')

    def dbhl_splash(self):
        return self._key_for_mysql('localhost', 'luoyl25', 'S198641cn', 'splash', 3306, 'utf8')

    def dbhr_pd(self):
        return self._key_for_mysql('133.0.31.14', 'WGZX', 'Aa123!@#', 'eprocess', 3306, 'utf8')

    def dbhr_zh(self):
        return ('nmosdb', 'nmosoptr', '133.0.129.203:1521/oracle')

    def dbhr_cs(self):
        return ('tnms', 'tnms', '133.0.129.225:1521/tnmsdb')

    def dbhr_front(self):
        return ('res_guangxi_s', 'res_guangxi_s', '172.30.0.2:9999/resjt')

    def _key_for_mysql(self, *par):
        key_index = {}
        index = ['host', 'user', 'passwd', 'db', 'port', 'charset']
        for i in range(6):
            key_index[index[i]] = par[i]
        return key_index

    def o_2_m(self, o_strs):
        if not isinstance(o_strs, (list, tuple)):
            return None
        m_strs = filter(lambda x: x, map(lambda o_str: o_str.decode('gbk').encode('utf8'), o_strs))
        return m_strs
