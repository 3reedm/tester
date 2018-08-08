#!/usr/bin/python3.6
import json
import psycopg2
#import psycopg2.extras
import sys
from http.client import HTTPConnection
from urllib.parse import urlencode
from random import randint, choice
#import shlex, subprocess
from datetime import datetime
from functools import reduce

class Connector:
    def __init__(self, params=None):
        self._params = params
        self._links = []

    def request(self, params=None):
        pass

    # TODO  perhaps you need to add update, set methods?
    def get_params(self):
        return self._params

    def close(self):
        pass

class DBConnector(Connector):
    def __init__(self, params=None):
        init_params = {
            "host":  "fix-osr-db4.unix.tensor.ru",
            "dbname": "ext",
            "user": "viewer",
            "password": "Viewer1234"
        }
        if (params):
            init_params.update(params)

        super().__init__(init_params)
        self.db = None

    def __del__(self):
        self.close()

    def __db_connect(self, params=None, b_close=False):
        if (not self.db or b_close):
            self.__db_close()
            if (params):
                self._params.update(params)

            auth_params = "host='" + self._params["host"] + "' dbname='" + self._params["dbname"] + "' user='" + self._params["user"] + "' password='" + self._params["password"] + "'"
            try:
                self.db = psycopg2.connect(auth_params)
            except psycopg2.DatabaseError as e:
                if (self.db):
                    self.db.rollback()
                print('Error %s' % e)
                sys.exit(1)

    def __db_close(self):
        if (self.db):
            self.db.close()

    def __db_get(self, str):
        self._links.clear()
        cur = self.db.cursor()
        cur.execute(str)

        while True:
            row = cur.fetchone()
            if row == None:
                break
            self._links.append(row)

        return self._links

    def request(self, params=None):
        tmp_params = {
            "b_close": False,
            "b_save": True,
            "request": "",
            "file_name": "",
            "params": None
        }
        if (params):
            tmp_params.update(params)

        self.__db_connect(tmp_params["params"], tmp_params["b_close"])
        response = self.__db_get(tmp_params["request"])

        return response

    def close(self):
        self.__db_close()

class HTTPConnector(HTTPConnection, Connector):
    def __init__(self, params=None):
        init_params = {
            "server": {
                "host": "test-online.sbis.ru",
                "port": 80
            },
            "request": {
                "method": "POST",
                "url": "http://test-online.sbis.ru/auth/service/sbis-rpc-service300.dll",
                "headers": {
                    "Content-Type": "application/json; charset=UTF-8;",
                    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64)"
                                  " AppleWebKit/537.36 (KHTML, like Gecko)" +
                                  " Chrome/53.0.2785.143 Safari/537.36",
                    "Connection": "keep-alive"
                },
                "body": urlencode({ # TODO json.dumps is better?
                    "jsonrpc": "2.0",
                    "protocol": 4,
                    "method": "LinkDecorator.DecorateAsSvgExt",
                    "params": {
                        "LinksArray": ["https://habr.com/post/414971/",
                                       "https://pre-test-online.sbis.ru/opendoc.html?" +
                                       "guid=d012b11c-b521-4c34-a0ec-a904fb89f1e0"],
                        "login": "{}".format("Демо_тензор"),
                        "password": "{}".format("Демо123")
                    }
                })
            }
        }
        if (params):
            init_params.update(params)

        self._params = init_params
        self._links = []
        super().__init__(host=self._params["server"]["host"], port=self._params["server"]["port"])

    def request(self, params=None):
        tmp_params = {
            "files": ["uuid.csv", "domens.csv"],
            "params": None
        }
        if (params):
            tmp_params.update(params)

        if (tmp_params["params"]):
            self._params["request"].update(tmp_params["params"])

        super().request(method=self._params["request"]["method"], url=self._params["request"]["url"],
                        body=self._params["request"]["body"], headers=self._params["request"]["headers"])

        return self.load_from_files(tmp_params["files"])

    def load_from_files(self, files, n=10, m=1000):
        if (len(files) != 0):
            load_files = {"uuids": None, "domens": None}

            try:
                load_files["uuids"] = [line.strip() for line in open(files[0], 'r')]
                load_files["domens"] =  [line.strip() for line in open(files[1], 'r')]

            except IOError as e:
                print("Ошибка чтения файла при инициализации: ", e)

            n_uuids = len(load_files["uuids"])
            n_domens = len(load_files["domens"])
            for i in range(n):
                self._links.append([])
                for j in range(m):
                    rand_dom = randint(0,n_domens-1)
                    rand_uuid = randint(0,n_uuids-1)
                    link = '"http://%s"' % load_files["domens"][rand_dom]
                    self._links[-1].append(link)
                    link = '"https://test-online.sbis.ru/person/%s"' % load_files["uuids"][rand_uuid]
                    self._links[-1].append(link)
            return self._links
        else:
            return 0

    def get_response(self):
        try:
            response = self.getresponse()
            return response
        except Exception as e:
            print("Connection error: ", e)

    def close(self):
        pass

#abstractclass
class Printer:
    def __init__(self):
        pass

    def out(self, response, type, file_name=None, b_random=False, delim=","):
        pass

    def close(self):
        pass

class CSVPrinter(Printer):
    def __init__(self):
        super().__init__()

    def out(self, response, type, file_name=None, b_random=False, delim=","):
        if (type == 'console'):
            if (response.status == 200):
                data = json.loads(response.read())
                print(data)
            else:
                print("Error: ", response.status, response.reason)

        elif (type == 'file'):
            if (file_name):
                file_out_name = file_name
            else:
                file_out_name = "data.csv" # "out_" + str(datetime.now().strftime("%Y-%m-%d-%H.%M.%S")) + ".csv"

            file_out = open(file_out_name, 'a')
            for line in response:
                try:
                    line.isalpha()
                    rec = ""
                    for elem in line:
                        rec += elem + delim
                    file_out.write(rec[:-1] + ';')
                except:
                    rec = line
                    if (isinstance(rec,tuple)):
                        rec = tuple(item.replace("\"","\'").replace("\\","\\\\") if isinstance(item,str) else item for item in rec)

                        if (len(rec) == 1 and b_random):
                            b_xlist = choice([True,False])
                            b_plain = False if b_xlist else choice([True,False])
                            b_def = True if b_xlist == b_plain else False
                            file_out.write((('%s'+delim)*3+'%s\n') % (rec[0],str(b_xlist).lower(),str(b_plain).lower(),str(b_def).lower()))
                        else:
                            out_format_str = str(('%s'+delim) * len(rec))[:-1] + '\n'
                            file_out.write(out_format_str % rec)
                    elif (isinstance(rec,int)):
                        out_format_str = '%s\n'
                        file_out.write(out_format_str % rec)
                    else:
                        return 0

        elif (type == 'massive'):
            query_face_ids = ', '.join([str(link[0]) for link in response])
            return query_face_ids

        else:
            pass

    def close(self):
        pass

class Tester:
    def __init__(self, connector=None, printer=None):
        self.connector = connector if connector else Connector()
        self.printer = printer if printer else Printer()
        self.params = self.connector.get_params()

    def __enter__(self):
        return self
    #def __exec_console(self, command_line="ls -l"):
        #   python -u ..\utils\sleeps\sleep.py 120
    #    args = shlex.split(command_line)
    #    subprocess.Popen(args)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connector.close()
        self.printer.close()
        if exc_val:
            raise


    def link_decorator_test(self, files=None):
        self.console_log()
        response = self.request()
        self.printer.out(response, "file")

        #command_line = r'echo LinkDecorator test \
        #    call jmeter -n -t .\scripts\link_decor.jmx -l scripts\Reports\linkdecorator.jtl \
        #    python -u ..\utils\apdex_jmeter\apdex.py -l "scripts\Reports\linkdecorator.jtl" -f "scripts\Reports\APDEX_linkdecorator" --new --read "readme.txt"'
        #self.__exec_console(command_line)

    def process_vas_test(self):
        request = 'SELECT "ПроцессВАС", COALESCE("Контрагент", 0) "Контрагент" FROM "СвязьЛицаСуд" WHERE "ТипСвязи" IN (7, 8) LIMIT 1000'
        file_name = "data1.csv"
        response = self.request({"request": request, "file_name": file_name})
        self.printer.out(response, "file", file_name)

        request = 'SELECT "@Лицо" "Суд" FROM "АрбитражныйСуд"'
        response = self.request({"request": request, "file_name": file_name})

        request = 'SELECT DISTINCT ON ("Контрагент") "Контрагент" FROM "СвязьЛицаСуд" WHERE "ТипСвязи" IN (7, 8) AND "Контрагент" > 0 LIMIT 1000'
        file_name = "data2.csv"
        response = self.request({"request": request, "file_name": file_name})
        self.printer.out(response, "file", file_name, True)
        # Повторяем запрос, чтобы избежать кэширования данных
        #response = self.request({"request": request, "b_save": False})
        #query_face_ids = self.printer.out(response, "massive")
        #params = {
        #    "host":  "test-spp-db2.unix.tensor.ru",
        #    "dbname": "agentcheck",
        #    "user": "service_user",
        #    "password": "iiS"
        #}
        #request = 'SELECT "ОГРН" FROM "Контрагент" WHERE "@Лицо" IN (' + query_face_ids + ')'
        request = 'SELECT DISTINCT ON ("Контрагент") "Контрагент" FROM "СвязьЛицаСуд" WHERE "ТипСвязи" IN (7, 8) AND "Контрагент" > 0 LIMIT 3000'
        file_name = "data4.csv"
        response = self.request({"request": request, "file_name": file_name})
        self.printer.out(response, "file", file_name, True)

    def staff_statistics_test(self):
        #StaffStatistics.AgeList
        #StaffStatistics.ChildrenList
        #StaffStatistics.ChildrenListForPrint
        #StaffStatistics.ExperienceList
        #StaffStatistics.FiredList
        #StaffStatistics.GenderList
        #StaffStatistics.HiredList
        #StaffStatistics.LongtimersList
        #StaffStatistics.ProbationList
        #StaffStatistics.TurnoverList
        #StaffStatistics.WorkersList
        #StaffStatistics.DepartmentList
        #StaffStatistics.GetChildren
        #StaffStatistics.GetDocTypeId
        #StaffStatistics.Main
        #StaffStatistics.Update
        pass

    def lps_test(self):
        request = 'SELECT "@Номенклатура", "Наименование" FROM "_000b28b5"."Номенклатура" LIMIT 1000'
        file_name = "data1.csv"
        response = self.request({"request": request})
        #print(response)
        #response = [(res[0], res[1], randint(0,22700)) for res in response]
        self.printer.out(response, "file", file_name)

        request = 'SELECT "@Номенклатура" FROM "_000b28b5"."Номенклатура" LIMIT 2000'
        file_name = "data2.csv"
        response = self.request({"request": request})
        def random(arr,n,m):
            l = len(arr)
            tmp_arr = []
            item = None
            for i in range(n):
                rand_b = randint(0,l-m)
                rand_e = rand_b + m
                item = [str(i[0]) for i in arr[rand_b:rand_e]]
                tmp_arr.append((",".join(item),))
            return tmp_arr
        file_name = "data2.csv"
        response = random(response,1000,10)
        self.printer.out(response, "file", file_name)
        tmp_response = []
        for item in response:
            tmp_item = item[0].split(",")
            for i in list(tmp_item):
                tmp_response.append(int(i))
        file_name = "data3.csv"
        self.printer.out(tmp_response, "file", file_name)


        #request = 'SELECT DISTINCT ON ("Контрагент") "Контрагент" FROM "СвязьЛицаСуд" WHERE "ТипСвязи" IN (7, 8) AND "Контрагент" > 0 LIMIT 1000'
        #file_name = "data3.csv"
        #response = self.request({"request": request, "file_name": file_name})
        #self.printer.out(response, "file", file_name, True)

    def spp_events_test(self):
        db_config = {
            "host":  "test-spp-db2.unix.tensor.ru",
            "dbname": "agentcheck",
            "user": "service_user",
            "password": "iiS"
        }
        request = 'SELECT "@Лицо" FROM "Контрагент" WHERE coalesce("Reliability", 0) > 50 AND "@Лицо" > 0 LIMIT 900'
        response = self.request({"request": request,
                                 "params": db_config})
        file_name = "data1.csv"
        self.printer.out(response, "file", file_name)
        #file_name = "data2.csv"
        #self.printer.out(response[300:600], "file", file_name)
        #file_name = "data3.csv"
        #self.printer.out(response[600:], "file", file_name)

    def service_center_auto_test(self):
        db_config = {
            "host":  "test-spp-db2.unix.tensor.ru",
            "dbname": "agentcheck",
            "user": "service_user",
            "password": "iiS"
        }
        request = 'SELECT "@Лицо" FROM "Контрагент" WHERE coalesce("Reliability", 0) > 50 AND "@Лицо" > 0 LIMIT 1000'
        file_name = "data1.csv"
        response = self.request({"request": request,
                                 "params": db_config})
        self.printer.out(response, "file", file_name)

    def spp_sphinx_test(self):
        db_config = {
            "host":  "test-spp-db2.unix.tensor.ru",
            "dbname": "agentcheck",
            "user": "service_user",
            "password": "iiS"
        }
        request = 'SELECT "Название" FROM "Контрагент" LIMIT 1000'
        file_name = "data1.csv"
        response = self.request({"request": request,
                                 "params": db_config})
        response = [(str(item[0]).replace("\"", "\\\""),) for item in response]
        self.printer.out(response, "file", file_name)

    def spp_monitoring_test(self):
        db_config = {
            "host":  "test-spp-db2.unix.tensor.ru",
            "dbname": "sppm",
            "user": "service_user",
            "password": "iiS"
        }
        request = 'SELECT "Robot"."ServiceName", "Metric"."MetricName" FROM "Metric" LEFT JOIN "Robot" ON "Metric"."Robot"="Robot"."@Robot" LIMIT 1000'
        file_name = "data1.csv"
        response = self.request({"request": request,
                                 "params": db_config})
        dates = [(datetime(1900, i+1, 1, i, i, i, i).isoformat(sep=" "), datetime(2018, i+1, 28, i, i, i, i).isoformat(sep=" ")) for i in range(12)] * 100
        response = [item[0]+item[1] for item in zip(response, dates)]
        self.printer.out(response, "file", file_name)
        #MonitoringService.RobotsAndMetricsList
        #MonitoringMetrics.InsertFromClient
        #RequestState.InsertFromClient

    def billing_main_test(self):
        db_config = {
            "host":  "test-reg-db.unix.tensor.ru",
            "dbname": "reg.tensor.ru",
            "user": "viewer",
            "password": "Viewer1234"
        }

        def get_random_items(n):
            dates = (('2017-10-01', '2017-12-31'),
                     ('2018-01-01', '2018-03-31'),
                     ('2018-04-01', '2018-06-30'),
                     ('2018-07-01', '2018-09-30'))
            owners = ('1,СтруктураПредприятия', '17914,ЧастноеЛицо', '23472171,ЧастноеЛицо')

            for i in range(n):
                owner = choice(owners)
                date = choice(dates)

                yield (owner, date[0], date[1])

        response = [item for item in get_random_items(1000)]
        file_name = "data1.csv"
        self.printer.out(response, "file", file_name, delim=' ')

        request = ''
        request += 'SELECT\r\n'
        request += '    "Параметры" params,\r\n'
        request += '    "КлиентСБиС" account\r\n'
        request += 'FROM "Начисление"\r\n'
        request += 'WHERE "Тип" = 11\r\n'
        request += 'ORDER BY "@Начисление"'

        def get_parse_item(item):
            json_b = item[0]
            reg_id = json_b["reg_id"]
            date_begin = json_b["real_begin_date"] if getattr(json_b, "real_begin_date", None) else json_b["begin_date"]
            date_begin = [int(item) for item in date_begin.split("-")]
            date_end = json_b["real_end_date"] if getattr(json_b, "real_end_date", None)  else json_b["end_date"]
            date_end = [int(item) for item in date_end.split("-")]

            dates_pair = (datetime(*date_begin).timestamp(), datetime(*date_end).timestamp())
            dates_pair = [item for item in range(int(dates_pair[0]), int(dates_pair[1] + 86400), 86400)]
            dates = [datetime.fromtimestamp(date).date() for date in dates_pair]

            return [(reg_id, item[1], date) for date in dates]

        file_name = "data2.csv"
        response = self.request({"request": request,
                                 "params": db_config})

        result = []
        for item in response:
            result.extend(get_parse_item(item))

        self.printer.out(result, "file", file_name)

    def history_test(self):
        db_config = {
            "host":  "test-osr-ah-db2.unix.tensor.ru",
            "dbname": "history2",
            "user": "viewer",
            "password": "Viewer1234"
        }
        request = 'SELECT "Объект" FROM "ИсторияОбъект" LIMIT 2000'
        file_name = "data1.csv"
        response = self.request({"request": request,
                                 "params": db_config})
        self.printer.out(response, "file", file_name)

    def request(self, params=None):
        return self.connector.request(params)

    def console_log(self, params=None):
        response = self.connector.get_response()
        print(response)
        #self.printer.out(response, "console")

def __main__():
    connectors = [DBConnector(), HTTPConnector()]
    printers = [CSVPrinter()]
    tests = [
        'link_decorator_test',
        'process_vas_test',
        'staff_statistics_test',
        'lps_test',
        'spp_events_test',
        'service_center_auto_test',
        'spp_sphinx_test',
        'spp_monitoring_test',
        'billing_main_test',
        'history_test'
    ]

    with Tester(connectors[0], printers[0]) as tester:
        current_test = getattr(tester, tests[-1])
        current_test()

if __name__ == '__main__':
    __main__()
