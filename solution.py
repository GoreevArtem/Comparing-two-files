# -*- coding: utf-8 -*-
import itertools
import re
import sys
import warnings

warnings.simplefilter(action='ignore')
import numpy
import pandas as pd
from tldextract import extract


# класс для сравнения данных
class ExportCsv2Excel:
    def __init__(self, path2csv=None, path2excel=None):
        # вид организаций
        self.organization = ("ООО", "ОАО", "ЗАО", "ПАО", "АО",)
        # название столбцов по 1му заданию
        # из excel файла
        self.__dtlabel1 = "Название организации"
        # из csv файла
        self.__dblabel1 = "CompanyName"
        # 2е задание
        self.__dtlabel2 = "Веб-сайт"
        self.__dblabel2 = "WebSiteUrl"
        # 3е задание
        self.__dtlabel3 = "Общий телефон"
        self.__dblabel3_1 = "Telephone1"
        self.__dblabel3_2 = "Telephone3"
        self.__dblabel3_3 = "MobilePhone"
        # 4е задание
        self.__dtlabel4 = "Email"
        self.__dblabel4 = "EMailAddress1"
        # инициализвация путей до файлов
        self.__path2csv = path2csv
        self.__path2excel = path2excel
        # чтение файлов
        # чтение excel
        self.__ExcelData = pd.read_excel(self.__path2excel)
        # чтение csv (тут ставите код для обращения к бд)
        self.__Database = pd.read_csv(self.__path2csv, encoding="utf-8")
        # копии считанной инфориации, тк далее мы будем ее изменять
        self.__ExcelDataDC, self.__DatabaseDC = self.__ExcelData.copy(deep=True), self.__Database.copy(deep=True)

        # по первомму заданию отбрасываем виды организаций
        def ValidCompanyName(data, label: str):
            data[label] = data[label].str.replace(r'[^\w+\s]', '', regex=True)
            data[label] = data[label].str.replace('\n', '').str.upper()
            data[label] = [" ".join(filter(lambda x: x not in self.organization,
                                           str(item).replace(',', '').split())) for item in data[label]]
            return data[label]

        self.__ExcelData[self.__dtlabel1] = ValidCompanyName(self.__ExcelData, self.__dtlabel1)
        self.__Database[self.__dblabel1] = ValidCompanyName(self.__Database, self.__dblabel1)

        # сравнение данных между двумя файлами по столбцам

    def __out_date(self, dblabel, dtlabel):
        out_con = self.__Database[
            self.__Database[dblabel].isin([org for org in self.__ExcelData[dtlabel].dropna()])]
        tmp = pd.Series(self.__ExcelData[dtlabel])
        diff_out = self.__ExcelData[~tmp.isin(list(set([org for org in out_con[dblabel]])))]
        return self.__DatabaseDC.iloc[out_con.index], self.__ExcelDataDC.iloc[diff_out.index]

        # 1) Название организации (Проверка (1)) - CompanyName (база (2))

    def CompanyName(self):
        out_con, diff_out = self.__out_date(self.__dblabel1, self.__dtlabel1)
        return out_con, diff_out

        # 2) Веб-сайт (Проверка (1)) - WebSiteUrl (база (2))

    def WebSiteUrl(self):
        # отбрасываем http www и оставляем только название домена
        def get_domain(data, label: str):
            data[label] = [re.compile(r'[^\w+\s]').sub("", ''.join(extract(str(i))).replace("www", "").upper())
                           for i in data[label].fillna("Not found email")]
            return data[label]

        self.__ExcelData[self.__dtlabel2] = get_domain(self.__ExcelData, self.__dtlabel2)
        self.__Database[self.__dblabel2] = get_domain(self.__Database, self.__dblabel2)
        out_con, diff_out = self.__out_date(self.__dblabel2, self.__dtlabel2)
        return out_con, diff_out

        # 4) Email (Проверка (1)) - EMailAddress1 (база (2))

    def EMailAddress(self):
        out_con, diff_out = self.__out_date(self.__dblabel4, self.__dtlabel4)
        return out_con, diff_out

        # 3) Общий телефон (Проверка (1)) - Telephone1, Telephone3 и MobilePhone(база (2))

    def getAllTelephone(self):
        number = [int(i) for i in list(
            itertools.chain.from_iterable(
                [str(i).replace(".", ",").split(",") for i in self.__ExcelData[self.__dtlabel3]]))]

        def Tel(label):
            dat_con = pd.Series([j in number for j in [i for i in self.__Database[label].fillna(0)]])
            out_con = self.__DatabaseDC.iloc[dat_con[dat_con == True].index.tolist()]
            return out_con

        out_con1 = Tel(self.__dblabel3_1)
        out_con2 = Tel(self.__dblabel3_2)
        out_con3 = Tel(self.__dblabel3_3)
        res_con = pd.concat([out_con1, out_con2, out_con3]).drop_duplicates()
        return res_con

        # получение результата

    def GetResult(self):
        out_con1, diff_out1 = self.CompanyName()
        out_con2, diff_out2 = self.WebSiteUrl()
        out_con3, diff_out3 = self.EMailAddress()
        out_con4 = self.getAllTelephone()
        frames_con = [out_con1, out_con2, out_con3, out_con4]
        # (out_con1, out_con2, out_con3, out_con4, sep="\n\n\n")
        res_con = pd.concat(frames_con).drop_duplicates().sort_index()
        # print(diff_out1, diff_out2, diff_out3, sep="\n\n\n")
        res_diff = diff_out3.merge(diff_out1.merge(diff_out2, how='inner', on=[self.__dtlabel1]), how='inner',
                                   on=[self.__dtlabel1])

        # общий номер телефона (объединяю 3 столбца с телефонами в 1)
        def unionTel(Dataframe):
            Dataframe[self.__dblabel3_1] = Dataframe[self.__dblabel3_1].astype('float').astype('Int64')
            Dataframe[self.__dblabel3_2] = Dataframe[self.__dblabel3_2].astype('float').astype('Int64')
            Dataframe[self.__dblabel3_3] = Dataframe[self.__dblabel3_3].astype('float').astype('Int64')
            Dataframe = Dataframe.astype(str)
            Dataframe[self.__dtlabel3] = Dataframe[self.__dblabel3_1] + ',' + Dataframe[self.__dblabel3_2] + ',' + \
                                         Dataframe[self.__dblabel3_3]
            Dataframe[self.__dtlabel3] = Dataframe[self.__dtlabel3].str.replace(r'[^\d\,]', '', regex=True).str.strip(
                ",").str.replace(
                ",,", ",").str.split(",")
            Dataframe[self.__dtlabel3] = [numpy.unique(i) for i in Dataframe[self.__dtlabel3]]
            Dataframe[self.__dtlabel3] = Dataframe[self.__dtlabel3].str.join(',')
            return Dataframe

        res_con = unionTel(res_con)

        # обращение по нужным столбцам
        indexes = [self.__dblabel1, self.__dblabel2, self.__dtlabel3, self.__dblabel4, "idLead", "Lead_DateChange",
                   "Lead_DateCall", "idOpportunity",
                   "Opportunity_DateChange", "Opportunity_DateCall", "gm_name2", "gm_name"]
        res_con = res_con[indexes].fillna('')
        # переименование столбцов по названию столбцов из excel файла
        res_con.rename(columns={self.__dblabel1: self.__dtlabel1, self.__dblabel2: self.__dtlabel2,
                                self.__dblabel4: self.__dtlabel4}, inplace=True)
        res_diff = self.__ExcelDataDC[
            self.__ExcelDataDC[self.__dtlabel1].isin([org for org in res_diff[self.__dtlabel1].dropna()])]
        # запись данных в excel
        res_con.to_excel("Совпадения(3).xlsx", sheet_name='Совпадения')
        res_diff.to_excel("Различия(3).xlsx", sheet_name='Различия')
        return res_con, res_diff


def main():
    path2csv = r"база(2).csv"
    path2excel = "Проверка(1).xlsx"
    example = ExportCsv2Excel(path2csv, path2excel)
    res, diff = example.GetResult()
    print("Совпадения: ", res, "Различие", diff, sep="\n\n\n")


#
if __name__ == '__main__':
    sys.exit(main() or 0)
