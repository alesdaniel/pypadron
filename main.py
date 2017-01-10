#!/usr/bin/env python
"""
pypadron
Copyright (C) 2017 Petrotandil S.A.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Script para generar padron de rentas de los 2 padronres arma uno por si hay que actualizar de emergencia y no traen version
nueva
02/01/2017 - Inicial - Daniel
05/01/2017 - Rearma como corrende cvs - Daniel
"""
import sys
import csv
import datetime
import pymysql
#import sqlite3


class padron:
    conn = 0
    cur = 0

    def __init__(self):
        self.conn = 0

    def abre(self):
        try:
            #self.conn = sqlite3.connect('padron.db')
             self.conn = pymysql.connect(host='192.168.1.50', port=3306, user='padron', passwd='padron', db='padron')
        except pymysql.Error as e:
            print ("Error %s:" % e)
            exit(2)
        finally:
            self.cursor()

    def cursor(self):
        self.cur = self.conn.cursor()

    def crea(self):
        self.pad_per = 'create table if not exists percep (' \
                       'r text not null,' \
                       'fp date,' \
                       'fi date,' \
                       'ff date,' \
                       'cuit bigint primary key,' \
                       'tipo text,' \
                       'mab text,' \
                       'mc text,' \
                       'al real NOT NULL DEFAULT 0.00,' \
                       'grup text' \
                       ') ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;'
        self.pad_ret = 'create table if not exists reten (' \
                       'r text not null,' \
                       'fp date,' \
                       'fi date,' \
                       'ff date,' \
                       'cuit bigint primary key,' \
                       'tipo text,' \
                       'mab text,' \
                       'mc text,' \
                       'al real NOT NULL DEFAULT 0.00,' \
                       'grup text' \
                       ') ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;'

        try:
            self.cur.execute(self.pad_per)
            self.cur.execute(self.pad_ret)
        except pymysql.Error as e:
            print ("Error %s:" % e)
            exit(3)

    def afecha(self,fecha):
        dia = fecha[0:2]
        mes = fecha[2:4]
        anio = fecha[4:8]
    #    fec = datetime.datetime(anio, mes, dia, 0, 0, 0, 0)
        fec = anio + "/" + mes +"/"+dia
        return fec

    def copia_pad(self):
        cont = 0;
        with open('Padron_Ret.txt', newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';')
            for row in spamreader:
                to_db = [row[0], self.afecha(row[1]), self.afecha(row[2]), self.afecha(row[3]), row[4], row[5], row[6], row[7],row[8], row[9]]
                cont += 1
                sys.stdout.write("\r registro: " + str(cont))
                sys.stdout.flush()
                #print("INSERT INTO reten (r,fp,fi,ff,cuit,tipo,mab,mc,al,grup) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", row[0], self.afecha(row[1]), self.afecha(row[2]), self.afecha(row[3]), row[4], row[5], row[6], row[7],row[8], row[9])
                self.cur.execute("INSERT INTO reten (r,fp,fi,ff,cuit,tipo,mab,mc,al,grup) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", to_db)
          #      self.conn.commit()

        sys.stdout.write("\n")

        cont = 0;
        with open('Padron_Per.txt', newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';')
            for row in spamreader:
                to_db = [row[0],self.afecha(row[1]),self.afecha(row[2]),self.afecha(row[3]),row[4],row[5],row[6],row[7],row[8],row[9]]
                cont += 1
                sys.stdout.write("\r registro: " + str(cont))
                sys.stdout.flush()
                self.cur.execute("INSERT INTO percep (r,fp,fi,ff,cuit,tipo,mab,mc,al,grup) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",to_db)
         #       self.conn.commit()

    def arma(self):
        self.abre()
       # self.crea()
       # self.copia_pad()
        self.exporta()

    def cierra(self):
        self.conn.close()

    def exporta(self):
        cont = 0
        self.pad_exp = 'SELECT ' \
                       'DATE_FORMAT(percep.fp,"%d%m%Y") as f1,DATE_FORMAT(percep.fi,"%d%m%Y") as f2, DATE_FORMAT(percep.ff,"%d%m%Y") as f3, percep.cuit, percep.tipo, percep.mab, percep.mc, percep.al, reten.al, percep.grup,reten.grup' \
                       ' FROM ' \
                       ' percep ' \
                       ' left ' \
                       ' join ' \
                       ' reten ' \
                       ' on ' \
                       'percep.cuit = reten.cuit'#  \
                       #' limit ' \
                       #' 500'

        self.cur.execute(self.pad_exp);
        with open('PadRentas.txt', 'w', newline='\n') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in self.cur:
                if row[8] is None:
                    retencion = 0
                else:
                    retencion = row[8]
                cont += 1
                sys.stdout.write("\r renglon: " + str(cont))
                sys.stdout.flush()

                cuit = row[3]
                fechapublica = row[0]
                fechavigdesde = row[1]
                fechavighasta = row[2]
                tipo = row[4]
                marca = row[5]
                cambio = row[6]
                percepcion = row[7]
                #retencion = row[8]
                grupopercepcion = row[9]
                gruporetencion = row[10]
                if not gruporetencion:
                    gruporetencion = grupopercepcion

        #        spamwriter.writerow(row)
                spamwriter.writerow([fechapublica, fechavigdesde, fechavighasta, cuit, tipo,marca, cambio, percepcion, retencion, grupopercepcion, gruporetencion])
    def __del__(self):
        self.cur.close()
        self.conn.close()

if __name__ == "__main__":
    if sys.version_info < (3, 0, 0):
        sys.stderr.write("You need python 3.0 or later to run this script\n")
        exit(1)

    p = padron()
    p.arma()

