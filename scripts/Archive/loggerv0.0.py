import threading

import asyncio
import time
from datetime import datetime
import os
from pathlib import Path

import serial
from datetime import datetime
from pathlib import Path
import xmltodict
import glom

debug = 0 # 1=gps 2=co2 3=partical 4= so2 5=all 0 =none

# init values 

data_path = Path("C:/DATA/")

# init values GPS

datafile_GPS = 'GPS_data_'
com_port_GPS = "COM3"

# init values li-850

datafile_CO2 = 'CO2_data_'

com_port_co2 = "COM5"

# init values TSI

datafile_name = 'OPS_data_'
#data_path = Path("C:/DATA/")
command = "RMMEAS"  #RMLOGGEDMEAS

# init values

datafile_so2 = 'SO2_data_'
com_so2 = "com4"

 

class ReadLine:
    def __init__(self, s):
        self.buf = bytearray()
        self.s = s
    
    def readline(self):
        i = self.buf.find(b"\n")
        if i >= 0:
            r = self.buf[:i+1]
            self.buf = self.buf[i+1:]
            return r
        while True:
            i = max(1, min(2048, self.s.in_waiting))
            data = self.s.read(i)
            i = data.find(b"\n")
            if i >= 0:
                r = self.buf + data[:i+1]
                self.buf[0:] = data[i+1:]
                return r
            else:
                self.buf.extend(data)
 
def gps(stop): 
    try:
        serialPort = serial.Serial(port=com_port_GPS, baudrate=4800, bytesize=8, timeout=5, stopbits=serial.STOPBITS_ONE)
        
        serialPort.flushInput()
        serialString = ""
        reader = ReadLine(serialPort)

        while stop() is False:
            now = datetime.now()
            mounth_now = now.strftime("%m")
            year_now = now.strftime("%Y")
            day_now = now.strftime("%d")
            date_now = now.strftime("%d-%m-%Y")
            file=datafile_GPS + date_now + '.txt'
            file_name=data_path / year_now / file          #file_name=data_path / year_now / mounth_now / file
            os.makedirs(os.path.dirname(file_name), exist_ok=True)        
            try:
                
                while (day_now == datetime.now().strftime("%d")) and not stop():
                    
                    try:
                         
                        serialString=reader.readline().rstrip()                          #.strip()
                        now = datetime.now()
                        date_now = now.strftime("%d-%m-%Y")
                        mounth_now = now.strftime("%m")
                        time_now = now.strftime("%H:%M:%S.%f")[:-4]
                        data_str=serialString.decode("ASCII") 
                      #  print(data_str.find('$GNGGA'))
                        if data_str.find('$GNGGA') > -1:
                            if debug == 1 : print(date_now + "," + time_now + "," + data_str)
                            f = open(file_name, "a")
                            f.write(date_now + "," + time_now + "," + data_str+"\n")
                            f.close()
                            
                    except Exception as error:
                        print("Error ",error)
                        pass
                
            except KeyboardInterrupt:
                print("User you have pressed ctrl-c button.")
                break
            
    except serial.SerialException as e:
        print("Error", e)       
 
 #read serial data from so2 sensor

def so2(stop): 
    try:
        serialPort = serial.Serial(port=com_so2, baudrate=115200, bytesize=8, timeout=2, parity=serial.PARITY_EVEN, stopbits=serial.STOPBITS_ONE)
        

        serialPort.flushInput()
        serialString = ""
        reader = ReadLine(serialPort)

        while not stop():
            now = datetime.now()
            mounth_now = now.strftime("%m")
            year_now = now.strftime("%Y")
            day_now = now.strftime("%d")
            date_now = now.strftime("%d-%m-%Y")
            file=datafile_so2 + date_now + '.txt'
            file_name=data_path / year_now / file          #file_name=data_path / year_now / mounth_now / file
            os.makedirs(os.path.dirname(file_name), exist_ok=True)        
            try:
                
                while (day_now == datetime.now().strftime("%d")) and not stop():
                    try:
                         
                        serialString=reader.readline().rstrip()                          #.strip()
                        now = datetime.now()
                        date_now = now.strftime("%d-%m-%Y")
                        mounth_now = now.strftime("%m")
                        time_now = now.strftime("%H:%M:%S.%f")[:-4]
                        data_str=serialString.decode("ASCII")   
                        
                        if data_str.find('CONC1=') > -1 or data_str.find('CONC2=') > -1 :
                                if debug == 3 : print(date_now + "," + time_now + "," + data_str)
                                f = open(file_name, "a")
                                f.write(date_now + "," + time_now + "," + data_str+"\n")
                                f.close()
                        
                    except Exception as error:
                        print("Error ",error)
                        pass
                
            except KeyboardInterrupt:
                print("User you have pressed ctrl-c button.")
                break
            
    except serial.SerialException as e:
        print("Error", e)     
                
                
def co2(stop):
    try:
        serialPort = serial.Serial(port=com_port_co2, baudrate=9600, bytesize=8, timeout=5, stopbits=serial.STOPBITS_ONE)
        
        serialPort.flushInput()
        serialString = ""
        reader = ReadLine(serialPort)

        while stop() is False:
            now = datetime.now()
            mounth_now = now.strftime("%m")
            year_now = now.strftime("%Y")
            day_now = now.strftime("%d")
            date_now = now.strftime("%d-%m-%Y")
            file=datafile_CO2 + date_now + '.txt'
            file_name=data_path / year_now / file          #file_name=data_path / year_now / mounth_now / file
            os.makedirs(os.path.dirname(file_name), exist_ok=True)        
            try:
                
                while (day_now == datetime.now().strftime("%d")) and not stop():
                   
                    try:
                         
                        serialString=reader.readline().rstrip()                          #.strip()
                        now = datetime.now()
                        date_now = now.strftime("%d-%m-%Y")
                        mounth_now = now.strftime("%m")
                        time_now = now.strftime("%H:%M:%S.%f")[:-4]
                        data_str=serialString.decode("ASCII")
                        data_dict = xmltodict.parse(data_str)
                        
                        co2 = glom.glom(data_dict, 'li820.data.co2')
                        co2abs = glom.glom(data_dict, 'li820.data.co2abs')
                        celltemp = glom.glom(data_dict, 'li820.data.celltemp')
                        cellpres = glom.glom(data_dict, 'li820.data.cellpres')
             
                        data_csv = (date_now + "," + time_now + "," + co2 + "," + co2abs + "," + celltemp + "," +cellpres)
                        if debug == 2 : print(data_csv)
                        f = open(file_name, "a")
                        f.write(date_now + "," + time_now + "," + data_csv+"\n")
                        f.close()
                        
                    except Exception as error:
                        print("Error ",error)
                        pass
                        
            except KeyboardInterrupt:
                print("User you have pressed ctrl-c button.")
                break
            
    except serial.SerialException as e:
        print("Error", e)       

#twi 3330 get data by ethernet 

async def telnet_client(host: str, port: int, username: str, password: str, stop) :
    try:
        reader, writer = await asyncio.open_connection(host, port)
        print(f"connected to ({host}, {port})")
        
        data_before=0
        print(stop())
        while not stop() :
            print(stop())
            now = datetime.now()
            mounth_now = now.strftime("%m")
            year_now = now.strftime("%Y")
            day_now = now.strftime("%d")
            date_now = now.strftime("%d-%m-%Y")
            file=datafile_name + date_now + '.txt'
            file_name=data_path / year_now / file          #file_name=data_path / year_now / mounth_now / file
            os.makedirs(os.path.dirname(file_name), exist_ok=True)   
            while (day_now == datetime.now().strftime("%d") and not stop() ):
                try:
                    now = datetime.now()
                    date_now = now.strftime("%d-%m-%Y")
                    mounth_now = now.strftime("%m")
                    time_now = now.strftime("%H:%M:%S.%f")[:-4]
                    writer.write(f"{command}\r".encode())
                    data = await reader.read(100000)
                    await asyncio.sleep(2)
                    if (data != data_before):
                        f = open(file_name, "a")
                        f.write(date_now + "," + time_now + "," + data.decode().strip())
                        f.close()
                        if debug == 4 :print(data.decode().strip())
                        data_before=data
                except KeyboardInterrupt:
                    print("User you have pressed ctrl-c button.")
                    break
    except OSError: 
        print("partical connection error")

def tsi3330(host: str, port: int, username: str, password: str, stop) :
    asyncio.run(telnet_client("10.10.100.50", 3602, "username", "password",stop))



if __name__ =="__main__":

    print("logger for gps co2, so2 and partical sensor")
    stop_threads = False
    t_co2 =  threading.Thread(target=co2    ,  args =(lambda :stop_threads, ))
    t_gps =  threading.Thread(target=gps    ,  args =(lambda :stop_threads, ))
    t_so2 =  threading.Thread(target=so2    ,  args =(lambda :stop_threads, ))
    t_part = threading.Thread(target=tsi3330,  args =("10.10.100.50", 3602, "username", "password",lambda : stop_threads, ))
 
    t_co2.start()
    t_gps.start()
    t_so2.start()
    t_part.start()
    while True:
        command = str(input())
        
        if command == "stop" :
            stop_threads = True
            print(">stop");
            break
        if command == "debug=non": print(">none");debug=0
        if command == "debug=co2": print(">co2");debug=2
        if command == "debug=so2": print(">so2");debug=3
        if command == "debug=gps": print(">gps");debug=1
        if command == "debug=par": print(">par");debug=4
        if command == "debug=all": print(">all");debug=5
        print(">", end =" ")
        
    t_gps.join()        
    t_co2.join()
    t_so2.join()
    t_part.join()
    
    print("Done!")