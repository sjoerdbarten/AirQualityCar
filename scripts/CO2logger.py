import serial
from datetime import datetime
import os
from pathlib import Path
import xmltodict
import glom

# init values

datafile_name = 'CO2_data_'
data_path = Path("C:/DATA/")
com_port = "COM5"
debug = 1


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

try:
    serialPort = serial.Serial(port=com_port, baudrate=9600, bytesize=8, timeout=2, stopbits=serial.STOPBITS_ONE)
    

    serialPort.flushInput()
    serialString = ""
    reader = ReadLine(serialPort)

    while True:
        now = datetime.now()
        mounth_now = now.strftime("%m")
        year_now = now.strftime("%Y")
        day_now = now.strftime("%d")
        date_now = now.strftime("%d-%m-%Y")
        file=datafile_name + date_now + '.txt'
        file_name=data_path / year_now / file          #file_name=data_path / year_now / mounth_now / file
        os.makedirs(os.path.dirname(file_name), exist_ok=True)        
        try:
            
            while (day_now == datetime.now().strftime("%d")):
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
                    if debug == 1 : print(data_csv)
                    f = open(file_name, "a")
                    f.write(data_csv+"\n")
                    f.close()
                except Exception as error:
                    print("Error ",error)
                    pass
            
        except KeyboardInterrupt:
            print("User you have pressed ctrl-c button.")
            break
        
except serial.SerialException as e:
    print("Error", e)       

