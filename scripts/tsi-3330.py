import asyncio
import time
from datetime import datetime
import os
from pathlib import Path

# init values

datafile_name = 'OPS_data_'
data_path = Path("C:/DATA/")
command = "RMMEAS"  #RMLOGGEDMEAS

async def telnet_client(host: str, port: int, username: str, password: str) :
    reader, writer = await asyncio.open_connection(host, port)
    print(f"connected to ({host}, {port})")
    
    data_before=0
    while True:
        now = datetime.now()
        mounth_now = now.strftime("%m")
        year_now = now.strftime("%Y")
        day_now = now.strftime("%d")
        date_now = now.strftime("%d-%m-%Y")
        file=datafile_name + date_now + '.txt'
        file_name=data_path / year_now / file          #file_name=data_path / year_now / mounth_now / file
        os.makedirs(os.path.dirname(file_name), exist_ok=True)   
        while (day_now == datetime.now().strftime("%d")):
            try:
                now = datetime.now()
                date_now = now.strftime("%d-%m-%Y")
                mounth_now = now.strftime("%m")
                time_now = now.strftime("%H:%M:%S.%f")[:-4]
                writer.write(f"{command}\r".encode())
                data = await reader.read(100000)
                await asyncio.sleep(1)
                if (data != data_before):
                    f = open(file_name, "a")
                    f.write(date_now + "," + time_now + "," + data.decode().strip() +"\n")
                    f.close()
                    print(data.decode().strip())
                    data_before=data
            except KeyboardInterrupt:
                print("User you have pressed ctrl-c button.")
                break
       
        
asyncio.run(telnet_client("10.10.100.50", 3602, "username", "password"))
