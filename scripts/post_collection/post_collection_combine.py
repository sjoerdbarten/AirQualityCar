#Load packages
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import os
import pynmea2
import re
import matplotlib.dates as mdates
import cartopy.crs as ccrs
import osmnx as ox



#Switches
date = dt.date(2025,1,7)       #YYYY,MM,DD
GPS_switch = True              #True/False
OPS_switch = True              #True/False
CO2_switch = True              #True/False
SO2_switch = True              #True/False
UFP_switch = True              #True/False
NO2_switch = True              #True/False
plot_figures = True             #True/False



#Core code
print('Processing data for '+str(date))
datapath = 'C:/data/'+str(date.year)+'/'
filespath = 'C:/scripts/post_collection/Files'
figurespath = 'C:/scripts/post_collection/Figures'
dir1 = os.path.join(filespath, str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year))
dir2 = os.path.join(figurespath, str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year))
if not os.path.exists(dir1):
    os.mkdir(dir1)
if not os.path.exists(dir2):
    os.mkdir(dir2)



#Code to process the different sensors
if GPS_switch == True:
    GPS = datapath+'GPS_data_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.txt'
    
    with open(GPS, 'r') as file:
        gps_data = file.read()

    lines = gps_data.strip().split("\n")
    
    gga_data = []
    rmc_data = []

    for line in lines:
        if not line.strip():  # Skip empty lines
            continue
        
        parts = line.split(',')
        
        if len(parts) < 3:
            print(f"Skipping line due to insufficient parts: {line}")
            continue
        
        timestamp = parts[0] + ' ' + parts[1]
        sentence = ','.join(parts[2:])
        
        if sentence.startswith('$GNGGA'):
            try:
                msg = pynmea2.parse(sentence)
                gga_data.append({
                    'Timestamp': timestamp,
                    'Time': msg.timestamp,
                    'Latitude': msg.latitude,
                    'Longitude': msg.longitude,
                    'Altitude': msg.altitude,
                    'HDOP': getattr(msg, 'horizontal_dilution', None),
                })
            except pynmea2.ParseError as e:
                print(f"Failed to parse $GNGGA sentence: {sentence} - Error: {e}")
                continue
        
        elif sentence.startswith('$GNRMC'):
            try:
                msg = pynmea2.parse(sentence)
                rmc_data.append({
                    'Timestamp': timestamp,
                    'Time': msg.timestamp,
                    'Latitude': msg.latitude,
                    'Longitude': msg.longitude,
                    'Speed (km/h)': getattr(msg, 'spd_over_grnd_kmph', None),
                    'Course': getattr(msg, 'true_course', None),
                    'Date': msg.datestamp,
                })
            except pynmea2.ParseError as e:
                print(f"Failed to parse $GNRMC sentence: {sentence} - Error: {e}")
                continue

    gga_df = pd.DataFrame(gga_data)
    rmc_df = pd.DataFrame(rmc_data)
    
    if not gga_df.empty and not rmc_df.empty:
        df = pd.merge(gga_df, rmc_df, on='Time', suffixes=('_GGA', '_RMC'))
        df['Latitude'] = df['Latitude_GGA'].combine_first(df['Latitude_RMC']).round(7)
        df['Longitude'] = df['Longitude_GGA'].combine_first(df['Longitude_RMC']).round(7)
        df['Datetime'] = df['Timestamp_GGA'].combine_first(df['Timestamp_RMC'])
        df.set_index('Datetime', inplace=True)
        df.drop(['Timestamp_GGA', 'Timestamp_RMC','Latitude_GGA', 'Longitude_GGA', 'Latitude_RMC', 'Longitude_RMC','Date','HDOP','Time'], axis=1, inplace=True)
    else:
        if not gga_df.empty:
            df = gga_df
            df['Datetime'] = df['Timestamp']
            df.set_index('Datetime', inplace=True)
            df.drop(['Timestamp','HDOP','Time'], axis=1, inplace=True)
        if not rmc_df.empty:
            df = rmc_df
            df['Datetime'] = df['Timestamp']
            df.set_index('Datetime', inplace=True)
            df.drop(['Timestamp','Date','HDOP','Time'], axis=1, inplace=True)
        print("No GGA or RMC data to merge.")
        
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    df = df.reindex(columns=['Latitude', 'Longitude', 'Altitude', 'Speed (km/h)', 'Course'])
    df = df[~((df['Latitude'] == 0.0) & (df['Longitude'] == 0.0))]
    
    df_resampled = df.resample('1s').first()

    print('Example dataframe GPS')
    print(df_resampled.head())
    print('Saving dataframe GPS')
    
    df_resampled.to_csv(filespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/GPS_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.csv')

    if plot_figures == True:
        # Calculate bounding box based on the dataframe with some buffer
        north = df_resampled['Latitude'].max() + 0.01
        south = df_resampled['Latitude'].min() - 0.01
        east = df_resampled['Longitude'].max() + 0.01
        west = df_resampled['Longitude'].min() - 0.01
        G = ox.graph_from_bbox(north, south, east, west, network_type='all')
        roads = ox.graph_to_gdfs(G, nodes=False, edges=True)

        fig, ax = plt.subplots(figsize=(10, 8), subplot_kw={'projection': ccrs.PlateCarree()})
        roads.plot(ax=ax, linewidth=1, edgecolor='black', alpha=0.7, transform=ccrs.PlateCarree())        
        ax.plot(df_resampled['Longitude'], df_resampled['Latitude'], marker='o', color='red', markersize=5, linestyle='-', transform=ccrs.PlateCarree())   
        ax.set_title('GPS '+str(date))
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/GPS_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()

if OPS_switch == True:
    OPS = datapath+'OPS_data_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.txt'

    with open(OPS,'r') as file:
        lines = file.readlines()
    
    data = []
    timestamp = None

    for i, line in enumerate(lines):
        if (i % 19) == 0:
            timestamp_line = line.strip()
            parts = timestamp_line.split(',')
            date_ops = parts[0]
            time_ops = parts[1]
            datetime_format = "%d-%m-%Y,%H:%M:%S.%f"
            timestamp = dt.datetime.strptime(f"{date_ops},{time_ops}", datetime_format).strftime("%Y-%m-%d %H:%M:%S")
        if (i % 19) == 8:
            data_line = line.strip()
            parts = data_line.split(',')
            p1 = parts[0]
            p2 = parts[1]
            p3 = parts[2]
            p4 = parts[4]
            data.append((timestamp, p1, p2, p3, p4))

    df = pd.DataFrame(data, columns=['datetime', 'bin1', 'bin2', 'bin3', 'bin4'])
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    df.sort_index(inplace=True)
    df = df.apply(pd.to_numeric)
    df_resampled = df.resample('1s').mean()
    print('Example dataframe OPS')
    print(df_resampled.head())
    print('Saving dataframe OPS')
    df_resampled.to_csv(filespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/OPS_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.csv')

    if plot_figures == True:
        plt.plot(df_resampled['bin1'],label='0.3 - 1 µm') #BIN 1 = 0.3 - 1 µm
        plt.plot(df_resampled['bin2'],label='1 - 2.5 µm') #BIN 2 = 1 - 2.5 µm
        plt.plot(df_resampled['bin3'],label='2.5 - 4 µm') #BIN 3 = 2.5 - 4 µm
        plt.plot(df_resampled['bin4'],label='4 - 10 µm') #BIN 4 = 4 - 10 µm
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Number concentration [# cm$^{-3}$]')
        plt.title('OPS '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/OPS_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()

if CO2_switch == True:
    df = pd.read_csv(datapath+'CO2_data_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.txt',
                      header=None)

    #What do these columns represent?
    df.columns = ['Date', 'Time', 'CO2_1', 'CO2_2', 'CO2_3', 'CO2_4']
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S.%f')
    df = df.drop(columns=['Date', 'Time'])
    df.set_index('Datetime', inplace=True)
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    df_resampled = df.resample('1s').mean()
    #df['CO2_calibrated'] = df['CO2_1']*calfactor+offset           #Maybe something like this?
    print('Example dataframe CO2')
    print(df_resampled.head())
    print('Saving dataframe CO2')
    df_resampled.to_csv(filespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/CO2_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.csv')

    if plot_figures == True:
        plt.plot(df_resampled['CO2_1'],label='CO2_1')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Mixing ratio [ppm]')
        plt.title('CO2v1 '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/CO2v1_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()

        plt.plot(df_resampled['CO2_2'],label='CO2_2')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Mixing ratio [ppm]')
        plt.title('CO2v2 '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/CO2v2_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()
        
        plt.plot(df_resampled['CO2_3'],label='CO2_3')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Mixing ratio [ppm]')
        plt.title('CO2v3 '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/CO2v3_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()

        plt.plot(df_resampled['CO2_4'],label='CO2_4')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Mixing ratio [ppm]')
        plt.title('CO2v4 '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/CO2v4_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()

    
if SO2_switch == True:
    file_path = datapath+'SO2_data_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.txt'

    with open(file_path, 'r') as file:
        data = file.readlines()
    
    df = pd.DataFrame(data, columns=['Raw'])

    df['Datetime'] = df['Raw'].str.extract(r'(\d{2}-\d{2}-\d{4},\d{2}:\d{2}:\d{2}\.\d{2})')
    df['Datetime'] = pd.to_datetime(df['Datetime'], format='%d-%m-%Y,%H:%M:%S.%f')

    df['CONC1'] = df['Raw'].str.extract(r'CONC1=(\d+\.?\d*) PPB').astype(float)
    df['CONC2'] = df['Raw'].str.extract(r'CONC2=(\d+\.?\d*) PPB').astype(float)

    df = df.dropna(subset=['CONC1', 'CONC2'], how='all')

    df = df.drop(columns=['Raw'])
    df.set_index('Datetime', inplace=True)

    pivoted_df = df.groupby(df.index).agg({
        'CONC1': 'first',
        'CONC2': 'first'
    })

    df_resampled = pivoted_df.resample('1min').apply(lambda x: np.nanmean(x, axis=0))
    df_resampled = df_resampled.dropna(how='all')

    print('Example dataframe SO2')
    print(df_resampled.head())
    print('Saving dataframe SO2')
    df_resampled.to_csv(filespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/SO2_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.csv')

    if plot_figures == True:
        plt.plot(df_resampled['CONC1'],label='SO2 CONC1')
        plt.plot(df_resampled['CONC2'],label='SO2 CONC2')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Mixing ratio [ppb]')
        plt.title('SO2 '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/SO2_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()

if UFP_switch == True:
    UFP_path = datapath+'/UFP_data_'+str(date.year)+'_'+str(f"{date:%m}")+'_'+str(f"{date:%d}")+'/'

    for filename in os.listdir(UFP_path):
        df = pd.read_csv(UFP_path+filename,skiprows=20,names=['time','number','diam','LDSA','surface','mass','A1','A2','idiff','HV','EM1','EM2','DV','T','RH','P','flow','bat','Ipump','error','PWMpump'],sep='\t')
        with open(os.path.join(UFP_path, filename), 'r') as f:
            lines = f.readlines()
            
    startdate = [line for line in lines if line.startswith('Start: ')][0][7:]
    startdate = dt.datetime.strptime(startdate.strip(), '%d.%m.%Y %H:%M:%S')

    df['Datetime'] = np.array([startdate + dt.timedelta(seconds=i) for i in range(len(df))])
    df_resampled = df.set_index(df['Datetime'])
    df_resampled = df_resampled.drop(columns=['time','Datetime'])
    
    print('Example dataframe UFP')
    print(df_resampled.head())
    print('Saving dataframe UFP')
    df_resampled.to_csv(filespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/UFP_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.csv')

    if plot_figures == True:
        plt.plot(df_resampled['number'],label='#')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Concentration [# particles]')
        plt.title('UFP '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/UFP_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()

        plt.plot(df_resampled['T'],label='T')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Temperature [degC]')
        plt.title('T '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/UFP_T_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()
        
        plt.plot(df_resampled['RH'],label='RH')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Relative Humidity [%]')
        plt.title('RH '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/UFP_RH_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()
        
        plt.plot(df_resampled['P'],label='P')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Pressure [hPa]')
        plt.title('P '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/UFP_P_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()


if NO2_switch == True:
    file_path = datapath+'NO2_data_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.txt'

    df = pd.read_csv(file_path,skiprows=20,names=['Date','Time','NO2','NO','NOx','Tc','P','Fc','Foz','PDVs','PDVg','Ts','Er','date2','time2','Mode'])

    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S.%f')
    df = df.drop(columns=['Date', 'Time', 'date2', 'time2'])
    df.set_index('Datetime', inplace=True)
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    df_resampled = df.resample('10s').mean()
    
    print('Example dataframe NO2')
    print(df_resampled.head())
    print('Saving dataframe NO2')
    df_resampled.to_csv(filespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/NO2_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.csv')

    if plot_figures == True:
        plt.plot(df_resampled['NO2'],label='NO')
        plt.plot(df_resampled['NO'],label='NO2')
        plt.plot(df_resampled['NOx'],label='NOx')
        plt.xlim([df_resampled.index[0],df_resampled.index[-1]])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xlabel('Time [UTC]')
        plt.ylabel('Concentration [ppb]')
        plt.title('NO2 '+str(date))
        plt.legend(loc='best')
        plt.savefig(figurespath+'/'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'/NO2_'+str(f"{date:%d}")+'-'+str(f"{date:%m}")+'-'+str(date.year)+'.png')
        plt.show()

    

