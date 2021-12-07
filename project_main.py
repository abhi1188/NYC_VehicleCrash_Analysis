import pandas as pd
import numpy as np
import streamlit as st
import time
from datetime import datetime, timedelta
import folium
from folium import plugins
from streamlit_folium import folium_static
from uszipcode import SearchEngine
from folium.plugins import HeatMap
import pickle
import json,urllib.request
import altair as alt

# App token - o4zuGaRztD7CZUutUDTALPc61

# SETTING PAGE CONFIG TO WIDE MODE
st.set_page_config(layout="wide")

# Add Title
st.title('NYC Motor Vehicle Crash Analysis')


# Fetch Data
data_url = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv?$$app_token=o4zuGaRztD7CZUutUDTALPc61&$where=crash_date>='2021-12-04'&$limit=3000000"

#Define Global Variables

if 'opt1' not in st.session_state:
    st.session_state.opt1 = ""

if 'opt2' not in st.session_state:
    st.session_state.opt2 = ""

if 'opt3' not in st.session_state:
    st.session_state.opt3 = ""

if 'opt4' not in st.session_state:
    st.session_state.opt4 = ""

if 'opt5' not in st.session_state:
    st.session_state.opt5 = ""

if 'opt6' not in st.session_state:
    st.session_state.opt6 = ""

# Add options to select type of data for map
options = {
         "Persons Injured": 'number_of_persons_injured',
         "Persons killed": 'number_of_persons_killed',
         "Pedestrians Injured": 'number_of_pedestrians_injured',
         "Pedestrians killed": 'number_of_pedestrians_killed',
         "Cyclist Injured": 'number_of_cyclist_injured',
         "Cyclist killed": 'number_of_cyclist_killed',
         "Motorist injured": 'number_of_motorist_injured',
         "Motorist killed": 'number_of_motorist_killed'
         }

min_date = datetime.today().strftime("%Y/%m/%d")
max_date = datetime.today().strftime("%Y/%m/%d")
today_date = datetime.today()
data = ''
my_bar = st.progress(0)

@st.cache(hash_funcs={pd.DataFrame: lambda _: None},suppress_st_warning=True,show_spinner=False,persist=True)
def loadData(url):    
    for percent_complete in range(10):
         time.sleep(1)
         my_bar.progress(percent_complete + 1)    
    return preprocessData(pd.read_csv(url))

def get_zipcode(lat_,long_, radius_,returns_):
    y=0
    z=np.NaN
    search = SearchEngine(simple_zipcode=True)
    result = search.by_coordinates(lat=lat_, lng=long_, radius=radius_,returns=returns_)
    for i in result:
        y = i.zipcode
        z = i.county.upper()
    return pd.Series([y,z])

def get_city(zipCode):
    y=0
    z=np.NaN
    search = SearchEngine(simple_zipcode=True)
    result = search.by_zipcode(zipCode)

    return result.major_city.upper()

def getTimeOfDay(x):
    if ((x >= 4) & (x <= 8 )):
        return 'Early Morning'
    elif ((x > 8) & (x < 12 )):
        return 'Morning'
    elif ((x >= 12) & (x <= 15 )):
        return 'Early After Noon'
    elif ((x > 15) & (x < 17 )):
        return 'Late After Noon'
    elif ((x >= 17) & (x < 20 )):
        return 'Evening'
    elif ((x >= 20) & (x <= 24 )):
        return 'Night'
    else:
        return 'Late Night'

def prepare_df_for_model(zip,start_pred_date,end_pred_date,time_Of_Day,model_encode_borough,model_encode_timeOfDay,model_encode_day):
    tuple_list = []
    
    borough = data.loc[(data['postalCode'] == zip)]['borough'].values[0]
    lat_ = data.loc[(data['postalCode'] == zip)]['latitude'].values[0]
    long_ = data.loc[(data['postalCode'] == zip)]['longitude'].values[0]
    
    json_url = urllib.request.urlopen("https://api.openweathermap.org/data/2.5/onecall?lat="+str(lat_)+"&lon="+str(long_)+"&exclude=current,minutely,hourly,alerts&appid=7f777fd0f0067fa0afcc65916a504c70&units=imperial")
    temp_1 = json.loads(json_url.read().decode())    
    
    df_b = pd.DataFrame()
    df_b = df_b.append([{'borough' : borough, 'timeOfDay' : time_Of_Day}], ignore_index=True)
    encoded_borough = model_encode_borough.transform(df_b[['borough']])
    encoded_timeOfDay = model_encode_timeOfDay.transform(df_b[['timeOfDay']])
    for lst in temp_1['daily']:
        prcp,snow,temp_max,temp_min = 0.00,0.00,0.00,0.00
        for key,value in lst.items():
            if key == "dt":
                dt = (datetime.utcfromtimestamp(value)).date()
            if key == "temp":
                temp_min = lst["temp"]["min"]
                temp_max = lst["temp"]["max"]

            if key == "rain":
                prcp = lst["rain"]
            
            if key == "snow":
                snow = lst["snow"]
            
        tuple_list.append(tuple((dt,encoded_timeOfDay[0][0],encoded_borough[0][0],zip,prcp,snow,temp_max,temp_min)))
    
    df_weather = pd.DataFrame(tuple_list,columns=['date','timeOfDay_encoded','borough_encoded','postalCode','PRCP','SNOW','TMAX','TMIN'])

    df_model = pd.DataFrame({'date': pd.date_range(start_pred_date, end_pred_date, freq='D',)})
    df_model['day'] = df_model['date'].dt.strftime('%A')
    df_model['month'] = df_model['date'].dt.month
    df_model['date'] = pd.to_datetime(df_model['date']).dt.date

    encoded_day = model_encode_day.transform(df_model[['day']])
    df_model['day_encoded'] = encoded_day
    df_model = df_model.merge(df_weather,on='date',how='left')

    return df_model


def prepare_all_zip_df_for_model(all_zip,start_pred_date,end_pred_date,time_Of_Day,model_encode_borough,model_encode_timeOfDay,model_encode_day):
    tuple_list = []
    boroughs_list = ["MANHATTAN","BRONX","BROOKLYN","STATEN ISLAND","QUEENS"]
    
    for boroughs in boroughs_list:
        if boroughs == "MANHATTAN":
            lat_ = 40.7934 # 10025
            long_ = -73.9727
        elif boroughs == "BRONX":
            lat_ = 40.8150 # 10474
            long_ = -73.8940
        elif boroughs == "BROOKLYN":
            lat_ = 40.6498 # 11226
            long_ = -73.9622
        elif boroughs == "STATEN ISLAND":
            lat_ = 40.5737 # 10306
            long_ = -74.1125
        elif boroughs == "QUEENS":
            lat_ = 40.7330 # 11373
            long_ = -73.8852
        else:
            lat_ = 40.7330 # 11373
            long_ = -73.8852
        
        json_url = urllib.request.urlopen("https://api.openweathermap.org/data/2.5/onecall?lat="+str(lat_)+"&lon="+str(long_)+"&exclude=current,minutely,hourly,alerts&appid=7f777fd0f0067fa0afcc65916a504c70&units=imperial")
        temp_1 = json.loads(json_url.read().decode())  
        df_b = pd.DataFrame()
        df_b = df_b.append([{'borough' : boroughs}], ignore_index=True)
    
        for lst in temp_1['daily']:
            prcp,snow,temp_max,temp_min = 0.00,0.00,0.00,0.00
            for key,value in lst.items():
                if key == "dt":
                    dt = (datetime.utcfromtimestamp(value)).date()
                if key == "temp":
                    temp_min = lst["temp"]["min"]
                    temp_max = lst["temp"]["max"]

                if key == "rain":
                    prcp = lst["rain"]
                
                if key == "snow":
                    snow = lst["snow"]
                
            tuple_list.append(tuple((dt,boroughs,prcp,snow,temp_max,temp_min)))
    
    df_weather = pd.DataFrame(tuple_list,columns=['date','borough','PRCP','SNOW','TMAX','TMIN'])

    all_zip['date'] = all_zip.apply(lambda x: pd.date_range(start_pred_date, end_pred_date, freq='D'), axis=1) 
    all_zip = all_zip.explode('date')
  
    all_zip['day'] = all_zip['date'].dt.strftime('%A')
    all_zip['month'] = all_zip['date'].dt.month
    all_zip['date'] = pd.to_datetime(all_zip['date']).dt.date
    all_zip['timeOfDay'] = time_Of_Day
    all_zip['day_encoded'] = model_encode_day.transform(all_zip[['day']])
    all_zip['borough_encoded'] = model_encode_borough.transform(all_zip[['borough']])
    all_zip['timeOfDay_encoded'] = model_encode_timeOfDay.transform(all_zip[['timeOfDay']])
  
    df_model = all_zip.merge(df_weather,on=['date','borough'],how='left')
   
    return df_model


def execute_model(get_all_zip,zip,start_pred_date,end_pred_date,time_Of_Day):
  
    # loading the trained model
    pickle_inj = open('/Users/abhis/Documents/MSU-Repo/Project/prediction_inj.pkl', 'rb')
    pickle_kill = open('/Users/abhis/Documents/MSU-Repo/Project/prediction_kill.pkl', 'rb')
    pickle_crash = open('/Users/abhis/Documents/MSU-Repo/Project/prediction_crash.pkl', 'rb')
    pickle_encode_day = open('/Users/abhis/Documents/MSU-Repo/Project/encoder_day.pkl', 'rb') 
    pickle_encode_timeOfDay = open('/Users/abhis/Documents/MSU-Repo/Project/encoder_time_of_day.pkl', 'rb') 
    pickle_borough = open('/Users/abhis/Documents/MSU-Repo/Project/encoder_borough.pkl', 'rb')   
    model_inj = pickle.load(pickle_inj)
    model_kill = pickle.load(pickle_kill)
    model_crash = pickle.load(pickle_crash)
    model_encode_day = pickle.load(pickle_encode_day)
    model_encode_timeOfDay = pickle.load(pickle_encode_timeOfDay)
    model_encode_borough = pickle.load(pickle_borough)

    if get_all_zip:
        df_model = pd.DataFrame(columns=['date','timeOfDay_encoded','borough_encoded','postalCode','PRCP','SNOW','TMAX','TMIN'])
        all_zip = data.loc[~(data['borough']=='UNKNOWN')][["postalCode","borough"]].drop_duplicates('postalCode')
        df_model = prepare_all_zip_df_for_model(all_zip,start_pred_date,end_pred_date,time_Of_Day,model_encode_borough,model_encode_timeOfDay,model_encode_day)
    else:
        df_model = prepare_df_for_model(zip,start_pred_date,end_pred_date,time_Of_Day,model_encode_borough,model_encode_timeOfDay,model_encode_day)
     
    # loading the trained model
    y_pred_inj = model_inj.predict(df_model[["month","day_encoded","timeOfDay_encoded","borough_encoded","postalCode","PRCP","SNOW","TMAX","TMIN"]])
    y_pred_kill = model_kill.predict(df_model[["month","day_encoded","timeOfDay_encoded","borough_encoded","postalCode","PRCP","SNOW","TMAX","TMIN"]])
    y_pred_crash = model_crash.predict(df_model[["month","day_encoded","timeOfDay_encoded","borough_encoded","postalCode","PRCP","SNOW","TMAX","TMIN"]])

    return y_pred_inj,y_pred_kill,y_pred_crash


def preprocessData(df):
    # Read from CSV - The historical pre processed data
    final_df = pd.read_csv('/Users/abhis/Documents/MSU/Project/data.csv',dtype={'longitude':np.float64, 'latitude':np.float64})
    #Convert crash_date column to Date ONLY
    final_df["crash_date"]=pd.to_datetime(final_df['crash_date']).dt.date
    # replace field that's entirely space (or empty) with NaN
    final_df['postalCode'] = pd.to_numeric(final_df['postalCode'], errors='coerce')
    final_df["postalCode"] = final_df["postalCode"].astype("int").astype("str")

    if not df.empty:
        # Drop not required columns
        df = df.drop(columns=["collision_id","location","on_street_name","off_street_name","cross_street_name","vehicle_type_code2","vehicle_type_code_3","vehicle_type_code_4","vehicle_type_code_5","contributing_factor_vehicle_2","contributing_factor_vehicle_3","contributing_factor_vehicle_4","contributing_factor_vehicle_5"],axis=1)
        
        #Drop rows with null values in longitude, latitude
        df = df.dropna(subset=['longitude','latitude'])

        # Drop zeros
        df = df.loc[~((df['longitude'] == 0) & (df['latitude'] == 0))]

        #Convert crash_date column to Date ONLY
        df["crash_date"]=pd.to_datetime(df['crash_date']).dt.date

        # Reset Index
        df.reset_index(inplace=True)


        # replace field that's entirely space (or empty) with NaN
        df['zip_code'] = pd.to_numeric(df['zip_code'], errors='coerce')
        # Fill in Zip Code    
        df["zip_code"].fillna(0,inplace=True)
        df_1 = df.loc[df["zip_code"] == 0]
        df.drop(df.index[df['zip_code'] == 0], inplace = True)       
        df_1[['zip_code','borough']] = df_1.apply(lambda x: get_zipcode(x.latitude, x.longitude,4,1), axis=1)
                
        df_2 = pd.concat([df,df_1])
        # Convert zip code to string
        df_2["zip_code"] = df_2["zip_code"].astype("int").astype("str")

        # Replace NaN borough names with Unknown
        df_2.replace({'borough' : { np.nan : "UNKNOWN", "NEW YORK COUNTY" : "MANHATTAN", "KINGS COUNTY" : "BROOKLYN", "RICHMOND COUNTY" : "STATEN ISLAND", "NASSAU COUNTY" : "QUEENS", "QUEENS COUNTY" : "QUEENS", "BRONX COUNTY" : "BRONX", "WESTCHESTER COUNTY" : "BRONX", "BERGEN COUNTY" : "MANHATTAN", "HUDSON COUNTY" : "MANHATTAN", "BROOME COUNTY" : "UNKNOWN", "ERIE COUNTY" : "UNKNOWN", "MADISON COUNTY" : "UNKNOWN", "MIDDLESEX COUNTY" : "UNKNOWN", "PUTNAM COUNTY" : "UNKNOWN", "ROCKLAND COUNTY" : "UNKNOWN", "SOMERSET COUNTY" : "UNKNOWN"}},inplace=True)

        #Rename columns
        df_2.rename(columns={'zip_code': 'postalCode'}, inplace=True)
        df_2['timeOfDay'] = df_2['crash_time'].apply(lambda x: getTimeOfDay(int(x[:2].replace(":",''))) )

        # Export to CSV
        #df_2.to_csv('/Users/abhis/Documents/MSU/Project/data.csv',float_format='%.15f',index=False)

        # Check here in case new data does not load
        final_df = final_df.append(df_2,ignore_index=True)    
    
    d1 = final_df['crash_date'].min()
    d2 = final_df['crash_date'].max()

    return final_df,d1,d2

def displayData(data):
    if st.session_state.opt1 != "" and st.session_state.opt2 != "":
        with pd.option_context('display.precision', 10):
            st.write(data.loc[(data["crash_date"]>=st.session_state.opt1) & (data["crash_date"]<=st.session_state.opt2)])
    else:
        with pd.option_context('display.precision', 10):
            st.write(data)

# Function definition for Chroropleth maps
def map_choro(data, zoom):
    nyc_choro = folium.Map(location=[40.7128, -74.0060], zoom_start=zoom)
    curatedData = data.groupby(by=['postalCode'],as_index=False)[options[st.session_state.opt3]].sum()
    
    for_heatmap_data = data.loc[(data[options[st.session_state.opt3]]>0),['latitude','longitude']]
    testdata = for_heatmap_data[['latitude','longitude']]
    
    # create a marker cluster
    nyc_geo = "/Users/abhis/Documents/MSU-Repo/Project/nyc.json"   
    
    try1 = folium.Choropleth(
            geo_data=nyc_geo,
            name="choropleth",
            data=curatedData,
            columns=['postalCode',options[st.session_state.opt3]],
            key_on="feature.properties.postalCode",
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            nan_fill_color = "White",
            legend_name=options[st.session_state.opt3],
            highlight=True,
            reset=True
        ).add_to(nyc_choro)

    folium.GeoJsonTooltip(fields=['postalCode']).add_to(try1.geojson)
    
    # Add Heat Map
    HeatMap(data=testdata,name='Heat Map',show=False).add_to(nyc_choro)
    folium.LayerControl().add_to(nyc_choro)
    folium_static(
        nyc_choro,
        width=1400,
        height=700
    )


# Function definition for regular maps
def map_markers(data, zoom):
    # define the nyc map  with a low zoom level 10
    nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=zoom)
    d1 = data.loc[(data[options[st.session_state.opt3]]>0),['latitude','longitude','borough']]
    # create a marker cluster
    markers = plugins.MarkerCluster().add_to(nyc_map)
    for lat, lng, label in zip (d1['latitude'],d1['longitude'],d1['borough']):
        folium.Marker (
        
            location=[lat, lng],
            icon = None,
            popup = label,
        ).add_to(markers)

    folium_static(
        nyc_map,width=1400,
        height=700
    )


# Function definition for histogram 1
def histogram1(data):
    # Group by selected data for each borough
    st.subheader("Data by Borough")
    curatedData = data.groupby(by=['borough'])[options[st.session_state.opt3]].sum()
    #st.write(curatedData)
    st.bar_chart(curatedData,height=500)


# Function definition for histogram 2
def histogram2(data):
    # Group by selected data for each borough
    st.subheader("Top 5 crash reasons (based on date range only)")
    curatedData = data["contributing_factor_vehicle_1"]
    
    st.bar_chart(curatedData.value_counts().iloc[:5],height=500)

# Function definition for histogram 3
def histogram3(data):
    # Group by selected data for each borough
    st.subheader("Crash events by time of day")
    curatedData = data["timeOfDay"]
    
    st.bar_chart(curatedData.value_counts(),height=500)

    st.text_area('Legend','''
    Early Morning - 4 AM to 8 AM
    Morning - 8.01 AM to 11.59 AM
    Early AfterNoon - 12.00 PM to 3.00 PM
    Late AfterNoon - 3.01 PM to 4.59 PM
    Evening - 5.00 PM to 7.59 PM
    Night - 8.00 PM to 12.00 AM
    Late Night - 12.01 AM to 3.59 AM
    ''', height=250)

# Function definition for histogram 4
def histogram4(data):
    # Group by selected data for each borough
    st.subheader("Crash events by top 5 vehicle type")
    curatedData = data["vehicle_type_code1"]
    
    st.bar_chart(curatedData.value_counts().iloc[:5],height=500)

# Function definition for line chart
def line_chart(data):
    # Group by selected data for each borough
    st.subheader("Time Series Data")    
    curatedData = data[["crash_date",options[st.session_state.opt3]]]
    curatedData = data.groupby(by=['crash_date'],as_index=False)[options[st.session_state.opt3]].sum()
    
    base = alt.Chart(curatedData).encode(
        x='crash_date:T',
        y=options[st.session_state.opt3]+':Q'
        )
    
    st.altair_chart(base.mark_line(),use_container_width=True)


#Begin data load
msg = st.text("Downloading data from NYC open data web. Please wait !!")
data,min_date,max_date = loadData(data_url)
msg.empty()

my_bar.empty()
# Define Layout
st.sidebar.title("Filters:")

# Add options to vire Raw Data
select_cat2 = st.sidebar.radio("Do you want to view Raw Data ?",("No", "Yes"),index=0)

# Add a Start Date Input to the sidebar:
startDate = st.sidebar.date_input(
    'Select Start Date :',
    min_value=min_date,max_value=max_date,value=(datetime(2021, 1, 1))
)

# Add a End Date Input to the sidebar:
endDate = st.sidebar.date_input(
    'Select End Date :',
    min_value=min_date,max_value=max_date,value=max_date
)

select_cat1 = st.sidebar.radio("What data do you want to see on map ?",("Persons Injured", "Persons killed","Pedestrians Injured","Pedestrians killed","Cyclist Injured","Cyclist killed","Motorist injured","Motorist killed"),index=0)

st.session_state.opt1 = startDate
st.session_state.opt2 = endDate
st.session_state.opt3 = select_cat1
st.session_state.opt4 = select_cat2

# Display results
if st.session_state.opt4 == "Yes":
    displayData(data)

#Display Map
map_choro(data.loc[(data["crash_date"]>=st.session_state.opt1) & (data["crash_date"]<=st.session_state.opt2)],11)

col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

with col1:
    # Display Histogram 1
    histogram1(data.loc[(data["crash_date"]>=st.session_state.opt1) & (data["crash_date"]<=st.session_state.opt2)])

with col2:
    # Display Histogram 2
    histogram2(data.loc[(data["crash_date"]>=st.session_state.opt1) & (data["crash_date"]<=st.session_state.opt2)])

with col3:
    # Display Histogram 3
    histogram3(data.loc[(data["crash_date"]>=st.session_state.opt1) & (data["crash_date"]<=st.session_state.opt2) & (data[options[st.session_state.opt3]]>0)])

with col4:
    # Display Histogram 4
    histogram4(data.loc[(data["crash_date"]>=st.session_state.opt1) & (data["crash_date"]<=st.session_state.opt2) & (data[options[st.session_state.opt3]]>0)])

# Call the line chart
line_chart(data.loc[(data["crash_date"]>=st.session_state.opt1) & (data["crash_date"]<=st.session_state.opt2)])

st.header("Prediction")
with st.expander("ESTIMATE FUTURE INJURIES AND FATALITIES - "):
    with st.form("model-form"):
        zip, selectType, start_pred_date, end_pred_date, time_Of_Day = '','','','',''
        col5, col6, col7, col8 = st.columns(4)

        get_all_zip = st.checkbox("Include ALL Zip Codes ?")
 
        with col5:
            zip = st.text_input("Enter Zip Code",max_chars=5,value="11367")
        with col6:
            time_Of_Day = st.selectbox("Select time of day :",("Early Morning", "Morning","Early After Noon","Late After Noon","Evening","Night","Late Night"),index=0)
        with col7:
            start_pred_date = st.date_input("Select start date",min_value=today_date,max_value=today_date)
        with col8:
            end_pred_date = st.date_input("Select End date",min_value=today_date,max_value=today_date+timedelta(days=5))
        
        col10,col11 = st.columns(2)
        with col10:
            btn = st.form_submit_button("Click to run model")
            if btn:
                y_pred_inj,y_pred_kill,y_pred_crash = execute_model(get_all_zip,zip,start_pred_date,end_pred_date,time_Of_Day)
                st.write("Estimated Crash - ", y_pred_crash.round().astype("int").sum())
                st.write("Estimated Injuries - ", y_pred_inj.round().astype("int").sum())
                st.write("Estimated Fatalities - ", y_pred_kill.round().astype("int").sum())