#!/usr/bin/env python
# coding: utf-8

#  ## CooP - an organic, free range artisanal heritage chicken farm
#  ___
#  Providing improved packaging with lineage information for each individual bird.

#  ### Data Schema
#  ___
# <details>
#     
# ___
# The Data Schema Represents chicken and egg family trees
# 
# **Chickens have:**
# * Chicken ID (C_ID)
# * Name (Name)
# * Sex (Rooster or Hen) (Sex)
# * Feather color (Color)
# * Favorite song (Favorite_Song)
# * Each Hen lays eggs
# * Generation ID (G_ID) int
# 
# **Eggs have:** 
# * An identification number (E_ID) will end up mapping to C_ID
# * Location in the incubation hall where hens sit on their eggs. (IH_LOC)
# * Whether that spot is near a window (as 1/3rd of the spots should be) (NW - Boolean [True, False])
# * Parent IDs (PH_ID, PR_ID)(Will need to map to C_IDs)
# 
# **Chicken Genealogy:**
# * The egg that it came from (E_ID)
# * Its parents (PH_ID) - (PR_ID), and their eggs - (PHE_ID, PRE_ID)
# * The Grandparents, etc) - HGPH_ID, HGPR_ID, RGPH_ID, RGPR_ID 
# * any additional columns to the chicken and egg tables or any other way needed.
#     
# **Note:**
# You will probably need more columns than just the above minimum information.
#     </details>

# ### Generate data
# ___
# <details>
# Two weeks after your starting, all records were destroyed after a ransom malware attack scrambled the database filesystem. Despite the farmworkers trying to remember all the chicken's names, it's impossible to tell them apart now.
# 
# We need to recreate, (generate fake data) about all chickens currently on the farm (1000 chickens). \
# *Some of whom are parents to others.*
# 
# **Generate the required 1000 records**
# 
# What can you do to make these records seem as realistic as possible? (have realistic timelines and age)
# (Feel free to look up data as you need to, but tell us what you looked up?) 
#  
#  * Bonus: How could a government official check whether the dataset is faked or not? 
#     (most chicken species have documented egg rates and age before producing egges)
#  * Bonus Bonus: What can you do to cover up these checks?
#     (use the published ranges with randomization over actual calendar days to make data more realistic) 
# * Bonus Bonus Bonus: What can a government official check to see whether you're covering up their checks. Etc
#     </details>
# 

#  ### Name tags for Guided Tours
#  ___
#  <details>
# We Give guided tours of the farm and introduce all the chickens to visitors. 
# To make this possible we print tags and attached to each chicken's leg. 
# 
# The Tag includes:
# 
# * The Chicken's name
# * Their Favorite song
# * Their Parents
# * Their Grandparents
# * The Location each parent and grand parent was incubated
# * A randomly selected first cousin of the chicken
#     
#     https://github.com/aruljohn/popular-baby-names/blob/master/2000/boy_names_2000.csv \
#     https://github.com/fivethirtyeight/data/blob/master/classic-rock/classic-rock-song-list.csv
#     </details>

# ### Bonus
# ___
# <details>
# Create a dashboard in Metabase that shows some KPIs for this chicken farm. 
# Please include either a public link or a screenshot.
#     </details>

# ### Assumptions
# <details>
# 
# #### Start Date
# On March 11, 2020, the World Health Organization (WHO) declared COVID-19, the disease caused by the SARS-CoV-2, a pandemic. The announcement followed a rising sense of alarm in the preceding months over a new, potentially lethal virus that was swiftly spreading around the world.
# 
# #### Hen to Rooster Ratio
# https://www.thehappychickencoop.com/whats-the-perfect-ratio-of-hens-to-roosters/
# 
# #### List of Chicken Breeds
# https://www.typesofchicken.com/best-chickens-for-texas-humidity/
# 
# https://starmilling.com/poultry-chicken-breeds/#:~:text=There%20are%209%20recognized%20colors,tailed%20Buff%2C%20White%20and%20Columbian.
# 
# Breeds for Texas
# Best-egg-laying breed in Texas that doesn’t have the issues with humidity, heat, and fertility yet still lays pretty well, we suggest the Mediterranean sorts:
# 
# * Ancona
# * Catalana
# * Egyptian Fayoumi
# * Leghorn
# * Hamburg
# 
# #### Ancona Details for Texas
# https://www.typesofchicken.com/keeping-ancona-chickens/
#     
# #### Hatching Rates
# https://rosehillfarm.ca/2020/04/25/incubating-chickeneggs/#:~:text=Eggs%20typically%20hatch%20at%20a,just%20a%20law%20of%20averages
#     </details>

# In[1]:


import pandas as pd
import datetime
from datetime import date
from calendar import Calendar, monthrange
from dateutil.rrule import rrule, DAILY
import uuid
import numpy as np
np.random.seed(1)
import random

#We will use duckdb for Metabase analyztics and visualization
import duckdb
# Import jupysql Jupyter extension to create SQL cells
get_ipython().run_line_magic('load_ext', 'sql')
get_ipython().run_line_magic('config', 'SqlMagic.autopandas = True')
get_ipython().run_line_magic('config', 'SqlMagic.feedback = False')
get_ipython().run_line_magic('config', 'SqlMagic.displaycon = False')

import warnings
warnings.filterwarnings('ignore')

import plotly.tools as tls
import plotly.express as px
import plotly.io as pio


# In[2]:


## Paramanters

start_date = '03/11/2020'
start_hen_count =45
start_rooster_count = 1

breed = 'Anacona' # Anacona is heat tolerant and good for texas
color = 'Black&White'   #They are Black with white specs
expected_egg_rate_min = 180
expected_egg_rate_max = 220

min_egg_producing_age = '20 weeks' #5 months
anacona_lifespan_min = '8 years'
anacona_lifespan_max = '12 years'


#Daily Log is the Audit Log of every day since the Farm began operation.
#Log can be used to track Month, High&Lo Temps, Humidity, Number of Sunlight Hours, 
# Number of Eggs collected, Number of Eggs hatched, total number of chickens etc.

#Create initial daily log Pandas Dataframe
dailylog_df = pd.DataFrame()

#Initial Farm Operation Investment (4 hens & 1 rooster)
int_hen_list = [['Mary', 'Hen'], ['Pat', 'Hen'], ['Barb', 'Hen'], ['Liz', 'Hen'] ]
int_rooster_list = [['Ralph', 'Rooster']]

#Location
location = [['A', 'False'],['B', 'True'],['C', 'False']]
location_df = pd.DataFrame(location, columns = ['IH_Location', 'Near_Window'])

#Time delay from purchase to production as we purchased young chicks
delay_start = '1 Month or 5 weeks'
egg_production_start = '04/15/2020'

#Create Empyt Eggs Dataframe from list
egg_columns = ['E_ID', 'PH_ID', 'PR_ID', 'IH_LOC', 'NW' ]
egg_df = pd.DataFrame(columns=['E_ID', 'PH_Name', 'PH_ID', 'PR_ID', 'IH_LOC', 'NW' ])

#Songs Table - additional attributes can be uese to plot and analyze
songs_df = pd.read_csv('Coop/songlist.csv')

hens_names = pd.read_csv('Coop/census-female-names.csv', usecols=[0], header=None)
rooster_names = pd.read_csv('Coop/boy_names_2000.csv', usecols=[1], header=None)

sex_list = ['Rooster', 'Hen']
rooster_names_list = rooster_names[1].tolist()
hen_names_list = hens_names[0].tolist()



# In[3]:


#Convert Near_Window to Boolean Values
location_df['Near_Window'] = location_df['Near_Window'].map({'False':False, 'True':True})


# In[4]:


#Egg Production Rates
egg_rate_min = expected_egg_rate_min / 365
egg_rate_max = expected_egg_rate_max / 365
print("min_rate/day = ", egg_rate_min, "\nmax_rate/day = ", egg_rate_max)


# In[5]:


#Create initial Chicken Dataframe from investment:

chicken_list = int_hen_list + int_rooster_list

df = pd.DataFrame (chicken_list, columns = ['Name', 'Sex'])
df['Color'] = 'Black&White'
#df


# In[6]:


#Egg Rate Per Chicken (Rooster = 0)
#egg_rate_min egg_rate_max

#Randomly Assign Egg Rate to Hens in Base Family
rate = np.random.randint(expected_egg_rate_min, expected_egg_rate_max, size=4)
df['egg_rate_per_year'] = pd.DataFrame(rate, columns=['egg_rate'])


# In[ ]:





# ___

# ### Initial Flock
# The initial investment, Flock Size consisted of 4 Hens and 1 Rooster

# In[7]:


df.head(5)


# ___

# #### Generate Unique IDs for each bird in the Initial Flock

# In[8]:


#Map initial Family to unique Chicken_IDs
#generate the number of IDs that are needed to be applied to each bird with a unique ID
names = df['Name'].tolist()

# generte Unique ids
ids = np.random.randint(low=1e6, high=1e9, size = len(names))

# maps ids to names
maps = {k:v for k,v in zip(names, ids)}

# add new id column
df['C_ID'] = df[['Name']].agg(' '.join, 1).map(maps)

#Add Generation to Table - Initial_Flock, Gen1, Gen2, Gen3, Gen4
#The Generation column will help us track the operation growth and family, etc.
#Assign Initial_Flock to our initila flock birds
df['Generation'] = "Initial_Flock"


# In[9]:


#Uncomment to show Birds with Names and IDs
df


# #### Generating egg generation rates and totals eggs of initial Flock

# In[10]:


#Calculate end-date using today's date
end_date = date.today()
today = date.today()


# In[11]:


# initializing the start and end date
start_date = date(2020, 4, 15)
end_date = date(2020, 3, 31)


# In[12]:


# iterating over the dates
#for d in rrule(DAILY, dtstart=start_date, until=end_date):
 #   print(d.strftime("%Y-%m-%d"))


# ___
# Use pandas to Iterate through a range of dates \
# use the Pandas date_range() function method. It returns a fixed frequency DatetimeIndex.
# Syntax: pandas.date_range(start, end)
# 
# Parameter:\
# start is the starting date \
# end is the ending date

# #### Generate a Daily Farm Log that can be used to track daily operations

# In[13]:


# specify the start date is 2021 jan 1 st
# specify the end date is 2021 feb 1 st
dailylog_df['Dates']= pd.date_range(start='04/15/2020', end=today)


# In[14]:


df['diff_years'] = (dailylog_df.iloc[-1]['Dates'] - dailylog_df.iloc[0]['Dates']) / np.timedelta64(1, 'Y')
df['egg_count'] = df['egg_rate_per_year'] * df['diff_years']


# In[15]:


print("The total number of Farm Operations Days to date:", dailylog_df.shape[0])


# This provides an expectation on the number of total eggs from the Initial Flock since the start of Farm Operations

# In[16]:


df.head()


# In[17]:


#Round Egg Count down as egg counts have to be a whole number, Int.
df['egg_count_round'] = df['egg_count'].apply(np.floor)

#Create a seperate Hens and Rooster Table for reporting and Management.
hens_df = df[df['Sex'].str.match('Hen')]
roosters_df = df[df['Sex'].str.match('Rooster')]

#Convert the egg_count from Floats to Integers
hens_df['egg_count_round'] = hens_df['egg_count_round'].astype(int)


# In[18]:


#Generate the toal Egg expectancy from the Initial Flock which will be used to generate the Eggs Table.
print("We expect the initial Flock has produced the following number of Eggs by this time:")
hens_df['egg_count_round'].sum()


# In[19]:


#Uncommend to show the current Hens Table.
#hens_df


# In[20]:


print("The current Egg Table contains the following Columns:")
egg_df.columns


# In[21]:


print("The current Location Table contains the following Columns:")
location_df.columns


# In[22]:


#Uncomment to show the current Rooster Table:
#roosters_df


# ___

# ### Generate First Generation Eggs

# In[23]:


print("Looping through each Hen and their associated egg counts per Hen to generate the Egg Table")
#eggs_df = pd.DataFrame()

#convert location table to a list to radomonly assign egg location - egg can only have 1 of three locations based on assumptions.
location_list = location_df['IH_Location'].tolist()
number_of_samples = 1
NW = 'False'

#Convert Rooster table to List to radomon aassign Rooster to Egg:
rooster_list = roosters_df['C_ID'].tolist()

for index, row in hens_df.iterrows():
    eggs_per_hen = row['egg_count_round']
    hen = row['Name']
    PH_ID = row['C_ID']
    #print(row['egg_count_round'])
    print( row['Name'], eggs_per_hen)
    
    for i in range(eggs_per_hen):
        #Create a new random Egg ID - E_ID
        # generte random integer ids
        # generate egg id
        E_ID = np.random.randint(low=1e3, high=1e9)
        #generate Parent Rooster ID - PR_ID
        PR_ID = random.choices(population=rooster_list, k=number_of_samples)
        
        #Generate Location and assign to each egg
        IH_LOC = random.choices(population=location_list, k=number_of_samples)
                        
        #Append each egg to the Egg Table:
        egg_df = egg_df.append(pd.DataFrame({'E_ID': E_ID, 'PH_Name': hen, 'PH_ID': PH_ID, 'PR_ID': PR_ID, 'IH_LOC': IH_LOC, 'NW': NW}, index=[0]), ignore_index=True)


# In[24]:


#Add Parent_Gen Column to the Egg Table: The initial entry will be Initial_Flock, Gen1, Gen2, Gen3, Gen4
#The Parent_Gen column will help track the operation growth and family, etc.
#Assign Initial_Flock to our initila flock birds
egg_df['Parent_Gen'] = "Initial_Flock"


# In[25]:


print("The Inital Flock has generated", egg_df.shape[0] ,"total eggs and added to the Egg Table")


# In[26]:


print("The total number of unique Egg IDs:", egg_df['E_ID'].nunique())


# In[27]:


egg_df.head(2)


# In[28]:


#Check for duplicate IDs
egg_df[egg_df.duplicated(['E_ID'], keep=False)]


# In[29]:


#Set Location values for Window Location based on IH_LOC == B
egg_df.loc[(egg_df['IH_LOC'] == 'B'), 'NW'] = True 


# In[30]:


ev_df = egg_df['PH_Name'].value_counts().rename_axis('Hen').reset_index(name='egg_count')
ev_df


# In[31]:


#Plot Sensor Execution Counts per hour
fig = px.scatter(ev_df, x='Hen', y='egg_count', 
              template = 'none',
              color = 'egg_count',
              size = 'egg_count',
            title=f"Egg count per hen")
pio.write_html(fig,  f'totaleggs.html')
fig.show()


# In[32]:


df.columns


# In[33]:


# Initial Flock Chickens Table
chickens_df = df[['Name', 'Sex', 'Color', 'C_ID', 'Generation']]
#Assign NaN values to First Generation Chickens for E_ID, PH_Name, PH_ID, PR_ID


# ### Assign Favorite Songs to Initial Flock Chickens

# In[34]:


songs_df.head(5)


# In[35]:


#Create Song list and assign to Chickens
Generation = "Initial_Flock"
song_list = songs_df['Song Clean'].to_list()
chickens_df["Favorite_Song"] = np.random.choice(song_list, size=len(chickens_df))
#chickens_df['Genration'] = Generation


# In[36]:


chickens_df


# In[37]:


#Check for duplicate IDs
chickens_df[chickens_df.duplicated(['C_ID'], keep=False)]


# ___

# ### Convert Initial Flock Eggs to Gen 1 Chickens
# 

# In[38]:


eggconversion_ratio = 0.25


# In[39]:


hens_df


# In[40]:


#Calculate Selection Rate to hatch per Hen
first_window = 30 * 6 * 0.4
#time = 6 months or egg_rate_per_year / 2


# In[41]:


first_gen_selection = hens_df[['Name', 'Sex', 'egg_rate_per_year', 'C_ID', 'Generation']]
first_gen_selection['Counts'] = (first_gen_selection['egg_rate_per_year'] / 2 * 0.27).apply(np.floor)
#df['egg_count_round'] = df['egg_count'].apply(np.floor)


# In[42]:


first_gen_selection


# In[43]:


#Select Eggs from Hens - 53% hens - 47% Roosters


# In[44]:


first_gen_slicer = first_gen_selection[['Name', 'C_ID', 'Counts']]
first_gen_slicer['Counts'] = first_gen_slicer['Counts'].astype(int)


# In[45]:


egg_df.columns


# In[46]:


chicken_first = pd.DataFrame()
for index, row in first_gen_slicer.iterrows():
    chicks_per_hen = row['Counts']
    hen = row['Name']
    PH_ID = row['C_ID']
    
    print( row['Name'], chicks_per_hen)
    
    # Creating a second dataframe that will be the subset of main dataframe
    #print("Second data frame")
    dataframe_first = egg_df[['E_ID', 'PH_Name', 'PH_ID', 'PR_ID']].sample(n=chicks_per_hen)
    chicken_first = chicken_first.append(dataframe_first)
    chicken_first['Generation'] = 'Gen1'
    chicken_first['Color'] = color
    chicken_first['C_ID'] = chicken_first['E_ID']
    


# In[47]:


#chicken_first


# In[48]:


#Assign Favorite Song to First Gen Chickens
chicken_first["Favorite_Song"] = np.random.choice(song_list, size=len(chicken_first))

#Assign Sex to First Gen Chickents
chicken_first['Sex'] = np.random.choice(sex_list, size=len(chicken_first))

first_gen_rooster_count = chicken_first[chicken_first['Sex'] == "Rooster"].shape[0]
first_gen_rooster = chicken_first[chicken_first['Sex'] == "Rooster"]

first_gen_hen_count = chicken_first[chicken_first['Sex'] == "Hen"].shape[0]
first_gen_hen = chicken_first[chicken_first['Sex'] == "Hen"]

print("The first Geration has produced:")
print("================================")
print(first_gen_rooster_count, "Roosters")
print(first_gen_hen_count, "Hens")

#Gerate the an appropriate nummber of Rooster Names for Fist Generation and assign
first_gen_rooster_names = random.sample(rooster_names_list, first_gen_rooster_count)
first_gen_rooster['Name'] = np.random.choice(rooster_names_list, size=len(first_gen_rooster))

#Generate the appropriate number/list of Hens Names for First Generation and assign
first_gen_hens_names = random.sample(hen_names_list, first_gen_hen_count)
first_gen_hen['Name'] = np.random.choice(hen_names_list, size=len(first_gen_hen))


first_gen_chickens = pd.concat([first_gen_hen, first_gen_rooster], axis=0)


# In[49]:


first_gen_chickens.head(5)


# ### Combine Initial Flock and Gen 1 Tables for Chickens, Hens and Roosters

# In[50]:


chickens_df.columns
chickens_df


# In[51]:


#Check for duplicate IDs
chickens_df[chickens_df.duplicated(['C_ID'], keep=False)]


# In[52]:


#Reorder first_gen_chickens dataframe columns to match Initial Flock Table Format so we can Concatenate them


# In[53]:


chickens_df.columns


# In[54]:


#Copy original chickens_df dataframe to initial_flock_chickens for record keeping
initial_flock_chickens = chickens_df
initial_flock_eggs = egg_df


# In[55]:


first_gen_chickens.columns


# In[56]:


first_gen_chickens = first_gen_chickens[['Name', 'Sex', 'Color', 'C_ID', 'Generation', 'Favorite_Song', 'E_ID',
       'PH_Name', 'PH_ID', 'PR_ID']]


# In[57]:


#first_gen_chickens


# In[58]:


#Combine Initial Flock Chickens and First Gen Chickens into Chickens Table
chickens_combined_df = pd.concat([chickens_df, first_gen_chickens], axis=0 )


# In[59]:


chickens_combined_df.head(9)


# In[60]:


print("The initial Flock Hens generated", egg_df.shape[0], "eggs and", first_gen_chickens.shape[0], "chickens")


# In[61]:


chickens_df['C_ID'].nunique()


# In[62]:


#Check for duplicate IDs
chickens_df[chickens_df.duplicated(['C_ID'], keep=False)]


# In[63]:


chickens_df.to_csv('Coop/gen_one_flock_df.csv')


# ___

# ## Gen 1 Hens to Generate Eggs for Gen 2 Chicks

# #### List and Count for Hens from Gen 1

# In[64]:


#Assign Egg Rates to Hens from Gen 1
#Randomly Assign Egg Rate to Hens in Base Family
rate1 = np.random.randint(expected_egg_rate_min, expected_egg_rate_max, size=first_gen_hen_count)

first_gen_hen['egg_rate_per_year'] = np.random.choice(rate1, size=len(first_gen_hen))


# In[65]:


#first_gen_hen.head(5)


# In[66]:


#Create initial eggloggen1_df daily log Pandas Dataframe

eggloggen1_df = pd.DataFrame()
eggloggen1_df['Dates']= pd.date_range(start='10/15/2020', end=today)

eggloggen1_df['diff_years'] = (eggloggen1_df.iloc[-1]['Dates'] - eggloggen1_df.iloc[0]['Dates']) / np.timedelta64(1, 'Y')
gen1_diff_years = (eggloggen1_df.iloc[-1]['Dates'] - eggloggen1_df.iloc[0]['Dates']) / np.timedelta64(1, 'Y')
first_gen_hen['egg_count'] = first_gen_hen['egg_rate_per_year'] * gen1_diff_years

#Round Egg Count down as egg counts have to be a whole number, Int.
first_gen_hen['egg_count_round'] = first_gen_hen['egg_count'].apply(np.floor)

#Convert the egg_count from Floats to Integers
first_gen_hen['egg_count_round'] = first_gen_hen['egg_count_round'].astype(int)


# In[67]:


print("The total number of Farm Operations Days to date for Gen 1 Hens:", eggloggen1_df.shape[0])


# In[68]:


#The roosters could be from the Original Flock (1) or the  Gen1 Roosters
roosters_df = chickens_combined_df[chickens_combined_df.Sex == 'Rooster']


# In[69]:


#The Hens for laying Gen 2 eggs would only include the Gen 1 Hens - first_gen_hen
#first_gen_hen.head(10)
first_gen_hen.columns


# In[70]:


print("The number of potentail Roosters for Gen 2 is:", roosters_df.shape[0])


# #### Eggs

# In[71]:


print("Looping through each Hen and their associated egg counts per Hen to generate the Egg Table")
#Second Generation of Eggs
eggs2_df = pd.DataFrame()

#Add Parent_Gen Column to the Egg Table: The is Gen1 generating Gen2 eggs
#The Parent_Gen column will help track the operation growth and family, etc.
#Assign Gen1 to the Gen 2 Egg routine
eggs2_df['Parent_Gen'] = "Gen1"

#convert location table to a list to radomonly assign egg location - egg can only have 1 of three locations based on assumptions.
location_list = location_df['IH_Location'].tolist()
number_of_samples = 1
NW = 'False'

#Convert Complete Rooster table to List to radomon aassign Rooster to Egg:
#Use Complete Rooster List
rooster_list = roosters_df['C_ID'].tolist()

#Iterate over each Gen1 Hen

for index, row in first_gen_hen.iterrows():
    eggs_per_hen = row['egg_count_round']
    hen = row['Name']
    PH_ID = row['C_ID']
    #print(row['egg_count_round'])
    print( row['Name'], eggs_per_hen)
    
    for i in range(eggs_per_hen):
        #Create a new random Egg ID - E_ID
        # generte random integer ids
        # generate egg id
        E_ID = np.random.randint(low=1e3, high=1e9)
        Parent_Gen = "Gen1"
        #generate Parent Rooster ID - PR_ID
        PR_ID = random.choices(population=rooster_list, k=number_of_samples)
        
        #Generate Location and assign to each egg
        IH_LOC = random.choices(population=location_list, k=number_of_samples)
                        
        #Append each egg to the Egg Table:
        eggs2_df = eggs2_df.append(pd.DataFrame({'E_ID': E_ID, 'PH_Name': hen, 'PH_ID': PH_ID, 'PR_ID': PR_ID, 'IH_LOC': IH_LOC, 'NW': NW, 'Parent_Gen': Parent_Gen }, index=[0]), ignore_index=True)


# In[72]:


eggs2_df.head(3)


# In[73]:


eggs2_df.shape[0]


# In[74]:


#Check for duplicate IDs
eggs2_df[eggs2_df.duplicated(['E_ID'], keep=False)]


# In[75]:


#Set Location values for Window Location based on IH_LOC == B
eggs2_df.loc[(eggs2_df['IH_LOC'] == 'B'), 'NW'] = True 


# In[76]:


#eggs2_df.head(10)


# ### Convert Gen 2 Eggs to Chickens and Combine
# * combine Total Eggs Table
# * combine Total Chicken Table

# In[77]:


#Provide a smaller rate than the first generation
gen2_eggconversion_ratio = 0.43


# In[78]:


second_gen_selection = first_gen_hen[['Name', 'Sex', 'egg_rate_per_year', 'C_ID', 'Generation']]
second_gen_selection['Counts'] = (second_gen_selection['egg_rate_per_year'] / 2 * 0.23).apply(np.floor)

second_gen_selection['Counts'] = second_gen_selection['Counts'].astype(int)
#df['egg_count_round'] = df['egg_count'].apply(np.floor)
print("This selection ratio should provide the farm with", second_gen_selection['Counts'].sum(), "Gen 2 Chickens")


# In[79]:


#second_gen_selection


# In[80]:


second_gen_slicer = second_gen_selection[['Name', 'C_ID', 'Counts']]


# In[81]:


#second_gen_slicer


# In[82]:


#Check for duplicate IDs
eggs2_df[eggs2_df.duplicated(['E_ID'], keep=False)]


# In[83]:


chicken_second = pd.DataFrame()

for index, row in second_gen_slicer.iterrows():
    chicks_per_hen = row['Counts']
    hen = row['Name']
    PH_ID = row['C_ID']
    
    
    # Creating a second dataframe that will be the subset of main dataframe
    #print("Second data frame")
    dataframe_second = eggs2_df[['E_ID', 'PH_Name', 'PH_ID', 'PR_ID']].sample(n=chicks_per_hen, replace= False)
    
    #Line to fix loop - Convert Egg ID (E_ID) to Chicken ID before appending to second generation dataframe
    dataframe_second['C_ID'] =dataframe_second['E_ID']
    
    chicken_second = chicken_second.append(dataframe_second)
    chicken_second['Generation'] = 'Gen2'
    chicken_second['Color'] = color
    
    #chicken_second['C_ID'] = chicken_second['E_ID']
    


# In[84]:


chicken_second.columns


# In[85]:


#Check for duplicate IDs
chicken_second[chicken_second.duplicated(['E_ID'], keep=False)].sort_values("E_ID")


# In[86]:


chicken_second['C_ID'].nunique()


# In[87]:


chicken_second.shape[0]


# In[88]:


#Check for duplicate IDs
#chicken_second[chicken_second.duplicated(['C_ID'], keep=False)].sort_values("C_ID")

#df = df.drop_duplicates('column_name', keep='last')
c2_clean_df = chicken_second.drop_duplicates('C_ID', keep='first')
c2_clean_df.shape[0]


# In[89]:


#Verify Eggs ID duplicate IDs are removed and
c2_clean_count = len(c2_clean_df[c2_clean_df.duplicated(['E_ID'], keep=False)].sort_values("E_ID"))
c2_clean_df[c2_clean_df.duplicated(['E_ID'], keep=False)].sort_values("E_ID")


# In[90]:


#Reassign if = 0
print(c2_clean_count)
if c2_clean_count == 0:
    print("Re-assigning clean dataframe to second generation of Chickens")
    chicken_second = c2_clean_df
else:
    print("There are still some duplicate records")


# In[91]:


#Assign Favorite Song to First Gen Chickens
chicken_second["Favorite_Song"] = np.random.choice(song_list, size=len(chicken_second))

#Assign Sex to First Gen Chickents
chicken_second['Sex'] = np.random.choice(sex_list, size=len(chicken_second))

second_gen_rooster_count = chicken_second[chicken_second['Sex'] == "Rooster"].shape[0]
second_gen_rooster = chicken_second[chicken_second['Sex'] == "Rooster"]

second_gen_hen_count = chicken_second[chicken_second['Sex'] == "Hen"].shape[0]
second_gen_hen = chicken_second[chicken_second['Sex'] == "Hen"]

print("The Second Geration has produced:")
print("================================")
print(second_gen_rooster_count, "Roosters")
print(second_gen_hen_count, "Hens")

#Gerate the an appropriate nummber of Rooster Names for Fist Generation and assign
second_gen_rooster_names = random.sample(rooster_names_list, second_gen_rooster_count)
second_gen_rooster['Name'] = np.random.choice(rooster_names_list, size=len(second_gen_rooster))

#Generate the appropriate number/list of Hens Names for First Generation and assign
second_gen_hens_names = random.sample(hen_names_list, second_gen_hen_count)
second_gen_hen['Name'] = np.random.choice(hen_names_list, size=len(second_gen_hen))


second_gen_chickens = pd.concat([second_gen_hen, second_gen_rooster], axis=0)


# In[92]:


second_gen_chickens.shape[0]


# In[93]:


second_gen_chickens.head(5)


# In[94]:


#Combine Initial Flock Chickens and First Gen Chickens into Chickens Table
chickens_total_df = pd.concat([chickens_combined_df, second_gen_chickens], axis=0 )


# In[95]:


chickens_total_df.shape[0]


# In[96]:


chickens_total_df.columns


# In[97]:


chickens_total_df['C_ID'].nunique()


# In[98]:


#c2_clean_df = chicken_second.drop_duplicates('C_ID', keep='first')

chickens_total_df = chickens_total_df.drop_duplicates('C_ID', keep='first')


# In[99]:


#Verify Duplicates are removed
chickens_total_df[chickens_total_df.duplicated(['C_ID'], keep=False)]


# In[100]:


chickens_total_df.to_csv('Coop/chickens_total_df.csv')


# In[101]:


#### Combine Both Generation Eggs into Complete Table
eggs_total_df = pd.concat([egg_df, eggs2_df], axis=0)


# In[102]:


eggs_total_df.shape[0]


# In[103]:


eggs_total_df.to_csv('Coop/eggs_total_df.csv')


# ___

# # Exercise 3 Name Tags Table Generation

# In[104]:


chickens_total_df.columns


# In[105]:


chickens_total_df.shape[0]


# In[106]:


ctags_df = chickens_total_df[['Name', 'Favorite_Song', 'C_ID', 'E_ID', 'PH_Name', 'PH_ID', 'PR_ID']]


# In[107]:


#Rename PH_Name coloumn to Mother - To provide cleanar data
ctags_df.rename({'PH_Name': 'Mother'}, axis=1, inplace=True)


# In[108]:


etags_df = eggs_total_df


# In[109]:


etags_df.columns


# In[110]:


itag_df = etags_df[['E_ID', 'IH_LOC']]


# In[111]:


#Merge Chickens with Eggs to map Chickens to Eggs.
tags_df = pd.merge(ctags_df, etags_df, how='outer', on = 'E_ID' , suffixes=['_Chicken','_Egg'])


# In[112]:


tags_df.head(7)


# In[113]:


#tags_df.shape[0]


# ### Add Father Name - Join Rooster Names

# In[114]:


rooster_total_df = chickens_total_df[chickens_total_df['Sex'] == 'Rooster']


# In[115]:


rooster_total_df.shape[0]


# In[116]:


#rooster_total_df.columns


# In[117]:


rooster_tags_df = rooster_total_df[['Name', 'C_ID', 'Generation', 'E_ID', 'PH_Name', 'PH_ID', 'PR_ID']]


# In[118]:


#twr_df: Tags with Rooster Data
twr_df = pd.merge(tags_df, rooster_tags_df, how='inner', left_on='PR_ID_Chicken', right_on='C_ID', suffixes=['_t', '_Father'])


# In[119]:


twr_df.shape[0]


# In[120]:


#twr_df.head(5)
twr_df.columns


# In[121]:


twr_df[['Name_t', 'Favorite_Song', 'Mother', 'Name_Father']].head(5)


# In[122]:


#Export Tags with Roosers to CSV file for data analysis
twr_df.to_csv('Coop/twr_df.csv')


# In[123]:


#twr_df.columns


# In[124]:


#ctags_df.columns


# In[125]:


#Chicken Grand Parents Tags Datafrmae to use for merging so we have less data in the dataframe to merge
cgptags_df = ctags_df[['Name', 'Mother', 'C_ID', 'E_ID']]
#cgptags_df = ctags_df[['Name', 'C_ID', 'E_ID']]


# ### Merge in GrandParents using IDs

# In[126]:


#tags_with_rooster (Father) and GrandMother - twrgm_df
twrgm_df = pd.merge(twr_df, cgptags_df, how='outer', left_on='PH_ID_Chicken', right_on='C_ID', suffixes=['twr', '_Maternal_Mother'])
#tags_with_rooster_grandmother_and_grandfather
twrgf_df = pd.merge(twrgm_df, cgptags_df, how='outer', left_on='PR_ID', right_on='C_ID', suffixes=['CMM', '_Maternal_Father'])


# In[127]:


twrgf_df.columns


# In[128]:


#Rename PH_Name coloumn to Mother - To provide cleanar data
twrgf_df.rename({'Mother_Maternal_Mother': 'GrandMother'}, axis=1, inplace=True)
twrgf_df.rename({'Name_Maternal_Father': 'GrandFather'}, axis=1, inplace=True)
twrgf_df.rename({'Name_Father': 'Father'}, axis=1, inplace=True)


# ### Merge in Incubation Location

# In[129]:


#twrgf_df['IH_LOC'].head(3)


# In[130]:


twrgf_df[['GrandMother', 'GrandFather', 'Father']].head(5)


# In[131]:


#itag_df.columns


# In[132]:


EIDs = [ 'E_ID_', 'PH_ID_Egg', 'PR_ID_Egg',  'E_ID_Father', 'E_ID_Maternal_Mother', 'E_ID']


# In[133]:


#twrgd2_loc_df.columns


# In[134]:


#Merge in Incubation Location for Mother, Father, GrandMother and GrandFather using Egg Table - E_IDs
twrgd_loc_df = pd.merge(twrgf_df, itag_df, left_on=['PH_ID_Chicken'], right_on = ['E_ID'], how='inner', suffixes=['Chicken', '_MotherIL'])
twrgd1_loc_df = pd.merge(twrgd_loc_df, itag_df, left_on=['PR_ID_Egg'], right_on = ['E_ID'], how='inner', suffixes=['CM', '_FatherIL'])
twrgd2_loc_df = pd.merge(twrgd1_loc_df, itag_df, left_on=['E_IDCMM'], right_on = ['E_ID'], how='inner', suffixes=['CMF', '_GrandMother_IL'])
twrgd3_loc_df = pd.merge(twrgd2_loc_df, itag_df, left_on=['E_ID'], right_on = ['E_ID'], how='inner', suffixes=['C', '_GrandFather_IL'])


# In[135]:


#Analyze existing Column Names
twrgd3_loc_df.columns


# In[136]:


int_tags_df = twrgd3_loc_df.drop('Mother', axis=1)
int_tags_df.head(3)


# In[137]:


int_tags_df[['IH_LOCChicken',
       'Parent_Gen','Mothertwr', 'Father', 
        'GrandMother', 
       'GrandFather',  
        'IH_LOC_MotherIL', 'IH_LOCCMF',
        'IH_LOC_GrandMother_IL', 'IH_LOC']].head(5)


# In[138]:


#Rename PH_Name coloumn to Mother - To provide cleanar data
twrgd3_loc_df.rename({'Mothertwr': 'Mother'}, axis=1, inplace=True)
twrgd3_loc_df.rename({'Nametwr': 'Name'}, axis=1, inplace=True)
twrgd3_loc_df.rename({'IH_LOCChicken': 'I_LOC'}, axis=1, inplace=True)
twrgd3_loc_df.rename({'IH_LOC_MotherIL': 'Mother_I_LOC'}, axis=1, inplace=True)
twrgd3_loc_df.rename({'IH_LOCCMF': 'Father_I_LOC'}, axis=1, inplace=True)
twrgd3_loc_df.rename({'IH_LOC_GrandMother_IL': 'GrandMother_I_LOC'}, axis=1, inplace=True)
twrgd3_loc_df.rename({'IH_LOC': 'GrandFather_I_LOC'}, axis=1, inplace=True)


# In[139]:


#Rename PH_Name coloumn to Mother - To provide cleanar data
int_tags_df.rename({'Mothertwr': 'Mother'}, axis=1, inplace=True)
int_tags_df.rename({'Name_t': 'Name'}, axis=1, inplace=True)
int_tags_df.rename({'IH_LOCChicken': 'I_LOC'}, axis=1, inplace=True)
int_tags_df.rename({'IH_LOC_MotherIL': 'Mother_I_LOC'}, axis=1, inplace=True)
int_tags_df.rename({'IH_LOCCMF': 'Father_I_LOC'}, axis=1, inplace=True)
int_tags_df.rename({'IH_LOC_GrandMother_IL': 'GrandMother_I_LOC'}, axis=1, inplace=True)
int_tags_df.rename({'IH_LOC': 'GrandFather_I_LOC'}, axis=1, inplace=True)


# In[140]:


int_tags_df.head(5)


# In[141]:


int_tags_df.to_csv('Coop/tags.csv')


# In[142]:


int_tags_df.columns


# In[143]:


int_tags_df[['Name', 'Favorite_Song', 'Mother', 'Father', 'GrandMother', 'GrandFather', 'I_LOC', 'Mother_I_LOC', 'Father_I_LOC', 'GrandMother_I_LOC', 'GrandFather_I_LOC' ]].head(6)


# In[144]:


int_tags_df.shape[0]


# In[145]:


grand = int_tags_df[twrgf_df['Parent_Gen'] == 'Gen1']


# In[146]:


grand.columns


# In[147]:


grand[['Name',  'Mother', 
       'Parent_Gen', 'Father',
       'GrandMother', 'GrandFather']].head(6)


# In[148]:


int_tags_df['GrandMother'].nunique()


# ___

# In[ ]:





# ### Create Cousins from GrandMother - GroupBy GrandMother and Assign back to the Table for Tags

# In[149]:


cousins_df = int_tags_df.groupby('GrandMother')


# In[150]:


cousins_df.first()


# In[151]:


barb_family_list = cousins_df.get_group('Barb')['Name'].tolist()
liz_family_list = cousins_df.get_group('Liz')['Name'].tolist()
mary_family_list = cousins_df.get_group('Mary')['Name'].tolist()
pat_family_list = cousins_df.get_group('Pat')['Name'].tolist()


# In[152]:


size=len(barb_family_list)
size


# In[153]:


#type(#Assign Favorite Song to First Gen Chickens
#chicken_first["Favorite_Song"] = np.random.choice(song_list, size=len(chicken_first)))
 #   df['b'] = np.where(df.a.values == 0, np.nan, df.b.values)
barb_df = int_tags_df[int_tags_df['GrandMother'] == 'Barb']
liz_df = int_tags_df[int_tags_df['GrandMother'] == 'Liz']
mary_df = int_tags_df[int_tags_df['GrandMother'] == 'Mary']
pat_df = int_tags_df[int_tags_df['GrandMother'] == 'Pat']

barb_df['Cousin'] = np.random.choice(barb_family_list, size=len(barb_family_list))
liz_df['Cousin'] = np.random.choice(liz_family_list, size=len(liz_family_list))
mary_df['Cousin'] = np.random.choice(mary_family_list, size=len(mary_family_list))
pat_df['Cousin'] = np.random.choice(pat_family_list, size=len(pat_family_list))

#int_tags_df['Cousin'] = np.where(int_tags_df['GrandMother'] == 'Barb'), np.random.choice(barb_family_list, size=len(barb_family_list))

barb_c_df = barb_df[['Name', 'C_ID_t', 'Cousin']]
liz_c_df = liz_df[['Name', 'C_ID_t', 'Cousin']]
mary_c_df = mary_df[['Name', 'C_ID_t', 'Cousin']]
pat_c_df = pat_df[['Name', 'C_ID_t', 'Cousin']]

#Stack the individual family cousins into a single datafrme for merging into Chicken Tags with GrandParents and Location
cousins_selected_df = pd.concat([barb_c_df, liz_c_df, mary_c_df, pat_c_df], axis=0)


# In[154]:


#Uncomment and run to verify Chicken Name and Cousin Name
#cousins_selected_df.head(10)


# In[155]:


int_tags_df.columns


# In[156]:


int_tags_df[['Name', 'C_ID_t']].head(5)


# In[157]:


name_tags_full_df = pd.merge(int_tags_df, cousins_selected_df, how='outer', on='C_ID_t') 


# In[158]:


name_tags_full_df.columns


# In[159]:


name_tags_full_df.rename({'Name_x': 'Name'}, axis=1, inplace=True)
name_tags_full_df[['Name', 'Cousin']].head(5)


# In[160]:


name_tags_full_df[['Name', 'Favorite_Song', 'Mother', 'Father', 'GrandMother', 'GrandFather', 'I_LOC', 'Mother_I_LOC', 'Father_I_LOC', 'GrandMother_I_LOC', 'GrandFather_I_LOC', 'Cousin' ]].head(6)


# In[161]:


name_tags_full_df.shape[0]


# In[162]:


name_tags_full_df.columns


# In[163]:


duplicateIDs_nt_df = name_tags_full_df[name_tags_full_df.duplicated(['C_ID_t'], keep=False)]


# In[164]:


duplicateIDs_nt_df.to_csv('Coop/duplicated_nt_ids.csv')


# In[165]:


### Name Tag clean up to remove any possible duplicates and trim to only 1000 records as that is the number
### defined in the exercise: 1000
#df.drop_duplicates(subset='A', keep="last")
name_tags_full_df.drop_duplicates(subset='Name', keep='first')


# In[166]:


name_tags_full_df.to_csv('Coop/name_tags_complete_df.csv')


# In[167]:


name_tags_df = name_tags_full_df[['Name', 'Favorite_Song', 'Mother', 'Father', 'GrandMother', 'GrandFather', 'I_LOC', 'Mother_I_LOC', 'Father_I_LOC', 'GrandMother_I_LOC', 'GrandFather_I_LOC', 'Cousin' ]]


# In[168]:


name_tags_df


# In[169]:


name_tags_full_df.shape[0]


# In[170]:


#Per the exercise instruction we will just generate the firt 1000 Tags for the Table.
name_tags_1000_df = name_tags_full_df.head(1000)


# In[171]:


#Verify Name Tag Table Shape
name_tags_1000_df.shape


# In[172]:


name_tags_df.to_csv('Coop/name_tags_final_df.csv')
name_tags_1000_df.to_csv('Coop/name_tags_1000_final_df.csv')


# In[173]:


chickens_total_df['Generation'].nunique()


# In[174]:


chickens_total_df.columns


# In[175]:


print("Checking for any duplicate Chickens in the Table = ", len(chickens_total_df[chickens_total_df.duplicated(['C_ID'], keep=False)]))
chickens_total_df[chickens_total_df.duplicated(['C_ID'], keep=False)]


# ___

# ### Bonus Assignment - Show Hatch Dates and make sure the data makes sense

# In[176]:


## Bonus Assign Egg Date and Hatch Dates to Generation - use to plot
#dailylog_df
#Assume Gen 1 has about 150 days for intial flock starts producing eggs and the eggs hatch 
#eggloggen1_df
gen_one_dates_list = dailylog_df[150:]['Dates'].tolist()
gen_two_dates_list = eggloggen1_df[150:]['Dates'].tolist()


# In[177]:


#eggloggen1_df[150:]['Dates']


# In[178]:


gen_one_lenght = chickens_total_df[chickens_total_df.Generation == 'Gen1'].shape[0]
gen_two_lenght = chickens_total_df[chickens_total_df.Generation == 'Gen2'].shape[0]


# In[179]:


#Dataframes:
#chickens_total_df.head(5)
gen_one_length = chickens_total_df.Generation == 'Gen1'
#Assign Nan values to Chickens with Generation of Initial_Flock for hatch_date
chickens_total_df.loc[chickens_total_df.Generation == 'Initial_Flock', 'Hatch_Date'] = np.nan
chickens_total_df.loc[chickens_total_df.Generation == 'Gen1', 'Hatch_Date'] = np.random.choice(gen_one_dates_list, size=gen_one_lenght)
chickens_total_df.loc[chickens_total_df.Generation == 'Gen2', 'Hatch_Date'] = np.random.choice(gen_two_dates_list, size=gen_two_lenght)

#eggs_total_df
chickens_total_df.head(7)


# In[180]:


chickens_total_df.columns


# In[181]:


liz_df


# In[182]:


eggs_total_df.shape[0]


# In[183]:


#Remove Eggs that Hatched into Chickens using a left outer join
egg_only_df = pd.merge(eggs_total_df, chickens_total_df, how='outer', on='E_ID', indicator=True).query('_merge=="left_only"')


# In[184]:


egg_only_df.shape[0]


# In[185]:


#egg_only_df.columns


# In[186]:


# Add Egg Dates to Eggs.
#df.drop(['column_nameA', 'column_nameB'], axis=1, inplace=True)
egg_only_df.drop(['Name', 'Sex', 'Color', 'C_ID', 'Generation', 'Favorite_Song', 'PH_Name_y', 'PH_ID_y', 'PR_ID_y', 'Hatch_Date', '_merge'], axis=1, inplace=True)

egg_only_df.head(5)


# In[187]:


egg_lenght = egg_only_df.shape[0]
egg_only_df['Egg_Date'] = np.random.choice(gen_one_dates_list, size=egg_lenght)
egg_only_df.head(5)


# ___

# ## Bonus Convert Dataframes to DuckDB database for integration with Metabse

# In[188]:


#Connect jupysql to DuckDB using a SQLAlchemy-style connection string. Either connect to an in memory DuckDB, or a file backed db.

get_ipython().run_line_magic('sql', 'duckdb:///Coop/coop_farm_db.duckdb')


# In[189]:


### Convert all the necessary Dataframes to DuckDB database for integration with Metabse
# create the table "my_table" from the DataFrame "my_df"
get_ipython().run_line_magic('sql', 'CREATE TABLE chicken_name_tags as select * from name_tags_df ;')


# In[190]:


# insert into the table "chicken_name_tags" from the DataFrame "name_tags_df"
#%sql INSERT INTO chicken_name_tags SELECT * FROM name_tags_df;


# In[191]:


#Execute a Select query to see all the table data in the chicken_name_tags table
#%sql select * from chicken_name_tags;


# In[192]:


#Show Tables in the DucDB Database
get_ipython().run_line_magic('sql', 'show tables;')


# In[193]:


name_tags_full_df.shape


# In[194]:


#Investigate and import the Dataframes in to DuckDB Tables.


# In[195]:


#List of Dataframes to ingest into DuckDB Tables
#Songs table dataframe - 
dataframes = ['songs_df', 'egg_only_df', 'chickens_total_df','int_tags_df', 'barb_df', 'liz_df', 'mary_df', 'pat_df', 'name_tags_full_df']
#name_tags_df - Already Ingested


# In[196]:


dataframes


# In[197]:


#Loop and Create Table, import and convert to Parquet in Duckdb.duckdb

for dataframe in dataframes:
    print("creating table:", dataframe)
    get_ipython().run_line_magic('sql', 'CREATE TABLE {dataframe} as select * from {dataframe} ;')
   # %sql INSERT INTO {dataframe} SELECT * FROM {dataframe};
    
    #%sql COPY {table} TO '{table}.parquet' (FORMAT PARQUET);


# In[198]:


get_ipython().run_line_magic('sql', 'select * from chicken_name_tags;')


# In[199]:


#After testing - discovered Metabase doesn't currently support the latest version of DuckDB so we export the database as a set
#of Parquet files which allow us to use in the Metabase DuckDB connection as files.d
get_ipython().run_line_magic('sql', "EXPORT DATABASE 'Coop' (FORMAT PARQUET);")


# In[200]:


get_ipython().run_line_magic('sql', 'select count(*) from name_tags_full_df;')


# In[ ]:




