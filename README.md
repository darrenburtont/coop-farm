# coop-farm
Chicken Farm Data Exercise

Providing improved packaging with lineage information for each individual bird.

The Python code and notes are documented within the Python Jupyter Notebook. A HTML version of the notebook
is included to allow access to users without Python Jupyter Notebooks. The collapsable Details section below
includes some of the same details wihin the Notebook.

### Data Schema
<details>
    
___
The Data Schema Represents chicken and egg family trees

**Chickens have:**
* Chicken ID (C_ID)
* Name (Name)
* Sex (Rooster or Hen) (Sex)
* Feather color (Color)
* Favorite song (Favorite_Song)
* Each Hen lays eggs
* Generation ID (G_ID) int

**Eggs have:** 
* An identification number (E_ID) will end up mapping to C_ID
* Location in the incubation hall where hens sit on their eggs. (IH_LOC)
* Whether that spot is near a window (as 1/3rd of the spots should be) (NW - Boolean [True, False])
* Parent IDs (PH_ID, PR_ID)(Will need to map to C_IDs)

**Chicken Genealogy:**
* The egg that it came from (E_ID)
* Its parents (PH_ID) - (PR_ID), and their eggs - (PHE_ID, PRE_ID)
* The Grandparents, etc) - HGPH_ID, HGPR_ID, RGPH_ID, RGPR_ID 
* any additional columns to the chicken and egg tables or any other way needed.
    
**Note:**
You will probably need more columns than just the above minimum information.
    </details>
    
### Generate data
<details>
Two weeks after your starting, all records were destroyed after a ransom malware attack scrambled the database filesystem. Despite the farmworkers trying to remember all the chicken's names, it's impossible to tell them apart now.

We need to recreate, (generate fake data) about all chickens currently on the farm (1000 chickens). \
*Some of whom are parents to others.*

**Generate the required 1000 records**

What can you do to make these records seem as realistic as possible? (have realistic timelines and age)
(Feel free to look up data as you need to, but tell us what you looked up?) 
 
 * Bonus: How could a government official check whether the dataset is faked or not? 
    (most chicken species have documented egg rates and age before producing egges)
 * Bonus Bonus: What can you do to cover up these checks?
    (use the published ranges with randomization over actual calendar days to make data more realistic) 
* Bonus Bonus Bonus: What can a government official check to see whether you're covering up their checks. Etc
    </details>
    
    ### Name tags for Guided Tours
 
 <details>
We Give guided tours of the farm and introduce all the chickens to visitors. 
To make this possible we print tags and attached to each chicken's leg. 

The Tag includes:

* The Chicken's name
* Their Favorite song
* Their Parents
* Their Grandparents
* The Location each parent and grand parent was incubated
* A randomly selected first cousin of the chicken
    
    https://github.com/aruljohn/popular-baby-names/blob/master/2000/boy_names_2000.csv \
    https://github.com/fivethirtyeight/data/blob/master/classic-rock/classic-rock-song-list.csv
    </details>
    
    
    ### Bonus
<details>
Create a dashboard in Metabase that shows some KPIs for this chicken farm. 
Please include either a public link or a screenshot.
    </details>
