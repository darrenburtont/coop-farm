# coop-farm
Chicken Farm Data Exercise

Providing improved packaging with lineage information for each individual bird.

### Data Schema
 ___
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
