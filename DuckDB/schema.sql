


CREATE TABLE chicken_name_tags("Name" VARCHAR, "Favorite_Song" VARCHAR, "Mother" VARCHAR, "Father" VARCHAR, "GrandMother" VARCHAR, "GrandFather" VARCHAR, "I_LOC" VARCHAR, "Mother_I_LOC" VARCHAR, "Father_I_LOC" VARCHAR, "GrandMother_I_LOC" VARCHAR, "GrandFather_I_LOC" VARCHAR, "Cousin" VARCHAR);
CREATE TABLE songs_df("Song Clean" VARCHAR, "ARTIST CLEAN" VARCHAR, "Release Year" VARCHAR, "COMBINED" VARCHAR, "First?" BIGINT, "Year?" BIGINT, "PlayCount" BIGINT, "F*G" BIGINT);
CREATE TABLE egg_only_df("E_ID" DOUBLE, "PH_Name_x" VARCHAR, "PH_ID_x" DOUBLE, "PR_ID_x" DOUBLE, "IH_LOC" VARCHAR, "NW" VARCHAR, "Parent_Gen" VARCHAR, "Egg_Date" TIMESTAMP);
CREATE TABLE chickens_total_df("Name" VARCHAR, "Sex" VARCHAR, "Color" VARCHAR, "C_ID" DOUBLE, "Generation" VARCHAR, "Favorite_Song" VARCHAR, "E_ID" DOUBLE, "PH_Name" VARCHAR, "PH_ID" DOUBLE, "PR_ID" DOUBLE, "Hatch_Date" TIMESTAMP);
CREATE TABLE int_tags_df("Name" VARCHAR, "Favorite_Song" VARCHAR, "C_ID_t" DOUBLE, "E_ID_t" DOUBLE, "Mother" VARCHAR, "PH_ID_Chicken" DOUBLE, "PR_ID_Chicken" DOUBLE, "PH_Name_t" VARCHAR, "PH_ID_Egg" DOUBLE, "PR_ID_Egg" DOUBLE, "I_LOC" VARCHAR, "NW" VARCHAR, "Parent_Gen" VARCHAR, "Father" VARCHAR, "C_ID_Father" INTEGER, "Generation" VARCHAR, "E_ID_Father" INTEGER, "PH_Name_Father" VARCHAR, "PH_ID" INTEGER, "PR_ID" INTEGER, "NameCMM" VARCHAR, "GrandMother" VARCHAR, "C_IDCMM" INTEGER, "E_IDCMM" INTEGER, "GrandFather" VARCHAR, "C_ID_Maternal_Father" INTEGER, "E_ID_Maternal_Father" INTEGER, "E_IDCM" INTEGER, "Mother_I_LOC" VARCHAR, "E_ID_FatherIL" INTEGER, "Father_I_LOC" VARCHAR, "E_ID" INTEGER, "GrandMother_I_LOC" VARCHAR, "GrandFather_I_LOC" VARCHAR);
CREATE TABLE barb_df("Name" VARCHAR, "Favorite_Song" VARCHAR, "C_ID_t" DOUBLE, "E_ID_t" DOUBLE, "Mother" VARCHAR, "PH_ID_Chicken" DOUBLE, "PR_ID_Chicken" DOUBLE, "PH_Name_t" VARCHAR, "PH_ID_Egg" DOUBLE, "PR_ID_Egg" DOUBLE, "I_LOC" VARCHAR, "NW" VARCHAR, "Parent_Gen" VARCHAR, "Father" VARCHAR, "C_ID_Father" INTEGER, "Generation" VARCHAR, "E_ID_Father" INTEGER, "PH_Name_Father" VARCHAR, "PH_ID" INTEGER, "PR_ID" INTEGER, "NameCMM" VARCHAR, "GrandMother" VARCHAR, "C_IDCMM" INTEGER, "E_IDCMM" INTEGER, "GrandFather" VARCHAR, "C_ID_Maternal_Father" INTEGER, "E_ID_Maternal_Father" INTEGER, "E_IDCM" INTEGER, "Mother_I_LOC" VARCHAR, "E_ID_FatherIL" INTEGER, "Father_I_LOC" VARCHAR, "E_ID" INTEGER, "GrandMother_I_LOC" VARCHAR, "GrandFather_I_LOC" VARCHAR, "Cousin" VARCHAR);
CREATE TABLE liz_df("Name" VARCHAR, "Favorite_Song" VARCHAR, "C_ID_t" DOUBLE, "E_ID_t" DOUBLE, "Mother" VARCHAR, "PH_ID_Chicken" DOUBLE, "PR_ID_Chicken" DOUBLE, "PH_Name_t" VARCHAR, "PH_ID_Egg" DOUBLE, "PR_ID_Egg" DOUBLE, "I_LOC" VARCHAR, "NW" VARCHAR, "Parent_Gen" VARCHAR, "Father" VARCHAR, "C_ID_Father" INTEGER, "Generation" VARCHAR, "E_ID_Father" INTEGER, "PH_Name_Father" VARCHAR, "PH_ID" INTEGER, "PR_ID" INTEGER, "NameCMM" VARCHAR, "GrandMother" VARCHAR, "C_IDCMM" INTEGER, "E_IDCMM" INTEGER, "GrandFather" VARCHAR, "C_ID_Maternal_Father" INTEGER, "E_ID_Maternal_Father" INTEGER, "E_IDCM" INTEGER, "Mother_I_LOC" VARCHAR, "E_ID_FatherIL" INTEGER, "Father_I_LOC" VARCHAR, "E_ID" INTEGER, "GrandMother_I_LOC" VARCHAR, "GrandFather_I_LOC" VARCHAR, "Cousin" VARCHAR);
CREATE TABLE mary_df("Name" VARCHAR, "Favorite_Song" VARCHAR, "C_ID_t" DOUBLE, "E_ID_t" DOUBLE, "Mother" VARCHAR, "PH_ID_Chicken" DOUBLE, "PR_ID_Chicken" DOUBLE, "PH_Name_t" VARCHAR, "PH_ID_Egg" DOUBLE, "PR_ID_Egg" DOUBLE, "I_LOC" VARCHAR, "NW" VARCHAR, "Parent_Gen" VARCHAR, "Father" VARCHAR, "C_ID_Father" INTEGER, "Generation" VARCHAR, "E_ID_Father" INTEGER, "PH_Name_Father" VARCHAR, "PH_ID" INTEGER, "PR_ID" INTEGER, "NameCMM" VARCHAR, "GrandMother" VARCHAR, "C_IDCMM" INTEGER, "E_IDCMM" INTEGER, "GrandFather" VARCHAR, "C_ID_Maternal_Father" INTEGER, "E_ID_Maternal_Father" INTEGER, "E_IDCM" INTEGER, "Mother_I_LOC" VARCHAR, "E_ID_FatherIL" INTEGER, "Father_I_LOC" VARCHAR, "E_ID" INTEGER, "GrandMother_I_LOC" VARCHAR, "GrandFather_I_LOC" VARCHAR, "Cousin" VARCHAR);
CREATE TABLE pat_df("Name" VARCHAR, "Favorite_Song" VARCHAR, "C_ID_t" DOUBLE, "E_ID_t" DOUBLE, "Mother" VARCHAR, "PH_ID_Chicken" DOUBLE, "PR_ID_Chicken" DOUBLE, "PH_Name_t" VARCHAR, "PH_ID_Egg" DOUBLE, "PR_ID_Egg" DOUBLE, "I_LOC" VARCHAR, "NW" VARCHAR, "Parent_Gen" VARCHAR, "Father" VARCHAR, "C_ID_Father" INTEGER, "Generation" VARCHAR, "E_ID_Father" INTEGER, "PH_Name_Father" VARCHAR, "PH_ID" INTEGER, "PR_ID" INTEGER, "NameCMM" VARCHAR, "GrandMother" VARCHAR, "C_IDCMM" INTEGER, "E_IDCMM" INTEGER, "GrandFather" VARCHAR, "C_ID_Maternal_Father" INTEGER, "E_ID_Maternal_Father" INTEGER, "E_IDCM" INTEGER, "Mother_I_LOC" VARCHAR, "E_ID_FatherIL" INTEGER, "Father_I_LOC" VARCHAR, "E_ID" INTEGER, "GrandMother_I_LOC" VARCHAR, "GrandFather_I_LOC" VARCHAR, "Cousin" VARCHAR);
CREATE TABLE name_tags_full_df("Name" VARCHAR, "Favorite_Song" VARCHAR, "C_ID_t" DOUBLE, "E_ID_t" DOUBLE, "Mother" VARCHAR, "PH_ID_Chicken" DOUBLE, "PR_ID_Chicken" DOUBLE, "PH_Name_t" VARCHAR, "PH_ID_Egg" DOUBLE, "PR_ID_Egg" DOUBLE, "I_LOC" VARCHAR, "NW" VARCHAR, "Parent_Gen" VARCHAR, "Father" VARCHAR, "C_ID_Father" INTEGER, "Generation" VARCHAR, "E_ID_Father" INTEGER, "PH_Name_Father" VARCHAR, "PH_ID" INTEGER, "PR_ID" INTEGER, "NameCMM" VARCHAR, "GrandMother" VARCHAR, "C_IDCMM" INTEGER, "E_IDCMM" INTEGER, "GrandFather" VARCHAR, "C_ID_Maternal_Father" INTEGER, "E_ID_Maternal_Father" INTEGER, "E_IDCM" INTEGER, "Mother_I_LOC" VARCHAR, "E_ID_FatherIL" INTEGER, "Father_I_LOC" VARCHAR, "E_ID" INTEGER, "GrandMother_I_LOC" VARCHAR, "GrandFather_I_LOC" VARCHAR, "Name_y" VARCHAR, "Cousin" VARCHAR);




