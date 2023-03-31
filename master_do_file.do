clear
global root = "C:\Users\d57917il\Documents\GitHub\municipal_structural_transformation"
global data = "C:\Users\d57917il\Documents\1paper1\5_ENOE_databases\Bases ENOE"
global store_collapse = "C:\Users\d57917il\Documents\GitHub\municipal_structural_transformation\store_collapse"

local X /// 
105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 /// datasets from the 1st quarter of 2005 to 2019 
205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 /// datasets from the 2nd quarter of 2005 to 2019
305 306 307 308 309 310 311 312 313 314 315 316 317 318 319 /// datasets from the 3rd quarter of 2005 to 2019
405 406 407 408 409 410 411 412 413 414 415 416 417 418 419 // datasets from the 4th quarter of 2005 to 2019


foreach year_quarter of local X {
	
	* For each year & quarter execute the following actions 
	* 1) Open SDEM dataset, 2) clean it based on INEGI criteria, 3) merge it with COE1 dataset, 4) Keep relevant variables 
use "$data/enoe_`year_quarter'/SDEMT`year_quarter'.dta", clear
drop if eda<=11 // Drop all kids below 12 years old because they weren't interviewed in the employment survey
drop if eda==99 // INEGI indicates that with age 99 should be dropped from the sample. 
drop if r_def!=00 // INEGI recommends to drop all the individual that didn't complete the interview. "00" in "r_def" indicates that they finished the interview
drop if c_res==2 // INEGI recommends to drop all the interviews of people who were absent during the interview, "2" in "c_res" is for definitive absentees.  
quietly merge 1:1 cd_a ent con v_sel n_hog h_mud n_ren using "$data/enoe_`year_quarter'/COE1T`year_quarter'.dta", force
keep if _merge==3
keep ent mun per p4a fac
	
	* Change values of states that end with 0. If you don't do this, the variable "ent_mun" will erase zeros and they will be some municipalities that will have the same code.  
replace ent=33 if ent==10 // Durango, with entity code 10, will now have the entity code 33 
replace ent=34 if ent==20 // Oaxaca, with entity code 20, will now have the entity code 34
replace ent=35 if ent==30 // Veracruz, with entity code 30, will now have the entity code 35
		
	* Specifying labels for each state in Mexico.
label define ent 1 "Aguascalientes" 2 "Baja California" 3 "Baja California Sur" 4 "Campeche" 5 "Coahuila" 6 "Colima" /// 
7 "Chiapas" 8 "Chihuahua" 9 "Mexico City"  11 "Guanajuato" 12 "Guerrero" 13 "Hidalgo" 14 "Jalisco" 15 "Edo. Mex" /// 
16 "Michoacan" 17 "Morelos" 18 "Nayarit" 19 "Nuevo Leon"  21 "Puebla" 22 "Queretaro" 23 "Quintana Roo" /// 
24 "San Luis Potosi" 25 "Sinaloa" 26 "Sonora" 27 "Tabasco" 28 "Tamaulipas" 29 "Tlaxcala" /// 
31 "Yucatan" 32 "Zacatecas" 33 "Durango" 34 "Veracruz" 35 "Oaxaca", replace
label value ent ent

	* Generate a unique identification variable for each mexican municipality 
egen per_ent_mun = concat(per mun ent), punct(.) // unique_id for each municipality where "ent" represents entity and "mun" represents municipality. 

	* Generate a categorical variable to identify the economic sector where each individual in the sample is working.  
generate P4A_Sector=.
replace P4A_Sector=1 if p4a>=1100 & p4a<=1199 // If values in P4A are between 1100 & 1199, the individual is working in the PRIMARY SECTOR
replace P4A_Sector=2 if p4a>=2100 & p4a<=3399 // If values in P4A are between 2100 & 2399, the individual is working in the SECONDARY SECTOR
replace P4A_Sector=3 if p4a>=4300 & p4a<=9399 // If values in P4A are between 4300 & 9399, the individual is working in the TERCIARY SECTOR
replace P4A_Sector=4 if p4a>=9700 & p4a<=9999 // *If values in P4A are between 9700 & 9999, the individual is working in the UNSPECIFIED ACTIVITIES
label var P4A_Sector "Economic Sector Categories"
label define P4A_Sector 1 "Primary Sector" 2 "Secondary Sector" 3 "Terciary Sector" 4 "Unspecified Sector"
label value P4A_Sector P4A_Sector
tab P4A_Sector 
	
	* Generate dummy variables to identify if the individual works in the primary, secondary or terciary sector. 
generate agri_sector=.
replace agri_sector=1 if P4A_Sector==1 
replace agri_sector=0 if P4A_Sector!=1
generate ind_sector=.
replace ind_sector=1 if P4A_Sector==2 
replace ind_sector=0 if P4A_Sector!=2 
generate serv_sector=.
replace serv_sector=1 if P4A_Sector==3
replace serv_sector=0 if P4A_Sector!=3 
generate unsp_sector=.
replace unsp_sector=1 if P4A_Sector==4 
replace unsp_sector=0 if P4A_Sector!=4  

	* Estimate sectoral distribution of employment at the municipal level 
preserve
collapse (mean) agri_sector [fweight=fac], by(per_ent_mun) 
rename agri_sector agrishare_mun
save "$store_collapse/agri_`year_quarter'.dta", replace
restore

preserve
collapse (mean) ind_sector [fweight=fac], by(per_ent_mun) 
rename ind_sector indushare_mun
save "$store_collapse/indu_`year_quarter'.dta", replace
restore

preserve
collapse (mean) serv_sector [fweight=fac], by(per_ent_mun) 
rename serv_sector servishare_mun
save "$store_collapse/serv_`year_quarter'.dta", replace
restore

preserve
collapse (mean) unsp_sector [fweight=fac], by(per_ent_mun) 
rename unsp_sector unspeshare_mun
save "$store_collapse/unsp_`year_quarter'.dta", replace
restore
	
clear	
}


