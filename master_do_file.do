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
	* 1) Open SDEM dataset for the respective year_quarter
use "$data/enoe_`year_quarter'/SDEMT`year_quarter'.dta"

	* 2) Clean it based on INEGI criteria,
drop if eda<=11 // Drop all kids below 12 years old because they weren't interviewed in the employment survey
drop if eda==99 // INEGI indicates that with age 99 should be dropped from the sample. 
drop if r_def!=00 // INEGI recommends to drop all the individual that didn't complete the interview. "00" in "r_def" indicates that they finished the interview
drop if c_res==2 // INEGI recommends to drop all the interviews of people who were absent during the interview, "2" in "c_res" is for definitive absentees. 
 
	* 3) Merge it with COE1 dataset for the respective year_quarter
quietly merge 1:1 cd_a ent con v_sel n_hog h_mud n_ren using "$data/enoe_`year_quarter'/COE1T`year_quarter'.dta", force
keep if _merge==3

	* 4) Keep relevant variables 
keep ent mun per p4a fac sex clase1

	* 5) Change values of states that end with 0. If you don't do this, the variable "ent_mun" will erase zeros and they will be some municipalities that will have the same code.  
replace ent=33 if ent==10 // Durango, with entity code 10, will now have the entity code 33 
replace ent=34 if ent==20 // Oaxaca, with entity code 20, will now have the entity code 34
replace ent=35 if ent==30 // Veracruz, with entity code 30, will now have the entity code 35

	* 7) Generate a unique identification variable for each mexican municipality 
egen per_ent_mun = concat(per mun ent), punct(.) // unique_id for each municipality where "ent" represents entity and "mun" represents municipality. 

	* 8) Generate a categorical variable to identify if the person works in the primary, secondary or terciary sector. 
generate P4A_Sector=.
rename p4a P4A 
replace P4A_Sector=1 if P4A>=1100 & P4A<=1199 // If values in P4A are between 1100 & 1199 classify as PRIMARY SECTOR
replace P4A_Sector=2 if P4A>=2100 & P4A<=3399 // If values in P4A are between 2100 & 2399 classify as SECONDARY SECTOR
replace P4A_Sector=3 if P4A>=4300 & P4A<=9399 // If values in P4A are between 4300 & 9399 classify as TERCIARY SECTOR
replace P4A_Sector=4 if P4A>=9700 & P4A<=9999 // *If values in P4A are between 9700 & 9999 classify as UNSPECIFIED ACTIVITIES
label var P4A_Sector "Economic Sector Categories"
label define P4A_Sector 1 "Primary Sector" 2 "Secondary Sector" 3 "Terciary Sector" 4 "Unspecified Sector"
label value P4A_Sector P4A_Sector
tab P4A_Sector // Data quality check. Result: 0 missing values

	* 9) Generate dummy variables to identify if the person works in the primary, secondary or terciary sector. 
generate agri_sector=.
replace agri_sector=1 if P4A>=1100 & P4A<=1199 // If values in P4A are between 1100 & 1199 classify as PRIMARY SECTOR
replace agri_sector=0 if P4A>=2100 & P4A<=3399 // If values in P4A are between 2100 & 2399 do not classify as PRIMARY SECTOR
replace agri_sector=0 if P4A>=4300 & P4A<=9399 // If values in P4A are between 4300 & 9399 do not classify as PRIMARY SECTOR
replace agri_sector=0 if P4A>=9700 & P4A<=9999 // If values in P4A are between 9700 & 9999 do not classify as PRIMARY SECTOR

generate ind_sector=.
replace ind_sector=0 if P4A>=1100 & P4A<=1199 // If values in P4A are between 1100 & 1199 do not classify as SECONDARY SECTOR
replace ind_sector=1 if P4A>=2100 & P4A<=3399 // If values in P4A are between 2100 & 3399 classify as SECONDARY SECTOR
replace ind_sector=0 if P4A>=4300 & P4A<=9399 // If values in P4A are between 4300 & 9399 do not classify as SECONDARY SECTOR
replace ind_sector=0 if P4A>=9700 & P4A<=9999 // If values in P4A are between 9700 & 9999 do not classify as SECONDARY SECTOR

generate serv_sector=.
replace serv_sector=0 if P4A>=1100 & P4A<=1199 // If values in P4A are between 1100 & 1199 do not classify as TERCIARY SECTOR
replace serv_sector=0 if P4A>=2100 & P4A<=3399 // If values in P4A are between 2100 & 2399 do not classify as TERCIARY SECTOR
replace serv_sector=1 if P4A>=4300 & P4A<=9399 // If values in P4A are between 4300 & 9399 classify as TERCIARY SECTOR
replace serv_sector=0 if P4A>=9700 & P4A<=9999 // If values in P4A are between 9700 & 9999 do not classify as TERCIARY SECTOR

generate unsp_sector=.
replace unsp_sector=0 if P4A>=1100 & P4A<=1199 // If values in P4A are between 1100 & 1199 do not classify as UNSPECIFIED SECTOR
replace unsp_sector=0 if P4A>=2100 & P4A<=3399 // If values in P4A are between 2100 & 2399 do not classify as UNSPECIFIED SECTOR
replace unsp_sector=0 if P4A>=4300 & P4A<=9399 // If values in P4A are between 4300 & 9399 do not classify as UNSPECIFIED SECTOR
replace unsp_sector=1 if P4A>=9700 & P4A<=9999 // If values in P4A are between 9700 & 9999 classify as UNSPECIFIED SECTOR


	* 10) Estimate Sectoral Distribution of Employment at the municipal level
preserve
collapse (mean) agri_sector [fweight=fac], by(per_ent_mun) 
rename agri_sector agrishare_mun
save "$store_collapse\agri_`year_quarter'.dta", replace
restore

preserve
collapse (mean) ind_sector [fweight=fac], by(per_ent_mun) 
rename ind_sector indushare_mun
save "$store_collapse\indu_`year_quarter'.dta", replace
restore

preserve
collapse (mean) serv_sector [fweight=fac], by(per_ent_mun) 
rename serv_sector servishare_mun
save "$store_collapse\serv_`year_quarter'.dta", replace
restore

preserve
collapse (mean) unsp_sector [fweight=fac], by(per_ent_mun) 
rename unsp_sector unspeshare_mun
save "$store_collapse\unsp_`year_quarter'.dta", replace
restore

	* 11) Merge the datasets to create one dataset per year_quarter
clear 
cd "$store_collapse"
use agri_`year_quarter'.dta 

merge m:1 per_ent_mun using indu_`year_quarter'
rename _merge merge_industrial

merge m:1 per_ent_mun using serv_`year_quarter'
rename _merge merge_services

merge m:1 per_ent_mun using unsp_`year_quarter'
rename _merge merge_unspecified

tab merge_industrial // Data quality check: all observations matched
drop merge_industrial

tab merge_services // Data quality check: all observations matched
drop merge_services

tab merge_unspecified // Data quality check: all observations matched
drop merge_unspecified


* Confirm that all sectors for each municipality are equal to 100%

gen one_agri = (100 * agrishare_mun)
gen one_ind = (100 * indushare_mun)
gen one_serv = (100 * servishare_mun)
gen one_unsp = (100 * unspeshare_mun)

drop agrishare_mun indushare_mun servishare_mun unspeshare_mun
rename one_agri sde_mun_agri
rename one_ind sde_mun_indu
rename one_serv sde_mun_serv
rename one_unsp sde_mun_unsp


// Data quality checks

* With the following code, I want to evaluate if the sum between the four variables created are equal to 100.
gen total_sde_mun = sde_mun_agri + sde_mun_indu + sde_mun_serv + sde_mun_unsp
tab total_sde_mun 

save "$store_collapse/sde_`year_quarter'.dta", replace
}



clear 
use "$store_collapse\sde_105.dta"

forvalues i=106(1)119 {
append using "$store_collapse\sde_`i'.dta"	
}

forvalues i=205(1)219 {
append using "$store_collapse\sde_`i'.dta"	
}

forvalues i=305(1)319 {
append using "$store_collapse\sde_`i'.dta"	
}

forvalues i=405(1)419 {
append using "$store_collapse\sde_`i'.dta"	
}

split per_ent_mun, parse(.) // Split variable pet_ent_mun into three variables 
rename per_ent_mun1 per // First split variable refers to year_quarter
rename per_ent_mun2 municipality // Second split variable refers to municipality 
rename per_ent_mun3 entity_name // Third split variable referes to federal entity, also known as State
egen ent_mun = concat(municipality entity_name), punct(.) // unique_id for each municipality  
rename sde_mun_agri pct_agri // % of agricultural jobs in each municipality in their respective year_quarter
rename sde_mun_indu pct_indu // % of industrial jobs in each municipality in their respective year_quarter
rename sde_mun_serv pct_serv // % of service jobs in each municipality in their respective year_quarter
rename sde_mun_unsp pct_unsp // % of unspecified jobs in each municipality in their respective year_quarter
rename total_sde_mun total_sde // Sectoral Distribution of Employment in each municipality in their respective year_quarter. Note: it should always be equal to 100
tab total_sde // Data quality check: All values equal to 100 
drop total_sde // Drop this variable as it is no longer necessary. 
generate year = substr(per,2,2) // Obtain the year of the survey using the last two digits of the variable "per"
generate quarter = substr(per,1,1) // Obtain the quarter of the survey using the last two digits of the variable "per"
order entity_name municipality year quarter  // Order variables 
order per_ent_mun per, last // Order variables 
sort entity_name municipality year quarter // Sort variables 

destring entity_name, replace
* Return entity codes to their original values. 
replace entity_name=10 if entity_name==33 // Durango, with entity code 10, will now have the entity code 33 
replace entity_name=20 if entity_name==34 // Oaxaca, with entity code 20, will now have the entity code 34
replace entity_name=30 if entity_name==35 // Veracruz, with entity code 30, will now have the entity code 35
* Specify labels for each state in Mexico.
label define entity_name 1 "Aguascalientes" 2 "Baja California" 3 "Baja California Sur" 4 "Campeche" 5 "Coahuila" 6 "Colima" /// 
7 "Chiapas" 8 "Chihuahua" 9 "Mexico City"  11 "Guanajuato" 12 "Guerrero" 13 "Hidalgo" 14 "Jalisco" 15 "Edo. Mex" /// 
16 "Michoacan" 17 "Morelos" 18 "Nayarit" 19 "Nuevo Leon"  21 "Puebla" 22 "Queretaro" 23 "Quintana Roo" /// 
24 "San Luis Potosi" 25 "Sinaloa" 26 "Sonora" 27 "Tabasco" 28 "Tamaulipas" 29 "Tlaxcala" /// 
31 "Yucatan" 32 "Zacatecas" 10 "Durango" 20 "Oaxaca" 30 "Veracruz", replace
label value entity_name entity_name
tab entity_name

destring quarter, replace
destring year, replace 
forvalues i=5(1)9 {
replace year=200`i' if year==`i' 		
}
forvalues i=10(1)19 {
replace year=20`i' if year==`i' 		
}

* Give a number to each of the municipalities in the sample. 
egen count_entmun = group(ent_mun)
summarize count_entmun
* Result : There are 1827 municipalities in the dataset.




save "$store_collapse/total_sde_municipal_2005_2019", replace
