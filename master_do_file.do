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
	
	* 6) Specify labels for each state in Mexico.
label define ent 1 "Aguascalientes" 2 "Baja California" 3 "Baja California Sur" 4 "Campeche" 5 "Coahuila" 6 "Colima" /// 
7 "Chiapas" 8 "Chihuahua" 9 "Mexico City"  11 "Guanajuato" 12 "Guerrero" 13 "Hidalgo" 14 "Jalisco" 15 "Edo. Mex" /// 
16 "Michoacan" 17 "Morelos" 18 "Nayarit" 19 "Nuevo Leon"  21 "Puebla" 22 "Queretaro" 23 "Quintana Roo" /// 
24 "San Luis Potosi" 25 "Sinaloa" 26 "Sonora" 27 "Tabasco" 28 "Tamaulipas" 29 "Tlaxcala" /// 
31 "Yucatan" 32 "Zacatecas" 33 "Durango" 34 "Veracruz" 35 "Oaxaca", replace
label value ent ent

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
append using "$store_collapse\sde_106.dta"
append using "$store_collapse\sde_107.dta"
append using "$store_collapse\sde_108.dta"
append using "$store_collapse\sde_109.dta"
append using "$store_collapse\sde_110.dta"
append using "$store_collapse\sde_111.dta"
append using "$store_collapse\sde_112.dta"
append using "$store_collapse\sde_113.dta"
append using "$store_collapse\sde_114.dta"
append using "$store_collapse\sde_115.dta"
append using "$store_collapse\sde_116.dta"
append using "$store_collapse\sde_117.dta"
append using "$store_collapse\sde_118.dta"
append using "$store_collapse\sde_119.dta"
append using "$store_collapse\sde_206.dta"
append using "$store_collapse\sde_207.dta"
append using "$store_collapse\sde_208.dta"
append using "$store_collapse\sde_209.dta"
append using "$store_collapse\sde_210.dta"
append using "$store_collapse\sde_211.dta"
append using "$store_collapse\sde_212.dta"
append using "$store_collapse\sde_213.dta"
append using "$store_collapse\sde_214.dta"
append using "$store_collapse\sde_215.dta"
append using "$store_collapse\sde_216.dta"
append using "$store_collapse\sde_217.dta"
append using "$store_collapse\sde_218.dta"
append using "$store_collapse\sde_219.dta"
append using "$store_collapse\sde_306.dta"
append using "$store_collapse\sde_307.dta"
append using "$store_collapse\sde_308.dta"
append using "$store_collapse\sde_309.dta"
append using "$store_collapse\sde_310.dta"
append using "$store_collapse\sde_311.dta"
append using "$store_collapse\sde_312.dta"
append using "$store_collapse\sde_313.dta"
append using "$store_collapse\sde_314.dta"
append using "$store_collapse\sde_315.dta"
append using "$store_collapse\sde_316.dta"
append using "$store_collapse\sde_317.dta"
append using "$store_collapse\sde_318.dta"
append using "$store_collapse\sde_319.dta"
append using "$store_collapse\sde_406.dta"
append using "$store_collapse\sde_407.dta"
append using "$store_collapse\sde_408.dta"
append using "$store_collapse\sde_409.dta"
append using "$store_collapse\sde_410.dta"
append using "$store_collapse\sde_411.dta"
append using "$store_collapse\sde_412.dta"
append using "$store_collapse\sde_413.dta"
append using "$store_collapse\sde_414.dta"
append using "$store_collapse\sde_415.dta"
append using "$store_collapse\sde_416.dta"
append using "$store_collapse\sde_417.dta"
append using "$store_collapse\sde_418.dta"
append using "$store_collapse\sde_419.dta"
save "$store_collapse/total_sde_municipal_2005_2019", replace







clear 
use "$store_collapse/total_sde_municipal_2005_2019"

split per_ent_mun, parse(.) 
rename per_ent_mun1 per
rename per_ent_mun2 municipio
rename per_ent_mun3 entidad
rename sde_mun_agri pct_agri
rename sde_mun_indu pct_indu
rename sde_mun_serv pct_serv
rename sde_mun_unsp pct_unsp
rename total_sde_mun total_sde
generate anio = substr(per,2,2)
generate trimestre = substr(per,1,1)
order ent mun anio trimestre 
order per_ent_mun per, last
sort ent mun anio trimestre