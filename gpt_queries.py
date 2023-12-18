# PROGRAM: gpt_queries.py
# DATE CREATED: 2023-12-15
# PURPOSE: This module pre-generates queries to be sent to GPT API as is.
#          This module is to support package drugdata
#          The output from the function synmatching.get_verbatim_synonyms_matched_labels(...)
#          is used as the input to this file.
#
# AUTHOR: Alexander Safronov
# NOTES: (formerly called pregen_queries.py)


# INPUT FORMAT: 
'''
[
  {
    "label_number": "BLA125387",
    "app_nbr_6d": "125387",
    "brand_name": "EYLEA",
    "indications_and_usage": "1 INDICATIONS AND USAGE EYLEA is indicated for the treatment of: EYLEA is a vascular endothelial growth factor (VEGF) inhibitor indicated for the treatment of patients with: Neovascular (Wet) Age-Related Macular Degeneration (AMD) ( 1.1 ) Macular Edema Following Retinal Vein Occlusion (RVO) ( 1.2 ) Diabetic Macular Edema (DME) ( 1.3 ) Diabetic Retinopathy (DR) ( 1.4 ) Retinopathy of Prematurity (ROP) ( 1.5 ) 1.1 Neovascular (Wet) Age-Related Macular Degeneration (AMD) 1.2 Macular Edema Following Retinal Vein Occlusion (RVO) 1.3 Diabetic Macular Edema (DME) 1.4 Diabetic Retinopathy (DR) 1.5 Retinopathy of Prematurity (ROP)",
    "verbatim_emtree_matches": [
      "retinopathy",
      "diabetic",
      "vein occlusion",
      "retinal vein occlusion",
      "diabetic macular edema",
      "occlusion",
      "degeneration",
      "edema",
      "prematurity",
      "retinopathy of prematurity",
      "diabetic retinopathy",
      "macular edema",
      "macular degeneration"
    ]
  },
  {
    "label_number": "NDA021920",
    "app_nbr_6d": "021920",
    "brand_name": "Naproxen Sodium",
    "indications_and_usage": "Uses temporarily relieves minor aches and pains due to: minor pain of arthritis muscular aches backache menstrual cramps headache toothache the common cold temporarily reduces fever",
    "verbatim_emtree_matches": [
      "backache",
      "cramps",
      "fever",
      "headache",
      "common cold",
      "arthritis",
      "toothache"
    ]
  }
]
'''

import sys, os, json
sys.path.append(r"C:\py\drugdata")
import datasources as ds
import ctinversion as cti
import synmatching as synm


# The example query: 

components = [None] * 5
components[0] = "Please only return the comma-separated list of the corresponding indices enclosed in square brackets without explaining what you are doing. "
components[1] = "For example: [5, 6, 7, 8]. "
components[2] = "Make sure to only include the indices for medical conditions for which the drug is indicated. "
components[3] = "If a medical condition is not indicated according to the label, then do not include it in the list. "

json_file = open(ds.verbatim_synonyms_matched_labels_fn, "r")
# json_file = open(os.path.join(ds.staging_path, "verbatim_synonyms_matched_labels_2023_12_14.json"), "r")
json_obj = json.load(json_file)

def one_query(index) :
    context = json_obj[index]['indications_and_usage']
    conditions = json_obj[index]['verbatim_emtree_matches']
    numbered_conditions = []
    for idx, condition in enumerate(conditions) :
        numbered_conditions.append( str(idx) + ": " + condition)
    # conditions.append(context)
    query = "The following is an ordered list of the medical conditions: " + ", ".join(numbered_conditions) + ". " + \
            "Please give me a comma-separated list of the corresponding indices from 0 to " + str(len(numbered_conditions)-1) + \
            ", enclosed in square brackets, of the conditions which are indicated according to the drug label I will provide. " + \
            components[0] + components[1] + components[2] + components[3] + \
            "Here is the drug label: " + context + ""
    return(query)
    
ds.config("ddconfig.json")
print(ds.label_data_path)

for idx in range(4, 5):
    print(one_query(idx), "\n\n")


