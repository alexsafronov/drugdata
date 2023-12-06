# The following PoC files are to be refactored in this file:
# ..\additional_data\emtree_interventions\create_sorted_internames.py
# ..\additional_data\emtree_indications\create_sorted_indic_names.py

import ast, sys, json, os, re
from datetime import datetime
import string
import datasources as ds


# create_sorted_internames.py :

# ct_json_dir_path = ds.ct_data_path_extracted # "C:/Users/as23i485/Documents/CT.gov_py_scripts/JSON/"

def get_NCT_list_for_interventions(large_json_slicing_limits=(None, None)) :
    i = 0
    res = []
    k = 0
    max_length = 0
    for file_path in os.listdir(ds.ct_data_path_extracted)[large_json_slicing_limits[0]: large_json_slicing_limits[1]]:
        # check if current file_path is a file
        if os.path.isfile(os.path.join(ds.ct_data_path_extracted, file_path)):
            # add filename to list
            file_handle = open(os.path.join(ds.ct_data_path_extracted, file_path))
            file_data = json.load(file_handle)
            study_data = file_data['StudyInfo']
            for one_study_info in study_data:
                i += 1
                all_inter_names = one_study_info['InterventionNames'] + one_study_info['ArmGroupInterventionNames'] + one_study_info['InterventionOtherNames']
                # all_inter_names = one_study_info['InterventionNames']
                for i_name in set(all_inter_names) :
                    one_entry = {}
                    one_entry['NCTId'] = one_study_info['NCTId']
                    one_entry['PhaseList'] = one_study_info['PhaseList']
                    # one_entry['OriginalStudyId'] = one_study_info['OriginalStudyId']
                    one_entry['InterventionName'] = i_name.upper()
                    # if one_study_info['NCTId'] == "NCT05511831" : print("\n", one_entry, "\n")
                    res.append(one_entry)
                    k += 1
                all_inter_mesh_names = one_study_info['InterventionMeshTerms']
                for i_name in all_inter_mesh_names :
                    one_entry = {}
                    one_entry['NCTId'] = one_study_info['NCTId']
                    one_entry['PhaseList'] = one_study_info['PhaseList']
                    one_entry['InterventionName'] = i_name.upper()
                    # if one_study_info['NCTId'] == "NCT05511831" : print("\n", one_entry, "\n")
                    res.append(one_entry)
                    k += 1
        sys.stderr.write(".")
        sys.stderr.flush()
    return(res)

def get_NCT_list_for_indications(large_json_slicing_limits=(None, None)) :
    i = 0
    res = []
    k = 0
    max_length = 0
    for file_path in os.listdir(ds.ct_data_path_extracted)[large_json_slicing_limits[0]: large_json_slicing_limits[1]]:
        if os.path.isfile(os.path.join(ds.ct_data_path_extracted, file_path)):
            file_handle = open(os.path.join(ds.ct_data_path_extracted, file_path))
            file_data = json.load(file_handle)
            study_data = file_data['StudyInfo']
            for one_study_info in study_data:
                i += 1
                all_indic_mesh_names = one_study_info['ConditionMeshTerms']
                for i_name in all_indic_mesh_names :
                    one_entry = {}
                    one_entry['NCTId'] = one_study_info['NCTId']
                    one_entry['PhaseList'] = one_study_info['PhaseList']
                    one_entry['IndicationName'] = i_name.upper()
                    res.append(one_entry)
                    k += 1
        sys.stderr.write(".")
        sys.stderr.flush()
    return(res)

def create_sorted_internames(large_json_slicing_limits=(None, None)) :
    
    back_hash = {}
    printable = set(string.printable)
    
    drug_list = get_NCT_list_for_interventions(large_json_slicing_limits=large_json_slicing_limits)
    print("\nNow iterating over the CT intervention list. A total of " + str(len(drug_list)) +
    " intervention names to be checked, starting now.")
    
    for counter, study in enumerate(drug_list) :
        if counter % 1000 == 0 :
            sys.stderr.write(".")
        if counter % 10000 == 0 :
            sys.stderr.write(" " + str(counter) + " ")
        if counter % 100000 == 0 : sys.stderr.write(" Cur Time = " + str(datetime.now())[0:19] + "   " + "\n")
        sys.stderr.flush()
        
        NCTId     = study['NCTId']
        intername_printable = ''.join(filter(lambda x: x in printable, study['InterventionName']))
        intername = intername_printable.strip().replace("DRUG: ", "")
        intername = intername.replace("DEVICE: ", "")
        intername = intername.replace("BIOLOGICAL: ", "")
        intername = intername.replace("PROCEDURE: ", "")
        intername = intername.replace("RADIATION: ", "")
        intername = intername.replace("DIETARY SUPPLEMENT: ", "")
        intername = intername.replace("DIAGNOSTIC TEST: ", "")
        intername = intername.replace("COMBINATION PRODUCT: ", "")
        intername = intername.replace("OTHER: ", "")
        intername = intername.replace("EXPERIMENTAL: ", "")
        intername = intername.replace("GENETIC: ", "")
        intername = re.sub('\s+[\d\.]+\s*MG$', '', intername)
        intername = re.sub('\s+[\d\.]+\s*MG\/KG$', '', intername)
        intername = re.sub('\s+[\d\.]+\s*MG\/ML$', '', intername)
        intername = re.sub('\s+[\d\.]+\s*MG QD$', '', intername)
        intername = re.sub('^[\d\.]+\s*MG\s*', '', intername)
        PhaseList = study['PhaseList']
        to_append = {'NCTId' : NCTId, 'PhaseList' : PhaseList}
        if back_hash.get(intername):
            back_hash[intername].append(to_append)
        else:
            back_hash[intername] = [to_append]

    print("\n")
    back_list = []
    for intername in back_hash :
        srt = list({v['NCTId']:v for v in back_hash[intername]}.values()) # deduped list of studies
        pair = (intername, srt)
        back_list.append(pair)

    sorted_back_list = sorted(back_list, key=lambda x: x[0])
    full_out_fn = os.path.join(ds.staging_path, ds.ctinversion_interventions_fn)
    output_fh = open(full_out_fn, "w") 
    json.dump(sorted_back_list, output_fh)
    print(f"Function cti.create_sorted_internames() output data to {full_out_fn}.")

def create_sorted_indicnames(large_json_slicing_limits=(None, None)) :
    pass
