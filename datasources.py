# PURPOSE: This module is focused on extraction of data from the original sources.
# AUTHOR: Alex Safronov
# DATE FIRST CREATED: 2023 12 04

import ast, sys, json, os, re, chardet, random, time
from datetime import datetime
# label_data_path = r"C:\Users\as23i485\Documents\additional_data\openfda_labels\JSON"
from operator import itemgetter
from urllib.request import urlopen

label_data_path = None
ct_data_path_extracted = None
emtree_nodes_w_unique_terms_indic_path_fn = None
label_subselected_path_fn = None
staging_path = None
verbatim_synonyms_matched_labels = None

def config(fn) :
    config_data = json.load(open(fn, "r"))
    
    global ctinversion_interventions_fn
    ctinversion_interventions_fn    = config_data['ctinversion_interventions_fn']
    
    global ctinversion_indications_fn
    ctinversion_indications_fn    = config_data['ctinversion_indications_fn']
    
    global staging_path
    staging_path = "."
    
    if config_data.get('base_path') :
    
        global label_data_path
        label_data_path = os.path.join(config_data['base_path'], config_data['label_data_path'])
        
        global ct_data_path
        ct_data_path    = os.path.join(config_data['base_path'], config_data['ct_data_path'])
        
        global ct_data_path_extracted
        ct_data_path_extracted    = os.path.join(config_data['base_path'], config_data['ct_data_path_extracted'])
        
        global emtree_nodes_w_unique_terms_indic_path_fn
        emtree_nodes_w_unique_terms_indic_path_fn    = os.path.join(config_data['base_path'], config_data['emtree_nodes_w_unique_terms_indic_path_fn'])
        
        staging_path    = os.path.join(config_data['base_path'], config_data['staging_dir'])
        
        global label_subselected_path_fn
        label_subselected_path_fn    = os.path.join(config_data['base_path'], config_data['label_subselected_path_fn'])
        
        global emtree_indic_data_path_fn
        emtree_indic_data_path_fn    = os.path.join(config_data['base_path'], config_data['emtree_indic_data_path_fn'])
        
        global emtree_inter_data_path_fn
        emtree_inter_data_path_fn    = os.path.join(config_data['base_path'], config_data['emtree_inter_data_path_fn'])
        
        global fda_raw_master_data_path
        fda_raw_master_data_path    = os.path.join(config_data['base_path'], config_data['fda_master_appl_data_html_source_path'])
        # fda_raw_master_data_path    = os.path.join(config_data['base_path'], config_data['ORIGINAL_fda_raw_master_data_path'])
        
        global fda_master_appl_data_html_source_path
        fda_master_appl_data_html_source_path    = os.path.join(config_data['base_path'], config_data['fda_master_appl_data_html_source_path'])
        
        global verbatim_synonyms_matched_labels
        verbatim_synonyms_matched_labels    = os.path.join(staging_path, config_data['verbatim_synonyms_matched_labels_fn'])
    else :
        label_data_path = config_data['label_data_path']
        ct_data_path    = config_data['ct_data_path']
        ct_data_path_extracted    = config_data['ct_data_path_extracted']
        emtree_indic_data_path_fn    = config_data['emtree_indic_data_path_fn']
        emtree_inter_data_path_fn    = config_data['emtree_inter_data_path_fn']
        fda_raw_master_data_path     = config_data['fda_raw_master_data_path']


# Labels
# Application number is extracted from 'openfda' section under the field name 'application_number' :
#    app_nbr = result.get('openfda', []).get('application_number', [])[0]
# Application type is assigned as the sequence of first non-digit characters, followed by at least one digit :
# app_type = re.findall("^(\D*)\d", app_nbr)[0]
# 

def get_FDA_label_list(max_json_file_count=None, max_sample_size_per_json_src=None, nda_bla_only=True) :
    i = 0
    res = []
    json_file_names = [filename for filename in os.listdir(label_data_path) if filename.endswith('.json')]
    
    if max_json_file_count :
        json_file_names = [filename for filename in os.listdir(label_data_path) if filename.endswith('.json')][0:max_json_file_count]
    else :
        json_file_names = [filename for filename in os.listdir(label_data_path) if filename.endswith('.json')]
        
    for counter, json_file_name in enumerate(json_file_names):
        print(counter, json_file_name)
        with open(os.path.join(label_data_path, json_file_name), encoding="utf-8") as json_file:
            json_obj = json.load(json_file)
            '''
                        if max_sample_size_per_json_src :
                            results = json_obj['results'][0:max_sample_size_per_json_src]
                        else :
                            results = json_obj['results']
            '''
            results = json_obj['results']
            ct_counts = []
            label_results = []
            inner_counter = 0
            for result in results :
            
                try:
                    app_nbr = result.get('openfda', []).get('application_number', [])[0]
                except IndexError:
                    app_nbr = ''
                    
                try:
                    app_type = re.findall("^(\D*)\d", app_nbr)[0]
                except IndexError:
                    app_type = ''
                    
                if app_type.upper() in ['NDA', 'BLA'] :
                    label_results.append(result)
                    inner_counter += 1
                else :
                    if not nda_bla_only :
                        label_results.append(result)
                        inner_counter += 1
                if inner_counter >= max_sample_size_per_json_src :
                    break
        res += label_results
        sys.stderr.write(str(len(label_results)) + "  ")
        sys.stderr.flush()
    sys.stderr.write("\n")
    sys.stderr.flush()
    return(res)

def extract_ct_data_from_json_subfolder(subfolder_name, max_file_count=None) :
    
    path_to_json_files = os.path.join(ct_data_path, subfolder_name) #  ct_data_path + subfolder_name
    if max_file_count :
        json_file_names = [filename for filename in os.listdir(path_to_json_files) if filename.endswith('.json')][0:max_file_count]
    else :
        json_file_names = [filename for filename in os.listdir(path_to_json_files) if filename.endswith('.json')]
    
    # print(f"json_file_names length = {len(json_file_names)}")

    multiple_study_objects = []
    for counter, json_file_name in enumerate(json_file_names):
        with open(os.path.join(path_to_json_files, json_file_name), encoding="utf-8") as json_file:
            study_object = {}
            study_object['n_items'] = 0
            json_obj = json.load(json_file)
            
            ProtocolSection = json_obj['FullStudy']['Study']['ProtocolSection']
            Study  = json_obj['FullStudy']['Study']
            # for key in list(ProtocolSection.keys()):
            #    if key == 'OutcomeMeasuresModule': print (j, key)
            
            IdentificationModule = json_obj['FullStudy']['Study']['ProtocolSection']['IdentificationModule']

            study_object['NCTId'] = IdentificationModule['NCTId']
            study_object['IsADrug'] = False
            study_object['InterventionType'] = ""
            study_object['StatusModule'] = {}
            study_object['OriginalStudyId'] = ""
            study_object['PhaseList'] = []

            study_object.update({'outcome_measure_type': "", 'outcome_measure_desc': "", 'primary_outcome_1': ""})
            ResultsSection = Study.get('ResultsSection', False)
            if ResultsSection :
                OutcomeMeasuresModule =  ResultsSection.get('OutcomeMeasuresModule', False)
                if OutcomeMeasuresModule :
                    # if OutcomeMeasuresModule.get('OutcomeMeasuresModule', False):
                    # print(j, OutcomeMeasuresModule)
                    outcome_measure_type      = OutcomeMeasuresModule['OutcomeMeasureList']['OutcomeMeasure'][0]['OutcomeMeasureType']
                    outcome_measure_desc      = OutcomeMeasuresModule['OutcomeMeasureList']['OutcomeMeasure'][0].get('OutcomeMeasureDescription', "")
                    study_object['outcome_measure_type'] = outcome_measure_type
                    study_object['outcome_measure_desc'] = outcome_measure_desc
                    
            study_object['LeadSponsorName'] = ""
            study_object['LeadSponsorClass'] = ""
            if ProtocolSection.get('SponsorCollaboratorsModule') :
                SponsorCollaboratorsModule   = ProtocolSection['SponsorCollaboratorsModule']
                if SponsorCollaboratorsModule.get('LeadSponsor') :
                    LeadSponsor = SponsorCollaboratorsModule['LeadSponsor']
                    study_object['LeadSponsorName'] = LeadSponsor.get('LeadSponsorName')
                    study_object['LeadSponsorClass'] = LeadSponsor.get('LeadSponsorClass')

            if ProtocolSection.get('OutcomesModule'):
                OutcomesModule   = ProtocolSection['OutcomesModule']
                if OutcomesModule.get('PrimaryOutcomeList') :
                    primary_outcome_list   = OutcomesModule['PrimaryOutcomeList']['PrimaryOutcome']
                study_object['primary_outcome_1'] = primary_outcome_list[0].get('PrimaryOutcomeDescription')
                
            if IdentificationModule.get('OrgStudyIdInfo'):
                OrgStudyIdInfo = IdentificationModule['OrgStudyIdInfo']
                if OrgStudyIdInfo.get('OrgStudyId'):
                    study_object['OriginalStudyId'] = OrgStudyIdInfo['OrgStudyId']
                study_object['OrgStudyIdInfo'] = OrgStudyIdInfo
                
            if ProtocolSection.get('DesignModule'):
                DesignModule = ProtocolSection['DesignModule']
                if DesignModule.get('PhaseList') :
                    PhaseList  = ProtocolSection['DesignModule']['PhaseList']
                    study_object['PhaseList'] = (PhaseList['Phase'])
                    
            if ProtocolSection.get('StatusModule'):
                study_object['StatusModule'] = ProtocolSection['StatusModule']
                    
            study_object['IsADrug'] = False
            study_object['InterventionNames'] = []
            study_object['InterventionOtherNames'] = []
            study_object['ArmGroupInterventionNames'] = []
            study_object['ConditionMeshTerms'] = []
            if ProtocolSection.get('ConditionsModule') : # ArmsInterventionsModule.get('ArmGroupList'):
                ConditionsModule = ProtocolSection['ConditionsModule']
                if ConditionsModule.get('ConditionList') :
                    for cond in ConditionsModule['ConditionList']['Condition'] :
                        study_object['ConditionMeshTerms'].append(cond)
                if ConditionsModule.get('KeywordList') :
                    for cond in ConditionsModule['KeywordList']['Keyword'] :
                        study_object['ConditionMeshTerms'].append(cond)
            if ProtocolSection.get('ArmsInterventionsModule') : # ArmsInterventionsModule.get('ArmGroupList'):
                ArmsInterventionsModule = ProtocolSection['ArmsInterventionsModule']
                if ArmsInterventionsModule.get('InterventionList') :
                    i_list  = ArmsInterventionsModule['InterventionList']['Intervention']
                    study_object['InterventionList'] = i_list
                    study_object['n_items'] = len(i_list)
                    for item in i_list :
                        InterventionName = item.get('InterventionName')
                        
                        study_object['InterventionNames'].append(str(InterventionName))
                        if item.get('InterventionOtherNameList') :
                            Intervention_Other_Name_List = item.get('InterventionOtherNameList')
                            # TODO: To assert that there only one other name in the list for each arm :
                            other_inter_name = Intervention_Other_Name_List['InterventionOtherName'][0]
                            study_object['InterventionOtherNames'].append(str(other_inter_name))
                        if item['InterventionType'] == "Drug" :
                            study_object['IsADrug'] = True
                        study_object['InterventionType'] = item['InterventionType']
                if ArmsInterventionsModule.get('ArmGroupList') :
                    ArmGroupList = ArmsInterventionsModule['ArmGroupList']
                    if ArmGroupList.get('ArmGroup') :
                        for item in ArmsInterventionsModule['ArmGroupList']['ArmGroup'] :
                            if item.get('ArmGroupInterventionList') :
                                for sub_item in item['ArmGroupInterventionList']['ArmGroupInterventionName'] :
                                    study_object['ArmGroupInterventionNames'].append(sub_item)
            ######################################################
            DerivedSection  = json_obj['FullStudy']['Study']['DerivedSection']
            study_object['InterventionMeshTerms'] = []
            if DerivedSection.get('InterventionBrowseModule') :
                InterventionBrowseModule = DerivedSection.get('InterventionBrowseModule')
                if InterventionBrowseModule.get('InterventionMeshList') :
                    i_mesh_list  = InterventionBrowseModule['InterventionMeshList']['InterventionMesh']
                    study_object['InterventionMeshList'] = i_mesh_list
                    for item in i_mesh_list :
                        InterventionMeshTerm = item.get('InterventionMeshTerm')
                        study_object['InterventionMeshTerms'].append(str(InterventionMeshTerm))
                        
            if DerivedSection.get('ConditionBrowseModule') :
                ConditionBrowseModule = DerivedSection.get('ConditionBrowseModule')
                if ConditionBrowseModule.get('ConditionMeshList') :
                    c_mesh_list  = ConditionBrowseModule['ConditionMeshList']['ConditionMesh']
                    study_object['ConditionMeshList'] = c_mesh_list
                    for item in c_mesh_list :
                        ConditionMeshTerm = item.get('ConditionMeshTerm')
                        study_object['ConditionMeshTerms'].append(str(ConditionMeshTerm))
            #############################################
            # if "NCT013127" in study_object['NCTId'] : print(study_object['NCTId'], study_object['InterventionType'])
            if True or study_object['IsADrug'] :
                filtered_study_object = dict((k, study_object[k]) for k in [
                    'NCTId', 'IsADrug', 'OriginalStudyId', 'InterventionNames', 'ArmGroupInterventionNames'
                    , 'InterventionOtherNames', 'StatusModule', 'PhaseList'
                    , 'outcome_measure_type', 'outcome_measure_desc', 'primary_outcome_1'
                    , 'LeadSponsorName', 'LeadSponsorClass'
                    , 'InterventionMeshTerms', 'ConditionMeshTerms'
                    ])
                multiple_study_objects.append(filtered_study_object)
    return(multiple_study_objects)

# The data extracted from CT.gov, after unzipping, comes in subfolders named NCT####xxxx, where #### are four digits from 0000 to 0596.
# It returns False iff there are not enough 
def extract_ct_data_from_stack_of_json_subfolders(folder_step_idx, folder_step_size=10, max_folder_count=None, max_file_count_per_subfolder=None, out_folder='.') :
    folder_list = []
    i = 0
    list_of_all_dir_items = os.listdir(ct_data_path)
    dir_item_count = len(list_of_all_dir_items)
    
    if dir_item_count <= folder_step_idx * folder_step_size :
        return(False)
        
    for file_path in list_of_all_dir_items:
        if (folder_step_idx) * folder_step_size <= i and i <  (folder_step_idx + 1) * folder_step_size :
            # print(file_path)
            if os.path.isdir(os.path.join(ct_data_path, file_path)):
                folder_list.append(file_path)
        i += 1

    sys.stderr.write("A total of " + str(len(folder_list)) + " folders. ")
    fn = "CT_" + str(folder_step_idx).zfill(3) + ".json"
    file_to_output = open(os.path.join(out_folder, fn), "w", encoding="utf-8")
    
    multiple_study_objects = []
    for n, file_path in enumerate(folder_list) :
        if n % 100 == 0 :
            sys.stderr.write("Cur. time = " +datetime.now().strftime("%H:%M:%S") + ". Folder count x 10 : ")
        if n % 10 == 0 :
            sys.stderr.write(str(n // 10) + "")
            sys.stderr.flush()
            sys.stdout.flush()
        elif n % 1 == 0 :
            sys.stderr.write(".")
            sys.stderr.flush()
        # print(file_path)
        multiple_study_objects.extend(extract_ct_data_from_json_subfolder(file_path, max_file_count_per_subfolder)) 
    
    sys.stderr.write("\n")
    json.dump(multiple_study_objects, file_to_output)
    return(True)

def extract_ct_data_from_all_stacks_of_json_subfolders(folder_step_size=10, max_folder_count=None, max_file_count_per_subfolder=None, out_folder=".") :
    folder_step_idx = 0
    while extract_ct_data_from_stack_of_json_subfolders(folder_step_idx, folder_step_size=10, out_folder=out_folder, max_file_count_per_subfolder=1) :
        folder_step_idx += 1


def extract_emtree_diseases_and_dedup() :
    file = open(emtree_indic_data_path_fn, "r")
    data = file.readlines()
    file.close()

    id_list = [];
    str_list = [];
    node_list = [];
    for counter, item in enumerate(data):
        str_list.append(item.replace("\n",""))
    sys.stderr.write("\nExtracting the data from the raw emtree file: " + emtree_indic_data_path_fn + "\n")
    sys.stderr.write("\nNew List items: " + str(len(str_list)) + "\n")
    sys.stderr.flush()

    # for counter, item in enumerate(str_list[75860:75869]):
    for counter, item in enumerate(str_list):
        if item == "" : continue
        if counter % 1000 == 0 :
            sys.stderr.write(".")
        if counter % 10000 == 0 :
            sys.stderr.write(" " + str(counter) + " ")
        sys.stderr.flush()
        single_node = ast.literal_eval(item)
        node_id = single_node[0][0]
        id_list.append(node_id)
        if len(single_node) < 5 : single_node.append([])
        node_item = {
             'node_id'  : single_node[0][0]
            ,'term'     : single_node[1][0]
            ,'synonyms' : single_node[2]
            ,'parent'   : single_node[3][0]
            ,'children' : single_node[4]
        }
        node_list.append(node_item)
    print("\ntotal  items: ", len(id_list), flush=True)
    print("unique items: ", len(set(id_list)), flush=True)
    print("Now deduplicating the emtree diseases.", flush=True)
    
    terms = []
    for item in node_list :
        terms.append(item['term'])
        
    unique_terms = set(terms)
    unique_nodes = []
    print("Total node count = ", len(node_list), flush=True)
    print("Output of nodes with unique terms. Total unique terms: ", len(unique_terms), ". Now counting unique terms:", flush=True)
    for counter, unique_term in enumerate(sorted(unique_terms)) :
        if counter % 5000 == 0 :
            sys.stderr.write(" " + str(counter) + " ")
            sys.stderr.flush()
        elif counter % 1000 == 0 :
            sys.stderr.write(".")
            sys.stderr.flush()
        for node in node_list :
            if node['term'] == unique_term :
                unique_nodes.append({'term':node['term'], 'synonyms':node['synonyms']})
                break
    print("", flush=True)
    print("total node count from original emtree: ", len(node_list), "", flush=True)
    print("total node count from deduped emtree : ", len(unique_nodes), "", flush=True)
    return(unique_nodes)


# PoC: read_emtree_into_json.py
def extract_emtree_drugs_and_dedup() :
    file = open(emtree_inter_data_path_fn, "r")
    data = file.readlines()
    file.close()

    id_list = [];
    str_list = [];
    node_list = [];
    for counter, item in enumerate(data):
        str_list.append(item.replace("\n",""))
    sys.stderr.write("\nExtracting the data from the raw emtree file: " + emtree_inter_data_path_fn + "\n")
    sys.stderr.write("\nNew List items: " + str(len(str_list)) + "\n")
    sys.stderr.flush()

    # for counter, item in enumerate(str_list[75860:75869]):
    for counter, item in enumerate(str_list):
        if item == "" : continue
        if counter % 1000 == 0 :
            sys.stderr.write(".")
        if counter % 10000 == 0 :
            sys.stderr.write(" " + str(counter) + " ")
        sys.stderr.flush()
        single_node = ast.literal_eval(item)
        node_id = single_node[0][0]
        id_list.append(node_id)
        if len(single_node) < 5 : single_node.append([])
        node_item = {
             'node_id'  : single_node[0][0]
            ,'term'     : single_node[1][0]
            ,'synonyms' : single_node[2]
            ,'parent'   : single_node[3][0]
            ,'children' : single_node[4]
        }
        node_list.append(node_item)
    print("\ntotal  items: ", len(id_list), flush=True)
    print("unique items: ", len(set(id_list)), flush=True)
    print("Now deduplicating the emtree drugs.", flush=True)
    
    terms = []
    for item in node_list :
        terms.append(item['term'])
        
    unique_terms = set(terms)
    unique_nodes = []
    print("Total node count = ", len(node_list), flush=True)
    print("Output of nodes with unique terms. Total unique terms: ", len(unique_terms), ". Now counting unique terms:", flush=True)
    for counter, unique_term in enumerate(sorted(unique_terms)) :
        if counter % 5000 == 0 :
            sys.stderr.write(" " + str(counter) + " ")
            sys.stderr.flush()
        elif counter % 1000 == 0 :
            sys.stderr.write(".")
            sys.stderr.flush()
        for node in node_list :
            if node['term'] == unique_term :
                unique_nodes.append({'term':node['term'], 'synonyms':node['synonyms']})
                break
    print("", flush=True)
    print("total node count from original emtree: ", len(node_list), "", flush=True)
    print("total node count from deduped emtree : ", len(unique_nodes), "", flush=True)
    return(unique_nodes)



########################################################################
#   PoC:    load_fda_applic_from_raw_folder_to_json.py
########################################################################

def encoding(full_path) :
        rawdata = open(full_path, 'rb').read()
        result = chardet.detect(rawdata)
        return (result['encoding'])

def add_application_basic_data(data_to_return, raw_html) :

    application_data = { "Application Requested Number" : '', 'Application Extracted Number' : '' }

    # Extracting data from the first subtitle
    regex_to_extract_data_from_first_subtitle = '<span style="font-size:1.1em"><span class="prodBoldText"><strong>([^<]*)</strong>:</span> <span class="appl-details-top">([^<]*)</span>'
    matches_for_first_subtitle = re.findall(regex_to_extract_data_from_first_subtitle, raw_html, re.DOTALL)
    application_data['Application Type'], application_data['Application Extracted Number'] = matches_for_first_subtitle[0]
    # TODO: To remove the following assignment
    application_data['Application Requested Number'] = application_data['Application Extracted Number']
    data_to_return['meta']['Columns']['Application Requested Number'] = "application_data['Application Requested Number'] = application_data['Application Extracted Number']"
    
    
    # Extracting data from the second subtitle
    regex_to_extract_data_from_second_subtitle = '<span class="prodBoldText">Company:</span> <span class="appl-details-top">([^<]*)</span>'
    matches_for_second_subtitle = re.findall(regex_to_extract_data_from_second_subtitle, raw_html, re.DOTALL)
    application_data['Company'] = matches_for_second_subtitle[0].strip()


    ####################################
    # Extracting data from the first HTML table
    ####################################

    regex_to_extract_data_from_first_table_1 = '<tr class="prodBoldText">\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>'
    regex_to_extract_data_from_first_table_2 = '<tr class="prodBoldText">\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>\s*<span style="white-space:wrap">([^<]*)</span>\s*</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>'
    # TODO: To try and implement the second regex:  regex_to_extract_data_from_first_table_2, so the spans match too
    matches_for_first_table_1 = re.findall(regex_to_extract_data_from_first_table_1, raw_html, re.DOTALL)
    matches_for_first_table_2 = re.findall(regex_to_extract_data_from_first_table_2, raw_html, re.DOTALL)
    if   matches_for_first_table_1 :
        the_first_extracted_tuple = matches_for_first_table_1[0] # TODO: I may want to assert that there is only one element in this list (!)
        keys_for_the_first_extracted_tuple = ("Drug Name", "Active Ingredients", "Strength", "Dosage Form/Route", "Marketing Status", "TE Code", "RLD", "RS")
        application_data.update(dict(zip(keys_for_the_first_extracted_tuple, the_first_extracted_tuple)))
    elif matches_for_first_table_2 :
        the_first_extracted_tuple = matches_for_first_table_2[0] # TODO: I may want to assert that there is only one element in this list (!)
        keys_for_the_first_extracted_tuple = ("Drug Name", "Active Ingredients", "Strength", "Dosage Form/Route", "Marketing Status", "TE Code", "RLD", "RS")
        application_data.update(dict(zip(keys_for_the_first_extracted_tuple, the_first_extracted_tuple)))
    else :
        print(application_data['Application Requested Number'])
    
    ####################################
    # Second HTML table
    ####################################
    
    # ASSUMPTIONS: So we are assuming that we only have two (2) tr tags with no parameters (ie exactly like <tr>)
    # Also, keep in mind : the last column (Url) is hidden !
    regex_to_extract_data_from_second_table = '<tr>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*</tr>'
    matches_for_second_table= re.findall(regex_to_extract_data_from_second_table, raw_html, re.DOTALL)
    # Only first list element is the table I need now, so I only consider the first one.
    if matches_for_second_table :
        the_second_extracted_tuple = matches_for_second_table[0]
        # (An assertion must go here)
        keys_for_the_second_extracted_tuple = ("Action Date", "Submission", "Action Type", "Submission Classification", "Review Priority; Orphan Status", "Letters, Reviews, Labels, Patient Package Insert", "Notes", "Url")
        application_data.update(dict(zip(keys_for_the_second_extracted_tuple, the_second_extracted_tuple)))
        application_data["Urls"] = application_data['Url'].split()
        data_to_return['meta']['Columns']['Urls'] = 'This column is derived from the column Url.'


    #for list_elem in matches :
    #    if list_passed_validation_by_closing_td(list_elem) :
    # A list of tuple was returned. If any of them has '</td>' then it's a wrong list

    # In the following section I am extracting URLs for the cfm-files.
    regex_to_match_a_URL = "(https://www.accessdata.fda.gov[^\s\"]+)[\s\"]"
    matches_for_URLs = re.findall(regex_to_match_a_URL, raw_html, re.DOTALL)
    application_data["cfm_data"] = []
    for n, item in enumerate(matches_for_URLs):
        filename_extension = item.rsplit('.', 30)[-1]
        if filename_extension == "cfm":
            #application_data.update({"first_cfm_URL" : item})
            application_data["cfm_data"].append({"URL" : item})
    
    data_to_return['Applications'].append(application_data)


def iterate_raw_FDA_master_data(starting_application_number_hundred, out_folder = ".") :
    source_dir = os.listdir(fda_raw_master_data_path)
    starting_app_nbr_hun_char = str(starting_application_number_hundred).zfill(2)
    sys.stderr.write("Starting at appl number " + starting_app_nbr_hun_char +  " x 100 of total "+ str(len(source_dir)) + ". Cur Time = " + str(datetime.now()) + ".     ")
    application_basic_data = {"meta" : {'Program name' : os. path. basename(__file__), 'Timestamp' : str(datetime.now()), 'Columns' : { }}, "Applications" : [] }
    small_step = 100
    big_step = 1000
    sys.stderr.write(" " + str(small_step) + " x : ")
    ret = False
    for n, file_path in enumerate(source_dir):
        #if 0 and n > 3000  : # or n < int(starting_application_number_hundred) * big_step or n > (int(starting_application_number_hundred) + 1) * big_step - 1 :
        if n < int(starting_application_number_hundred) * big_step or n > (int(starting_application_number_hundred) + 1) * big_step - 1 :
            continue
        if n % small_step == 0 :
            sys.stderr.write(str(n // small_step) + "")
            sys.stderr.flush()
            sys.stdout.flush()
        elif n % 10 == 0 :
            sys.stderr.write(".")
            sys.stderr.flush()
        full_path = os.path.join(fda_raw_master_data_path, file_path)
        with open(full_path)  as html_file: #, encoding="utf8") as html_file:
            raw_html = html_file.read()
            if encoding(full_path) != "ascii" :
                continue
            ret = True
            add_application_basic_data(application_basic_data, raw_html)
    fn = "FDA_" + starting_app_nbr_hun_char + ".json"
    path_fn = os.path.join(out_folder, fn)
    json_file = open(path_fn, "w")
    json_file.write(json.dumps(application_basic_data))
    json_file.close()
    sys.stderr.write(".\n")
    return(ret)


def extract_FDA_master_data(out_folder=".") :
    starting_application_number_hundred = 0
    return_succes = True
    while return_succes :
        return_succes = iterate_raw_FDA_master_data(starting_application_number_hundred, out_folder = out_folder)
        starting_application_number_hundred += 1

def make_a_random_pause() :
    average_seconds_to_wait = 3
    for i in range(average_seconds_to_wait):
        milliseconds = 2 * random.randint(1, 1000)
        time.sleep(milliseconds * 1.0 / 1000) 
        #print( str(i).zfill(2) + datetime.utcnow().strftime('%F %T.%f')[:-3] + " " + str(milliseconds) )

def scrape_one_application_from_Drugs_at_FDA(data, ApplNo, alphabetLetter) :
    url = 'https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=' + ApplNo
    with urlopen( url ) as webpage:
        content = webpage.read().decode()
    text_file_name = os.path.join(fda_master_appl_data_html_source_path, alphabetLetter + "_" + ApplNo + ".txt")
    text_file = open(text_file_name, "w")
    n = text_file.write(content)
    text_file.close()

    application_data = { "Application Requested Number" : ApplNo, 'Application Extracted Number' : '' } # , "cfm_data" : []

    # Extracting data from the first subtitle
    regex_to_extract_data_from_first_subtitle = '<span style="font-size:1.1em"><span class="prodBoldText"><strong>([^<]*)</strong>:</span> <span class="appl-details-top">([^<]*)</span>'
    matches_for_first_subtitle = re.findall(regex_to_extract_data_from_first_subtitle, content, re.DOTALL)
    application_data['Application Type'], application_data['Application Extracted Number'] = matches_for_first_subtitle[0]
    
    
    # Extracting data from the second subtitle
    regex_to_extract_data_from_second_subtitle = '<span class="prodBoldText">Company:</span> <span class="appl-details-top">([^<]*)</span>'
    matches_for_second_subtitle = re.findall(regex_to_extract_data_from_second_subtitle, content, re.DOTALL)
    application_data['Company'] = matches_for_second_subtitle[0].strip()


    ####################################
    # Extracting data from the first HTML table
    ####################################

    regex_to_extract_data_from_first_table = '<tr class="prodBoldText">\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>'
    regex_to_extract_data_from_first_table_2 = '<tr class="prodBoldText">\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>\s*<span style="white-space:wrap">([^<]*)</span>\s*</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>'
    # TODO: To try and implement the second regex:  regex_to_extract_data_from_first_table_2, so the spans match too
    matches_for_first_table = re.findall(regex_to_extract_data_from_first_table, content, re.DOTALL)
    if matches_for_first_table :
        the_first_extracted_tuple = matches_for_first_table[0] # I may want to assert that there is only one element in this list (!)
        keys_for_the_first_extracted_tuple = ("Drug Name", "Active Ingredients", "Strength", "Dosage Form/Route", "Marketing Status", "TE Code", "RLD", "RS")
        application_data.update(dict(zip(keys_for_the_first_extracted_tuple, the_first_extracted_tuple)))
    
    ####################################
    # Second HTML table
    ####################################
    
    # ASSUMPTIONS: So we are assuming that we only have two (2) tr tags with no parameters (ie exactly like <tr>)
    # Also, keep in mind : the last column (Url) is hidden !
    regex_to_extract_data_from_second_table = '<tr>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*</tr>'
    matches_for_second_table= re.findall(regex_to_extract_data_from_second_table, content, re.DOTALL)
    # Only first list element is the table I need now, so I only consider the first one.
    if matches_for_second_table :
        the_second_extracted_tuple = matches_for_second_table[0]
        # (An assertion must go here)
        keys_for_the_second_extracted_tuple = ("Action Date", "Submission", "Action Type", "Submission Classification", "Review Priority; Orphan Status", "Letters, Reviews, Labels, Patient Package Insert", "Notes", "Url")
        application_data.update(dict(zip(keys_for_the_second_extracted_tuple, the_second_extracted_tuple)))
        application_data["Urls"] = application_data['Url'].split()
        data['meta']['Columns']['Urls'] = 'This column is derived from the column Url.'


    #for list_elem in matches :
    #    if list_passed_validation_by_closing_td(list_elem) :
    # A list of tuple was returned. If any of them has '</td>' then it's a wrong list

    # In the following section I am extracting URLs for the cfm-files.
    regex_to_match_a_URL = "(https://www.accessdata.fda.gov[^\s\"]+)[\s\"]"
    matches_for_URLs = re.findall(regex_to_match_a_URL, content, re.DOTALL)
    application_data["cfm_data"] = []
    for n, item in enumerate(matches_for_URLs):
        filename_extension = item.rsplit('.', 30)[-1]
        if filename_extension == "cfm":
            #application_data.update({"first_cfm_URL" : item})
            application_data["cfm_data"].append({"URL" : item})
    
    data['Applications'].append(application_data)


def scrape_one_letter_from_Drugs_at_FDA(alphabetLetter, slicing_limits_per_letter=(None, None)) :
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    sys.stderr.write(alphabetLetter + " started. Current Time = " + current_time + ".     ")
    alphabet_letter_url = 'https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=browseByLetter.page&productLetter=' + alphabetLetter + '&ai=0'
    with urlopen( alphabet_letter_url ) as webpage:
        content = webpage.read().decode()
        regex = '/scripts/cder/daf/index.cfm\?event\=overview\.process\&ApplNo\=(\d{6})'
        ApplNumbers = re.findall(regex, content, re.DOTALL)
        
    application_basic_data = {"meta" : {'Columns' : { }}, "Applications" : [] }
    sys.stderr.write("x 100 : ")
    for n, ApplNo in enumerate(ApplNumbers[slicing_limits_per_letter[0]: slicing_limits_per_letter[1]]):
        if n % 100 == 0 :
            sys.stderr.write(str(n // 100) + "")
            sys.stderr.flush()
            sys.stdout.flush()
        elif n % 10 == 0 :
            sys.stderr.write(".")
            sys.stderr.flush()
        make_a_random_pause()
        scrape_one_application_from_Drugs_at_FDA(application_basic_data, ApplNo, alphabetLetter)
        # if n > 150 :
        #   break
    sys.stderr.write(".\n")
    json_file = open("json_" + alphabetLetter + ".txt", "w")
    n = json_file.write(json.dumps(application_basic_data))
    json_file.close()

def scrape_whole_alphabet_from_Drugs_at_FDA(slicing_limits_per_letter=(None, None)) :
    ascii_code_at_A = 65
    ascii_code_after_Z = 91
    for ascii_code in range(ascii_code_at_A, ascii_code_after_Z):
        alphabetLetter = chr(ascii_code)
        scrape_one_letter_from_Drugs_at_FDA(alphabetLetter, slicing_limits_per_letter = slicing_limits_per_letter)