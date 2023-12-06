# This change was done on 2023 12 05 0015

import ast, sys, json, os, re
from datetime import datetime
# label_data_path = r"C:\Users\as23i485\Documents\additional_data\openfda_labels\JSON"
from operator import itemgetter
from urllib.request import urlopen

label_data_path = None


def config(fn) :
    config_data = json.load(open(fn, "r"))
    
    global ctinversion_interventions_fn
    ctinversion_interventions_fn    = config_data['ctinversion_interventions_fn']
    
    global staging_path
    staging_path = "."
    
    if config_data.get('base_path') :
    
        global label_data_path
        label_data_path = os.path.join(config_data['base_path'], config_data['label_data_path'])
        
        global ct_data_path
        ct_data_path    = os.path.join(config_data['base_path'], config_data['ct_data_path'])
        
        global ct_data_path_extracted
        ct_data_path_extracted    = os.path.join(config_data['base_path'], config_data['ct_data_path_extracted'])
        
        staging_path    = os.path.join(config_data['base_path'], config_data['staging_dir'])
    else :
        label_data_path = config_data['label_data_path']
        ct_data_path    = config_data['ct_data_path']
        ct_data_path_extracted    = config_data['ct_data_path_extracted']

# Labels
# 
# Labels
# Labels
# Labels
# Labels

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

    '''
    json_to_report = {}
    json_to_report['timesamp'] = str(datetime.now())
    json_to_report['program'] = "extract_ct_info_from_json.py"
    json_to_report['StudyInfo'] = multiple_study_objects
    file_to_output.write(json.dumps(json_to_report))
    '''
    
    json.dump(multiple_study_objects, file_to_output)
    return(True)

def extract_ct_data_from_all_stacks_of_json_subfolders(folder_step_size=10, max_folder_count=None, max_file_count_per_subfolder=None, out_folder='.') :
    folder_step_idx = 0
    while extract_ct_data_from_stack_of_json_subfolders(folder_step_idx, folder_step_size=10, out_folder="./ct_out", max_file_count_per_subfolder=1) :
        folder_step_idx += 1


