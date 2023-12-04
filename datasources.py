# This change was done on 2023 12 05 0015

import ast, sys, json, os, re
from datetime import datetime
# label_data_path = r"C:\Users\as23i485\Documents\additional_data\openfda_labels\JSON"
label_data_path = None

def config(fn) :
    config_data = json.load(open(fn, "r"))
    global label_data_path
    if config_data.get('base_path') :
        label_data_path = os.path.join(config_data['base_path'], config_data['label_data_path'])
    else :
        label_data_path = config_data['label_data_path']


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

def count_label_data() :
    print(label_data_path)

