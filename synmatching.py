import ast, sys, json, os, re
from datetime import datetime
import string
import datasources as ds
ds.config("ddconfig.json")

def get_FDA_label_list(slicing_limits=(None, None)) :
    label_path_fn = ds.label_subselected_path_fn # "..\\subselected_fda_labels.json"
    print(f"label_path_fn - = {label_path_fn}")

    with open(label_path_fn, encoding="utf-8") as json_file:
        label_results = []
        counter = 0
        selected_labels = json.load(json_file)[slicing_limits[0]:slicing_limits[1]]
        # print("\nCount labels to be selected for processing = ", len(selected_labels))
        for result in selected_labels :
            if counter % 1 == 0 :
                sys.stderr.write(".")
            if counter % 10 == 0 :
                sys.stderr.write(" " + str(counter) + " ")
            if counter % 100 == 0 : sys.stderr.write(" Cur Time = " + str(datetime.now())[0:19] + "   " + "\n")
            counter += 1
            sys.stderr.flush()
            
            try:
                indications_and_usage = result.get('indications_and_usage', [])[0]
            except IndexError:
                indications_and_usage = ''
            result['indications_and_usage_0'] = indications_and_usage
            result['indications_and_usage_trunc'] = indications_and_usage[0:16000]
            label_results.append(result)
    return(label_results)

def isACapitalLetter(letter) :
    return (65 <= ord(letter) <= 90)

def matches_with_wordboundaries(synonym, text) :
    text_to_match = text.upper()
    l_end_of_synon = text_to_match.find(synonym.upper())
    if l_end_of_synon == -1 :
        return(False)
    else :
        l_end_matches_wboundary = (not isACapitalLetter(text_to_match[l_end_of_synon-1]) or l_end_of_synon == 0)
        r_after_end_of_synon = l_end_of_synon + len(synonym)
        r_end_matches_wboundary = (r_after_end_of_synon == len(text_to_match) or not isACapitalLetter(text_to_match[r_after_end_of_synon]))
        rhs_letter = "END_OF_STRING" if r_after_end_of_synon == len(text_to_match) else text_to_match[r_after_end_of_synon]
        return(l_end_matches_wboundary and r_end_matches_wboundary)    

def multi_match_to_emtree_syn_list(text, node_list) :
    nodes_to_ret = []
    for node in node_list :
        synonyms = node['synonyms']
        term     = node['term']
        for synonym in synonyms:
            if matches_with_wordboundaries(synonym.upper().strip(), text.upper().strip()) :
                node['the_match'] = synonym
                nodes_to_ret.append(node)
    return(nodes_to_ret)

# Now take the indications and use the matching functions to extract the indication list for each label.

def extract_the_matches(node_list) :
    ret = []
    for node in node_list :
        ret.append(node['the_match'])
    return(list(set(ret)))


emtree_nodes_w_unique_terms_indic_path_fn = ds.emtree_nodes_w_unique_terms_indic_path_fn # "..\\emtree_nodes_w_unique_terms.json"

def get_verbatim_synonyms_matched_labels(slicing_limits=(None, None)) :

    label_list = get_FDA_label_list(slicing_limits=slicing_limits)
    print(f"\nCount labels selected = {len(label_list)}\n")
    sys.stderr.write("\nNow iterating over the label list. A total of " + str(len(label_list)) +
    " labels to be checked, starting now.")
    sys.stderr.flush()

    json_file = open(emtree_nodes_w_unique_terms_indic_path_fn, "r")
    node_list = json.load(json_file) # [0:10]
    print("total node count from emtree: ", len(node_list), "")

    label_matching_info = []
    for counter, label in enumerate(label_list) : 
        # print(f"\n\n\n")
        if counter % 10 == 0 :
            sys.stderr.write(".")
        if counter % 100 == 0 :
            sys.stderr.write(" " + str(counter) + " ")
        if counter % 1000 == 0 : sys.stderr.write(" Cur Time = " + str(datetime.now())[0:19] + "   " + "\n")
        sys.stderr.flush()
        determinist_emtree_matching_info = multi_match_to_emtree_syn_list(label['indications_and_usage_trunc'], node_list)
        the_matches = extract_the_matches(determinist_emtree_matching_info)

        try:
            indications_and_usage = label.get('indications_and_usage', [])[0]
        except IndexError:
            indications_and_usage = ''
        try:
            clinical_studies = label.get('clinical_studies', [])[0]
        except IndexError:
            clinical_studies = ''
        try:
            label_number = label.get('openfda', []).get('application_number', [])[0]
        except IndexError:
            label_number = ''
        try:
            brand_name = label.get('openfda', []).get('brand_name', [])[0]
        except IndexError:
            brand_name = ''
        try:
            app_nbr_6d = re.findall("\d{6}$", label_number)[0]
        except IndexError:
            app_nbr_6d = ''
        try:
            app_type = re.findall("^(\D*)\d", label_number)[0]
        except IndexError:
            app_type = ''

        info = {
            'label_number'   : label_number,
            'app_nbr_6d'   : app_nbr_6d,
            'brand_name'   : brand_name,
            'indications_and_usage' : label['indications_and_usage_trunc'],
            'verbatim_emtree_matches'  : the_matches
        }
        #    'clinical_studies' : clinical_studies,
        #    'emtree_matching_info'  : determinist_emtree_matching_info,
        label_matching_info.append(info)

    out_file = open(os.path.join(ds.staging_path, "verbatim_synonyms_matched_labels.json"), "w")
    json.dump(label_matching_info, out_file)


