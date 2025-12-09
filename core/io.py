import pandas as pd
import tables as tb
import core.df_classes as df_class 

import ast
import configparser
import logging



def read_config_file(file_path  :  str) -> dict:
    '''
    Read config file and extract relevant information returned as a dictionary.
    
    Extracted explicitly from MULE:
    https://github.com/nu-ZOO/MULE/blob/abeab70/packs/core/io.py#L68

    Parameters
    ----------

    file_path (str)  :  Path to config file

    Returns
    -------

    arg_dict (dict)  :  Dictionary of relevant arguments for the pack
    '''
    # setup config parser
    config = configparser.ConfigParser()

    try:
        # read in arguments, require the required ones
        config.read(file_path)
    except TypeError as e:
        logging.error(f"Error reading config file '{file_path}': {e}")
        return None
    
    arg_dict = {}
    for section in config.sections():
        for key in config[section]:
            # the config should be written in such a way that the python evaluator
            # can determine its type
            #
            # we can setup stricter rules at some other time
            arg_dict[key] = ast.literal_eval(config[section][key])

    return arg_dict


def create_config_table(h5file : tb.File, dictionary : dict, name : str, description = "config"):
        
        # create config node if it doesnt exist already
        try:
                group = h5file.get_node("/", "config")
        except tb.NoSuchNodeError:
                group = h5file.create_group("/", "config", "Config parameters")
        
        # create table
        table = h5file.create_table(group, name, df_class.config_class, description)
        # assign the rows by component
        config_details = table.row
        for key, values in dictionary.items():
            if type(values) is dict:
                # single nest only! any more and you've made the config too complicated
                for key_2, value_2 in values.items():
                    print(key_2)
                    config_details['key'] = f'{key}/{key_2}'
                    config_details['value'] = value_2
                    config_details.append()
            else:
                config_details['key']   = key
                config_details['value'] = values
                config_details.append()
        table.flush()