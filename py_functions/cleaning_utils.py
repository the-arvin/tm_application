# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 21:55:23 2023

@author: Arvin Jay
"""

import pandas as pd
import dateparser
from jellyfish import soundex
from fuzzywuzzy import fuzz

hard_coded_dict = {
        'traffic':'transit',
        'misc':'miscellaneous',
        # 'leave':None,
        # 'random':None,
        # 'overthinking':None,
        # 'fatnesscheckin':None,
    }

def generate_url(csv_link):
    """
    Generate a Google Drive URL from a CSV link.

    Parameters:
    csv_link (str): A link to a CSV file from TM's Google Drive.

    Returns:
    str: The Google Drive URL.
    
    >>> generate_url('https://drive.google.com/file/d/1HAq1aVDrMO48BTmTM5DCrZgA1iPEDG6Y/view')
    'https://drive.google.com/uc?id=1HAq1aVDrMO48BTmTM5DCrZgA1iPEDG6Y'
    """
    source_id = csv_link.split('/')[-2]
    return f'https://drive.google.com/uc?id={source_id}'


def custom_parser(unparsed_timestamp):
    """
    Customized timestamp parser to handle various timestamp formats.

    Parameters:
    unparsed_timestamp (str): The unparsed timestamp.
        pd.to_datetime(unparsed_timestamp) handles the more common datetime formats
        dateparser.parse(unparsed_timestamp) handles the russian datetime formats
        
    Returns:
    pd.Timestamp: The parsed timestamp.
    
    >>> custom_parser('2019-09-27 00:00:00 UTC')
    '2019-09-27 00:00:00 UTC'
    >>> custom_parser('26 сентября 2019 00:00')
    '2019-09-26 00:00:00 UTC'
    """
    try:
        return pd.to_datetime(unparsed_timestamp).tz_localize(None)
    except ValueError:
        try:
            return dateparser.parse(unparsed_timestamp)
        except ValueError:
            return pd.NaT


def load_csv(url): # add try except
    """
    Load data from a CSV file and parse timestamps.
    
    Parameters:
    url (str): The URL of the CSV file.
    
    Returns:
    pd.DataFrame: The loaded DataFrame.
    
    >>> load_csv(url)
    pd.DataFrame of data from Google Drive where the column 'timestamp' has
    been parsed using custom parser
    """
    return pd.read_csv(url, parse_dates=['timestamp'], date_parser=custom_parser)


def clean_df(df,correction_dict):
    """
    Parameters
    ----------
    df : pandas Dataframe
        the working dataframe loaded previously.
    correction_dict : dictionary
        a dictionary created from fuzzy matching and sound indexing.

    Returns:
    df : where 
            null values in 'user' are dropped
            considered number of checkin hours is >0
            correction_dict is applied to 'project' column
        
    >>> clean_df(df,correction_dict)
    pd.DataFrame of cleaned data that has been loaded. The column 'project'
    has been corrected for fuzzymatched and phonetically matching terms
    """
    df = df[df['user'].notnull()]#df.fillna('Unknown')
    df = df[df['hours']>0]
    df['project']=df['project'].apply(lambda x: apply_correction(correction_dict,x))
    df['timestamp'] = df['timestamp'].apply(lambda x: make_naive(x))
    return df
    

def calculate_similarity(entry1, entry2):
    """
    Calculate the similarity between two strings using the fuzz.ratio method.

    Parameters:
    entry1 (str): The first string.
    entry2 (str): The second string.

    Returns:
    int: The similarity score.
    
    >>> calculate_similarity('project-30', 'project-31')
    90
    """
    return fuzz.ratio(entry1, entry2)


def construct_projects_df(df):
    """
    Construct a DataFrame of project names and their counts, soundex values, 
    shifted project names, along with similarity scores.

    Parameters:
    df (pd.DataFrame): The input loaded DataFrame.

    Returns:
    pd.DataFrame: The constructed projects DataFrame.
    
    >>> construct_projects_df(df)
    # Projects DataFrame
    """
    projects_df = df.project.value_counts().reset_index()
    projects_df['soundex'] = projects_df['index'].map(soundex)
    projects_df = projects_df.sort_values(by=['soundex','project'])
    
    projects_df['shifted'] = projects_df['index'].shift(1)
    projects_df['similarity_score'] = projects_df.apply(lambda row: calculate_similarity(row['index'], row['shifted']), axis=1)
    return projects_df
        

def process_group(df_temp, correction_dict):
    """
    Process a group of similar project names and update the correction 
    dictionary for similar terms in the 'project' column.

    Parameters:
    df_temp (list): A list of project names with similarity.
    correction_dict (dict): The correction dictionary for project names.

    Returns:
    dict: Updated correction dictionary.
    
    >>> process_group(sample_group, sample_correction_dict)
    # Updated correction dictionary
    """    
    df_temp = pd.DataFrame(df_temp)
    df_temp = df_temp.sort_values(by='project', ascending=False)
    elements = list(set(df_temp['index'].tolist()+df_temp['shifted'].tolist()))
    for entry in elements:
        correction_dict[entry] = df_temp['index'].iloc[0]
    return correction_dict


def generate_correction_dict(df):
    """
    Parameters
    ----------
    df : pandas DataFrame
        
        The default is projects_df-  which contains columns 
        for the index (project name), project (count),
        shifted (shifted project name), and similarity_score (fuzzywuzzy 
        similarity score). This function iterates over the rows of df to create 
        a correction dictionary. The criteria for the correction is having a 
        similar soundex score and a similarity score of greater than 90.

    Returns
    -------
    correction_dict : dictionary
    
        Dictionary that would be used as a tool to clean-up project 
        descriptions
        
        >>> generate_correction_dict(sample_dataframe)
        
                    {
                      "blog-ideas": "blogideas",
                      "cultureandmangement": "cultureandmanagement",
                      "cultureandmanagemen": "cultureandmanagement",
                      "clutureandmanagement": "cultureandmanagement",
                      "culturandmanagement": "cultureandmanagement",
                      "hirng": "hiring",
                      "internal": "internals",
                      "machine-learning": "machinelearning",
                      "opsandamin": "opsandadmin",
                      "opsandadmin": "opsandadmin",
                      "opsadadmin": "opsandadmin",
                      "opsandandmin": "opsandadmin",
                      "opssandadmin": "opsandadmin",
                      "opdandadmin": "opsandadmin"
                    }
    """
    df_temp = list()
    correction_dict = dict()
    for index, row in df.iterrows():
        if row['similarity_score'] > 90:
            df_temp.append(row)
        elif len(df_temp)> 0:
            correction_dict = process_group(df_temp, correction_dict)
            df_temp = list()
        else:
            pass
    if df_temp:
        correction_dict = process_group(df_temp, correction_dict)
    return correction_dict
        

def make_naive(x):
    """
    Convert a datetime series to a naive datetime series.

    Parameters:
    x : datetime object that may or may not be timezone aware

    Returns:
    x : naive datetime object

    >>> make_naive(pd.Series(['2023-10-25 12:00:00']))
    Timestamp('2023-10-25 12:00:00')
    """
    try:
        x = pd.to_datetime(x).dt.tz_convert(None)
    except:
        pass
    return x


def apply_correction(correction_dict, value):
    """
    Apply corrections to a project name using the correction dictionary.

    Parameters:
    correction_dict (dict): The correction dictionary for project names.
    value (str): The project name to be corrected.

    Returns:
    str: The corrected project name.
    
    >>> apply_correction(correction_dict, 'opdandadmin')
    'opsandadmin'
    """
    try:
        value = correction_dict[value]
    except KeyError:
        pass
    return value


def cleaning_process(hard_coded_link):
    """
    Perform the data cleaning process, including loading data, 
    constructing a correction dictionary, and cleaning the DataFrame,
    essentially the main algorithm for the cleaning of the raw data

    Parameters:
    hard_coded_link (str): The hard-coded link to the data source.

    Returns:
    pd.DataFrame, pd.DataFrame: The original DataFrame and the cleaned DataFrame.
    
    >>> cleaning_process(hard_coded_link)
    # Original and cleaned DataFrames
    """
    url = generate_url(hard_coded_link)
    df_raw = load_csv(url)
    projects_df = construct_projects_df(df_raw)
    correction_dict = generate_correction_dict(projects_df)
    df = clean_df(df_raw,correction_dict)
    return df_raw,df
# """
# Sample implementation
# """
if __name__ == '__main__':
    cleaning_process('https://drive.google.com/file/d/1UAO5oOF1-BdDUFSySa9roIkOwytqtguO/view')
