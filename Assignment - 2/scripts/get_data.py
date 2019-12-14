import os
import pandas as pd


def get_data():
    print("In get_data")
    print(os.environ['AZUREML_DATAREFERENCE_agg_rates_data'])
    X_train  = pd.read_csv(os.environ['AZUREML_DATAREFERENCE_agg_rates_data'] + "/part-00000", header=0)
    
    return { "X" : X_train['period'].values, "y" : X_train['value'].values.flatten() }