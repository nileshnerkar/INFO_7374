import argparse
import os
import pandas as pd
import azureml.dataprep as dprep

print("Aggregate data to same same frequency")

parser = argparse.ArgumentParser("agg")
parser.add_argument("--input_cleanse", type=str, help="cleaned data directory")
parser.add_argument("--output_cleanse", type=str, help="Aggregated data directory")

args = parser.parse_args()

print("Argument 1(input data path): %s" % args.input_cleanse)
print("Argument 3(output data path): %s" % args.output_cleanse)

p_df=dprep.read_csv(path=args.input_cleanse).to_pandas_dataframe()

p_df['date'] = pd.to_datetime(p_df['date'])
p_df['value']= p_df['value'].astype(float)

p_df['year'] = p_df['date'].dt.year
p_df['Qtr'] = p_df['date'].dt.quarter
p_df['Qtr'] = p_df['Qtr'].apply(lambda x : "-Q"+ str(x))
p_df['period'] = p_df['year'].astype(str) + p_df['Qtr'].astype(str)
#p_df['period']=pd.to_datetime(p_df.period).dt.to_period('Q')
p_df['period'] = pd.PeriodIndex(p_df['period'], freq='Q').to_timestamp()
p_df=p_df[['period', 'value']].groupby(['period'], as_index=False).agg({'value':'mean'})

print(p_df.dtypes)
print(p_df.head())
df=dprep.read_pandas_dataframe(df=p_df, in_memory=True)

if not (args.output_cleanse is None):
    os.makedirs(args.output_cleanse, exist_ok=True)
    print("%s created" % args.output_cleanse)
    write_df = df.write_to_csv(directory_path=dprep.LocalFileOutput(args.output_cleanse))
    write_df.run_local()