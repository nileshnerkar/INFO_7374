import argparse
import os
import azureml.dataprep as dprep
import azureml.core

def write_output(df, path):
    os.makedirs(path, exist_ok=True)
    print("{} created".format(path))
    df.to_csv(path + "/TrainTest_Data")


print("Split the data into train and test set 80-20")

parser = argparse.ArgumentParser("split")
parser.add_argument("--input_data_frame", type=str, help="input data frame")
parser.add_argument("--output_train_frame", type=str, help="output train frame")
parser.add_argument("--output_val_frame", type=str, help="output validation frame")

args = parser.parse_args()

print("Argument 1(input data frame path): {}" .format (args.input_data_frame))
print("Argument 2(output training frame path): {}" .format (args.output_train_frame))
print("Argument 3(output validation frame path): {}" .format (args.output_val_frame))

input_df = dprep.read_csv(path=args.input_data_frame, header=dprep.PromoteHeadersMode.SAMEALLFILES).to_pandas_dataframe()

idx = int(input_df.shape[0] * 0.8) #Getting the index of the first 80% of rows

input_df = input_df[['date','value']]

train_df = input_df[:idx]
val_df = input_df[idx:]

if not (args.output_train_frame is None and args.output_val_frame is None):
    write_output(train_df, args.output_train_frame)
    write_output(val_df, args.output_val_frame)