from flask import Flask, render_template, request
import json
import pandas as pd
from AWS.AWSClient import AWSClient
from AWS.MTurk import MTurk
from PlateRecognizer.PlateRecognizer import PlateRecognizer
from DBConnection.DBConnector import DBConnector

app = Flask(__name__)

@app.route("/")
def home():
    connector = DBConnector()
    rec = connector.execute("SELECT hit, image_url, mturk_status from imagedetails where mturk_status='Assignable';")
    if rec:
        df = pd.DataFrame(data=rec, columns=['hit', 'image_url', 'mturk_status'])
        all_columns = list(df) # Creates list of all column headers
        df[all_columns] = df[all_columns].astype(str)
        print(df.dtypes)
    else:
        return "No Records in DB...!"
    return render_template('index.html', data = df)


@app.route("/hit", methods=['GET', 'POST'])
def hit():
    if request.method == 'POST':
        #create mturk hit for uploaded image
        pass
    else:
        #Get method table
        pass

@app.route("/LicensePlate-classifier/<hit>/<bucket>/<key>")
def predict_and_verify(hit, bucket, key):
    #call api for license prediction
    pr = PlateRecognizer()
    api_res = pr.predict(bucket, key)
    mt = MTurk()
    mturk_resp = mt.getHitResults(hit)
    image_url = f"https://{bucket}.s3.amazonaws.com/{key}"
    d = {'api_res': api_res, 'mturk':mturk_resp, 'image_url': image_url}
    return render_template('test.html', content=d)
    #return "Done"
    

if __name__ == "__main__":
    app.run(debug=True)