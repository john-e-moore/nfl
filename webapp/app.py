from flask import Flask, render_template, request
import xgboost as xgb
import pandas as pd
import sqlite3
from sqlite3 import Error

app = Flask(__name__)

# Load the trained model
bst = xgb.Booster({'nthread': 4})  # init model
bst.load_model('model.bin')  # load data

# Connect to SQLite database
conn = sqlite3.connect('queries_results.db')

@app.route('/', methods=['GET', 'POST'])
def home():
    prediction = ''
    if request.method == 'POST':
        # Get user inputs
        user_inputs = request.form

        # Perform feature calculation and model prediction here (not shown)

        # Save user query and result to database
        cursor = conn.cursor()
        cursor.execute("INSERT INTO queries_results (query, result) VALUES (?, ?)", 
                       (str(user_inputs), str(prediction)))
        conn.commit()

    return render_template('index.html', prediction=prediction)

if __name__ == '__main__':
    app.run(debug=True)
