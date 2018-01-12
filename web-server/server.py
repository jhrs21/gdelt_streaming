from flask import Flask, flash, redirect, render_template, request, session, abort, json, jsonify
from impala.dbapi import connect


app = Flask(__name__)
 
@app.route("/")
def index():
    return render_template('overview.html')


@app.route("/streaming")
def geochart_streaming():
    return render_template('streaming.html')


@app.route("/batching")
def geochart_batching():
    countries = [
        ['Country', 'Popularity'],
        ['Germany', 200],
        ['United States', 300],
        ['Brazil', 400],
        ['Canada', 500],
        ['France', 600],
        ['Spain', 400],
        ['RU', 700]
    ]
    return render_template('batching.html', countries=countries)


@app.route("/batching_data")
def batching_data():
    input_date = '2015-01-03'

    # Connection to database
    conn = connect(host='localhost', port=10000, auth_mechanism='PLAIN', user='xxx', password='xxx',
                   database='gdelt_db')
    c = conn.cursor()

    # Avg per country from an initial date
    query = """
        SELECT ActionGeo_CountryCode, AVG(AvgTone)
        FROM gdelt_temporal
        where to_date(SQLDATE) >= {}
        group by ActionGeo_CountryCode
    """

    # Execution of query
    c.execute(query.format(input_date))
    result_set = c.fetchall()

    # From list to dictionary
    header = ['country', 'avg_tone']
    avg_per_country = [dict(zip(header, c)) for c in cou]


    return jsonify(result_set)

 
if __name__ == "__main__":
    app.run()