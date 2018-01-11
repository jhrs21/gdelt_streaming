from flask import Flask, flash, redirect, render_template, request, session, abort, json
 
app = Flask(__name__)
 
@app.route("/")
def index():
    return "Flask App!"
 
@app.route("/hello/<string:name>/")
def hello(name):
    return render_template(
        'test.html',name=name)

@app.route("/geochart")
def geochart():
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
    return render_template(
        'geochart.html', countries=countries)

 
if __name__ == "__main__":
    app.run()