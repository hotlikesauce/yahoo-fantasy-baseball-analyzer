from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    message = "Hello, Flask!"
    return render_template('index.html', message=message)