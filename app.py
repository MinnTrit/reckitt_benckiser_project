from flask import Flask, request, jsonify, redirect, url_for, flash, make_response
from werkzeug.utils import secure_filename
import os
import time
import pandas as pd
from save import Saver
from processing import Processor
from tasks import main_task
from worker import make_celery

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/home/ubuntu/reckitt_benckiser_project/uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 
app.secret_key = 'supersecretkey'

# Celery configuration
broker_url = os.getenv("broker_url")
app.config['CELERY_BROKER_URL'] = broker_url
app.config['CELERY_RESULT_BACKEND'] = broker_url
celery = make_celery(app)

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

ALLOWED_EXTENSIONS = {'txt', 'csv', 'json', 'xlsx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return '''
    <!doctype html>
    <title>Upload File</title>
    <h1>Upload a File</h1>
    <form method="post" action="/upload" enctype="multipart/form-data">
      <input type="file" name="file">
      <input type="submit" value="Upload">
    </form>
    '''

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        return redirect(url_for('uploaded_file', filename=filename))
    else:
        flash('File type not allowed')
        return redirect(request.url)

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    host_name = os.getenv('instance_host')
    file_path = app.config['UPLOAD_FOLDER'] + f'/{filename}'
    print(f'The file path is {file_path}')
    if os.path.exists(file_path): 
        task = main_task.delay(file_path)
        task_id = task.id
        response = {
            'status_code': 200,
            'filename': filename,
            'task_id': task_id,
            'task_status_endpoint':f'http://{host_name}:5000/status/{task_id}',
        }
        return make_response(jsonify(response), 200)
    else:
        response = {
            'status_code': 404,
            'error': 'File not found'
        }
        return make_response(jsonify(response), 404)
    
@app.route('/status/<task_id>', methods=['GET'])
def task_status(task_id):
    task = celery.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'status': 'pending',
            'message': 'Task is still processing.'
        }
    elif task.state == 'SUCCESS':
        response = {
            'status': 'success',
            'result': task.result
        }
    elif task.state == 'FAILURE':
        response = {
            'status': 'failure',
            'message': str(task.result)
        }
    else:
        response = {
            'status': 'unknown',
            'message': 'Task state is unknown.'
        }
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
