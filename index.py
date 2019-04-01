import os
from flask import Flask, render_template, request, redirect, Response, send_from_directory, send_file, url_for
from werkzeug.utils import secure_filename
import datafix_2

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/')
def index():
    clear_folder()
    return render_template('long_to_wide.html')


@app.route('/handle_data', methods=['POST'])
def handle_data():

    f = request.files['original_file_name']
    original_filename = secure_filename(f.filename)
    f.save(os.path.join(app.config['UPLOAD_FOLDER'], original_filename))

    my_form = request.form

    new_file = my_form['new_file_name']
    display_option = my_form['timeptdisplay']

    label = datafix_2.datafix(original_filename, new_file, display_option)
    filename = new_file
    return render_template('results.html', label=label, filename=filename)


def clear_folder():
    folder = 'uploads'
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
             print(str(e))



@app.route('/return_files/<path:filename>')
def return_files(filename):
    try:
        source = os.path.join(app.root_path, app.config['UPLOAD_FOLDER'])

        return send_from_directory(source, filename=filename, as_attachment=True, mimetype='text/csv' ,attachment_filename=filename)
    except Exception as e:
        return str(e)


if __name__ == "__main__":
    #app.run(host='0.0.0.0', port=80)
    app.run(threaded=True)
