import os
from flask import Flask, render_template, request, redirect, Response, send_from_directory, send_file, url_for
from werkzeug.utils import secure_filename
import datafix_2
import pandas as pd

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

#TODO: files named the same thing as previous uploads output the same thing as the previous upload
#rather than outputting the proper

@app.route('/')
def index():
    clear_folder()
    return render_template('long_to_wide.html')


@app.route('/handle_data', methods=['POST'])
def handle_data():
    clear_folder()
    f = request.files['original_file_name']
    original_filename = secure_filename(f.filename)
    f.save(os.path.join(app.config['UPLOAD_FOLDER'], original_filename))

    my_form = request.form

    new_file = my_form['new_file_name']
    display_option = my_form['timeptdisplay']
    is_redcap = my_form['isredcap']

    if is_redcap == 'True':
        id_col = None
        tp_col = None
    else:
        id_col = my_form['subject_id_col']
        tp_col = my_form['timepoint_col']

    duplicates, missingTPs, isError, errors = datafix_2.datafix2(original_filename, new_file, display_option, is_redcap, id_col, tp_col)

    if isError:
        return handle_error(errors)
    else:
        if isinstance(duplicates, pd.Series):
            duplicates = duplicates.unique()
        if isinstance(missingTPs, pd.Series):
            missingTPs = missingTPs.unique()
        filename = new_file
        return render_template('results.html', duplicates=duplicates, missingTPs=missingTPs, filename=filename)


@app.route('/handle_error')
def handle_error(return_errors):
    return render_template('error_handler.html', message1=return_errors)


def clear_folder():
    folder = 'uploads'

    filelist = [ f for f in os.listdir(folder) if f.endswith(".csv") ]
    for f in filelist:
        os.remove(os.path.join(folder, f))


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
