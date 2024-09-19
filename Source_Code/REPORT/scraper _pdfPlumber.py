import codecs
import os
import re

import pandas as pd
import pdfplumber

# source_path = os.getcwd() + '\\reports'

mypath = 'G:\\reports2'

columns = ['StudyUID', 'LesionRate', 'Conclusion', 'Covid']

df_global = pd.DataFrame(columns=columns)

log_file = None


def getDirectoryData(dirName):
    info = '╔ processing Directory : ' + dirName
    global log_file
    log_file.write(info + '\n')
    print(info)
    data = []
    for entry in os.listdir(dirName):
        # Create full path
        full_path = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(full_path):
            getDirectoryData(full_path)
        else:
            row = getFileData(full_path)
            if row is not None:
                data.append(row)
    global df_global
    df = df.append(data, ignore_index=True)
    df.to_csv(mypath+'_report.csv', index=False)
    info = '╚ Ending Directory : ' + dirName
    log_file.write(info + '\n')
    print(info)


def getFileData(file_name):
    if file_name.endswith('.pdf'):
        global log_file
        info = '╟ processing File : ' + file_name
        # log_file.write(info + '\n')
        print(info)
        row = {}
        try:
            row['StudyUID'] = os.path.basename(file_name)
            report_text = ''
            with pdfplumber.open(file_name) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text is not None:
                        report_text = report_text + page_text

            # save the extracted text
            txt_file = codecs.open(file_name.replace('.pdf', '_pdfplumber.txt'), 'a', "utf-8")
            txt_file.write(report_text)
            txt_file.close()

            # Conclusion
            x = re.search('.*Conclusion.*', report_text, flags=re.IGNORECASE | re.DOTALL)
            if x:
                row['Conclusion'] = 'True'
            else:
                row['Conclusion'] = 'False'

            # Covid
            x = re.search('.*COVID.*', report_text, flags=re.IGNORECASE | re.DOTALL)
            if x:
                row['Covid'] = 'True'
            else:
                row['Covid'] = 'False'

            # LesionRate
            x = re.findall('([0-9]+)%.*parenchyme', report_text, flags=re.IGNORECASE | re.DOTALL)

            if len(x) > 0:
                lesion_rate = x[0]
                row['LesionRate'] = lesion_rate
            else:
                row['LesionRate'] = 0
        except Exception as err:
            info = 'Error : ' + file_name + '\n' + err
            log_file.write(info + '\n')
            print(info)
            row = None
        finally:
            return row


def main():
    global log_file
    log_file = codecs.open(mypath + '_log.txt', 'a', "utf-8")
    getDirectoryData(mypath)
    df_global.to_csv(mypath + '_report.csv', index=False)


if __name__ == "__main__":
    main()
