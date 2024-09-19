import codecs
import os
import shutil
import threading
from datetime import datetime
from pathlib import Path
import glob

import pandas as pd

path = r'F:\DataSet\2022-02-22\metadata'  # use your path

columns_name = ['file',
                'SpecificCharacterSet',
                'ImageType',
                'SOPClassUID',
                'SOPInstanceUID',
                'StudyDate',
                'SeriesDate',
                'ContentDate',
                'StudyTime',
                'SeriesTime',
                'ContentTime',
                'AccessionNumber',
                'Modality',
                'Manufacturer',
                'InstitutionName',
                'InstitutionAddress',
                'ReferringPhysicianName',
                'StationName',
                'StudyDescription',
                'SeriesDescription',
                'PhysiciansOfRecord',
                'PerformingPhysicianName',
                'ManufacturerModelName',
                'PatientName',
                'PatientID',
                'PatientBirthDate',
                'PatientSex',
                'PatientAge',
                'PregnancyStatus',
                'DeviceSerialNumber',
                'SoftwareVersions',
                'DateOfLastCalibration',
                'TimeOfLastCalibration',
                'StudyInstanceUID',
                'SeriesInstanceUID',
                'StudyID',
                'SeriesNumber',
                'InstanceNumber',
                'SamplesPerPixel',
                'PhotometricInterpretation',
                'Rows',
                'Columns',
                'BitsAllocated',
                'BitsStored',
                'HighBit',
                'PixelRepresentation',
                'WindowCenter',
                'WindowWidth',
                'RequestingPhysician',
                'RequestedProcedureDescription',
                'PatientState',
                'RequestAttributesSequence']
columns_types = {'file': object,
         'SpecificCharacterSet': object,
         'ImageType': object,
         'SOPClassUID': object,
         'SOPInstanceUID': object,
         'StudyDate': object,
         'SeriesDate': object,
         'ContentDate': object,
         'StudyTime': object,
         'SeriesTime': object,
         'ContentTime': object,
         'AccessionNumber': object,
         'Modality': object,
         'Manufacturer': object,
         'InstitutionName': object,
         'InstitutionAddress': object,
         'ReferringPhysicianName': object,
         'StationName': object,
         'StudyDescription': object,
         'SeriesDescription': object,
         'PhysiciansOfRecord': object,
         'PerformingPhysicianName': object,
         'ManufacturerModelName': object,
         'PatientName': object,
         'PatientID': object,
         'PatientBirthDate': object,
         'PatientSex': object,
         'PatientAge': object,
         'PregnancyStatus': object,
         'DeviceSerialNumber': object,
         'SoftwareVersions': object,
         'DateOfLastCalibration': object,
         'TimeOfLastCalibration': object,
         'StudyInstanceUID': object,
         'SeriesInstanceUID': object,
         'StudyID': object,
         'SeriesNumber': object,
         'InstanceNumber': object,
         'SamplesPerPixel': object,
         'PhotometricInterpretation': object,
         'Rows': object,
         'Columns': object,
         'BitsAllocated': object,
         'BitsStored': object,
         'HighBit': object,
         'PixelRepresentation': object,
         'WindowCenter': object,
         'WindowWidth': object,
         'RequestingPhysician': object,
         'RequestedProcedureDescription': object,
         'PatientState': object,
         'RequestAttributesSequence': object}
patien_col = [
    'PatientName',
    'PatientID',
    'PatientBirthDate',
    'PatientSex'
    # , 'PatientAge'
]
study_col = [
    'StudyDate',
    # 'StudyTime',
    # 'Modality',
    'StudyDescription',
    'PatientID',
    'StudyInstanceUID',
    'StudyID'  # ,
    # 'RequestedProcedureDescription'
]
serie_col = [
    'file',
    'ImageType',
    'SeriesDate',
    'SeriesTime',
    'SeriesDescription',
    'StudyInstanceUID',
    'SeriesInstanceUID',
    'SeriesNumber',
    'InstanceNumber',
    'PhotometricInterpretation',
    'Rows',
    'Columns',
    'WindowCenter',
    'WindowWidth'
]

def group_data(delimiter):
    file_type= 'semicolon' if delimiter == ';' else 'comma'
    dir_name = f'F:/DataSet/2022-02-22/metadata/{file_type}/*.csv'
    all_files = glob.glob(dir_name)
    li = []
    for filename in all_files:
        try:
            df_file = pd.read_csv(filename, index_col=None, header=0, sep=delimiter, dtype=columns_types)
            # if df_file.columns == columns_name:
            #     print(f'{filename} : Ok')
            # else:
            #     print(f'{filename} : No')
            li.append(df_file)
        except Exception as e:
            print(f'{filename} : Error({e})')
    df = pd.concat(li, axis=0, ignore_index=True)
    df = df.drop_duplicates()
    df.to_csv(f'F:/DataSet/2022-02-22/metadata/output/{file_type}.csv', index=False, sep=';')
    return df
    # extract_data(df)


def extract_data(data_frame):
    # Select a set of distinct Columns
    patien_set = set(patien_col)
    study_set = set(study_col)

    patien_study_col = set(patien_col +
                           list(study_set - patien_set))
    print('patien_study_col:' + str(patien_study_col))

    serie_set = set(serie_col)

    patien_study_serie_col = list(patien_study_col) + list(serie_set - patien_study_col)
    print('patien_study_serie_col:' + str(patien_study_serie_col))

    # select patien + study + serie cols
    df_patien_study_serie = data_frame.loc[:, patien_study_serie_col]
    df_patien_study_serie = df_patien_study_serie.drop_duplicates()
    df_patien_study_serie.to_csv(path + r'\output\patien_study_serie_dataset.csv', index=False)
    # select serie cols
    df_serie = df_patien_study_serie.loc[:, serie_col]
    df_serie = df_serie.drop_duplicates()
    df_serie.to_csv(path + r'\output\serie_dataset.csv', index=False)
    # select patien + study cols
    df_patien_study = df_patien_study_serie.loc[:, patien_study_col]
    df_patien_study = df_patien_study.drop_duplicates()
    df_patien_study.to_csv(path + r'\output\patien_study_dataset.csv', index=False)
    # select study cols
    df_study = df_patien_study.loc[:, study_col]
    df_study = df_study.drop_duplicates()
    df_study.to_csv(path + r'\output\study_dataset.csv', index=False)
    # select patien cols
    df_patien = df_patien_study.loc[:, patien_col]
    df_patien = df_patien.drop_duplicates()
    df_patien.to_csv(path + r'\output\patien_dataset.csv', index=False)


def main():
    # group_data()
    try:
        li=[]
        li.append(group_data(','))
        li.append(group_data(';'))
        df = pd.concat(li, axis=0, ignore_index=True)
        df = df.drop_duplicates()
        df.to_csv('F:/DataSet/2022-02-22/metadata/output.csv', index=False, sep=';')
        extract_data(df)
    except Exception as e:
        print(e)
        # print(f'{file_name} : Error({e})')


def move_series():
	delimiter = ';'
	# filename = 'F:/DataSet/2022-09-24/output/grouped_dataset.csv'
	# df = pd.read_csv(filename, index_col=None, header=0, sep=delimiter, dtype=columns_types)
	# df = df[['StudyDate', 'PatientID','StudyInstanceUID','SeriesNumber', 'SOPInstanceUID']]
	# df = df.drop_duplicates()
	# df.to_csv('F:/DataSet/2022-09-24/output/serie_dataset2.csv', index=False, sep=delimiter)
	filename = 'F:/DataSet/2022-09-24/output/serie_dataset2.csv'
	df = pd.read_csv(filename, index_col=None, header=0, sep=delimiter,dtype={'StudyDate': object, 'PatientID': object,'StudyInstanceUID': object,'SeriesNumber': object, 'SOPInstanceUID': object})
	# i=0
	for index, row in df.iterrows():
		try:
			src = f'I:/2 OK/{row.StudyDate[:4]}/{row.StudyDate[4:6]}/{row.PatientID}/{row["StudyInstanceUID"]}/{row["SOPInstanceUID"]}'
			if os.path.exists(src):
				destination = f'I:/2-5 OK/{row.StudyDate[:4]}/{row.StudyDate[4:6]}/{row.PatientID}/{row["StudyInstanceUID"]}/{row.SeriesNumber}'
				Path(destination).mkdir(parents=True, exist_ok=True)
				destination = f'{destination}/{row["SOPInstanceUID"]}'
				print(src,destination)
				# i=i+1
				# if i == 10:
				# 	break
				shutil.move(src, destination)
		except Exception as e:
			print(e)

# new_destination = f'{anonym_path}/{dcm_dataset.StudyDate[:4]}/{dcm_dataset.StudyDate[4:6]}/{dcm_dataset.PatientID}/{dcm_dataset.StudyInstanceUID}/{dcm_dataset.SeriesNumber}'
# {dcm_dataset.SOPInstanceUID}'

def merge_meta():
	delimiter = ';'
	df1 = pd.read_csv('F:/DataSet/2022-09-24/output/grouped_dataset.csv', index_col='SOPInstanceUID', header=0, sep=delimiter, dtype=columns_types)
	df2 = pd.read_csv('I:/2 OK/Input/_metadata.csv', index_col='SOPInstanceUID', header=0, sep=delimiter, dtype=columns_types)
	merged = df1.merge(df2, indicator=True, how='outer')
	merged[merged['_merge'] == 'right_only']
	merged.to_csv('I:/2 OK/Input/grouped_dataset.csv', index=False, sep=delimiter)

if __name__ == "__main__":
    merge_meta()
    # main()
