# import asyncio
import codecs
import os
import shutil
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd
import pydicom

source_path = 'J:/1-5 input/'
archiv_path = 'J:/1-5 Archive/'
anonym_path = 'J:/1-5 Anonyme/'
archiv_ok = 'H:/tmp/tmp/ok/Archive/'
anonym_ok = 'H:/tmp/tmp/ok/Anonyme/'

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

df_global = pd.DataFrame(columns=columns_name)
log_file = None


def read_directory(dir_name):
    try:
        log_info(f'╔ processing Directory : {dir_name}')
        for root, directories, files in os.walk(dir_name, topdown=False):
            for file in files:
                read_file(os.path.join(root, file))
        log_info(f'╚ Ending Directory : {dir_name}')
    except Exception:
        write_df(False)


def check_directory(dir_name):
    try:
        log_info(f'╔ processing Check Directory : {dir_name}')
        Path(anonym_ok).mkdir(parents=True, exist_ok=True)
        Path(archiv_ok).mkdir(parents=True, exist_ok=True)
        for root, directories, files in os.walk(dir_name, topdown=False):
            for file in files:
                check_file(os.path.join(root, file))
        log_info(f'╚ Ending Checking Directory : {dir_name}')
    except Exception:
        write_df(False)


def check_file(file_name):
    if not os.path.exists(file_name):
        return
    try:
        file_anonym = file_name.replace(archiv_path, anonym_path)
        if not os.path.exists(file_anonym):
            log_info(f'{file_name} not treated')
            return
        ds = pydicom.dcmread(file_anonym)
        if ds.InstitutionName or ds.InstitutionAddress or ds.ReferringPhysicianName or ds.PhysiciansOfRecord or ds.PerformingPhysicianName or ds.PatientName or ds.RequestingPhysician or ds.PatientState:
            log_info(f'{file_anonym} is badly cleaned')
            return
        log_info(f'{file_name} is ok')
        new_destination = Path(file_name.replace(
            archiv_path, archiv_ok)).parent
        Path(new_destination).mkdir(parents=True, exist_ok=True)
        shutil.move(file_name, new_destination)
        new_destination = Path(file_anonym.replace(
            anonym_path, anonym_ok)).parent
        Path(new_destination).mkdir(parents=True, exist_ok=True)
        shutil.move(file_anonym, new_destination)
    except Exception:
        return


def read_file(file_name, read=False, hide=True, archive=True):
    if not os.path.exists(file_name):
        return
    log_info(f'╟ processing File : {file_name}')
    try:
        ds = pydicom.dcmread(file_name)
        if ds.PatientID is None or ds.StudyInstanceUID is None:
            return
        row = {
            'file': f'{source_path}/{ds.StudyDate[:4]}/{ds.StudyDate[4:6]}/{ds.PatientID}/{ds.StudyInstanceUID}/{os.path.basename(file_name)}'}

        if hide:
            row['file'] = hide_elements(ds, file_name)
        if archive:
            archive_destination = f'{archiv_path}/{ds.StudyDate[:4]}/{ds.StudyDate[4:6]}/{ds.PatientID}/{ds.StudyInstanceUID}'

            archive_file(file_name, archive_destination)
        if read:
            row = read_elements(ds)
            df = pd.DataFrame([row])
            global df_global
            df_global = pd.concat([df_global, df], ignore_index=True)
    except Exception:
        write_df(False)
        return


def read_elements(dcm_dataset):
    # get elements
    return {
        attr: dcm_dataset[attr].value if hasattr(dcm_dataset, attr) else None
        for attr in columns_name
    }


def hide_elements(dcm_dataset, file_name):
    try:
        # Hide sensitive information
        dcm_dataset.InstitutionName = dcm_dataset.InstitutionAddress = dcm_dataset.ReferringPhysicianName = dcm_dataset.PhysiciansOfRecord = dcm_dataset.PerformingPhysicianName = dcm_dataset.PatientName = dcm_dataset.RequestingPhysician = dcm_dataset.PatientState = ''
        # Create a Anonym DICOM file
        new_destination = f'{anonym_path}/{dcm_dataset.StudyDate[:4]}/{dcm_dataset.StudyDate[4:6]}/{dcm_dataset.PatientID}/{dcm_dataset.StudyInstanceUID}/{dcm_dataset.SeriesNumber}'
        # new_file_name = f'{new_destination}/{os.path.basename(file_name)}'
        new_file_name = f'{new_destination}/{dcm_dataset.SOPInstanceUID}'
        Path(new_destination).mkdir(parents=True, exist_ok=True)
        dcm_dataset.save_as(new_file_name, write_like_original=False)
    finally:
        write_df(False)
        return new_file_name


def archive_file(file, destination):
    try:
        Path(destination).mkdir(parents=True, exist_ok=True)
        new_file_name = f'{destination}/{os.path.basename(file)}'
        shutil.move(file, new_file_name)
        return new_file_name
    except Exception:
        write_df(False)
        return os.path.basename(file)

def orgnize_file(file, destination):
    try:
        Path(destination).mkdir(parents=True, exist_ok=True)
        new_file_name = f'{destination}/{os.path.basename(file)}'
        shutil.move(file, new_file_name)
        return new_file_name
    except Exception:
        write_df(False)
        return os.path.basename(file)


def log_info(info):
    global log_file
    log_file.write(info + '\n')
    print(info)


def write_df(recursive=True):
    global df_global
    log_info('Save dataframe')
    df_global.to_csv(f'{source_path}_metadata.csv', index=False, sep=';')
    if recursive:
        threading.Timer(60, write_df).start()


def main():
    global log_file
    global df_global
    global anonym_path
    # source_path = input("Please enter Dicom Directory:\n")
    # anonym_path = input("Please enter the Distination:\n")
    print(source_path)
    print(anonym_path)
    now = datetime.now()
    now_string = now.strftime('%d-%m-%Y %H-%M-%S')
    log_file = codecs.open(f'{source_path} {now_string}_log.txt', 'a', 'utf-8')

    # check_directory(archiv_path)
    # return
    #
    # write_df()
    read_directory(source_path)
    df_global.to_csv(
        f'{source_path} {now_string}_metadata.csv', index=False, sep=';')
    log_file.close()


if __name__ == "__main__":
    main()
