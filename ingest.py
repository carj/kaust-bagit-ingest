"""
Ingest script for Bagit Packages at KAUST

"""

import zipfile

from pyPreservica import *

logger = logging.getLogger(__name__)

LOG_FILENAME = 'ingest.log'
logging.basicConfig(level=logging.INFO, filename=LOG_FILENAME, filemode="a")

consoleHandler = logging.StreamHandler()
logging.getLogger().addHandler(consoleHandler)

metadata_text = """<DeliverableUnit xmlns="http://www.tessella.com/XIP/v4">
<Title>CATREF</Title>
<ScopeAndContent>SCOPE</ScopeAndContent>
</DeliverableUnit>"""


def remove_comment(dirname, filename):
    file = os.path.join(dirname, filename)
    with open(file, encoding="utf-8", mode="rt") as fd:
        lines = fd.readlines()
        lines[0] = lines[0].replace("#", "", 1)
    with open(file, encoding="utf-8", mode="wt") as fd:
        fd.writelines(lines)


def add_comment(dirname, filename):
    file = os.path.join(dirname, filename)
    with open(file, encoding="utf-8", mode="rt") as fd:
        lines = fd.readlines()
    lines[0] = f"#{lines[0]}"
    with open(file, encoding="utf-8", mode="wt") as fd:
        fd.writelines(lines)


def fetch_title(dirname, filename, default):
    t = default
    d = default
    file = os.path.join(dirname, filename)
    with open(file, encoding="utf-8", mode="rt") as fd:
        lines = fd.readlines()
    for line in lines:
        if line.startswith("DC_Title:"):
            t = line.replace("DC_Title:", "").strip()
        if line.startswith("DC_description:"):
            d = line.replace("DC_description:", "").strip()
    return t, d


if __name__ == '__main__':
    entity = EntityAPI()
    logger.info(entity)
    upload = UploadAPI()

    config = configparser.ConfigParser()
    config.read('credentials.properties', encoding='utf-8')
    parent_folder_id = config['credentials']['parent.folder']

    security_tag = config['credentials']['security.tag']

    # check parent folder exists
    if parent_folder_id:
        parent = entity.folder(parent_folder_id)
        logger.info(f"Packages will be ingested into {parent.title}")
        parent = parent.reference
    else:
        parent = None

    data_folder = config['credentials']['data.folder']
    logger.info(f"Packages will be created from folders in {data_folder}")

    bucket = config['credentials']['bucket']

    max_submissions = int(config['credentials']['max.submissions'])

    num_submissions = 0

    # list archive folders in data directory
    level_1_folders = [f.name for f in os.scandir(data_folder) if f.is_dir()]
    for folder1 in level_1_folders:
        logger.info(f"Found {folder1} Folder")
        level_1_folder = os.path.join(os.path.join(data_folder, folder1))
        entities = entity.identifier("code", folder1)
        if len(entities) == 0:
            folder = entity.create_folder(folder1, folder1, security_tag, parent)
            entity.add_identifier(folder, "code", folder1)
            level_1_folder_reference = folder.reference
        else:
            folder = entities.pop()
            level_1_folder_reference = folder.reference

        level_2_folders = [f.name for f in os.scandir(level_1_folder) if f.is_dir()]
        for folder2 in level_2_folders:
            logger.info(f"Found {folder2} Folder")
            level_2_folder = os.path.join(os.path.join(level_1_folder, folder2))
            entities = entity.identifier("code", folder2)
            if len(entities) == 0:
                folder = entity.create_folder(folder2, folder2, security_tag, level_1_folder_reference)
                entity.add_identifier(folder, "code", folder2)
                level_2_folder_reference = folder.reference
            else:
                folder = entities.pop()
                level_2_folder_reference = folder.reference

            bagit_folders = [f.name for f in os.scandir(level_2_folder) if f.is_dir()]
            os.chdir(level_2_folder)

            # get a group of folders we can ingest together
            for bagit_folder in bagit_folders:

                if num_submissions >= max_submissions:
                    break

                title = bagit_folder
                description = bagit_folder
                result = entity.identifier("code", bagit_folder)
                if len(result) > 0:
                    logger.info(f"skipping folder {bagit_folder}")
                if len(result) == 0:
                    # if you want the title and description of the package to be the identifier
                    # and not the text from the dublin core metadata
                    # comment out this line
                    title, description = fetch_title(bagit_folder, "bag-info.txt", bagit_folder)

                    metadata_text = metadata_text.replace("CATREF", title)
                    metadata_text = metadata_text.replace("SCOPE", description)
                    metadata_file = f"{bagit_folder}.metadata"
                    with open(os.path.join(bagit_folder, metadata_file), encoding="utf-8", mode="wt") as fd:
                        fd.write(metadata_text)
                    zipfile_name = f"{bagit_folder}.zip"
                    zf = zipfile.ZipFile(zipfile_name, "w")
                    for dirname, subdirs, files in os.walk(bagit_folder):
                        zf.write(dirname)
                        for filename in files:
                            if filename == "bagit.txt":
                                add_comment(dirname, filename)
                                zf.write(os.path.join(dirname, filename))
                                remove_comment(dirname, filename)
                            else:
                                zf.write(os.path.join(dirname, filename))
                    zf.close()
                    os.remove(os.path.join(bagit_folder, metadata_file))

                    logger.info(f"Uploading {bagit_folder} to S3 bucket {bucket}")
                    print("\n")
                    upload.upload_zip_package_to_S3(path_to_zip_package=zipfile_name, folder=level_2_folder_reference,
                                                    bucket_name=bucket,
                                                    callback=UploadProgressConsoleCallback(zipfile_name),
                                                    delete_after_upload=True)

                    num_submissions = num_submissions + 1

                    logger.info(f"")
