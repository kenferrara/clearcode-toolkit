import os
import re

from datetime import datetime
from typing import List
from urllib.parse import urlparse


def construct_file_tree(package_hash: str, package_data: dict) -> List[dict]:
    """ """
    file_tree = list()

    # Add the root file for the package itself
    file_tree.append({
        'firmware_hash': package_hash,
        'file_hash': package_hash,
        'file_full_path': '/',
        'file_name': '',
        # FIXME: We don't know the size of the compressed package. This would
        # be nice to have, but we're not sure where that comes from.
        'file_size': 0,
        # FIXME
        # The package_data we're passing in right now does not include
        # the mime-type. Hand-wave and say that it's text/plain regardless
        # of what we're looking at.
        # The mime type information can be found in the scancode document
        # for a given package.
        'file_type_mime': 'text/plain',
        'file_type_full': 'ClearlyDefined Unknown'
    })

    # Add entries for each file in the package
    for file_entry in package_data['files']:
        # In the ClearlyDefined dataset, all paths are relative
        # with the root appearing as cwd. Instead, we should add a /
        # to the start of each of the dirents so that it conforms to
        # finitestate standards where the top level of the extracted
        # package is root.
        file_path = f"/{file_entry['path']}"
        file_tree.append(
            {
                'firmware_hash': package_hash,
                'file_hash': file_entry['hashes']['sha256'],
                'file_full_path': file_path,
                'file_name': os.path.basename(file_path),
                # FIXME: We don't know the size of the files. It would be nice to know
                # that later. That comes from the scancode document for a package.
                'file_size': 0,
                # FIXME
                # The package_data we're passing in right now does not include
                # the mime-type. Hand-wave and say that it's text/plain regardless
                # of what we're looking at.
                # The mime type information can be found in the scancode document
                # for a given package.
                'file_type_mime': 'text/plain',
                'file_type_full': 'ClearlyDefined Unknown',
            }
        )

    return file_tree


def construct_ground_truth_upload_metadata(package_hash: str, package_data: dict) -> dict:

    package_url = package_data['registryData']['manifest']['dist']['tarball']
    package_filename = os.path.basename(package_url)
    package_url_parsed = urlparse(package_url)
    supplier_base_url = f'{package_url_parsed.scheme}://{package_url_parsed.netloc}/'

    return {
        'additional_metadata': {
            'upload_date': package_data['registryData']['releaseDate'],
            'license': package_data['registryData']['license'],
            'project_name': package_data['registryData']['name'],
            'home_page': package_data['registryData']['homepage'],
            'file_count': package_data['registryData']['manifest']['dist']['fileCount'],
            'description': package_data['registryData']['manifest']['description'],
            'package_version': package_data['registryData']['manifest']['version']
        },
        'download_date': str(datetime.utcnow()),
        'download_location': package_url,
        'download_type': "clearlydefined-harvest",
        'download_method': "clearlydefined-scraper-npm",
        'file_name': package_filename,
        'supplier_base_url': supplier_base_url,
        'supplier_name': "npm",
        'file_hash': package_hash
    }


def get_package_json(file_tree: dict, data: dict):
    package_json_sha256_digest = str()
    for file in file_tree:
        # Look for a package.json either exactly at the root of the package
        # or a single directory down.
        # This regex captures paths of the form
        # /package.json
        # /arbitrary-string/package.json
        # and nothing else.
        if re.match(r'^(\/[^\/]*)?\/package.json', file['file_full_path']):
            package_json_sha256_digest = file['file_hash']

    package_json = data["package.json"]  # _string: str = json.dumps(data["package.json"], indent=True)

    return {
        'package_json_hash': package_json_sha256_digest,
        'package_json': package_json
    }