import json
import boto3
import pathlib
from urllib.parse import urlparse
import shutil
import os

s3 = boto3.client('s3')

output = {}

with open('final_buffer.json') as fp:
    parsed = json.load(fp)
    for sample in parsed.values():
        for mapped, sample_2 in sample.items():
            for sample_name, sample_3 in sample_2.items():

                # Work out the ideal filename
                simple_sample_name = pathlib.Path(sample_name).name

                # Create a directory for this sample
                sample_id = sample_name.split('/')[2]
                sample_dir = pathlib.Path(sample_id)
                sample_dir.mkdir(exist_ok=True)

                for file_type, url in sample_3.items():
                    # Parse the current location of the file
                    parsed_url = urlparse(url)
                    file_path = pathlib.Path(parsed_url.path[1:])

                    # Put it in the sample ID directory with a better name
                    src_path = 'reports' / file_path
                    dest_path = (sample_dir / simple_sample_name).with_suffix(file_path.suffix)

                    src_path.rename(dest_path)
