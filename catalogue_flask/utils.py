#!/usr/bin/env python
# Copyright 2017 ARC Centre of Excellence for Climate Systems Science
# author: Scott Wales <scott.wales@unimelb.edu.au>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function
from .model import *
import os
import hashlib
from magic import Magic

def scan_filesystem(rootdir, db):
    """
    Add files under rootdir to the database
    """
    session = db.session
    for root, dirs, files in os.walk(rootdir):
        for name in files:
            p = Path.add_from_filename(os.path.join(root,name), session)
    session.commit()

def checksums(path):
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
            sha256.update(chunk)
    return (sha256.hexdigest(), md5.hexdigest())

def update_content(db):
    """
    Update any missing content
    """
    session = db.session
    magic = Magic(mime=True)

    for path in Path.query.filter_by(content = None):
        try:
            sha256, md5 = checksums(path.path)
            content = Content.query.filter_by(sha256 = sha256).one_or_none()
            
            if content is None:
                content = Content(sha256 = sha256, md5 = md5, 
                        mime=magic.from_file(path.path))
                session.add(content)
                session.commit()

            path.content = content
            session.add(path)
        except FileNotFoundError:
            session.delete(path)

    session.commit()

