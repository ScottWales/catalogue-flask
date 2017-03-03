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
from flask_sqlalchemy import SQLAlchemy
import os
from datetime import datetime

db = SQLAlchemy()

class Path(db.Model):
    """
    A path in the filesystem
    """
    id = db.Column(db.Integer, primary_key=True)
    path = db.Column(db.Text, unique=True, index=True)
    basename = db.Column(db.Text, index=True)
    extension = db.Column(db.Text, index=True)
    uid = db.Column(db.Integer)
    gid = db.Column(db.Integer, index=True)
    size_bytes = db.Column(db.Integer)

    modified = db.Column(db.Integer)
    last_seen = db.Column(db.DateTime, index=True)

    content_id = db.Column(db.Integer, db.ForeignKey('content.id'))
    content = db.relationship("Content")

    def add_from_filename(filename, session):
        """
        Given a filename, add it to the database
        """
        if not os.path.isfile(filename):
            raise IOError("Not a file: %s"%filename)

        abspath = os.path.abspath(filename)
        path = Path.query.filter_by(path = abspath).one_or_none()

        stat = os.stat(filename)

        if path is not None:
            path.last_seen = datetime.now()
            if path.modified < stat.st_mtime:
                path.update(stat)
                session.add(path)
            return path

        path = Path()
        path.path = abspath
        path.update(stat)
        path.last_seen = datetime.now()

        session.add(path)

        return path

    def update(self, stat):
        """
        Updates the file with new info
        """
        self.basename = os.path.basename(self.path)
        self.extension = os.path.splitext(self.path)[1]

        self.uid = stat.st_uid
        self.gid = stat.st_gid
        self.size_bytes = stat.st_size

        self.modified = stat.st_mtime

        # Wipe the content link
        self.content = None

class Content(db.Model):
    """
    The contents of a file, identified via checksum
    May be at multiple paths on the filesystem

    sha256 is used for identification, md5 also provided for legacy
    """
    id = db.Column(db.Integer, primary_key=True)
    sha256 = db.Column(db.Text, unique=True, index=True)
    md5 = db.Column(db.Text, index=True)
    mime = db.Column(db.Text, index=True)

    paths = db.relationship("Path")

    

