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

    :var sha256: sha256 checksum
    :var md5: md5 checksum
    """
    id = db.Column(db.Integer, primary_key=True)
    sha256 = db.Column(db.String, unique=True, index=True, nullable=False)
    md5 = db.Column(db.String, index=True, nullable=False)
    type = db.Column(db.String)
    last_scanned = db.Column(db.DateTime)

    paths = db.relationship("Path")

    __mapper_args__ = {
            'polymorphic_identity':'content',
            'polymorphic_on':type
            }

netcdf_variable_association = db.Table('netcdf_variable_association', db.Model.metadata,
        db.Column('netcdf_id', db.Integer, db.ForeignKey('netcdf_content.id')),
        db.Column('concretevar_id', db.Integer, db.ForeignKey('concrete_variable.id'))
        )

class NetcdfContent(Content):
    """
    Content of a NetCDF file

    :var sha256: sha256 checksum
    :var md5: md5 checksum
    :var variables: list of :class:`~catalogue_flask.model.ConcreteVariable`
    """
    id = db.Column(db.Integer, db.ForeignKey('content.id'), primary_key=True)
    variables = db.relationship("ConcreteVariable",
            secondary=netcdf_variable_association)

    __mapper_args__ = {
            'polymorphic_identity':'netcdfcontent',
            }

class ConcreteVariable(db.Model):
    """
    An abstract variable, may have many aliased names

    :var cf_name: NetCDF-CF name
    :var aliases: List of :class:`~catalogue_flask.model.Variable`
    """
    id = db.Column(db.Integer, primary_key=True)
    cf_name = db.Column(db.String)
    aliases = db.relationship("Variable")

class Variable(db.Model):
    """
    An alternate name for a variable

    :var name: The name of this alias
    :var concrete: The concrete variable this aliases
    """
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    concretevariable_id = db.Column(db.Integer, db.ForeignKey('concrete_variable.id'), index=True)
    concrete = db.relationship("ConcreteVariable")

