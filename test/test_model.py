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
from fixtures import *
from catalogue_flask.model import *
from datetime import datetime

def test_path(session):
    assert Path.query.count() == 0

    p = Path.add_from_filename('test/test_model.py', session)
    session.commit()
    assert p.extension == '.py'
    assert Path.query.count() == 1

    # Adding the same file is a no-op
    Path.add_from_filename('test/test_model.py', session)
    session.commit()
    assert Path.query.count() == 1
