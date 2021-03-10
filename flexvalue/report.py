#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

   Copyright 2021 Recurve Analytics, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""
import os
import nbformat as nbf
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import HTMLExporter


class Notebook(object):
    def __init__(self):
        self.nb = nbf.v4.new_notebook()
        self.nb.cells = []

    def add_code_cell(self, content):
        cell = nbf.v4.new_code_cell(content)
        cell.metadata = {"nbconvert": {"show_code": False}}
        self.nb.cells = self.nb.cells + [cell]

    def add_markdown_cell(self, content):
        cell = nbf.v4.new_markdown_cell(content)
        cell.metadata = {"nbconvert": {"show_code": False}}
        self.nb.cells = self.nb.cells + [cell]

    def write_local(self, path):
        nbf.write(self.nb, path)

    def execute(self):
        ep = ExecutePreprocessor(timeout=1200, kernel_name="python3")
        ep.preprocess(self.nb, {"metadata": {"path": "."}})

    def to_html(self, path):
        html_exporter = HTMLExporter()
        html_exporter.template_data_paths = html_exporter.template_data_paths + [
            os.path.dirname(__file__)
        ]
        html_exporter.template_file = "index.html.j2"
        html_exporter.exclude_input_prompt = True
        html_exporter.exclude_output_prompt = True
        html_exporter.exclude_input = True
        html_exporter.allow_errors = False
        html_exporter.log_level = "CRITICAL"
        (body, resources) = html_exporter.from_notebook_node(self.nb)
        # images will be embedded as base64, so we shouldn't need the resources
        with open(path, "w") as f:
            f.write(body)
