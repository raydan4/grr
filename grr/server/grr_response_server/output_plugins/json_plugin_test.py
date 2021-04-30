# Lint as: python3
# -*- encoding: utf-8 -*-
"""Tests for JSON instant output plugin."""

import os
import zipfile

from absl import app
import json

from grr_response_core.lib.rdfvalues import client as rdf_client
from grr_response_core.lib.rdfvalues import client_fs as rdf_client_fs
from grr_response_core.lib.rdfvalues import paths as rdf_paths
from grr_response_server.output_plugins import test_plugins
from grr_response_server.output_plugins import json_plugin
from grr.test_lib import test_lib

class JsonInstantOutputPluginTest(test_plugins.InstantOutputPluginTestBase):
  """Tests the JSON instant output plugin."""

  plugin_cls = json_plugin.JsonInstantOutputPluginWithExportConversion

  def ProcessValuesToZip(self, values_by_cls):
    fd_path = self.ProcessValues(values_by_cls)
    file_basename, _ = os.path.splitext(os.path.basename(fd_path))
    return zipfile.ZipFile(fd_path), file_basename

  def testJsonPluginWithValuesOfSametypes(self):
    responses = []
    for i in range(10):
      responses.append(
          rdf_client_fs.StatEntry(
              pathspec=rdf_paths.PathSpec(
                  path="/foo/bar/%d" % i, pathtype="OS"),
              st_mode=33184,
              st_ino=1063090,
              st_dev=64512,
              st_nlink=1 + i,
              st_uid=139592,
              st_gid=5000,
              st_size=0,
              st_atime=1336469177,
              st_mtime=1336129892,
              st_ctime=1336129892))
    
    zip_fd, prefix = self.ProcessValuesToZip(
        {rdf_client_fs.StatEntry: responses})
    self.assertEqual(
        set(zip_fd.namelist()), {
            "%s/MANIFEST" % prefix,
            "%s/ExportedFile/from_StatEntry.json" % prefix
        })

    parsed_manifest = json.loads(zip_fd.read("%s/MANIFEST" % prefix))
    self.assertEqual(parsed_manifest,
                     {"export_stats": {
                        "StatEntry": {
                            "ExportedFile": 10
                        }}})

    parsed_output = json.loads(
        zip_fd.read("%s/ExportedFile/from_StatEntry.json" % prefix))
    self.assertLen(parsed_output, 10)
    for i in range(10):
      self.assertEqual(parsed_output[i]["metadata"]["client_urn"],
                       "aff4:/%s" % self.client_id)
      self.assertEqual(parsed_output[i]["metadata"]["source_urn"],
                       str(self.results_urn))
      self.assertEqual(parsed_output[i]["urn"],
                       "aff4:/%s/fs/os/foo/bar/%d" % (self.client_id, i))
      self.assertEqual(parsed_output[i]["st_mode"], "-rw-r-----")
      self.assertEqual(parsed_output[i]["st_ino"], "1063090")
      self.assertEqual(parsed_output[i]["st_dev"], "64512")
      self.assertEqual(parsed_output[i]["st_nlink"], str(1 + i))
      self.assertEqual(parsed_output[i]["st_uid"], "139592")
      self.assertEqual(parsed_output[i]["st_gid"], "5000")
      self.assertEqual(parsed_output[i]["st_size"], "0")
      self.assertEqual(parsed_output[i]["st_atime"], "2012-05-08 09:26:17")
      self.assertEqual(parsed_output[i]["st_mtime"], "2012-05-04 11:11:32")
      self.assertEqual(parsed_output[i]["st_ctime"], "2012-05-04 11:11:32")
      self.assertEqual(parsed_output[i]["st_blksize"], "0")
      self.assertEqual(parsed_output[i]["st_rdev"], "0")
      self.assertEqual(parsed_output[i]["symlink"], "")

  def testJsonPluginWithValuesOfMultipleTypes(self):
    zip_fd, prefix = self.ProcessValuesToZip({
        rdf_client_fs.StatEntry: [
            rdf_client_fs.StatEntry(
                pathspec=rdf_paths.PathSpec(path="/foo/bar", pathtype="OS"))
        ],
        rdf_client.Process: [rdf_client.Process(pid=42)]
    })
    self.assertEqual(
        set(zip_fd.namelist()), {
            "%s/MANIFEST" % prefix,
            "%s/ExportedFile/from_StatEntry.json" % prefix,
            "%s/ExportedProcess/from_Process.json" % prefix
        })
    
    parsed_manifest = json.loads(zip_fd.read("%s/MANIFEST" % prefix))
    self.assertEqual(
        parsed_manifest, {
            "export_stats": {
                "StatEntry": {
                    "ExportedFile": 1
                },
                "Process": {
                    "ExportedProcess": 1
                }
            }
        })

    parsed_output = json.loads(
        zip_fd.read("%s/ExportedFile/from_StatEntry.json" % prefix))
    self.assertLen(parsed_output, 1)

    self.assertEqual(parsed_output[0]["metadata"]["client_urn"],
                     "aff4:/%s" % self.client_id)
    self.assertEqual(parsed_output[0]["metadata"]["source_urn"],
                     str(self.results_urn))
    self.assertEqual(parsed_output[0]["urn"],
                     "aff4:/%s/fs/os/foo/bar" % self.client_id)

    parsed_output = json.loads(
        zip_fd.read("%s/ExportedProcess/from_Process.json" % prefix))
    self.assertLen(parsed_output, 1)
    self.assertEqual(parsed_output[0]["pid"], "42")

  def testJsonPluginWritesUnicodeValuesCorrectly(self):
    zip_fd, prefix = self.ProcessValuesToZip({
        rdf_client_fs.StatEntry: [
            rdf_client_fs.StatEntry(
                pathspec=rdf_paths.PathSpec(path="/中国新闻网新闻中", pathtype="OS"))
        ]
    })
    self.assertEqual(
        set(zip_fd.namelist()), {
            "%s/MANIFEST" % prefix,
            "%s/ExportedFile/from_StatEntry.json" % prefix
        })
    
    parsed_output = json.loads(
        zip_fd.read("%s/ExportedFile/from_StatEntry.json" % prefix))
    
    self.assertLen(parsed_output, 1)
    self.assertEqual(parsed_output[0]["urn"],
                     "aff4:/%s/fs/os/中国新闻网新闻中" % self.client_id)
    
  def testJsonPluginWritesMoreThanOnePatchOfRowsCorrectly(self):
    num_rows = self.__class__.plugin_cls.ROW_BATCH * 2 + 1

    responses = []
    for i in range(num_rows):
      responses.append(
          rdf_client_fs.StatEntry(
              pathspec=rdf_paths.PathSpec(
                  path="/foo/bar/%d" %i, pathtype="OS")))
    
    zip_fd, prefix = self.ProcessValuesToZip(
        {rdf_client_fs.StatEntry: responses})
    parsed_output = json.loads(
      zip_fd.read("%s/ExportedFile/from_StatEntry.json" % prefix))
    self.assertLen(parsed_output, num_rows)
    for i in range(num_rows):
      self.assertEqual(parsed_output[i]["urn"],
                       "aff4:/%s/fs/os/foo/bar/%d" % (self.client_id, i))


def main(argv):
  test_lib.main(argv)


if __name__ == "__main__":
  app.run(main)
