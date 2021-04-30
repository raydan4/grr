# Lint as: python3
"""Plugins that produce results in JSON."""
import io
import os
import zipfile

from grr_response_core.lib import utils
from grr_response_core.lib.rdfvalues import structs as rdf_structs
from grr_response_core.lib.util import collection
from grr_response_core.lib.util.compat import json
from grr_response_server import instant_output_plugin

def _SerializetoJson(value):
  if isinstance(value, rdf_structs.RDFProtoStruct):
    return json.Dump(value.ToPrimitiveDict(stringify_leaf_fields=True))
  else:
    return json.Dump(str(value))

class JsonInstantOutputPluginWithExportConversion(
    instant_output_plugin.InstantOutputPluginWithExportConversion):
  """Instant output plugin that flattens results into JSON."""

  plugin_name = "flattened-json-zip"
  friendly_name = "Flattened JSON (zipped)"
  description = "Output ZIP archive with JSON files (flattened)."
  output_file_extension = ".zip"

  ROW_BATCH = 100

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.archive_generator = None
    self.export_counts = {}

  @property
  def path_prefix(self):
    prefix, _ = os.path.splitext(self.output_file_name)
    return prefix

  def Start(self):
    self.archive_generator = utils.StreamingZipGenerator(
      compression=zipfile.ZIP_DEFLATED)
    self.export_counts = {}
    return []

  def ProcessSingleTypeExportedValues(self, original_value_type,
                                      exported_values):
    first_value = next(exported_values, None)
    if not first_value:
      return

    yield self.archive_generator.WriteFileHeader(
        "%s/%s/from_%s.json" % (self.path_prefix,
                               first_value.__class__.__name__,
                               original_value_type.__name__))

    yield self.archive_generator.WriteFileChunk("[".encode("utf-8"))
    serialized_value_bytes = _SerializetoJson(first_value).encode("utf-8")
    yield self.archive_generator.WriteFileChunk(serialized_value_bytes)
    counter = 1
    for batch in collection.Batch(exported_values, self.ROW_BATCH):
      counter += len(batch)

      buf = io.StringIO()
      for value in batch:
        buf.write(",")
        buf.write(_SerializetoJson(value))
      
      contents = buf.getvalue()
      yield self.archive_generator.WriteFileChunk(contents.encode("utf-8"))
    yield self.archive_generator.WriteFileChunk("]".encode("utf-8"))
    yield self.archive_generator.WriteFileFooter()

    counts_for_original_type = self.export_counts.setdefault(
        original_value_type.__name__, dict())
    counts_for_original_type[first_value.__class__.__name__] = counter

  def Finish(self):
    manifest = {"export_stats": self.export_counts}
    manifest_bytes = json.Dump(manifest).encode("utf-8")

    yield self.archive_generator.WriteFileHeader(self.path_prefix + "/MANIFEST")
    yield self.archive_generator.WriteFileChunk(manifest_bytes)
    yield self.archive_generator.WriteFileFooter()
    yield self.archive_generator.Close()
