return "loading mha tables";

// load MHA files and form relationships between mha files and dicom images
CALL apoc.load.json("file:///mha_sample.json")
YIELD value
MERGE (m:MHA {AccessionNumber: value.AccessionNumber})
SET
  m.path = value.path
WITH m, value
MATCH (d:DCM {AccessionNumber: m.AccessionNumber})
MERGE (m)-[:ANNOTATES]->(d);