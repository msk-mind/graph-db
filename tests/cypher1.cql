return "loading dicom tables";

// loads CT dicoms and form relationships between dicom images and patient
CALL apoc.load.json("file:///CT_table/part.json")
YIELD value
MERGE (d:DCM {path: value.path})
SET
  d.length = value.length,
  d.AccessionNumber = value.metadata.AccessionNumber,
  d.dicom_record_uuid = value.dicom_record_uuid
WITH d, value
MERGE (p:Patient {PatientID: value.metadata.PatientID})
SET
  p.PatientBirthDate = value.metadata.PatientBirthDate
MERGE (d)-[:BELONGS_TO]->(p);

return "loading mha tables";

// load MHA files and form relationships between mha files and dicom images
CALL apoc.load.json("file:///MHA_table/part.json")
YIELD value
MERGE (m:MHA {AccessionNumber: value.AccessionNumber})
SET
  m.path = value.path
WITH m, value
MATCH (d:DCM {AccessionNumber: m.AccessionNumber})
MERGE (m)-[:ANNOTATES]->(d);

