/** i2b2_crc_design - design-time names etc. from i2b2 CRC cell

ref: i2b2 Design Document: Data Repository (CRC) Cell
     Document Version: 1.7.1 09/13/12
     i2b2 Software Version: 1.7.00
     https://www.i2b2.org/software/files/PDF/current/CRC_Design.pdf
*/


/** i2b2_status

  The PATIENT_IDE_STATUS gives the status of the patient number in the source
  system. For example, if it is Active, Inactive, Deleted, or Merged.
  -- 3.9. PATIENT_MAPPING Table

  The ENCOUNTER_IDE_STATUS gives the status of the patient number in the source
  system. For example, if it is Active, Inactive, Deleted, or Merged.
  -- 3.10. ENCOUNTER_MAPPING Table

*/

create or replace view no_value as select '@' as no_value, '@' not_recorded from dual;

create or replace view valtype_cd as
select no_value
     , 'N' as numeric
     , 'T' as text
     -- D is not documented in CRC_Design.pdf but
     -- it appears in demo data and
     -- it's important for date shifting
     , 'D' as date_val
     , 'B' as raw_text
     , 'NLP' as nlp
from no_value;

create or replace view tval_char as
select 'E' equals
     , 'NE' not_equal
     , 'L' less_than
     , 'LE' less_than_and_equal_to
     , 'G' greater_than
     , 'GE' greater_than_and_equal_to
from dual;

create or replace view i2b2_status
as
  select 'A' active
  , 'I' inactive
  , 'D' deleted
  , 'M' merged
  from dual ;


select length(greatest(active, inactive, deleted, merged)) complete
from i2b2_status
;
