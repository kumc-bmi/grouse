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
