/** vdim_add_cols -- add providerid etc. columns to visit dimension

ref i2p-transform / schilhs-ontology
in particular: Documentation/INSTALL.md "Version 2.1.2 - 9/2016"
from https://github.com/SCILHS/scilhs-ontology b7fbac0 Mar 22, 2017

As the database account we use under luigi might not have alter table permission,
we're likely to run this manually.
*/

alter table "&&I2B2STAR".visit_dimension add(
      drg varchar(50) NULL,
      discharge_status varchar(25) NULL,
      discharge_disposition varchar(25) NULL,
      location_zip varchar(25) NULL,
      admitting_source varchar(25) NULL,
      facilityid varchar(25) NULL,
      providerid varchar(25) NULL
);

select count(providerid) + 1 complete from "&&I2B2STAR".visit_dimension
where rownum = 1
;
