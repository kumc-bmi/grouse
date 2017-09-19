/** pdim_add_cols -- add columns to patient dimension

In particular, ethnicity_cd as per i2p-transform.

As the database account we use under luigi might not have alter table permission,
we're likely to run this manually.
*/

alter table "&&I2B2STAR".patient_dimension add(ethnicity_cd varchar2(50 byte))
;

select count(ethnicity_cd) + 1 complete from "&&I2B2STAR".patient_dimension
where rownum = 1
;
