/** vdim_add_cols -- add columns to visit dimension

In particular, ethnicity_cd as per i2p-transform.

As the database account we use under luigi might not have alter table permission,
we're likely to run this manually.
*/

alter table "&&I2B2STAR".visit_dimension add(providerid varchar2(50 byte)) ;

select count(providerid) + 1 complete from "&&I2B2STAR".visit_dimension
where rownum = 1
;
