/** cms_enc_txform - view CMS encounters from an i2b2 lens
*/

select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

/** cms_visit_dimension -- view CMS part B carrier claims  as i2b2 patient_dimension

Note this view has bene_id where visit_dimension has patient_num.
Joining with the i2b2 patient_mapping happens in a later insert.

ref:
  - Carrier RIF
    https://www.resdac.org/cms-data/files/carrier-rif/data-documentation

ISSUE: one encounter per line? or per claim? use distinct on provider and such?
*/

create or replace view medpar_claim_type as
select * from xmltable('table/item' passing xmltype(
'<table><item code="10" value="HHA claim" />
    <item code="20" value="Non swing bed SNF claim" />
    <item code="30" value="Swing bed SNF claim" />
    <item code="40" value="Outpatient claim" />
    <item code="50" value="Hospice claim" />
    <item code="60" value="Inpatient claim" />
    <item code="61" value="Inpatient ''Full-Encounter'' claim" />
    <item code="62" value="Medicare Advantage IME/GME claims" />
    <item code="63" value="Medicare Advantage (no-pay) claims" />
    <item code="64" value="Medicare Advantage (paid as FFS) claim" />
    <item code="71" value="RIC O local carrier non-DMEPOS claim" />
    <item code="72" value="RIC O local carrier DMEPOS claim" />
    <item code="81" value="RIC M DMERC non-DMEPOS claim" />
    <item code="82" value="RIC M DMERC DMEPOS claim" />
    </table>')
    columns
    code varchar(2) path '@code',
    label varchar(120) path '@value'
)
;
comment on table medpar_claim_type
is 'ref <https://www.resdac.org/cms-data/variables/medpar-nch-claim-type-code>'
;

-- ISSUE: move this curated mapping to a .csv file?
create or replace view medpar_claim_enc_type as
select * from xmltable('table/item' passing xmltype(
'<table>
    <item code="10" enc_type="OA" value="HHA claim" />
    <item code="20" enc_type="IS" value="Non swing bed SNF claim" />
    <item code="30" enc_type="IS" value="Swing bed SNF claim" />
    <item code="40" enc_type="AV" value="Outpatient claim" />
    <item code="50" enc_type="IS" value="Hospice claim" />
    <item code="60" enc_type="IP" value="Inpatient claim" />
    <item code="61" enc_type="IP" value="Inpatient ''Full-Encounter'' claim" />
    <!-- TODO: curate these other types -->
    <item code="62" enc_type="OT" value="Medicare Advantage IME/GME claims" />
    <item code="63" enc_type="OT" value="Medicare Advantage (no-pay) claims" />
    <item code="64" enc_type="OT" value="Medicare Advantage (paid as FFS) claim" />
    <item code="71" enc_type="OT" value="RIC O local carrier non-DMEPOS claim" />
    <item code="72" enc_type="OT" value="RIC O local carrier DMEPOS claim" />
    <item code="81" enc_type="OT" value="RIC M DMERC non-DMEPOS claim" />
    <item code="82" enc_type="OT" value="RIC M DMERC DMEPOS claim" />
    </table>')
    columns
    code varchar(2) path '@code',
    enc_type varchar(2) path '@enc_type',
    label varchar(120) path '@value'
)
;


create or replace view cms_visit_dimension_bc
as
  select
    bc.bene_id
  , bc.clm_id
  , bl.line_num
  , fmt_clm_line(bc.clm_id, bl.line_num) encounter_ide
  , key_sources.clm_line_cd encounter_ide_source
  , fmt_patient_day(bc.bene_id, bc.clm_from_dt) patient_day
  , i2b2_status.active active_status_cd
  , bc.clm_from_dt start_date
  , bl.clm_thru_dt end_date
    -- ack: make_mapping_encounter_carr from etl_carr.sas from Duke
    -- ISSUE: a CSV mapping file would be nicer.
  , case
      when bl.line_place_of_srvc_cd in('05', '07', '11', '20', '24', '49', '50', '53', '71', '72') then 'AV'
      when bl.line_place_of_srvc_cd in('01', '03', '12', '15', '17', '57', '60', '62', '65', '81') then 'OA'
      when bl.line_place_of_srvc_cd in('04', '06', '08', '09', '13', '14', '16', '21', '22', '23', '25', '26', '31',
        '32', '33', '34', '35', '41', '42', '51', '52', '54', '55', '56', '61', '99') then 'OT'
        /*WHEN ('00', '02','10', '36', '40','84') ENC_TYPE  = 'UN';  */
      when bl.line_place_of_srvc_cd is null then 'NI'
      else 'UN'
    end inout_cd
, bl.line_place_of_srvc_cd location_cd
  -- TODO? location_path
, 1 +(bl.clm_thru_dt - bc.clm_from_dt) length_of_stay
  -- visit_blob
, nch_wkly_proc_dt update_date
, &&cms_source_cd sourcesystem_cd
from "&&CMS_RIF".bcarrier_claims bc
join "&&CMS_RIF".bcarrier_line bl on bl.clm_id = bc.clm_id
cross join i2b2_status
cross join cms_key_sources key_sources;

create or replace view cms_visit_dimension_medpar
as
  select
    ma.bene_id
  , ma.medpar_id
  , ma.medpar_id encounter_ide
  , key_sources.medpar_cd encounter_ide_source
  , i2b2_status.active active_status_cd
  , ma.admsn_dt start_date
  , ma.dschrg_dt end_date
   -- TODO: IP -> IE as in etl_ip.sas
  , coalesce(mcet.enc_type, 'UN') || '/' || ma.nch_clm_type_cd inout_cd
  , to_char(null) location_cd
  , los_day_cnt length_of_stay
  , ltst_clm_acrtn_dt update_date
  , &&cms_source_cd sourcesystem_cd
  from
    "&&CMS_RIF".medpar_all ma
    cross join i2b2_status
    cross join cms_key_sources key_sources
    left join medpar_claim_enc_type mcet on mcet.code = ma.nch_clm_type_cd
;

create or replace view cms_enc_txform as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_enc_txform where design_digest = &&design_digest;
