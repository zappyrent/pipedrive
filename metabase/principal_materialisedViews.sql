/* CREATE LEADS_DEALS_PERSON*/
CREATE VIEW leads_deals_person AS
WITH leads_cte AS (
    SELECT cast(person_id as decimal(10,0)) as person_id,
           leads.mkt_acquisition_channel as mkt_acquisition_channel,
           SUBSTRING_INDEX(leads.property_id, '_', -1) as property_id,
           leads.mkt_acquisition_content,
           leads.mkt_acquisition_campaign as mkt_acquisition_campaign,
           leads.codfisc as Codice_Fiscale,
           person.add_time as person_add_time,
           leads.add_time as leads_add_time,
           leads.Lead_created_Date as lead_created_Date,
           leads.is_archived,
           '' as deal_id,
           'deal' as status,
           person.phone,
           city,
           0 as pipeline_id,
           1 as is_lead,
           leads.lead_id as lead_id,
           leads.title,
           leads.Qualified_Ready_nel_passato,
           '' as owner_name,
           leads.creator_id as creator_user_id,
           leads.owner_id as pre_sales,
           person.fe5989bb199ee84a6842ba7120507599616a8200 as aircall_tags,
           '' as won_time,
           person.done_activities_count,
           leads.update_time,
           leads.mkt_acquisition_medium as mkt_acquisition_medium,
           leads.mkt_acquisition_term as mkt_acquisition_term,
           cast(CASE WHEN cast(cancelled_reason as decimal(10,0)) = 26 then 'No House'
                WHEN cast(cancelled_reason as decimal(10,0)) = 27 then 'Area not covered'
                WHEN cast(cancelled_reason as decimal(10,0)) = 28 then 'Tenant'
                WHEN cast(cancelled_reason as decimal(10,0)) = 29 then 'Wrong Number'
                WHEN cast(cancelled_reason as decimal(10,0)) = 30 then 'Short Period Availability'
                WHEN cast(cancelled_reason as decimal(10,0)) = 36 then 'Double item'
                WHEN cast(cancelled_reason as decimal(10,0)) = 37 then 'Existing Client'
                WHEN cast(cancelled_reason as decimal(10,0)) = 41 then 'Real State Agency'
                WHEN cast(cancelled_reason as decimal(10,0)) = 43 then 'Room'
                else cast(cancelled_reason as decimal(10,0))
                END as CHAR) as cancelled_reason
    FROM leads leads
        LEFT JOIN person person ON cast(person_id as decimal(10,0)) = person.id
),
deals_cte AS (
    SELECT deals.person_id,
           deals.mkt_acquisition_channel,
           SUBSTRING_INDEX(deals.property_id, '_', -1) as property_id,
           deals.mkt_acquisition_content,
           deals.mkt_acquisition_campaigns as mkt_acquisition_campaign,
           deals.codfisc as Codice_Fiscale,
           person.add_time as person_add_time,
           '' as leads_add_time,
           deals.Lead_created_Date as lead_created_Date,
           2 AS is_archived,
           deals.deal_id as deal_id,
           deals.status,
           person.phone,
           deals.city,
           deals.pipeline_id,
           0 as is_lead,
           '' as lead_id,
           deals.title,
           deals.Qualified_Ready_nel_passato,
           deals.owner_name,
           deals.creator_user_id_id,
           deals.creator_user_id_id as pre_sales,
           person.fe5989bb199ee84a6842ba7120507599616a8200 as aircall_tags,
           deals.won_time as won_time,
           person.done_activities_count,
           deals.update_time,
           deals.mkt_acquisition_medium as mkt_acquisition_medium,
           deals.mkt_acquisition_term as mkt_acquisition_term,
           CAST(CASE WHEN cast(cancelled_reason as decimal(10,0)) = 26 then 'No House'
                WHEN cast(cancelled_reason as decimal(10,0)) = 27 then 'Area not covered'
                WHEN cast(cancelled_reason as decimal(10,0)) = 28 then 'Tenant'
                WHEN cast(cancelled_reason as decimal(10,0)) = 29 then 'Wrong Number'
                WHEN cast(cancelled_reason as decimal(10,0)) = 30 then 'Short Period Availability'
                WHEN cast(cancelled_reason as decimal(10,0)) = 36 then 'Double item'
                WHEN cast(cancelled_reason as decimal(10,0)) = 37 then 'Existing Client'
                WHEN cast(cancelled_reason as decimal(10,0)) = 41 then 'Real State Agency'
                WHEN cast(cancelled_reason as decimal(10,0)) = 43 then 'Room'
                else cast(cancelled_reason as decimal(10,0))
                END as CHAR) as cancelled_reason
    FROM deals deals
        LEFT JOIN person person ON deals.person_id_value = person.id
)
SELECT * FROM leads_cte
UNION
SELECT * FROM deals_cte;

DROP VIEW leads_deals_person;


select  count(*)
from leads

select mkt_acquisition_term,
       count(*)
from leads
where  /*mkt_acquisition_channel = 'cpc' AND*/ add_time>= '2024-04-01'
group by mkt_acquisition_term;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'person';

/* CREATE ade+properties intestati*/

CREATE TABLE ade_properties_intestati AS
SELECT d.id_intestati,
       d.codfisc,
       (CASE WHEN d.IMM_MILANO_HINTERLAND_Small_cities >0 THEN 'Milano'
             WHEN d.IMM_ROMA_CITTA > 0  THEN 'Roma-Citta'
             WHEN d.IMM_TORINO_TERRITORIO > 0 or d.IMM_TORINO_PROVINCIA >0 THEN 'Torino'
             WHEN d.IMM_FIRENZE_TERRITORIO > 0 or d.IMM_FIRENZE_PROVINCIA >0 THEN 'Firenze'
             WHEN d.IMM_GENOVA_TERRITORIO > 0 or d.IMM_GENOVA_PROVINCIA >0 THEN 'Genova'
             WHEN d.IMM_OTHER > 0 THEN 'Other'
           END) as attribution,
       (CASE WHEN d.IMM_BUYER_PERSONA1 = 1 THEN 1 ELSE 0 END) as buyer_persona1_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA1 = 2 THEN 1 ELSE 0 END) as buyer_persona1_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA1 >= 3 THEN 1 ELSE 0 END) as buyer_persona1_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA2 = 1 THEN 1 ELSE 0 END) as buyer_persona2_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA2 = 2 THEN 1 ELSE 0 END) as buyer_persona2_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA2 >= 3 THEN 1 ELSE 0 END) as buyer_persona2_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA3 = 1 THEN 1 ELSE 0 END) as buyer_persona3_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA3 = 2 THEN 1 ELSE 0 END) as buyer_persona3_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA3 >= 3 THEN 1 ELSE 0 END) as buyer_persona3_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA4 = 1 THEN 1 ELSE 0 END) as buyer_persona4_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA4 = 2 THEN 1 ELSE 0 END) as buyer_persona4_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA4 >= 3 THEN 1 ELSE 0 END) as buyer_persona4_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA5 = 1 THEN 1 ELSE 0 END) as buyer_persona5_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA5 = 2 THEN 1 ELSE 0 END) as buyer_persona5_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA5 >= 3 THEN 1 ELSE 0 END) as buyer_persona5_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA6 = 1 THEN 1 ELSE 0 END) as buyer_persona6_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA6 = 2 THEN 1 ELSE 0 END) as buyer_persona6_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA6 >= 3 THEN 1 ELSE 0 END) as buyer_persona6_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA7 = 1 THEN 1 ELSE 0 END) as buyer_persona7_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA7 = 2 THEN 1 ELSE 0 END) as buyer_persona7_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA7 >= 3 THEN 1 ELSE 0 END) as buyer_persona7_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA8 = 1 THEN 1 ELSE 0 END) as buyer_persona8_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA8 = 2 THEN 1 ELSE 0 END) as buyer_persona8_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA8 >= 3 THEN 1 ELSE 0 END) as buyer_persona8_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA9 = 1 THEN 1 ELSE 0 END) as buyer_persona9_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA9 = 2 THEN 1 ELSE 0 END) as buyer_persona9_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA9 >= 3 THEN 1 ELSE 0 END) as buyer_persona9_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA10 = 1 THEN 1 ELSE 0 END) as buyer_persona10_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA10 = 2 THEN 1 ELSE 0 END) as buyer_persona10_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA10 >= 3 THEN 1 ELSE 0 END) as buyer_persona10_more3imm,
       (CASE WHEN d.IMM_BUYER_PERSONA11 = 1 THEN 1 ELSE 0 END) as buyer_persona11_1imm,
       (CASE WHEN d.IMM_BUYER_PERSONA11 = 2 THEN 1 ELSE 0 END) as buyer_persona11_2imm,
       (CASE WHEN d.IMM_BUYER_PERSONA11 >= 3 THEN 1 ELSE 0 END) as buyer_persona11_more3imm,
       d.ricerca_nazionale_done_at,
       d.status_ricerca_nazionale,
       d.sent,
       d.has_phone,
       d.is_lead,
       d.quota,
       d.titolarita,
       d.qualified_ready_nel_passato,
       d.person_add_time,
       d.Total_IMM,
       d.IMM_MILANO_HINTERLAND_No_small_cities,
       d.IMM_MILANO_HINTERLAND_Small_cities,
       d.IMM_ROMA_TERRITORIO,
       d.IMM_ROMA_CITTA,
       d.IMM_ROMA_PROVINCIA,
       d.IMM_TORINO_TERRITORIO,
       d.IMM_TORINO_PROVINCIA,
       d.IMM_FIRENZE_TERRITORIO,
       d.IMM_FIRENZE_PROVINCIA,
       d.IMM_GENOVA_TERRITORIO,
       d.IMM_GENOVA_PROVINCIA,
       d.IMM_OTHER
FROM       (
               SELECT c.id as id_intestati,
                      c.codfisc,
                      c.sent,
                      CASE WHEN c.phone is null THEN 0 ELSE 1 END  as has_phone,
                      c.is_lead                                                            as is_lead,
                      c.ricerca_nazionale_done_at,
                      c.status_ricerca_nazionale,
                      c.quota,
                      c.titolarita,
                      c.qualified_ready_nel_passato,
                      c.person_add_time,
                      COUNT(*)                                                             as Total_IMM,
                      COUNT(IF((c.citta = 'MONZA') OR (c.citta = 'MILANO') or (c.territorio_provincia = 'MI#MILANO')  , 1, NULL))  as IMM_MILANO_HINTERLAND_No_small_cities,
                      COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI') or (c.territorio_provincia = 'MI#MILANO') , 1, NULL))  as IMM_MILANO_HINTERLAND_Small_cities,
                      COUNT(IF(c.citta = 'Roma', 1, NULL))  as IMM_ROMA_CITTA,
                      COUNT(IF(c.territorio_provincia = 'ROMA Territorio-RM', 1, NULL))    as IMM_ROMA_TERRITORIO,
                      COUNT(IF(c.territorio_provincia = 'RM#ROMA', 1, NULL))               as IMM_ROMA_PROVINCIA,
                      COUNT(IF(c.territorio_provincia = 'TORINO Territorio-TO', 1, NULL))  as IMM_TORINO_TERRITORIO,
                      COUNT(IF(c.territorio_provincia = 'TO#TORINO', 1, NULL))             as IMM_TORINO_PROVINCIA,
                      COUNT(IF(c.territorio_provincia = 'FIRENZE Territorio-FI', 1, NULL)) as IMM_FIRENZE_TERRITORIO,
                      COUNT(IF(c.territorio_provincia = 'FI#FIRENZE', 1, NULL))            as IMM_FIRENZE_PROVINCIA,
                      COUNT(IF(c.territorio_provincia = 'GENOVA Territorio-GE', 1, NULL))  as IMM_GENOVA_TERRITORIO,
                      COUNT(IF(c.territorio_provincia = 'GE#GENOVA', 1, NULL))             as IMM_GENOVA_PROVINCIA,
                      COUNT(IF((c.territorio_provincia <> 'MILANO Territorio-MI') AND (c.territorio_provincia <> 'MI#MILANO')
                                   and (c.territorio_provincia <> 'ROMA Territorio-RM') and (c.territorio_provincia <> 'RM#ROMA')
                                   and (c.territorio_provincia <> 'TORINO Territorio-TO') and (c.territorio_provincia <> 'TO#TORINO')
                                   and (c.territorio_provincia <> 'FIRENZE Territorio-FI') and (c.territorio_provincia <> 'FI#FIRENZE')
                                   and (c.territorio_provincia <> 'GENOVA Territorio-GE') and (c.territorio_provincia <> 'GE#GENOVA'), 1, NULL)) as IMM_OTHER,
                     COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.territorio_provincia = 'ROMA Territorio-RM' or c.territorio_provincia = 'RM#ROMA' or c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'
                                   or c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO'), 1, NULL)) as IMM_BUYER_PERSONA1,
                     COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.territorio_provincia = 'ROMA Territorio-RM' or c.territorio_provincia = 'RM#ROMA' or c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'
                                   or c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND ((c.quota = '1.00') or (c.quota = '1') or (cast(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1), 2) as DECIMAL(10,2)) >= 1)), 1, NULL)) as IMM_BUYER_PERSONA2,
                     COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.territorio_provincia = 'ROMA Territorio-RM' or c.territorio_provincia = 'RM#ROMA' or c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'
                                   or c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (CAST(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1),2) as DECIMAL(10,2)) >= 0.75), 1, NULL)) as IMM_BUYER_PERSONA3,
                     COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.territorio_provincia = 'ROMA Territorio-RM' or c.territorio_provincia = 'RM#ROMA' or c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'
                                   or c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.titolarita = 'Proprieta'' ') # Titolarita is Proprieta'
                               AND (CAST(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1),2) as DECIMAL(10,2)) >= 1), 1, NULL)) as IMM_BUYER_PERSONA4,
                     COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.territorio_provincia = 'ROMA Territorio-RM' or c.territorio_provincia = 'RM#ROMA' or c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'
                                   or c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.titolarita = 'Proprieta'' ') # Titolarita is Proprieta'
                               AND (CAST(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1),2) as DECIMAL(10,2)) >= 0.75), 1, NULL)) as IMM_BUYER_PERSONA5,
                     COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.territorio_provincia = 'ROMA Territorio-RM' or c.territorio_provincia = 'RM#ROMA' or c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'
                                   or c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.titolarita = 'Proprieta'' '), 1, NULL)) as IMM_BUYER_PERSONA6,
                     COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.territorio_provincia = 'ROMA Territorio-RM' or c.territorio_provincia = 'RM#ROMA' or c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'
                                   or c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (CAST(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1),2) as DECIMAL(10,2)) >= 0.5)
                               AND (CAST(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1),2) as DECIMAL(10,2)) <= 0.75), 1, NULL)) as IMM_BUYER_PERSONA7,
                     COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (c.territorio_provincia = 'ROMA Territorio-RM' or c.territorio_provincia = 'RM#ROMA' or c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'
                                   or c.territorio_provincia = 'MILANO Territorio-MI' or c.territorio_provincia = 'MI#MILANO')
                               AND (CAST(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1),2) as DECIMAL(10,2)) <= 0.5), 1, NULL)) as IMM_BUYER_PERSONA8,
                     COUNT(IF((c.territorio_provincia <> 'MILANO Territorio-MI')
                                   AND (c.territorio_provincia <> 'MI#MILANO')
                                   AND (c.citta = 'ROMA')
                                   AND (c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO'), 1, NULL)) as IMM_BUYER_PERSONA9,
                     COUNT(IF((c.territorio_provincia <> 'MILANO Territorio-MI' AND c.territorio_provincia <> 'MI#MILANO')
                                   AND (c.citta = 'ROMA')
                                   AND (c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO')
                                   AND ((c.quota = '1.00') or (c.quota = '1') or (cast(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1), 2) as DECIMAL(10,2)) >= 1)), 1, NULL)) as IMM_BUYER_PERSONA10,
                    COUNT(IF((c.territorio_provincia <> 'MILANO Territorio-MI')
                                   AND (c.territorio_provincia <> 'MI#MILANO')
                                   AND (c.citta = 'ROMA')
                                   AND (c.territorio_provincia = 'TORINO Territorio-TO' or c.territorio_provincia = 'TO#TORINO')
                                   AND ((c.quota = '1.00') or (c.quota = '1') or (cast(round(SUBSTRING_INDEX(c.quota,'/',1) / SUBSTRING_INDEX(c.quota,'/',-1), 2) as DECIMAL(10,2)) >= 0.75)), 1, NULL)) as IMM_BUYER_PERSONA11
               FROM (SELECT n.id,
                            n.keycolumn                      as keycolumn,
                            n.codfisc                        as codfisc,
                            max(n.created_at)                as created_at,
                            max(n.sent)                      as sent,
                            max(n.status_ricerca_nazionale)  as status_ricerca_nazionale,
                            max(n.ricerca_nazionale_done_at) as ricerca_nazionale_done_at,
                            max(n.id_immobili)               as id_immobili,
                            max(n.nominativo)                as nominativo,
                            max(n.quota) as quota,
                            max(n.titolarita) as titolarita,
                            #max(n.created_at_immobili) as created_at_immobili,
                            min(n.territorio_provincia)      as territorio_provincia,
                            min(n.citta) as citta,
                            #max(n.categoria) as categoria,
                            max(n.phone)                     as phone,
                            max(n.person_add_time)                  as person_add_time,
                            max(n.is_lead)                   as is_lead,
                            max(n.status)                    as status,
                            max(n.lead_id)                   as lead_id,
                            max(n.Qualified_Ready_nel_passato)                     as qualified_ready_nel_passato
                     FROM (SELECT i.id,
                                  concat(i.codfisc, im.fogliotipo, im.partnum, im.subanno) as keycolumn,
                                  i.codfisc,
                                  i.created_at,
                                  i.sent,
                                  i.status_ricerca_nazionale,
                                  i.ricerca_nazionale_done_at,
                                  inm.id_immobili,
                                  i.nominativo,
                                  inm.quota,
                                  inm.titolarita,
                                  #inm.created_at as created_at_immobili,
                                  c.territorio                                             as territorio_provincia,
                                  c.nome as citta,
                                  ld.phone,
                                  ld.person_add_time,
                                  ld.is_lead,
                                  ld.status,
                                  ld.lead_id,
                                  ld.Qualified_Ready_nel_passato
                           FROM ade_intestati i
                                    INNER JOIN ade_intestati_immobili inm on inm.id_intestati = i.id
                                    INNER JOIN ade_immobili im on im.id = inm.id_immobili
                                    LEFT JOIN ade_citta c on c.id = im.id_citta
                                    LEFT JOIN leads_deals_person ld on ld.Codice_Fiscale = i.codfisc
                           WHERE ((im.categoria = 'A01') OR (im.categoria = 'A02') OR (im.categoria = 'A03') OR
                                  (im.categoria = 'A04') OR (im.categoria = 'A05') OR (im.categoria = 'A06') OR
                                  (im.categoria = 'A07') OR (im.categoria = 'A08') OR (im.categoria = 'A09') OR (im.categoria = 'A10') OR (im.categoria = 'A11'))
                                 #and (status_ricerca_nazionale = 'done')
                           UNION
                           SELECT i.id,
                                  concat(i.codfisc, im.fogliotipo, im.partnum, im.subanno) as keycolumn,
                                  i.codfisc,
                                  i.created_at,
                                  i.sent,
                                  i.status_ricerca_nazionale,
                                  i.ricerca_nazionale_done_at,
                                  inm.id_immobilinazionale                                 AS id_immobili,
                                  i.nominativo,
                                  inm.quota,
                                  inm.titolarita,
                                  #inm.created_at as created_at_immobili,
                                  im.provincia                                             as territorio_provincia,
                                  '' as citta,
                                  ld.phone,
                                  ld.person_add_time,
                                  ld.is_lead,
                                  ld.status,
                                  ld.lead_id,
                                  ld.Qualified_Ready_nel_passato
                           FROM ade_intestati i
                                    INNER JOIN ade_intestati_immobilinazionale inm on inm.id_intestati = i.id
                                    INNER JOIN ade_immobili_nazionale im on im.id = inm.id_immobilinazionale
                                    LEFT JOIN leads_deals_person ld on ld.Codice_Fiscale = i.codfisc
                           WHERE ((im.categoria = 'A/1') OR (im.categoria = 'A/2') OR (im.categoria = 'A/3') OR
                                  (im.categoria = 'A/4') OR (im.categoria = 'A/5') OR (im.categoria = 'A/6') OR
                                  (im.categoria = 'A/7') OR (im.categoria = 'A/8')
                               OR (im.categoria = 'A/9') OR (im.categoria = 'A/10') OR (im.categoria = 'A/11'))
                             #and (status_ricerca_nazionale = 'done')
                           ) AS n
                     GROUP BY n.codfisc, n.keycolumn) as c
               GROUP BY c.codfisc
           ) as d;

DROP TABLE ade_properties_intestati;


/* SECOND CREATE ade+properties intestati*/

CREATE TABLE ade_properties_intestati2 AS

SELECT
       d.id_intestati,
       d.codfisc,
       d.ricerca_nazionale_done_at,
       d.status_ricerca_nazionale,
       d.sent,
       d.has_phone,
       d.person_add_time,
       d.aircall_tags,
       d.is_lead,
       d.quota,
       d.titolarita,
       d.mkt_acquisition_medium,
       d.Total_IMM,
       d.IMM_MILANO_HINTERLAND,
       d.IMM_ROMA_TERRITORIO,
       d.IMM_ROMA_CITTA,
       d.IMM_ROMA_PROVINCIA,
       d.IMM_TORINO_TERRITORIO,
       d.IMM_TORINO_PROVINCIA,
       d.IMM_FIRENZE_TERRITORIO,
       d.IMM_FIRENZE_PROVINCIA,
       d.IMM_GENOVA_TERRITORIO,
       d.IMM_GENOVA_PROVINCIA,
       d.IMM_OTHER,
       CASE WHEN ((d.IMM_MILANO_HINTERLAND > 0)
                 AND ((IMM_OTHER) = 0)) THEN 1
            END  as bp1_all_all_milano_h,
       CASE WHEN (d.IMM_MILANO_HINTERLAND > 0)  AND (IMM_OTHER = 0)
                 AND ((d.quota = '1.00') or (d.quota = '1') or (cast(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1), 2) as DECIMAL(10,2)) >= 1)) THEN 1
            END as bp2_all_100_milano_h,
       CASE WHEN ((d.IMM_MILANO_HINTERLAND > 0)
                 AND (IMM_OTHER = 0)
                 AND ((d.quota = '1.00') or (d.quota = '1') or (cast(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1), 2) as DECIMAL(10,2)) >= 0.75))) THEN 1
            END as bp3_all_75_milano_h,
       CASE WHEN (d.IMM_MILANO_HINTERLAND > 0)
                 AND (IMM_OTHER = 0)
                 AND ((d.quota = '1.00') or (d.quota = '1') or (cast(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1), 2) as DECIMAL(10,2)) >= 1))
                 AND (d.titolarita = 'Proprieta'' ') THEN 1
            END as bp4_proprieta_100_milano_h,
       CASE WHEN (d.IMM_MILANO_HINTERLAND > 0)
                 AND (IMM_OTHER = 0)
                 AND ((d.quota = '1.00') or (d.quota = '1') or (cast(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1), 2) as DECIMAL(10,2)) >= 0.75))
                 AND (d.titolarita = 'Proprieta'' ') THEN 1
            END as bp5_proprieta_75_milano_h,
       CASE WHEN (d.IMM_MILANO_HINTERLAND > 0)
                 AND (IMM_OTHER = 0)
                 AND (d.titolarita = 'Proprieta'' ') THEN 1
            END as bp6_proprieta_all_milano_h,
       CASE WHEN ((d.IMM_MILANO_HINTERLAND > 0) AND (IMM_OTHER = 0)
                 AND (d.titolarita = 'Proprieta'' ')
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) >= 0.5)
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) <= 0.75)) THEN 1
            END  as bp7_proprieta_50_75_milano_h,
       CASE WHEN ((d.IMM_MILANO_HINTERLAND > 0)
                 AND (IMM_OTHER = 0)
                 AND (d.titolarita = 'Proprieta'' ')
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) >= 0.5)) THEN 1
            END                                                   as bp8_proprieta_less50_milano_h,
       CASE WHEN ((d.IMM_ROMA_CITTA > 0) AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)) THEN  1
            END                                                                                                        as bp9_all_all_roma_c,
       CASE WHEN (d.IMM_ROMA_CITTA > 0)
                 AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) >= 1) THEN 1
            END                                                   as bp10_all_100_roma_c,
       CASE WHEN (d.IMM_ROMA_CITTA>=1)
                 AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) >= 0.75) THEN 1
            END                                                   as bp11_all_75_roma_c,
       CASE WHEN (d.IMM_ROMA_CITTA>=1)
                 AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) >= 0.5)
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) <= 0.75) THEN 1
            END                                                   as bp12_all_50_75_roma_c,
       CASE WHEN (d.IMM_ROMA_CITTA>=1)
                 AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) <= 0.5) THEN 1
            END                                                   as bp13_all_less50_roma_c,
       CASE WHEN (d.IMM_ROMA_CITTA>=1)
                 AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)
                 AND (d.titolarita = 'Proprieta'' ') THEN 1
            END as bp14_proprieta_all_roma_c,
       CASE WHEN (d.IMM_ROMA_CITTA>=1)
                 AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)
                 AND (d.titolarita = 'Proprieta'' ')
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) >= 1) THEN 1
            END                                                    as bp15_proprieta_100_roma_c,
      CASE WHEN (d.IMM_ROMA_CITTA>=1)
                 AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)
                 AND (d.titolarita = 'Proprieta'' ')
                 AND (CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) >= 0.75) THEN  1
           END                                                      as bp16_proprieta_75_roma_c,
      CASE WHEN ((d.IMM_TORINO_PROVINCIA > 0 OR IMM_TORINO_TERRITORIO > 0) AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0) AND (IMM_ROMA_CITTA = 0)) THEN  1
            END                                                                                                        as bp17_all_all_torino_h
FROM       (
               SELECT c.id as id_intestati,
                      c.codfisc,
                      c.sent,
                      CASE WHEN c.phone is null THEN 0 ELSE 1 END  as has_phone,
                      c.is_lead                                                          as is_lead,
                      c.person_add_time,
                      c.aircall_tags,
                      c.ricerca_nazionale_done_at,
                      c.status_ricerca_nazionale,
                      c.quota,
                      c.titolarita,
                      c.mkt_acquisition_medium,
                      COUNT(*)                                                             as Total_IMM,
                      COUNT(IF((c.territorio_provincia = 'MILANO Territorio-MI') or (c.territorio_provincia = 'MI#MILANO') , 1, NULL))  as IMM_MILANO_HINTERLAND,
                      COUNT(IF(c.citta = 'ROMA', 1, NULL))  as IMM_ROMA_CITTA,
                      COUNT(IF(c.territorio_provincia = 'ROMA Territorio-RM', 1, NULL))    as IMM_ROMA_TERRITORIO,
                      COUNT(IF(c.territorio_provincia = 'RM#ROMA', 1, NULL))               as IMM_ROMA_PROVINCIA,
                      COUNT(IF(c.territorio_provincia = 'TORINO Territorio-TO', 1, NULL))  as IMM_TORINO_TERRITORIO,
                      COUNT(IF(c.territorio_provincia = 'TO#TORINO', 1, NULL))             as IMM_TORINO_PROVINCIA,
                      COUNT(IF(c.territorio_provincia = 'FIRENZE Territorio-FI', 1, NULL)) as IMM_FIRENZE_TERRITORIO,
                      COUNT(IF(c.territorio_provincia = 'FI#FIRENZE', 1, NULL))            as IMM_FIRENZE_PROVINCIA,
                      COUNT(IF(c.territorio_provincia = 'GENOVA Territorio-GE', 1, NULL))  as IMM_GENOVA_TERRITORIO,
                      COUNT(IF(c.territorio_provincia = 'GE#GENOVA', 1, NULL))             as IMM_GENOVA_PROVINCIA,
                      COUNT(IF((c.territorio_provincia <> 'MILANO Territorio-MI') AND (c.territorio_provincia <> 'MI#MILANO')
                                   and (c.territorio_provincia <> 'ROMA Territorio-RM') and (c.territorio_provincia <> 'RM#ROMA')
                                   and (c.territorio_provincia <> 'TORINO Territorio-TO') and (c.territorio_provincia <> 'TO#TORINO')
                                   and (c.territorio_provincia <> 'FIRENZE Territorio-FI') and (c.territorio_provincia <> 'FI#FIRENZE')
                                   and (c.territorio_provincia <> 'GENOVA Territorio-GE') and (c.territorio_provincia <> 'GE#GENOVA'), 1, NULL)) as IMM_OTHER

               FROM (SELECT n.id,
                            n.keycolumn                      as keycolumn,
                            n.codfisc                        as codfisc,
                            max(n.created_at)                as created_at,
                            max(n.sent)                      as sent,
                            max(n.status_ricerca_nazionale)  as status_ricerca_nazionale,
                            max(n.ricerca_nazionale_done_at) as ricerca_nazionale_done_at,
                            max(n.id_immobili)               as id_immobili,
                            max(n.quota) as quota,
                            max(n.titolarita) as titolarita,
                            min(n.territorio_provincia)      as territorio_provincia,
                            min(n.citta) as citta,
                            max(n.phone)                     as phone,
                            max(n.person_add_time)                  as person_add_time,
                            max(n.is_lead)                   as is_lead,
                            max(n.aircall_tags)                     as aircall_tags,
                            max(n.mkt_acquisition_medium) as mkt_acquisition_medium
                     FROM (SELECT i.id,
                                  concat(i.codfisc, im.fogliotipo, im.partnum, im.subanno) as keycolumn,
                                  i.codfisc,
                                  i.created_at,
                                  i.sent,
                                  i.status_ricerca_nazionale,
                                  i.ricerca_nazionale_done_at,
                                  inm.id_immobili,
                                  inm.quota,
                                  inm.titolarita,
                                  c.territorio                                             as territorio_provincia,
                                  c.nome as citta,
                                  ld.phone,
                                  ld.person_add_time,
                                  ld.is_lead,
                                  ld.aircall_tags,
                                  ld.mkt_acquisition_medium
                           FROM ade_intestati i
                                    INNER JOIN ade_intestati_immobili inm on inm.id_intestati = i.id
                                    INNER JOIN ade_immobili im on im.id = inm.id_immobili
                                    LEFT JOIN ade_citta c on c.id = im.id_citta
                                    LEFT JOIN leads_deals_person ld on ld.Codice_Fiscale = i.codfisc
                           WHERE ((im.categoria = 'A01') OR (im.categoria = 'A02') OR (im.categoria = 'A03') OR
                                  (im.categoria = 'A04') OR (im.categoria = 'A05') OR (im.categoria = 'A06') OR
                                  (im.categoria = 'A07') OR (im.categoria = 'A08') OR (im.categoria = 'A09') OR (im.categoria = 'A10') OR (im.categoria = 'A11'))
                                 #and (status_ricerca_nazionale = 'done')
                           UNION
                           SELECT i.id,
                                  concat(i.codfisc, im.fogliotipo, im.partnum, im.subanno) as keycolumn,
                                  i.codfisc,
                                  i.created_at,
                                  i.sent,
                                  i.status_ricerca_nazionale,
                                  i.ricerca_nazionale_done_at,
                                  inm.id_immobilinazionale                                 AS id_immobili,
                                  inm.quota,
                                  inm.titolarita,
                                  im.provincia                                             as territorio_provincia,
                                  '' as citta,
                                  ld.phone,
                                  ld.person_add_time,
                                  ld.is_lead,
                                  ld.aircall_tags,
                                  ld.mkt_acquisition_medium
                           FROM ade_intestati i
                                    INNER JOIN ade_intestati_immobilinazionale inm on inm.id_intestati = i.id
                                    INNER JOIN ade_immobili_nazionale im on im.id = inm.id_immobilinazionale
                                    LEFT JOIN leads_deals_person ld on ld.Codice_Fiscale = i.codfisc
                           WHERE ((im.categoria = 'A/1') OR (im.categoria = 'A/2') OR (im.categoria = 'A/3') OR
                                  (im.categoria = 'A/4') OR (im.categoria = 'A/5') OR (im.categoria = 'A/6') OR
                                  (im.categoria = 'A/7') OR (im.categoria = 'A/8')
                               OR (im.categoria = 'A/9') OR (im.categoria = 'A/10') OR (im.categoria = 'A/11'))
                             #and (status_ricerca_nazionale = 'done')
                           ) AS n
                     GROUP BY n.codfisc, n.keycolumn) as c
               GROUP BY c.codfisc
           ) as d;

DROP TABLE ade_properties_intestati2;


#Last Ricerca Nazionale
Select *
from ade_intestati
order by created_at desc

