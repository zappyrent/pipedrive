/* This is the main view and do the append between deals and leads and join with person table*/
-- When a view is created, this automatically refresh when the tables in the db get an update

CREATE VIEW leads_deals_person AS
-- First Common table expression (CTE) or subquery, join the leads table with the person table
-- To add/modify/delete any column, you just add the field here
WITH leads_cte AS (
    SELECT cast(person_id as decimal(10,0)) as person_id,
           leads.mkt_acquisition_channel as mkt_acquisition_channel,
           SUBSTRING_INDEX(leads.property_id, '_', -1) as property_id,
           leads.mkt_acquisition_content,
           leads.mkt_acquisition_campaign as mkt_acquisition_campaign,
           leads.codfisc as Codice_Fiscale,
           person.add_time as person_add_time,
           leads.add_time as leads_add_time,
           '' as deals_add_time,
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
           cast(CASE WHEN cancelled_reason IN (26) then 'No House'
                WHEN cancelled_reason IN (27) then 'Area not covered'
                WHEN cancelled_reason IN (28) then 'Tenant'
                WHEN cancelled_reason IN (29) then 'Wrong Number'
                WHEN cancelled_reason IN (30) then 'Short Period Availability'
                WHEN cancelled_reason IN (36) then 'Double item'
                WHEN cancelled_reason IN (37) then 'Existing Client'
                WHEN cancelled_reason IN (41) then 'Real State Agency'
                WHEN cancelled_reason IN (43) then 'Room'
                else 'Other'
                END as CHAR) as cancelled_reason,
           person.next_activity_date as person_next_activity_date,
           '' as stage_id,
           '' as deals_next_activity_date,
           leads.note as note
    FROM leads leads
        LEFT JOIN person person ON cast(leads.person_id as decimal(10,0)) = person.id
),
-- Second CTE, join the deals table with the person table
-- To add/modify/delete any column, you just add the field here
deals_cte AS (
    SELECT deals.person_id,
           deals.mkt_acquisition_channel,
           SUBSTRING_INDEX(deals.property_id, '_', -1) as property_id,
           deals.mkt_acquisition_content,
           deals.mkt_acquisition_campaigns as mkt_acquisition_campaign,
           deals.codfisc as Codice_Fiscale,
           person.add_time as person_add_time,
           '' as leads_add_time,
           deals.add_time as deals_add_time,
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
           cast(CASE WHEN cancelled_reason IN (26) then 'No House'
                WHEN cancelled_reason IN (27) then 'Area not covered'
                WHEN cancelled_reason IN (28) then 'Tenant'
                WHEN cancelled_reason IN (29) then 'Wrong Number'
                WHEN cancelled_reason IN (30) then 'Short Period Availability'
                WHEN cancelled_reason IN (36) then 'Double item'
                WHEN cancelled_reason IN (37) then 'Existing Client'
                WHEN cancelled_reason IN (41) then 'Real State Agency'
                WHEN cancelled_reason IN (43) then 'Room'
                else 'Other'
                END as CHAR) as cancelled_reason,
           person.next_activity_date as person_next_activity_date,
           deals.stage_id as stage_id,
           deals.next_activity_date as deals_next_activity_date,
           deals.note as note
    FROM deals deals
        LEFT JOIN person person ON deals.person_id_value = person.id
)
-- Final SELECT statement, append the leads and deals CTEs
-- Here is important that the deals_CTE and leads_CTE have the same columns in the same order
-- To modify anything in any of leads and deals, you should add or modify in the other table to make the append succesful
SELECT * FROM leads_cte
UNION
SELECT * FROM deals_cte;

-- Once you modify the view, you just need to drop the view, and create it again.
-- After that, it would get refresh automatically in metabase.
DROP VIEW leads_deals_person;


-- Create View
CREATE VIEW aircalls_view AS
    SELECT
        aircalls_calls_dag.id,
        aircalls_calls_dag.direction,
        aircalls_calls_dag.status,
        aircalls_calls_dag.missed_call_reason,
        aircalls_calls_dag.started_at,
        aircalls_calls_dag.answered_at,
        aircalls_calls_dag.ended_at,
        aircalls_calls_dag.duration,
        aircalls_calls_dag.raw_digits,
        aircalls_calls_dag.user_id,
        aircalls_calls_dag.archived,
        aircalls_calls_dag.assigned_to,
        aircalls_calls_dag.transferred_by,
        aircalls_calls_dag.transferred_to,
        aircalls_calls_dag.number_id,
        aircalls_calls_dag.cost,
        aircalls_calls_dag.country_code_a2,
        aircalls_calls_dag.pricing_type,
        aircalls_users_dag.name as user_name
    FROM aircalls_calls_dag
        LEFT JOIN  aircalls_users_dag ON aircalls_calls_dag.user_id = aircalls_users_dag.id




-- The following table is for all the "Buyer persona", full overview dashbaords in metabase. Everything that joins pipedrive and ade production
-- As this contains more data was needed to be created a TABLLE instead of view. Which to be updated need to be dropped and created again.

drop table ade_properties_intestati2;

/* CREATE ade+properties intestati2*/
CREATE TABLE ade_properties_intestati2 AS
-- First Common table expression (CTE) or subquery, join de production with pipedrive and do an append between immobili and immobili nazionale
WITH big_table AS (
    SELECT
        i.id,
        concat(i.codfisc, im.fogliotipo, im.partnum, im.subanno) AS keycolumn,
        i.codfisc,
        i.created_at,
        i.sent,
        i.status_ricerca_nazionale,
        i.ricerca_nazionale_done_at,
        inm.id_immobili,
        inm.quota,
        inm.titolarita,
        c.territorio AS territorio_provincia,
        c.nome AS citta,
        ld.phone,
        ld.person_add_time,
        ld.is_lead,
        ld.aircall_tags,
        ld.mkt_acquisition_medium
    FROM ade_intestati i
    INNER JOIN ade_intestati_immobili inm ON inm.id_intestati = i.id
    INNER JOIN ade_immobili im ON im.id = inm.id_immobili
    LEFT JOIN ade_citta c ON c.id = im.id_citta
    LEFT JOIN base_ade_properties ld ON ld.Codice_Fiscale = i.codfisc
    WHERE im.categoria IN ('A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11')
            and i.codfisc REGEXP '^[a-zA-Z]' = 1

    UNION

    SELECT
        i.id,
        concat(i.codfisc, im.fogliotipo, im.partnum, im.subanno) AS keycolumn,
        i.codfisc,
        i.created_at,
        i.sent,
        i.status_ricerca_nazionale,
        i.ricerca_nazionale_done_at,
        inm.id_immobilinazionale AS id_immobili,
        inm.quota,
        inm.titolarita,
        im.provincia AS territorio_provincia,
        '' AS citta,
        ld.phone,
        ld.person_add_time,
        ld.is_lead,
        ld.aircall_tags,
        ld.mkt_acquisition_medium
    FROM ade_intestati i
    INNER JOIN ade_intestati_immobilinazionale inm ON inm.id_intestati = i.id
    INNER JOIN ade_immobili_nazionale im ON im.id = inm.id_immobilinazionale
    LEFT JOIN base_ade_properties ld ON ld.Codice_Fiscale = i.codfisc
    WHERE im.categoria IN ('A/1', 'A/2', 'A/3', 'A/4', 'A/5', 'A/6', 'A/7', 'A/8', 'A/9', 'A/10', 'A/11')
        and i.codfisc REGEXP '^[a-zA-Z]' = 1
),
-- Second CTE, group by the codfisc and do the count of the immobili and immobili nazionale
-- The porpuse of this is to have avoid duplicate immobili (Using the triple key column)
immobili_duplicate AS (
    SELECT
        n.id,
        n.keycolumn,
        n.codfisc,
        MAX(n.created_at) AS created_at,
        MAX(n.sent) AS sent,
        MAX(n.status_ricerca_nazionale) AS status_ricerca_nazionale,
        MAX(n.ricerca_nazionale_done_at) AS ricerca_nazionale_done_at,
        MAX(n.id_immobili) AS id_immobili,
        MAX(n.quota) AS quota,
        MAX(n.titolarita) AS titolarita,
        MIN(n.territorio_provincia) AS territorio_provincia,
        MIN(n.citta) AS citta,
        MAX(n.phone) AS phone,
        MAX(n.person_add_time) AS person_add_time,
        MAX(n.is_lead) AS is_lead,
        MAX(n.aircall_tags) AS aircall_tags,
        MAX(n.mkt_acquisition_medium) AS mkt_acquisition_medium
    FROM big_table n
    GROUP BY n.codfisc, n.keycolumn
),
-- Third CTE, group by the codfisc
-- The porpuse of this is to count the immobilie by territory and citta by intestati
d AS (
    SELECT
        c.id AS id_intestati,
        c.codfisc,
        c.sent,
        CASE WHEN c.phone IS NULL THEN 0 ELSE 1 END AS has_phone,
        c.is_lead,
        c.person_add_time,
        c.aircall_tags,
        c.ricerca_nazionale_done_at,
        c.status_ricerca_nazionale,
        c.quota,
        c.titolarita,
        c.mkt_acquisition_medium,
        COUNT(*) AS Total_IMM,
        COUNT(IF(c.territorio_provincia IN ('MILANO Territorio-MI', 'MI#MILANO'), 1, NULL)) AS IMM_MILANO_HINTERLAND,
        COUNT(IF(c.citta = 'ROMA', 1, NULL)) AS IMM_ROMA_CITTA,
        COUNT(IF(c.territorio_provincia = 'ROMA Territorio-RM', 1, NULL)) AS IMM_ROMA_TERRITORIO,
        COUNT(IF(c.territorio_provincia = 'RM#ROMA', 1, NULL)) AS IMM_ROMA_PROVINCIA,
        COUNT(IF(c.territorio_provincia = 'TORINO Territorio-TO', 1, NULL)) AS IMM_TORINO_TERRITORIO,
        COUNT(IF(c.territorio_provincia = 'TO#TORINO', 1, NULL)) AS IMM_TORINO_PROVINCIA,
        COUNT(IF(c.territorio_provincia = 'FIRENZE Territorio-FI', 1, NULL)) AS IMM_FIRENZE_TERRITORIO,
        COUNT(IF(c.territorio_provincia = 'FI#FIRENZE', 1, NULL)) AS IMM_FIRENZE_PROVINCIA,
        COUNT(IF(c.territorio_provincia = 'GENOVA Territorio-GE', 1, NULL)) AS IMM_GENOVA_TERRITORIO,
        COUNT(IF(c.territorio_provincia = 'GE#GENOVA', 1, NULL)) AS IMM_GENOVA_PROVINCIA,
        COUNT(IF(c.territorio_provincia NOT IN ('MILANO Territorio-MI', 'MI#MILANO', 'ROMA Territorio-RM', 'RM#ROMA', 'TORINO Territorio-TO', 'TO#TORINO', 'FIRENZE Territorio-FI', 'FI#FIRENZE', 'GENOVA Territorio-GE', 'GE#GENOVA'), 1, NULL)) AS IMM_OTHER
    FROM immobili_duplicate c
    GROUP BY c.codfisc
)
-- Final SELECT statement with calculations
-- In this CTE we do the calculations to get the customized buyer personas, to determine wich buyer persona the intestati belongs
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
    d.IMM_ROMA_CITTA,
    d.IMM_ROMA_TERRITORIO,
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
FROM d
GROUP BY d.id_intestati;

-- Drop the table and create it again to update the data
drop table ade_properties_intestati2;



-- The following view is for the base_ade_properties, this is only for the above table, to remove unnecesary columns and to have a clean view
CREATE VIEW base_ade_properties as (
WITH leads_cte AS (
    SELECT leads.person_id as person_id,
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
           leads.mkt_acquisition_term as mkt_acquisition_term
           /*CASE WHEN cancelled_reason IN (26) then 'No House'
                    WHEN cancelled_reason IN (27) then 'Area not covered'
                    WHEN cancelled_reason IN (28) then 'Tenant'
                    WHEN cancelled_reason IN (29) then 'Wrong Number'
                    WHEN cancelled_reason IN (30) then 'Short Period Availability'
                    WHEN cancelled_reason IN (36) then 'Double item'
                    WHEN cancelled_reason IN (37) then 'Existing Client'
                    WHEN cancelled_reason IN (41) then 'Real State Agency'
                    WHEN cancelled_reason IN (43) then 'Room'
                    WHEN cancelled_reason = 'NaN' then 'Other'
                    else 'Other' END as cancelled_reason2*/
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
           deals.mkt_acquisition_term as mkt_acquisition_term
           /*CASE WHEN cancelled_reason IN (26) then 'No House'
                    WHEN cancelled_reason IN (27) then 'Area not covered'
                    WHEN cancelled_reason IN (28) then 'Tenant'
                    WHEN cancelled_reason IN (29) then 'Wrong Number'
                    WHEN cancelled_reason IN (30) then 'Short Period Availability'
                    WHEN cancelled_reason IN (36) then 'Double item'
                    WHEN cancelled_reason IN (37) then 'Existing Client'
                    WHEN cancelled_reason IN (41) then 'Real State Agency'
                    WHEN cancelled_reason IN (43) then 'Room'
                    WHEN cancelled_reason = 'NaN' then 'Other'
                    else 'Other' END as cancelled_reason2*/
    FROM deals deals
        LEFT JOIN person person ON deals.person_id_value = person.id
)
SELECT * FROM leads_cte
UNION
SELECT * FROM deals_cte
    );

drop view base_ade_properties;


