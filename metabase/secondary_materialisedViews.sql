# Create View distinct(campaign_name) of marketing_facebook
CREATE VIEW temporal_campaigns AS
 With campaigns as (
    SELECT f.Campaign_Name as Campaign_Name
    FROM marketing_facebook f
    UNION
    SELECT l.mkt_acquisition_campaign as Campaign_Name
    FROM leads_deals_person l
    )
SELECT DISTINCT Campaign_Name
FROM campaigns;

SELECT * from temporal_campaigns

drop view temporal_campaigns

CREATE VIEW temporal_adset AS
    SELECT DISTINCT Adset_Name
    FROM marketing_facebook

drop view temporal_adset

CREATE VIEW marketing_adwords_campaign AS
    SELECT DISTINCT Campaign_metabase
    FROM marketing_adwords

CREATE VIEW temporal_date AS
    SELECT DISTINCT Day
    FROM marketing_facebook

drop view marketing_adwords_campaign

CREATE VIEW temporal_ad AS
    SELECT DISTINCT Ad_Name
    FROM marketing_facebook

# Create temporals for adwords
CREATE VIEW temporal_adwords_campaign AS
    SELECT DISTINCT Campaign_metabase
    FROM marketing_adwords

CREATE VIEW temporal_adwords_date AS
    SELECT DISTINCT Day
    FROM marketing_adwords

/* Create aircalls complete */
CREATE VIEW aircalls_calls_complete AS
SELECT id,
       direction,
       status,
       missed_call_reason,
       CAST(started_at as DATE) AS started_at,
       CAST(ended_at AS DATE) AS ended_at,
       duration,
       raw_digits,
       user_id,
       CAST(answered_at AS DATE) AS answered_at
    FROM pipedrive.aircalls_calls
UNION
SELECT `Call ID`,
       Direction,
       `Call Status`,
       `Missed Reason`,
       CAST(`Call Created Date` AS DATE) AS started_at,
       `Ended Time`,
       CAST(`In Call Duration` AS DATE) AS ended_at,
       `To`,
       `User ID`,
        CAST(`Answered Time` AS DATE) as answered_at
    FROM pipedrive.aircall_calls_historical;

Drop View aircalls_calls_complete

SELECT *
FROM aircalls_calls_complete
ORDER BY started_at DESC

/* update "To" column in aircall_calls_historical*/
SELECT *
FROM (
         SELECT DISTINCT (ade_citta.nome) as citta
         FROM ade_intestati
                  INNER JOIN ade_intestati_immobili on ade_intestati.id = ade_intestati_immobili.id_intestati
                  INNER JOIN ade_immobili on ade_immobili.id = ade_intestati_immobili.id_immobili
                  INNER JOIN ade_citta on ade_citta.id = ade_immobili.id_citta

         UNION
         SELECT DISTINCT (ade_citta.nome) as citta
         FROM ade_intestati
                  INNER JOIN ade_intestati_immobilinazionale on ade_intestati.id = ade_intestati_immobilinazionale.id_intestati
                  INNER JOIN ade_immobili_nazionale on ade_immobili_nazionale.id = ade_intestati_immobilinazionale.id_immobilinazionale
                  INNER JOIN ade_citta on ade_citta.id = ade_immobili_nazionale.id_citta) AS N


/* ADD '+' TO A 'TO' COLUMN IN AIRCALL_CALLS_HISTORICAL*/
ALTER TABLE aircall_calls_historical
MODIFY COLUMN `To` VARCHAR(255);

/* update "To" column in aircall_calls_historical*/
UPDATE aircall_calls_historical
    SET `To` = CONCAT('+', `To`);

/* update "To" column in aircall_calls_historical*/

ALTER TABLE aircall_calls_historical
MODIFY COLUMN `From` VARCHAR(255);

/* CREATE VIEW OF UNION BETWEEN ARCALL_CALLS_HISTORICAL AND AIRCALL_CALLS WHERE CREATED DATE IS BEFORE 2023-02-22 IN AIRCALL_CALLS_HISTORICAL*/

UPDATE aircall_calls_historical
    SET `From` = CONCAT('+', `From`);

/* drop view aircalls_calls_complete*/

