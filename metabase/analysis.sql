select *
from ade_intestati
WHERE status_ricerca_nazionale = 'done'
order by ricerca_nazionale_done_at desc

SELECT d.id_intestati,
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
       CASE WHEN ((d.IMM_MILANO_HINTERLAND > 0)
                 AND ((IMM_OTHER) = 0)) THEN 1
            END  as bp1_all_all_milano_h,
       CASE WHEN ((d.IMM_ROMA_CITTA > 0) AND (IMM_OTHER = 0) AND (IMM_MILANO_HINTERLAND = 0)) THEN  1
            END                                                                                                        as bp9_all_all_roma_c
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
                                    INNER JOIN leads_deals_person ld on ld.Codice_Fiscale = i.codfisc
                           WHERE ((im.categoria = 'A/1') OR (im.categoria = 'A/2') OR (im.categoria = 'A/3') OR
                                  (im.categoria = 'A/4') OR (im.categoria = 'A/5') OR (im.categoria = 'A/6') OR
                                  (im.categoria = 'A/7') OR (im.categoria = 'A/8')
                               OR (im.categoria = 'A/9') OR (im.categoria = 'A/10') OR (im.categoria = 'A/11'))
                             #and (status_ricerca_nazionale = 'done')
                           ) AS n
                     GROUP BY n.codfisc, n.keycolumn) as c
               GROUP BY c.codfisc
           ) as d
WHERE sent = 1 AND has_phone=0
LIMIT 10;
