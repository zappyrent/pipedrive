/*ANALYSIS OF PERFORMANCE OF DEALS*/

select month(person_add_time) as MONTH,
       year(person_add_time) as YEAR,
       count(*) as CNT
from ade_properties_intestati2
group by month(person_add_time), year(person_add_time)
order by 2 desc, 1 desc;

select -- avg(CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2))) as avg_ownership,
       -- avg(d.Total_IMM) AS avg_total_IMM,
       -- avg(d.IMM_ROMA_CITTA) AS avg_imm_roma_citta,
       -- avg(d.IMM_ROMA_PROVINCIA) AS avg_imm_roma_provincia
         count(*) as CNT
from ade_properties_intestati2 d
  left join base_ade_properties b on d.codfisc = b.Codice_Fiscale
where d.person_add_time >= '2024-04-01' and  d.bp9_all_all_roma_c = 1 and b.done_activities_count =0 and CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2)) >= .75
;

select avg(CAST(round(SUBSTRING_INDEX(d.quota,'/',1) / SUBSTRING_INDEX(d.quota,'/',-1),2) as DECIMAL(10,2))) as avg_ownership,
       avg(d.Total_IMM) AS avg_total_IMM,
       avg(d.IMM_ROMA_CITTA) AS avg_imm_roma_citta,
       avg(d.IMM_ROMA_PROVINCIA) AS avg_imm_roma_provincia
from ade_properties_intestati2 d
where d.person_add_time >= '2024-03-01' AND d.person_add_time <= '2024-03-30' and d.is_lead=0 and d.bp9_all_all_roma_c = 1
