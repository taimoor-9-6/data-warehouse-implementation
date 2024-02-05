----------------------------------------------------------
--Business Questions--
select * from complaint_fact;
select * from company_dimension;
select * from location_dimension;
----------------------------------------------------------

--Q1 Best and worst 10 companies analysis


--1a. Find the top ten companies that have had the most timed responses. 
--Similarly, find the bottom ten. 
create temporary table if not exists temp_timely as 
WITH CompanyResponseCounts AS (
    SELECT 
        cd.company, 
        COUNT(*) AS total_responses,
        COUNT(*) FILTER (WHERE cf.timely_response = 1) AS timely_response_count,
        COUNT(*) FILTER (WHERE cf.timely_response = 0) AS untimely_response_count
    FROM 
        complaint_fact cf
    JOIN 
        company_dimension cd ON cf.company_id = cd.company_id 
    GROUP BY 
        cd.company
    HAVING 
        COUNT(*) >= 1000
),
RankedCompanyResponseRatios AS (
    SELECT 
        company, 
        total_responses,
        CAST(timely_response_count AS DECIMAL) / total_responses AS timely_response_ratio,
        CAST(untimely_response_count AS DECIMAL) / total_responses AS untimely_response_ratio
    FROM 
        CompanyResponseCounts
)
SELECT 
    company, 
    timely_response_ratio,
    untimely_response_ratio,
    timely_rank,
    untimely_rank
FROM (
    SELECT 
        company, 
        timely_response_ratio,
        untimely_response_ratio,
        RANK() OVER (ORDER BY timely_response_ratio DESC) AS timely_rank,
        RANK() OVER (ORDER BY untimely_response_ratio DESC) AS untimely_rank
    FROM 
        RankedCompanyResponseRatios
) sq
WHERE 
    sq.timely_rank <= 10 OR sq.untimely_rank <= 10
ORDER BY 
    sq.untimely_rank;
	
--drop table temp_timely
select * from temp_timely

--1b. Now Find top and bottom 10 for who, the most responses were disputed. Fpr these companies, 
create temporary table if not exists temp_disputed as 
WITH CompanyResponseCounts AS (
    SELECT 
        cd.company, 
        COUNT(*) AS total_responses,
        COUNT(*) FILTER (WHERE cf.consumer_disputed = 1) AS disputed_count,
        COUNT(*) FILTER (WHERE cf.consumer_disputed = 0) AS undisputed_count
    FROM 
        complaint_fact cf
    JOIN 
        company_dimension cd ON cf.company_id = cd.company_id 
    GROUP BY 
        cd.company
    HAVING 
        COUNT(*) >= 1000
),
RankedCompanyResponseRatios AS (
    SELECT 
        company, 
        total_responses,
        CAST(disputed_count AS DECIMAL) / total_responses AS disputed_response_ratio,
        CAST(undisputed_count AS DECIMAL) / total_responses AS undisputed_response_ratio
    FROM 
        CompanyResponseCounts
)
SELECT 
    company, 
    disputed_response_ratio,
    undisputed_response_ratio,
    disputed_rank,
    undisputed_rank
FROM (
    SELECT 
        company, 
        disputed_response_ratio,
        undisputed_response_ratio,
        RANK() OVER (ORDER BY disputed_response_ratio DESC) AS disputed_rank,
        RANK() OVER (ORDER BY undisputed_response_ratio DESC) AS undisputed_rank
    FROM 
        RankedCompanyResponseRatios
) sq
WHERE 
    sq.disputed_rank <= 10 OR sq.undisputed_rank <= 10
ORDER BY 
    sq.undisputed_rank;
select * from temp_disputed
--1c. Extract all the companies from the above two questions and store them in a table
create temporary table if not exists temp_companies(company VARCHAR(255));
insert into temp_companies (company)
select distinct company
from (select *
from (select company from temp_timely union select company from temp_disputed
) sq1
) sq_final

select * from temp_companies

----------------------------------------------------------


--Q2 For these companies, ratio of timely responses and consumer_disputed_false

with CompanyStateInfo as 
(select 
cd.company, ld.state, cf.timely_response, cf.consumer_disputed
from complaint_fact cf
join company_dimension cd on cf.company_id=cd.company_id
join location_dimension ld on cf.location_id=ld.location_id
), GetCountPerState  AS (
SELECT 
    company, 
    state, 
	count(*) as total_cases,
    CAST(count(CASE WHEN timely_response = 1 THEN 1 END) AS DECIMAL) / count(*) AS timely_response_ratio,
    1-CAST(count(CASE WHEN consumer_disputed = 1 THEN 1 END) AS DECIMAL) / count(*) AS consumer_disputed_false
-- 	COUNT(CASE WHEN timely_response = 1 THEN 1 END) AS timely_response_true,
--     COUNT(CASE WHEN timely_response = 0 THEN 1 END) AS timely_response_false,
--     COUNT(CASE WHEN consumer_disputed = 1 THEN 1 END) AS consumer_disputed_true,
--     COUNT(CASE WHEN consumer_disputed = 0 THEN 1 END) AS consumer_disputed_false
FROM 
    CompanyStateInfo
GROUP BY 
    company, state
)
select cs.* from GetCountPerState cs
INNER JOIN temp_companies tc on tc.company=cs.company
order by cs.timely_response_ratio desc


--Q3 a. Create a view that gives the table you want to use. 
-- I will store it in a temporary table

create temporary table if not exists temp_cf as
select tcf.company, tcf.state, tcf.year, tcf.month, tcf.product, tcf.sub_product, 
tcf.issue, tcf.sub_issue, count(tcf.*) as total_cases, sum(tcf.timely_response) as timely_responses,
sum(tcf.consumer_disputed) as consumer_disputed
from (select cd.company, ld.state, cad.product, dd.year,dd.month,cad.sub_product, cad.issue, cad.sub_issue,
timely_response, consumer_disputed
from complaint_fact cf
join company_dimension cd on cd.company_id=cf.company_id
join location_dimension ld on cf.location_id=ld.location_id
join category_dimension cad on cf.category_id=cad.category_id
join date_dimension dd on cf.date_id_received=dd.date_id) tcf
join temp_companies tc on tc.company=tcf.company
group by  tcf.company,
    tcf.state,
	tcf.year, tcf.month,
    tcf.product,
    tcf.sub_product,
    tcf.issue,
    tcf.sub_issue;
	
select * from temp_cf
--3b. For the top 5 companies that have the best ratio of no customer disput, find the bottom 2 states
-- and from those states which the top 3 problems and issues
WITH Top5Companies AS (
    SELECT
        company,
        COUNT(*) AS total_cases,
        CAST(COUNT(CASE WHEN timely_responses = 1 THEN 1 END) AS DECIMAL) / COUNT(*) AS timely_response_ratio,
        1 - CAST(COUNT(CASE WHEN consumer_disputed = 1 THEN 1 END) AS DECIMAL) / COUNT(*) AS consumer_disputed_false
    FROM
        temp_cf
    GROUP BY
        company
    ORDER BY
        timely_response_ratio DESC
    LIMIT 5
), 
Top5Information AS (
    SELECT cf.*
    FROM temp_cf cf
    JOIN Top5Companies t5f ON cf.company = t5f.company
)
, RankedStates AS (
    SELECT
        sq.company,
        sq.state,
        CAST(COUNT(CASE WHEN sq.timely_responses = 1 THEN 1 END) AS DECIMAL) / COUNT(*) AS timely_response_ratio,
        ROW_NUMBER() OVER (PARTITION BY sq.company ORDER BY CAST(COUNT(CASE WHEN sq.timely_responses = 1 THEN 1 END) AS DECIMAL) / COUNT(*)) AS state_rank
    FROM (
        SELECT * FROM Top5Information
    ) sq
    GROUP BY sq.company, sq.state
)
, WeakestProduct AS (
    SELECT
        cf.company,
        cf.state,
        cf.product,
        CAST(COUNT(CASE WHEN cf.timely_responses = 1 THEN 1 END) AS DECIMAL) / COUNT(cf.*) AS timely_response_ratio,
		ROW_NUMBER() OVER (PARTITION BY cf.company, cf.state ORDER BY CAST(COUNT(CASE WHEN cf.timely_responses = 1 THEN 1 END) AS DECIMAL) / COUNT(cf.*) asc) AS product_rank
    FROM
        temp_cf cf
    JOIN Top5Information t5i ON cf.company = t5i.company AND cf.state = t5i.state
    GROUP BY
        cf.company, cf.state, cf.product
    ORDER BY
        timely_response_ratio asc
) , RankedIssues AS (
    SELECT
        company,
        state,
        product,
        issue,
        CAST(COUNT(CASE WHEN timely_responses = 1 THEN 1 END) AS DECIMAL) / COUNT(*) AS timely_response_ratio,
        ROW_NUMBER() OVER (PARTITION BY company, state, product ORDER BY CAST(COUNT(CASE WHEN timely_responses = 1 THEN 1 END) AS DECIMAL) / COUNT(*) ASC) AS issue_rank
    FROM
        temp_cf
    WHERE
        (company, state, product) IN (SELECT company, state, product FROM WeakestProduct WHERE product_rank <= 2)
    GROUP BY
        company, state, product, issue
)
SELECT * FROM RankedIssues
WHERE issue_rank <= 2 and timely_response_ratio <1;

--Q4 Find the states for which employed population is the lowest. In terms of percentage


--Q5 For the companies where timed responses were the lowest. 
--Find if there is a correlation between that and population
select * from population_fact



--- Functions----



---- To get company, state, category dimensions, timely response, consumer dispute
create temporary table if not exists temp_cf as
select cd.company, ld.state, cad.product, dd.year,dd.month,cad.sub_product, cad.issue, cad.sub_issue,
timely_response, consumer_disputed
from complaint_fact cf
join company_dimension cd on cd.company_id=cf.company_id
join location_dimension ld on cf.location_id=ld.location_id
join category_dimension cad on cf.category_id=cad.category_id
join date_dimension dd on cf.date_id_received=dd.date_id;

--drop table temp_cf
select * from temp_cf
