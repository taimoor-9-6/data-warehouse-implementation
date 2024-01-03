----------------------------------------------------------
--- Data Preview --- Staging Tables --
select * from complaints
select * from demographics

--temp
CREATE TABLE temp_table AS
SELECT *
FROM complaints
LIMIT 1000;

select * from temp_table
drop table temp_table

----------------------------------------------------------
--- Adding Data Dimension & Fact Tables ---

----------------

--1. Location Dimension--
-- Inserting initial data with conflict resolution
INSERT INTO location_dimension (state)
SELECT DISTINCT
    sq.state
FROM
    (SELECT DISTINCT state FROM demographics dd
    UNION
    SELECT DISTINCT "State" FROM complaints) sq
ON CONFLICT (state) DO UPDATE
SET state = EXCLUDED.state;
--View
select * from location_dimension;

----------------

--2.Date Dimension--
-- Generate and insert data into date_dimension table
INSERT INTO date_dimension (year, month, day)
SELECT
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    EXTRACT(DAY FROM d)::INTEGER
FROM generate_series('2000-01-01'::date, '2050-12-31'::date, '1 day'::interval) d
ON CONFLICT (year, month, day) DO NOTHING;
--View
SELECT * FROM date_dimension;

----------------


--3. Year Dimension --
-- Generate and insert data into year_dimension table
INSERT INTO year_dimension (year)
SELECT
    EXTRACT(YEAR FROM generate_series)::INTEGER
FROM generate_series('2000-01-01'::date, '2050-12-31'::date, '1 year'::interval)
ON CONFLICT (year) DO NOTHING;

--4.Company Dimension--
-- Insert data with ON CONFLICT
INSERT INTO company_dimension (company)
SELECT COALESCE(cc."Company", '')
FROM (SELECT DISTINCT "Company" FROM complaints) cc
ON CONFLICT (company) DO UPDATE
SET company = EXCLUDED.company;

--View
select * from company_dimension

----------------

--5.Category Dimension--
--Insert Data
INSERT INTO category_dimension (product, sub_product, issue, sub_issue)
SELECT DISTINCT
  COALESCE("Product", ''), 
  COALESCE("Sub-product", ''), 
  COALESCE("Issue", ''), 
  COALESCE("Sub-issue", '')
FROM complaints
ON CONFLICT (product, sub_product, issue, sub_issue) DO UPDATE
SET 
  product = EXCLUDED.product,
  sub_product = EXCLUDED.sub_product,
  issue = EXCLUDED.issue,
  sub_issue = EXCLUDED.sub_issue;
--View 
select * from category_dimension;
-------------------------------

--6. Issue Dimension--
--Insert data
-- Inserting data with SCD Type 2 implementation
INSERT INTO issue_dimension (complain_number, consumer_complaint_narrative, consumer_consent, submitted_via,
consumer_consent_old, effective_date)
SELECT 
    cc."Complaint ID",
    COALESCE("Consumer complaint narrative", ''),
    COALESCE("Consumer consent provided?", ''),
    COALESCE("Submitted via", ''),
    '',
    current_date::DATE
FROM complaints cc
ON CONFLICT (complain_number) DO nothing;

select * from issue_dimension;

-------------------------------
--7. Resolution Dimension--
--Insert data
INSERT INTO resolution_dimension (complain_number, public_response, response_to_consumer,
start_date, end_date, active)
SELECT 
    cc."Complaint ID",
    COALESCE("Company public response", ''),
    COALESCE("Company response to consumer", ''),
	current_date, '2099-12-31', true
FROM complaints cc;

select * from resolution_dimension;
-------------------------------

----------------

--A. Population Fact --

--Inserting data
INSERT INTO population_fact (year_id, location_id, population_over_18, population_over_65, employed_population, unemployed_population)
SELECT
    yd.year_id,
    ld.location_id,
    COALESCE(d."age.total.18_over", 0) as population_over_18, -- Assuming a default value of 0 for NULL
    COALESCE(d."age.total.65_over", 0) as population_over_65,
    COALESCE(d."unemployment.employed", 0) as employed_population,
    COALESCE(d."unemployment.unemployed", 0) as unemployed_population
FROM
    demographics d
JOIN
    year_dimension yd ON d."year" = yd.year
JOIN
    location_dimension ld ON d."state" = ld.state;

--View
select * from population_fact;

----------------

--B. Complaint Fact--
-- Add Data
INSERT INTO complaint_fact (issue_id, resolution_id,date_id_sent,date_id_received ,category_id, company_id, location_id,
						   timely_response, consumer_disputed)
SELECT
    i.issue_id,
	r.resolution_id,
    dd.date_id,
	ddd.date_id,
    cd.category_id,
    cod.company_id,
    ld.location_id,
	cc."Timely response?"::int,
    cc."Consumer disputed?"::int
FROM
    complaints cc --replace later with the actual table name
JOIN date_dimension dd ON dd.year = EXTRACT(YEAR FROM TO_DATE(cc."Date sent to company", 'YYYY-MM-DD'))
                     AND dd.month = EXTRACT(MONTH FROM TO_DATE(cc."Date sent to company", 'YYYY-MM-DD'))
                     AND dd.day = EXTRACT(DAY FROM TO_DATE(cc."Date sent to company", 'YYYY-MM-DD'))
JOIN date_dimension ddd ON ddd.year = EXTRACT(YEAR FROM cc."Date received")
                     AND ddd.month = EXTRACT(MONTH FROM cc."Date received")
                     AND ddd.day = EXTRACT(DAY FROM cc."Date received")
JOIN company_dimension cod ON COALESCE(cc."Company", '') = COALESCE(cod.company, '')
JOIN category_dimension cd ON COALESCE(cc."Product", '') = COALESCE(cd.product, '')
                             AND COALESCE(cc."Sub-product", '') = COALESCE(cd.sub_product, '')
                             AND COALESCE(cc."Issue", '') = COALESCE(cd.issue, '')
                             AND COALESCE(cc."Sub-issue", '') = COALESCE(cd.sub_issue, '')
JOIN location_dimension ld ON COALESCE(cc."State", '') = COALESCE(ld.state, '')
JOIN issue_dimension i ON cc."Complaint ID"=i.complain_number
join resolution_dimension r on cc."Complaint ID"=r.complain_number
where r.active=true;
--View
select * from complaint_fact
limit 1000;
-- DELETE FROM complaint_fact;
--drop table complaint_fact
select * from complaints

----------------------------------------------------------

--Final ables--
select * from location_dimension;
select * from date_dimension;
select * from year_dimension;
select * from company_dimension;
select * from category_dimension;
select * from issue_dimension;
select * from resolution_dimension;
select * from population_fact;
select * from complaint_fact;
----------------------------------------------------------
--For rough use--

--Dropping Tables--
drop table complaints;
drop table demographics;

---Dropping population tables
drop table population_fact;
drop table demographics;
drop table location_dimension;

---Dropping complaint tabls
drop table complaint_fact;
drop table issue_dimension;
drop table company_dimension;
drop table category_dimension;
drop table resolution_dimension;
