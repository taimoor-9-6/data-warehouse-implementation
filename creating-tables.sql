----------------------------------------------------------
---Data Preview---
select * from complaints
select * from demographics


----------------------------------------------------------
--- Creating Index ---

--Complaints--
CREATE INDEX idx_cc_date_received ON complaints ("Date received");
CREATE INDEX idx_cc_product ON complaints ("Product");
CREATE INDEX idx_cc_sub_product ON complaints ("Sub-product");
CREATE INDEX idx_cc_issue ON complaints ("Issue");
CREATE INDEX idx_cc_sub_issue ON complaints ("Sub-issue");
CREATE INDEX idx_cc_company ON complaints ("Company");
CREATE INDEX idx_cc_state ON complaints ("State");
CREATE INDEX idx_cc_date_sent_to_company ON complaints ("Date sent to company");
CREATE INDEX idx_cc_timely_response ON complaints ("Timely response?");
CREATE INDEX idx_cc_consumer_disputed ON complaints ("Consumer disputed?");
CREATE INDEX idx_cc_compalint_id ON complaints ("Complaint ID");

----------------------------------------------------------

----Dimensions----

--1. Location Dimension--
--creating table
create table location_dimension (
	location_id serial primary key,
	state text
);
--creating index
CREATE INDEX idx_state ON location_dimension(state);
--create constraint
ALTER TABLE location_dimension
ADD CONSTRAINT unique_state_constraint UNIQUE (state);

----------------

--2.Date Dimension--
-- Create date_dimension table
CREATE TABLE date_dimension (
    date_id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL
);
--Create indexes for better performance
CREATE INDEX date_dimension_year_index ON date_dimension(year);
CREATE INDEX date_dimension_month_index ON date_dimension(month);
CREATE INDEX date_dimension_day_index ON date_dimension(day);

----------------

--3. Yeaer Dimension --
-- Create year_dimension table
CREATE TABLE year_dimension (
    year_id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL
);
-- Create an index for better performance
CREATE INDEX year_dimension_year_index ON year_dimension(year);

----------------

--4.Company Dimension--
--Creating Table
create table company_dimension (company_id serial primary key, company text);
--Creating Index
CREATE INDEX idx_company ON company_dimension(company);
INSERT INTO company_dimension (Company) VALUES ('');
-- Add a unique constraint to the company column
ALTER TABLE company_dimension
ADD CONSTRAINT unique_company_constraint UNIQUE (company);
--drop table company_dimension

----------------

--5.Category Dimension--
--Create Table
create table category_dimension (
	category_id serial primary key, product text,
	sub_product text, issue text, sub_issue text);
--Create Index
CREATE INDEX idx_product ON category_dimension(product);
CREATE INDEX idx_sub_product ON category_dimension(sub_product);
CREATE INDEX idx_issue ON category_dimension(issue);
CREATE INDEX idx_sub_issue ON category_dimension(sub_issue);
-- Add a unique constraint to the combination of columns
ALTER TABLE category_dimension
ADD CONSTRAINT unique_category_constraint UNIQUE (product, sub_product, issue, sub_issue);
	
----------------

--6. Issue Dimension--
--Creating table
create table issue_dimension (issue_id serial primary key,
							  complain_number int,
							  consumer_complaint_narrative text, consumer_consent text,
							 submitted_via text,
							 consumer_consent_old text,
							effective_date DATE);
--constraint
ALTER TABLE issue_dimension
ADD CONSTRAINT unique_issue_constraint UNIQUE (complain_number);

--Creating Index
CREATE INDEX idx_consumer_complaint
ON issue_dimension ("complain_number");	
--drop table issue_dimension

----------------

--6. Resolution Dimension --

--creating table
create table resolution_dimension (resolution_id serial primary key,
								  complain_number int,
								  public_response text,
								  response_to_consumer text,
								  start_date date,
								  end_date date,
								  active boolean);
--creating index
CREATE INDEX idx_resolution_number
ON resolution_dimension ("complain_number");	
								  


----------------

--A. Population Fact --
-- Create population_fact table
CREATE TABLE population_fact (
    population_id SERIAL PRIMARY KEY,
    year_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    population_over_18 NUMERIC NOT NULL,
    population_over_65 NUMERIC NOT NULL,
    employed_population NUMERIC NOT NULL,
    unemployed_population NUMERIC NOT NULL,
    CONSTRAINT fk_population_fact_year
        FOREIGN KEY (year_id)
        REFERENCES year_dimension (year_id),
    CONSTRAINT fk_population_fact_location
        FOREIGN KEY (location_id)
        REFERENCES location_dimension (location_id)
);
-- Optional: Create indexes for better performance
CREATE INDEX population_fact_year_index ON population_fact(year_id);
CREATE INDEX population_fact_location_index ON population_fact(location_id);


----------------


--B. Complaint Fact--
--Create Complaint Fact Table
CREATE TABLE complaint_fact (
    complain_id SERIAL PRIMARY KEY,
    issue_id INT,
	resolution_id int,
    date_id_sent INT,
	date_id_received int, 
    category_id INT,
    company_id INT,
    location_id INT,
	timely_response int,
	consumer_disputed int,
    CONSTRAINT fk_issue FOREIGN KEY (issue_id) REFERENCES issue_dimension(issue_id),
    CONSTRAINT fk_resolution FOREIGN KEY (resolution_id) REFERENCES resolution_dimension(resolution_id),
    CONSTRAINT fk_date_sent FOREIGN KEY (date_id_sent) REFERENCES date_dimension(date_id),
    CONSTRAINT fk_date_received FOREIGN KEY (date_id_received) REFERENCES date_dimension(date_id),
    CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES category_dimension(category_id),
    CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES company_dimension(company_id),
    CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES location_dimension(location_id)
);
-- Creating Indexes
CREATE INDEX idx_ct_complain_id ON complaint_fact (complain_id);
CREATE INDEX idx_ct_issue_id ON complaint_fact (issue_id);
CREATE INDEX idx_ct_resolution_id ON complaint_fact (resolution_id);
CREATE INDEX idx_ct_date_sent ON complaint_fact (date_id_sent);
CREATE INDEX idx_ct_date_received ON complaint_fact (date_id_received);
CREATE INDEX idx_ct_category_id ON complaint_fact (category_id);
CREATE INDEX idx_ct_company_id ON complaint_fact (company_id);
CREATE INDEX idx_ct_location_id ON complaint_fact (location_id);

--drop table complaint_fact

----------------------------------------------------------

--Implementing Trigger
--Issue Dimension--
-- Creating a trigger function
CREATE OR REPLACE FUNCTION prevent_delete_issue()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Deleting records from issue_dimension is not allowed.';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Creating a trigger on issue dimension
CREATE TRIGGER prevent_delete_issue_trigger
BEFORE DELETE
ON issue_dimension
FOR EACH ROW
EXECUTE FUNCTION prevent_delete_issue();

--Create trigger on resolution dimension
CREATE TRIGGER prevent_delete_resolution_trigger
BEFORE DELETE
ON resolution_dimension
FOR EACH ROW
EXECUTE FUNCTION prevent_delete_issue();


----------------


--Resolution Dimension--
CREATE OR REPLACE function scd2_issue()
returns trigger 
AS $BODY$
	BEGIN
		update resolution_dimension 
		set end_date = current_date, active=false
		where complain_number=new.complain_number;
		return new;
	END;
$BODY$
LANGUAGE plpgsql;


CREATE TRIGGER scd2_resolution_trigger
before INSERT ON resolution_dimension
FOR EACH ROW
EXECUTE PROCEDURE scd2_issue();




----------------


-- Issue Dimension--
CREATE OR REPLACE FUNCTION public.scd3_issue_dimension()
RETURNS TRIGGER 
AS $BODY$
BEGIN
    -- Update the current record's end date and set the historical value
    UPDATE issue_dimension
    SET effective_date = current_date::DATE,
        consumer_consent_old = consumer_consent,
        consumer_consent = NEW.consumer_consent
    WHERE complain_number = NEW.complain_number
      AND OLD.consumer_consent IS DISTINCT FROM NEW.consumer_consent;

    RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;


-- Step 3: Create the SCD3 trigger
CREATE TRIGGER scd3_trigger_issue_dimension
BEFORE INSERT ON issue_dimension
FOR EACH ROW 
EXECUTE PROCEDURE scd3_issue_dimension();
