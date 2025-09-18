

CREATE OR REPLACE PROCEDURE sp_ingest_data_from_s3(v_file_path VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    v_iam_role TEXT := 'arn:aws:iam::891662393358:role/health-data-project-role';
    v_filename TEXT;
    v_target_table TEXT;
BEGIN
    -- Extract just the filename
    v_filename := REGEXP_SUBSTR(v_file_path, '[^/]+$');

    -- Decide target table based on filename pattern
    IF v_filename ILIKE 'DailyNurseStaffing%' THEN
        v_target_table := 'DailyNurseStaffing';
    ELSIF v_filename ILIKE 'ProviderInfo%' THEN
        v_target_table := 'ProviderInfo';
    ELSE
        RAISE EXCEPTION 'No matching table for file: %', v_filename;
    END IF;

    -- Load into the right Bronze table
    EXECUTE 'COPY bronze.' || v_target_table || '
             FROM ''' || v_file_path || '''
             IAM_ROLE ''' || v_iam_role || '''
             CSV
             IGNOREHEADER 1
             DELIMITER '',''
             QUOTE ''"''
             FILLRECORD
             TRUNCATECOLUMNS
             ACCEPTINVCHARS;';

    -- Record ingestion in manifest using MERGE with dummy WHEN MATCHED clause
    EXECUTE '
        MERGE INTO bronze.ingested_files
        USING (
            SELECT ''' || v_filename || ''' AS filename,
                   ''' || v_target_table || ''' AS table_name,
                   GETDATE() AS load_time
        ) source
        ON bronze.ingested_files.filename = source.filename
        WHEN MATCHED THEN
            UPDATE SET filename = bronze.ingested_files.filename
        WHEN NOT MATCHED THEN
            INSERT (filename, table_name, load_time)
            VALUES (source.filename, source.table_name, source.load_time);
    ';
END;
$$;


CREATE OR REPLACE PROCEDURE sp_generate_silver_provider_dim()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create silver table if not exists
    CREATE TABLE IF NOT EXISTS silver.providerdim (
        ccn VARCHAR,
        providername VARCHAR,
        provideraddress VARCHAR,
        city VARCHAR,
        state VARCHAR,
        zipcode INT,
        NumberOfBed INT,
        latitude DECIMAL(10,6),
        longitude DECIMAL(10,6),
        updated_at TIMESTAMP
    );

    -- Drop temp table if it exists
    DROP TABLE IF EXISTS stage_provider_dim;

    -- Create staging table with explicit column names
    CREATE TEMP TABLE stage_provider_dim (
        ccn VARCHAR,
        providername VARCHAR,
        provideraddress VARCHAR,
        city VARCHAR,
        state VARCHAR,
        zipcode INT,
        NumberOfBed INT,
        latitude DECIMAL(10,6),
        longitude DECIMAL(10,6),
        updated_at TIMESTAMP
    );

    -- Populate staging table
    INSERT INTO stage_provider_dim (
        ccn,
        providername,
        provideraddress,
        city,
        state,
        zipcode,
        numberofbed,
        latitude,
        longitude,
        updated_at
    )
    SELECT DISTINCT
        CMS_Certification_Number_CCN,
        Provider_Name,
        Provider_Address,
        City_Town,
        State,
        ZIP_Code,
        Number_of_Certified_Beds,
        Latitude,
        Longitude,
        CURRENT_TIMESTAMP
    FROM bronze.providerinfo;

    -- Update existing records
    UPDATE silver.providerdim AS d
    SET
        providername = s.providername,
        provideraddress = s.provideraddress,
        city = s.city,
        state = s.state,
        zipcode = s.zipcode,
        numberofbed=s.numberofbed,
        latitude = s.latitude,
        longitude = s.longitude,
        updated_at = CURRENT_TIMESTAMP
    FROM stage_provider_dim s
    WHERE d.ccn = s.ccn;

    -- Insert new records
    INSERT INTO silver.providerdim (
        ccn,
        providername,
        provideraddress,
        city,
        state,
        zipcode,
        numberofbed,
        latitude,
        longitude,
        updated_at
    )
    SELECT
        s.ccn,
        s.providername,
        s.provideraddress,
        s.city,
        s.state,
        s.zipcode,
        s.numberofbed,
        s.latitude,
        s.longitude,
        CURRENT_TIMESTAMP
    FROM stage_provider_dim s
    LEFT JOIN silver.providerdim d ON s.ccn = d.ccn
    WHERE d.ccn IS NULL;
END;
$$;


CREATE OR REPLACE PROCEDURE sp_generate_silver_staffingtype_dim()
LANGUAGE plpgsql
AS $$

BEGIN
    

    -- Optional: create silver table if not exists
    CREATE TABLE IF NOT EXISTS silver.StaffingTypeDim (
         StaffingTypeID INT,
         Title VARCHAR,
         StaffingType VARCHAR,
         Updated_At TIMESTAMP
    );

    --DROP TEMP TABLE
    -- Drop temp table if it exists
    DROP TABLE IF EXISTS stage_staffingtype_dim;
    
    -- Upsert using staging table
    CREATE TEMP TABLE stage_staffingtype_dim AS
    (
        SELECT 1 AS StaffingTypeID,'Full Time Registered Nurse' AS Title,'Full Time' AS StaffingType,CURRENT_TIMESTAMP AS Updated_At
        UNION
        SELECT 2 AS StaffingTypeID,'Contract Registered Nurse' AS Title,'Contractor' AS StaffingType,CURRENT_TIMESTAMP AS Updated_At
        UNION
        SELECT 3 AS StaffingTypeID,'Full Time CNA' AS Title,'Full Time' AS StaffingType,CURRENT_TIMESTAMP AS Updated_At
        UNION
        SELECT 4 AS StaffingTypeID,'Contract CNA' AS Title,'Contractor' AS StaffingType,CURRENT_TIMESTAMP AS Updated_At
    );

    -- Update existing records
    UPDATE silver.StaffingTypeDim AS d
    SET
        Title = s.Title,
        StaffingType=s.StaffingType,
        Updated_At = CURRENT_TIMESTAMP
    FROM stage_staffingtype_dim s
    WHERE d.StaffingTypeID = s.StaffingTypeID;

    -- Insert new records
    INSERT INTO silver.StaffingTypeDim (
        StaffingTypeID ,
        Title ,
        StaffingType,
        Updated_At)
    SELECT 
        s.StaffingTypeID, 
        s.Title, 
        s.StaffingType,
        CURRENT_TIMESTAMP
    FROM stage_staffingtype_dim s 
    LEFT JOIN silver.StaffingTypeDim d ON s.StaffingTypeID = d.StaffingTypeID
    WHERE d.StaffingTypeID IS NULL;
    
    
END;
$$;

CREATE OR REPLACE PROCEDURE sp_generate_silver_fact_table()
LANGUAGE plpgsql
AS $$

BEGIN
    -- Optional: create silver table if not exists
    CREATE TABLE IF NOT EXISTS silver.DailyFacilityLogFact (
        CCN VARCHAR,
        WorkDateID VARCHAR,
        NumOfPatient INTEGER,
        StaffingTypeID VARCHAR,
        WorkHours DECIMAL, 
        updated_at TIMESTAMP
    );

    --DROP TEMP TABLE
    -- Drop temp table if it exists
    DROP TABLE IF EXISTS stage_fact_table;
    
    -- Upsert using staging table
    CREATE TEMP TABLE stage_fact_table AS
    SELECT *
    FROM (
        SELECT 
            PROVNUM AS CCN, 
            WorkDate AS WorkDateID, 
            MDScensus AS NumOfPatient, 
            '1' AS StaffingTypeID, 
            Hrs_RNDON_emp AS WorkHours,
            CURRENT_TIMESTAMP AS Updated_At
        FROM BRONZE.DailyNurseStaffing
        UNION
        SELECT 
            PROVNUM, 
            WorkDate, 
            MDScensus, 
            '2', 
            Hrs_RNDON_ctr, 
            CURRENT_TIMESTAMP
        FROM BRONZE.DailyNurseStaffing
        UNION
        SELECT PROVNUM, 
            WorkDate,
            MDScensus, 
            '3', 
            Hrs_CNA_emp, 
            CURRENT_TIMESTAMP
        FROM BRONZE.DailyNurseStaffing
        UNION
        SELECT 
            PROVNUM, 
            WorkDate,
             MDScensus, 
             '4', 
             Hrs_CNA_ctr, 
             CURRENT_TIMESTAMP
        FROM BRONZE.DailyNurseStaffing
    ) AS unioned_data
    ORDER BY WorkDateID ASC;



    -- Update existing records
   UPDATE silver.DailyFacilityLogFact AS d
    SET
        NumOfPatient = CAST(s.NumOfPatient AS INTEGER),
    	WorkHours = CAST(s.WorkHours AS DECIMAL),
        updated_at = CURRENT_TIMESTAMP
    FROM stage_fact_table s
    WHERE d.CCN = s.CCN AND d.WorkDateID = s.WorkDateID AND d.StaffingTypeID = s.StaffingTypeID;


    -- Insert new records
    INSERT INTO silver.DailyFacilityLogFact 
    (CCN,WorkDateID, NumOfPatient, StaffingTypeID,WorkHours,Updated_At)
    SELECT 
        s.CCN,
    	s.WorkDateID,
    	CAST(s.NumOfPatient AS INTEGER),
    	s.StaffingTypeID,
    	CAST(s.WorkHours AS DECIMAL),
        CURRENT_TIMESTAMP
    FROM stage_fact_table s 
    LEFT JOIN silver.DailyFacilityLogFact d ON d.CCN = s.CCN AND d.WorkDateID = s.WorkDateID AND d.StaffingTypeID = s.StaffingTypeID
    WHERE d.CCN IS NULL AND d.WorkDateID IS NULL AND d.StaffingTypeID IS NULL;
END
$$;

CREATE OR REPLACE PROCEDURE sp_generate_silver_workdate_dim()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create silver table if not exists
    CREATE TABLE IF NOT EXISTS silver.WorkDateDim (
        WorkDateID INTEGER,
        WorkDate DATE,
        Month INTEGER,
        MonthName VARCHAR,
        Year INTEGER,
        Quarter INTEGER,
        updated_at TIMESTAMP
    );

    -- Drop temp table if it exists
    DROP TABLE IF EXISTS stage_workdate_dim;

    -- Create staging table with correct PostgreSQL syntax
    CREATE TEMP TABLE stage_workdate_dim AS
    SELECT DISTINCT 
        CAST(WorkDate AS INTEGER) AS WorkDateID,
        CAST(WorkDate AS DATE) AS WorkDate,
        EXTRACT(MONTH FROM WorkDate::DATE) AS Month,
        TO_CHAR(WorkDate::DATE, 'Month') AS MonthName,
        EXTRACT(YEAR FROM WorkDate::DATE) AS Year,
        EXTRACT(QUARTER FROM WorkDate::DATE) AS Quarter,
        CURRENT_TIMESTAMP AS updated_at
    FROM BRONZE.DailyNurseStaffing;

    -- Update existing records
    UPDATE silver.WorkDateDim AS d
    SET
        WorkDate = s.WorkDate,
        Month = s.Month,
        MonthName = s.MonthName,
        Year = s.Year,
        Quarter = s.Quarter,
        updated_at = CURRENT_TIMESTAMP
    FROM stage_workdate_dim s
    WHERE d.WorkDateID = s.WorkDateID;

    -- Insert new records
    INSERT INTO silver.WorkDateDim 
    (WorkDateID, WorkDate, Month, MonthName, Year, Quarter, updated_at)
    SELECT 
        s.WorkDateID,
        s.WorkDate,
        s.Month,
        s.MonthName,
        s.Year,
        s.Quarter,
        CURRENT_TIMESTAMP
    FROM stage_workdate_dim s
    LEFT JOIN silver.WorkDateDim d ON d.WorkDateID = s.WorkDateID
    WHERE d.WorkDateID IS NULL;
END
$$;


CREATE OR REPLACE PROCEDURE sp_generate_gold_provider_staffing_utilization_metric()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create gold table if not exists
    CREATE TABLE IF NOT EXISTS gold.provider_staffing_utilization_metric (
        ccn VARCHAR(10),                     -- Provider identifier
        providername VARCHAR(255),          -- Facility name
        city VARCHAR(100),                  -- City location
        state VARCHAR(2),                   -- State abbreviation
        title VARCHAR(100),                 -- Staffing role/title
        staffingtype VARCHAR(100),          -- Staffing category
        totalworkhour DECIMAL(10,2),        -- Sum of work hours
        bedutilizationrate DECIMAL(10,2),   -- Bed utilization percentage
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Optional: truncate or refresh logic
    DELETE FROM gold.provider_staffing_utilization_metric;

    -- Insert aggregated metrics
    INSERT INTO gold.provider_staffing_utilization_metric (
        ccn,
		month,
		monthname,
		year,
        providername,
        city,
        state,
		longitude,
		latitude,
        title,
        staffingtype,
        totalworkhour,
        bedutilizationrate,
        updated_at
    )
    SELECT
        p.ccn,
        d.month,
        d.monthname,
        d.year,
        p.providername,
        p.city,
        p.state,
        p.longitude,
        p.latitude,
        s.title,
        s.staffingtype,
        SUM(f.workhours) AS totalworkhour,
        CAST(
            AVG(f.numofpatient::DECIMAL(10,2) / NULLIF(p.numberofbed, 0)) * 100
            AS DECIMAL(10,2)
        ) AS bedutilizationrate,
        CURRENT_TIMESTAMP
    FROM silver.dailyfacilitylogfact f
    INNER JOIN silver.providerdim p ON f.ccn = p.ccn
    INNER JOIN silver.staffingtypedim s ON f.staffingtypeid = s.staffingtypeid
    inner join  silver.workdatedim d on d.workdateid=f.workdateid
    GROUP BY p.ccn, p.providername, p.city, p.state, s.title, s.staffingtype,d.month,d.monthname,d.year,p.longitude,p.latitude;
END;
$$;


