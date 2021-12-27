class SqlQueries1:
    immig_table_insert = ("""
        SELECT
            cicid,
            i94yr,
	        i94mon,
            i94cit,
            i94res,
            i94port,
            arrdate,
            i94mode,
            i94addr,
            depdate,
            i94bir,
            i94visa,
            count, dtadfile, visapost,occup,
            entdepa,entdepd,entdepu,matflag,
            biryear,dtaddto,gender,insnum,
            airline, admnum,fltno,visatype
            FROM  staging_immig
            
    """)

    demog_table_insert = ("""
         SELECT  City, State, 'Median Age' AS median_age, 
         'Male Population' AS male_population, 'Female Population' AS female_population, 
         'Total Population' AS total_population, 'Foreign-born' AS foreign_born, 
         'Average Household Size' AS average_household_size, 'State Code' AS state_code,
         Race, Count
         FROM staging_demographics
    """)

    airports_table_insert = ("""
        SELECT TRIM(ident) AS ident, type, name, elevation_ft, SUBSTR(iso_region, 4) AS state, 
        TRIM(UPPER(municipality)) AS municipality, iata_code
        FROM staging_airports
    """)

   
    time_table_insert = ("""
        SELECT date, YEAR(date) AS year, MONTH(date) AS month, DAY(date) AS day, WEEKOFYEAR(date) AS week, 
        DAYOFWEEK(date) as weekday, DAYOFYEAR(date) year_day
        FROM staging_time
        
    """)