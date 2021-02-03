        {{ 
            config(
                materialized='view',
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "users") }}


         SELECT ID, INITCAP(first_name) AS FIRST_NAME, INITCAP(last_name) AS LAST_NAME, INITCAP(first_name) || ' ' || INITCAP(last_name) AS NAME, AGE, age AS AGE_TIER_INTEGER, age AS AGE_TIER_CLASSIC, age AS AGE_TIER_INTERVAL, age AS AGE_TIER_RELATIONAL, GENDER, LOWER(LEFT(gender,1)) AS GENDER_SHORT, ('https://docs.looker.com/assets/images/'||LOWER(LEFT(gender,1))||'.jpg') AS USER_IMAGE, EMAIL, ('https://docs.looker.com/assets/images/'||LOWER(LEFT(gender,1))||'.jpg') AS IMAGE_FILE, CITY, STATE, ZIP, CASE WHEN country = 'UK' THEN TRANSLATE(LEFT(zip,2),'0123456789','') END AS UK_POSTCODE, CASE WHEN country = 'UK' THEN 'United Kingdom' ELSE country END AS COUNTRY, longitude||','||latitude AS LOCATION, round(longitude,1)||','||round(latitude,1) AS APPROX_LOCATION, id AS HISTORY, TRAFFIC_SOURCE, lpad(cast(round(random() * 10000, 0) as char(4)), 4, '0') AS SSN FROM PUBLIC.users 