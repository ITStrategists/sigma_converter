        {{ 
            config(
                materialized='view',
                ) 
        }}

        {{
            config(
                tags = ["VIEW"]
            )
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "users") }}


         SELECT ID, INITCAP(first_name) AS FIRST_NAME, INITCAP(last_name) AS LAST_NAME, INITCAP(first_name) || ' ' || INITCAP(last_name) AS NAME, ((CASE WHEN round(latitude,1) = latitude AND round(longitude,1) = longitude THEN 0 ELSE ACOS(SIN(RADIANS(round(latitude,1) )) * SIN(RADIANS(round(latitude,1))) + COS(RADIANS(round(latitude,1) )) * COS(RADIANS(round(latitude,1))) * COS(RADIANS(round(longitude,1) - round(longitude,1) ))) * 6371 END) / 1.60934) AS DISTANCE_TO_PICKUP, ((CASE WHEN round(latitude,1) = latitude AND round(longitude,1) = longitude THEN 0 ELSE ACOS(SIN(RADIANS(round(latitude,1) )) * SIN(RADIANS(round(latitude,1))) + COS(RADIANS(round(latitude,1) )) * COS(RADIANS(round(latitude,1))) * COS(RADIANS(round(longitude,1) - round(longitude,1) ))) * 6371 END) / 0.001) AS DISTANCE_TO_PICKUP_METERS, ((CASE WHEN round(latitude,1) = latitude AND round(longitude,1) = longitude THEN 0 ELSE ACOS(SIN(RADIANS(round(latitude,1) )) * SIN(RADIANS(round(latitude,1))) + COS(RADIANS(round(latitude,1) )) * COS(RADIANS(round(latitude,1))) * COS(RADIANS(round(longitude,1) - round(longitude,1) ))) * 6371 END) / 0.0003048) AS DISTANCE_TO_PICKUP_FEET, ((CASE WHEN round(latitude,1) = latitude AND round(longitude,1) = longitude THEN 0 ELSE ACOS(SIN(RADIANS(round(latitude,1) )) * SIN(RADIANS(round(latitude,1))) + COS(RADIANS(round(latitude,1) )) * COS(RADIANS(round(latitude,1))) * COS(RADIANS(round(longitude,1) - round(longitude,1) ))) * 6371 END) / 0.001) AS DISTANCE_TO_PICKUP_KILOMETERS, ((CASE WHEN round(latitude,1) = latitude AND round(longitude,1) = longitude THEN 0 ELSE ACOS(SIN(RADIANS(round(latitude,1) )) * SIN(RADIANS(round(latitude,1))) + COS(RADIANS(round(latitude,1) )) * COS(RADIANS(round(latitude,1))) * COS(RADIANS(round(longitude,1) - round(longitude,1) ))) * 6371 END) / 1.60934) AS DISTANCE_TO_PICKUP_NAUTICAL_MILES, ((CASE WHEN round(latitude,1) = latitude AND round(longitude,1) = longitude THEN 0 ELSE ACOS(SIN(RADIANS(round(latitude,1) )) * SIN(RADIANS(round(latitude,1))) + COS(RADIANS(round(latitude,1) )) * COS(RADIANS(round(latitude,1))) * COS(RADIANS(round(longitude,1) - round(longitude,1) ))) * 6371 END) / 0.0009144) AS DISTANCE_TO_PICKUP_YARDS, AGE, age AS AGE_TIER_INTEGER, age AS AGE_TIER_CLASSIC, age AS AGE_TIER_INTERVAL, age AS AGE_TIER_RELATIONAL, GENDER, LOWER(LEFT(gender,1)) AS GENDER_SHORT, ('https://docs.looker.com/assets/images/'||LOWER(LEFT(gender,1))||'.jpg') AS USER_IMAGE, EMAIL, ('https://docs.looker.com/assets/images/'||LOWER(LEFT(gender,1))||'.jpg') AS IMAGE_FILE, CITY, STATE, ZIP, CASE WHEN country = 'UK' THEN TRANSLATE(LEFT(zip,2),'0123456789','') END AS UK_POSTCODE, CASE WHEN country = 'UK' THEN 'United Kingdom' ELSE country END AS COUNTRY, longitude||','||latitude AS LOCATION, round(longitude,1)||','||round(latitude,1) AS APPROX_LOCATION, id AS HISTORY, TRAFFIC_SOURCE, lpad(cast(round(random() * 10000, 0) as char(4)), 4, '0') AS SSN FROM PUBLIC.users 