        {{ 
            config(
                materialized='view',
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "user_order_facts") }}


        