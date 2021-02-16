        {{ 
            config(
                materialized='view',
                ) 
        }}

        {{
            config(
                tags = ["@@VIEWTYPE@@"]
            )
        }}

        --these need to change by converter

        {{ config(schema = "@@SCHEMA@@") }}
        {{ config(alias = "@@ALIAS@@") }}


        @@SQL@@