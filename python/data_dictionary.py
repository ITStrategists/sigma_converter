import looker_sdk
import logging
import json
import re
import pandas as pd
from itertools import chain

logging.basicConfig(filename='data_dictionary.log',level=logging.INFO, filemode='w', format = '%(asctime)s:%(levelname)s:%(message)s')

total_models = 0
total_explores = 0
total_dimensions = 0
total_measures = 0
total_sets = 0
total_filteres = 0
total_joins = 0
total_connections = 0
total_dashboards = 0
total_datagroups = 0
total_groups = 0
total_users_in_groups = 0
total_users = 0
total_roles = 0
total_user_attributes = 0
total_views = 0
total_always_filter = 0
views = []
distinct_views = []
total_locales = 0
total_projects = 0
total_spaces = 0
total_liquid_dimensions = 0
total_dashboard_elements = 0
total_dynamic_fields = 0
total_looks = 0
total_aliases = 0

sdk = looker_sdk.init31('looker.ini')

models = sdk.all_lookml_models(fields= 'name, label, project_name, allowed_db_connection_names,has_content,can, unlimited_db_connections, explores')
model_DictList = []
explore_DictList = []
aliases_DictList = []
sets_DictList = []
dimension_DictList = []
parameter_DictList = []
joins_DictList = []
measure_type_DictList = []
always_filter_DictList = []
filters_DictList = []
liquid_dimension_DictList = []
view_DictList = []
user_attr_DictList = []
userDictList = []
spacesDictList = []
r_users_DictList = []
group_DictList = []
projects_DictList = []
looks_DictList = []
s_looks_DictList = []
locales_DictList = []
roles_DictList = []
data_groups_DictList = []
groups_DictList = []
dashboardDictList = []
conDictList = []
dashElementDictList = []
dash_DictList = []
measure_DictList = []


#----------------------------------------- Explore Model ~Start -------------------------------------------------------
for model in models:

    if model.name == "system__activity":
        continue
    modelDic = {
        "name":model.name,
        "label":model.label,
        "has_content":model.has_content,
        "project_name":model.project_name,
        "unlimited_db_connections":model.unlimited_db_connections,
        "allowed_db_connection_names":model.allowed_db_connection_names,
        "explores":model.explores,
        "can_index" : None if 'index' not in model.can else model.can["index"],
        "can_show" : None if 'show' not in model.can else model.can["show"],
        "can_create" : None if 'create' not in model.can else model.can["create"],
        "can_update" : None if 'update' not in model.can else model.can["update"],
        "can_destroy" : None if 'destroy' not in model.can else model.can["destroy"],
    }
    model_DictList.append(modelDic)
    total_models = total_models + 1
        
#----------------------------------------- explore  ~start ---------------------------------------------------------

    for explore in model.explores:

        if model.name == 'snowflake_poc':
            continue

        print("Trying ... {}:{}".format(model.name, explore.name))

        exploreObj = sdk.lookml_model_explore(lookml_model_name=model.name,explore_name=explore.name)
        print(exploreObj.id)
        exploreObj.aliases
        exploredict = {   
            "id":exploreObj.id,
            "model_name":model.name,
            "name":exploreObj.name,
            "description":exploreObj.description,
            "label":exploreObj.label,
            "scopes":exploreObj.scopes,
            "can_total":exploreObj.can_total,
            "can_save":exploreObj.can_save,
            "can_explain":exploreObj.can_explain,
            "can_pivot_in_db":exploreObj.can_pivot_in_db,
            "can_subtotal":exploreObj.can_subtotal,
            "has_timezone_support":exploreObj.has_timezone_support,
            "supports_cost_estimate":exploreObj.supports_cost_estimate,
            "connection_name":exploreObj.connection_name,
            "null_sort_treatment":exploreObj.null_sort_treatment,
            "files":exploreObj.files,
            "source_file":exploreObj.source_file,
            "project_name":exploreObj.project_name,
            "view_name":exploreObj.view_name,
            "hidden":exploreObj.hidden,
            "sql_table_name":exploreObj.sql_table_name,
            "access_filter_fields":exploreObj.access_filter_fields,
            "access_filters":exploreObj.access_filters,
            "group_label":exploreObj.group_label,
            "tags":exploreObj.tags
        }   
        explore_DictList.append(exploredict)
       
        total_explores = total_explores + 1

        for alias in exploreObj.aliases:
            aliasesdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id,   
                "name":alias.name,
                "value":alias.value,
            }
            aliases_DictList.append(aliasesdict)
            total_aliases = total_aliases + 1

#----------------------------------------- set  ~start ---------------------------------------------------------
        for set in exploreObj.sets:
            setsdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id,
                "name":set.name,
                "value":set.value,
            }
            sets_DictList.append(setsdict)
            total_sets = total_sets + 1

#----------------------------------------- set ~End ---------------------------------------------------------

#----------------------------------------- Dimension ~start ---------------------------------------------------------

        for dimension in exploreObj.fields.dimensions:
            #print(dimension)
            dimensionsdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id, 
                "align":dimension.align,
                "can_filter":dimension.can_filter,
                "category":dimension.category,
                "default_filter_value":dimension.default_filter_value,
                "description":dimension.description,
                "enumerations":dimension.enumerations,
                "error":dimension.error,
                "field_group_label":dimension.field_group_label,
                "field_group_variant":dimension.field_group_variant,
                "fill_style":dimension.fill_style,
                "fiscal_month_offset":dimension.fiscal_month_offset,
                "has_allowed_values":dimension.has_allowed_values,
                "hidden":dimension.hidden,
                "is_filter":dimension.is_filter,
                "is_fiscal":dimension.is_fiscal,
                "is_numeric":dimension.is_numeric,
                "is_timeframe":dimension.is_timeframe,
                "can_time_filter":dimension.can_time_filter,
                "time_interval":dimension.time_interval,
                "label":dimension.label,
                "label_from_parameter":dimension.label_from_parameter,
                "label_short":dimension.label_short,
                "lookml_link":dimension.lookml_link,
                "map_layer":dimension.map_layer,
                "measure":dimension.measure,
                "name":dimension.name,
                "strict_value_format":dimension.strict_value_format,
                "parameter":dimension.parameter,
                "permanent":dimension.permanent,
                "primary_key":dimension.primary_key,
                "project_name":dimension.project_name,
                "requires_refresh_on_sort":dimension.requires_refresh_on_sort,
                "scope":dimension.scope,
                "sortable":dimension.sortable,
                "source_file":dimension.source_file,
                "source_file_path":dimension.source_file_path,
                "sql":dimension.sql,
                "sql_case":dimension.sql_case,
                "filters":dimension.filters,
                "suggest_dimension":dimension.suggest_dimension,
                "suggest_explore":dimension.suggest_explore,
                "suggestable":dimension.suggestable,
                "suggestions":dimension.suggestions,
                "tags":dimension.tags,
                "type":dimension.type,
                "user_attribute_filter_types":dimension.user_attribute_filter_types,
                "value_format":dimension.value_format,
                "view":dimension.view,
                "view_label":dimension.view_label,
                "dynamic":dimension.dynamic,
                "week_start_day":dimension.week_start_day,
            }
            dimension_DictList.append(dimensionsdict)
            total_dimensions = total_dimensions + 1
            liquidCondition = re.search(r'\{%.*%\}', dimension.sql)        
            if liquidCondition:
                total_liquid_dimensions = total_liquid_dimensions + 1
                liquid_dimension_DictList.append(dimensionsdict)  
            liquidTemplate = re.search(r'\{\{.*\}\}', dimension.sql)        
            if liquidTemplate:
                total_liquid_dimensions = total_liquid_dimensions + 1
                liquid_dimension_DictList.append(dimensionsdict)
           # if dimension.measure == True or str(dimension.measure) == 'TRUE':
             #   measure_DictList.append(dimensionsdict)
            #    

#----------------------------------------- dimension  ~End ---------------------------------------------------------             

#----------------------------------------- measure  ~start ---------------------------------------------------------               
        for dimension in exploreObj.fields.dimensions: 
            viewsdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id,
                "view_name" :dimension.source_file_path,
            }
            view_DictList.append(viewsdict)
            total_views = total_views + 1
            #print(str(viewsdict))
            for val in viewsdict.values():
                if val in distinct_views:
                    continue
                else:
                    distinct_views.append(val)
        for measure in exploreObj.fields.measures: 
            dimensionsdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id, 
                "align":measure.align,
                "can_filter":measure.can_filter,
                "category":measure.category,
                "default_filter_value":measure.default_filter_value,
                "description":measure.description,
                "enumerations":measure.enumerations,
                "error":measure.error,
                "field_group_label":measure.field_group_label,
                "field_group_variant":measure.field_group_variant,
                "fill_style":measure.fill_style,
                "fiscal_month_offset":measure.fiscal_month_offset,
                "has_allowed_values":measure.has_allowed_values,
                "hidden":measure.hidden,
                "is_filter":measure.is_filter,
                "is_fiscal":measure.is_fiscal,
                "is_numeric":measure.is_numeric,
                "is_timeframe":measure.is_timeframe,
                "can_time_filter":measure.can_time_filter,
                "time_interval":measure.time_interval,
                "label":measure.label,
                "label_from_parameter":measure.label_from_parameter,
                "label_short":measure.label_short,
                "lookml_link":measure.lookml_link,
                "map_layer":measure.map_layer,
                "name":measure.name,
                "strict_value_format":measure.strict_value_format,
                "parameter":measure.parameter,
                "permanent":measure.permanent,
                "primary_key":measure.primary_key,
                "project_name":measure.project_name,
                "requires_refresh_on_sort":measure.requires_refresh_on_sort,
                "scope":measure.scope,
                "sortable":measure.sortable,
                "source_file":measure.source_file,
                "source_file_path":measure.source_file_path,
                "sql":measure.sql,
                "sql_case":measure.sql_case,
                "filters":measure.filters,
                "suggest_dimension":measure.suggest_dimension,
                "suggest_explore":measure.suggest_explore,
                "suggestable":measure.suggestable,
                "suggestions":measure.suggestions,
                "tags":measure.tags,
                "type":measure.type,
                "user_attribute_filter_types":measure.user_attribute_filter_types,
                "value_format":measure.value_format,
                "view":measure.view,
                "view_label":measure.view_label,
                "dynamic":measure.dynamic,
                "week_start_day":measure.week_start_day,
            }
            measure_DictList.append(dimensionsdict)
            total_measures = total_measures + 1 
#----------------------------------------- measure  ~End ---------------------------------------------------------

#----------------------------------------- always filter  ~start ---------------------------------------------------------
        for always_filter_item in exploreObj.always_filter:
            alwaysfilterdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id,   
                "name":always_filter_item.name,
                "value":always_filter_item.value,
            }
            always_filter_DictList.append(alwaysfilterdict)
            total_always_filter = total_always_filter + 1

#----------------------------------------- always filter  ~End ---------------------------------------------------------
        

#----------------------------------------- filters ~start ---------------------------------------------------------
        for filter in exploreObj.fields.filters:
            filterdict = {
               "model_name":model.name,
                "explore_id":exploreObj.id, 
                "align":filter.align,
                "can_filter":filter.can_filter,
                "category":filter.category,
                "default_filter_value":filter.default_filter_value,
                "description":filter.description,
                "enumerations":filter.enumerations,
                "error":filter.error,
                "field_group_label":filter.field_group_label,
                "field_group_variant":filter.field_group_variant,
                "fill_style":filter.fill_style,
                "fiscal_month_offset":filter.fiscal_month_offset,
                "has_allowed_values":filter.has_allowed_values,
                "hidden":filter.hidden,
                "is_filter":filter.is_filter,
                "is_fiscal":filter.is_fiscal,
                "is_numeric":filter.is_numeric,
                "is_timeframe":filter.is_timeframe,
                "can_time_filter":filter.can_time_filter,
                "time_interval":filter.time_interval,
                "label":filter.label,
                "label_from_parameter":filter.label_from_parameter,
                "label_short":filter.label_short,
                "lookml_link":filter.lookml_link,
                "map_layer":filter.map_layer,
                "measure":filter.measure,
                "name":filter.name,
                "strict_value_format":filter.strict_value_format,
                "parameter":filter.parameter,
                "permanent":filter.permanent,
                "primary_key":filter.primary_key,
                "project_name":filter.project_name,
                "requires_refresh_on_sort":filter.requires_refresh_on_sort,
                "scope":filter.scope,
                "sortable":filter.sortable,
                "source_file":filter.source_file,
                "source_file_path":filter.source_file_path,
                "sql":filter.sql,
                "sql_case":filter.sql_case,
                "suggest_explore":filter.suggest_explore,
                "suggestable":filter.suggestable,
                "suggestions":filter.suggestions,
                "tags":filter.tags,
                "type":filter.type,
                "user_attribute_filter_types":filter.user_attribute_filter_types,
                "value_format":filter.value_format,
                "view":filter.view,
                "view_label":filter.view_label,
                "dynamic":filter.dynamic,
                "week_start_day":filter.week_start_day,
            }
            filters_DictList.append(filterdict)
            total_filteres = total_filteres + 1 
 #----------------------------------------- filters  ~End ---------------------------------------------------------               
 
 #----------------------------------------- parameter  ~start ---------------------------------------------------------
        for parameter in exploreObj.fields.parameters:
            
            parametersdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id, 
                "align":parameter.align,
                "can_filter":parameter.can_filter,
                "category":parameter.category,
                "default_filter_value":parameter.default_filter_value,
                "description":parameter.description,
                "enumerations":parameter.enumerations,
                "error":parameter.error,
                "field_group_label":parameter.field_group_label,
                "field_group_variant":parameter.field_group_variant,
                "fill_style":parameter.fill_style,
                "fiscal_month_offset":parameter.fiscal_month_offset,
                "has_allowed_values":parameter.has_allowed_values,
                "hidden":parameter.hidden,
                "is_filter":parameter.is_filter,
                "is_fiscal":parameter.is_fiscal,
                "is_numeric":parameter.is_numeric,
                "is_timeframe":parameter.is_timeframe,
                "can_time_filter":parameter.can_time_filter,
                "time_interval":parameter.time_interval,
                "label":parameter.label,
                "label_from_parameter":parameter.label_from_parameter,
                "label_short":parameter.label_short,
                "lookml_link":parameter.lookml_link,
                "map_layer":parameter.map_layer,
                "measure":parameter.measure,
                "name":parameter.name,
                "strict_value_format":parameter.strict_value_format,
                "parameter":parameter.parameter,
                "permanent":parameter.permanent,
                "primary_key":parameter.primary_key,
                "project_name":parameter.project_name,
                "requires_refresh_on_sort":parameter.requires_refresh_on_sort,
                "scope":parameter.scope,
                "sortable":parameter.sortable,
                "source_file":parameter.source_file,
                "source_file_path":parameter.source_file_path,
                "sql":parameter.sql,
                "sql_case":parameter.sql_case,
                "filters":parameter.filters,
                "suggest_dimension":parameter.suggest_dimension,
                "suggest_explore":parameter.suggest_explore,
                "suggestable":parameter.suggestable,
                "suggestions":parameter.suggestions,
                "tags":parameter.tags,
                "type":parameter.type,
                "user_attribute_filter_types":parameter.user_attribute_filter_types,
                "value_format":parameter.value_format,
                "view":parameter.view,
                "view_label":parameter.view_label,
                "dynamic":parameter.dynamic,
                "week_start_day":parameter.week_start_day,
            }
            parameter_DictList.append(parametersdict)

#----------------------------------------- parameter  ~End ---------------------------------------------------------


#----------------------------------------- join  ~start ---------------------------------------------------------
        for join in exploreObj.joins:
            joinsdict =   {
                "model_name":model.name,
                "explore_id":exploreObj.id, 
                "name":join.name,
                "dependent_fields":join.dependent_fields,
                "fields":join.fields,
                "foreign_key":join.foreign_key,
                "from_":join.from_,
                "outer_only":join.outer_only,
                "relationship":join.relationship,
                "required_joins":join.required_joins,
                "sql_foreign_key":join.sql_foreign_key,
                "sql_on":join.sql_on,
                "sql_table_name":join.sql_table_name,
                "type":join.type,
                "view_label":join.view_label,
            }
            joins_DictList.append(joinsdict)

        for supported_measure_type in exploreObj.supported_measure_types:
            measuretypesdict =    {
                "model_name":model.name,
                "explore_id":exploreObj.id,
                "measure_types":supported_measure_type.measure_types,
            }
            measure_type_DictList.append(measuretypesdict)

#----------------------------------------- join  ~End ---------------------------------------------------------            

#----------------------------------------- Explore Model ~End -------------------------------------------------------

#----------------------------------------- Dashboard Element ~Start -------------------------------------------------------


s_dashboard = sdk.all_dashboards()

for dash in s_dashboard:
    dashboard = sdk.dashboard(dashboard_id=dash.id)
    for dash_element in dashboard.dashboard_elements:
        data_groupsDict = {
            "can_create" : None if 'create' not in dash_element.can else dash_element.can["create"],
            "can_update" : None if 'update' not in dash_element.can else dash_element.can["update"],
            "can_destroy" : None if 'destroy' not in dash_element.can else dash_element.can["destroy"],
            "can_see_aggregate_table_lookml" : None if 'see_aggregate_table_lookml' not in dash_element.can else dash_element.can["see_aggregate_table_lookml"],
            "can_index" : None if 'index' not in dash_element.can else dash_element.can["index"],
            "can_show" : None if 'show' not in dash_element.can else dash_element.can["show"],
            "can_explore" : None if 'explore' not in dash_element.can else dash_element.can["explore"],
            "can_run" : None if 'run' not in dash_element.can else dash_element.can["run"],
            "can_show_errors" : None if 'show_errors' not in dash_element.can else dash_element.can["show_errors"],
            "can_find_and_replace" : None if 'find_and_replace' not in dash_element.can else dash_element.can["find_and_replace"],

            "body_text" : dash_element.body_text,
            "body_text_as_html" : dash_element.body_text_as_html,
            "dashboard_id" : dash_element.dashboard_id,
            "edit_uri" : dash_element.edit_uri,
            "id" : dash_element.id,
            "look" : dash_element.look,
            "look_id" : dash_element.look_id,
            "lookml_link_id" : dash_element.lookml_link_id,
            "merge_result_id" : dash_element.merge_result_id,
            "note_display" : dash_element.note_display,
            "note_state" : dash_element.note_state,
            "note_text" : dash_element.note_text,
            "note_text_as_html" : dash_element.note_text_as_html,
            "query_model" : "" if dash_element.query is None else dash_element.query.model,
            "query_view" : "" if dash_element.query is None else dash_element.query.view,
            
            "query_can_run" : None if dash_element.query is None else dash_element.query.can["run"],
            "query_can_see_results" : None if dash_element.query is None else dash_element.query.can["see_results"],
            "query_can_explore" : None if dash_element.query is None else dash_element.query.can["explore"],
            "query_can_create" : None if dash_element.query is None else dash_element.query.can["create"],
            "query_can_show" : None if dash_element.query is None else dash_element.query.can["show"],
            "query_can_cost_estimate" : None if dash_element.query is None else dash_element.query.can["cost_estimate"],
            "query_can_index" : None if dash_element.query is None else dash_element.query.can["index"],
            "query_can_see_lookml" : None if dash_element.query is None else dash_element.query.can["see_lookml"],
            "query_can_see_aggregate_table_lookml" : None if dash_element.query is None else dash_element.query.can["see_aggregate_table_lookml"],
            "query_can_see_derived_table_lookml" : None if dash_element.query is None else dash_element.query.can["see_derived_table_lookml"],
            "query_can_see_sql" : None if dash_element.query is None else dash_element.query.can["see_sql"],
            "query_can_save" : None if dash_element.query is None else dash_element.query.can["save"],
            "query_can_generate_drill_links" : None if dash_element.query is None else dash_element.query.can["generate_drill_links"],
            "query_can_download" : None if dash_element.query is None else dash_element.query.can["download"],
            "query_can_download_unlimited" : None if dash_element.query is None else dash_element.query.can["download_unlimited"],
            "query_can_schedule" : None if dash_element.query is None else dash_element.query.can["schedule"],
            "query_can_render" : None if dash_element.query is None else dash_element.query.can["render"],

            "query_fields" : None if dash_element.query is None else dash_element.query.fields,
            "query_pivots" : None if dash_element.query is None else dash_element.query.pivots,
            "query_fill_fields" : None if dash_element.query is None else dash_element.query.fill_fields,
            
            "query_filters_order_items.created_date" : None if dash_element.query is None else dash_element.query.filters,

            "query_filter_expression":{} if dash_element.query is None else dash_element.query.filter_expression,
            "query_sorts":{} if dash_element.query is None else  dash_element.query.sorts,
            "query_limit":{} if dash_element.query is None else  dash_element.query.limit,
            "query_column_limit":{} if dash_element.query is None else dash_element.query.column_limit,
            "query_total":{} if dash_element.query is None else dash_element.query.total,
            "query_row_total":{} if dash_element.query is None else dash_element.query.row_total,
            "query_subtotals":{} if dash_element.query is None else dash_element.query.subtotals,

            "query_vis_config" :{} if dash_element.query is None else  dash_element.query.vis_config,
            "query_filter_config":{} if dash_element.query is None else dash_element.query.filter_config,
            "query_visible_ui_sections":{} if dash_element.query is None else dash_element.query.visible_ui_sections,
            "query_slug":{} if dash_element.query is None else dash_element.query.slug,
            "query_dynamic_fields":{} if dash_element.query is None else dash_element.query.dynamic_fields,
            "query_client_id":{} if dash_element.query is None else dash_element.query.client_id,
            "query_share_url":{} if dash_element.query is None else dash_element.query.share_url,
            "query_expanded_share_url":{} if dash_element.query is None else dash_element.query.expanded_share_url,
            "query_url":{} if dash_element.query is None else dash_element.query.url,

            "query_query_timezone":{} if dash_element.query is None else dash_element.query.query_timezone,
            "query_has_table_calculations":{} if dash_element.query is None else dash_element.query.has_table_calculations,
            "query_runtime":{} if dash_element.query is None else dash_element.query.runtime,

            "query_id":{} if dash_element.query is None else dash_element.query_id,
            "refresh_interval":{} if dash_element.query is None else dash_element.refresh_interval,
            "refresh_interval_to_i":dash_element.refresh_interval_to_i,

            "result_maker_dynamic_fields":None if dash_element.result_maker is None else dash_element.result_maker.dynamic_fields,
            "result_maker_sorts":None if dash_element.result_maker is None else dash_element.result_maker.sorts,
            "result_maker_merge_result_id":None if dash_element.result_maker is None else dash_element.result_maker.merge_result_id,
            "result_maker_total":None if dash_element.result_maker is None else dash_element.result_maker.total,
            "result_maker_query_id":None if dash_element.result_maker is None else dash_element.result_maker.query_id,
            "result_maker_sql_query_id":None if dash_element.result_maker is None else dash_element.result_maker.sql_query_id,
            
            "result_maker_query_model":None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.model,
            "result_maker_query_view":None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.view,
            "result_maker_query_can":None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.can,

            "result_maker_query_fields" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.fields,

            "result_maker_query_pivots" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.pivots,
            "result_maker_query_fill_fields" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.fill_fields,
            "result_maker_query_filters" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.filters,
            "result_maker_query_filter_expression" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.filter_expression,
            "result_maker_query_sorts" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.sorts,
            "result_maker_query_limit" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.limit,
            "result_maker_query_column_limit" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.column_limit,
            "result_maker_query_total" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.total,
            "result_maker_query_row_total" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.row_total,
            "result_maker_query_subtotals" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.subtotals,
            "result_maker_query_vis_config" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.vis_config,
            "result_maker_query_filter_config" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.filter_config,
            "result_maker_query_visible_ui_sections" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.visible_ui_sections,
            "result_maker_query_slug" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.slug,
            "result_maker_query_dynamic_fields" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.dynamic_fields,
            "result_maker_query_client_id" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.client_id,
            "result_maker_query_share_url" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.share_url,
            "result_maker_query_expanded_share_url" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.expanded_share_url,
            "result_maker_query_url" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.url,
            "result_maker_query_query_timezone" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.query_timezone,
            "result_maker_query_has_table_calculations" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.has_table_calculations,
            "result_maker_query_runtime" : None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.query.runtime,
            
            "result_maker_sql_vis_config":None if dash_element.result_maker is None or dash_element.result_maker.query is None else dash_element.result_maker.vis_config,

            "result_maker_id":dash_element.result_maker_id,
            "subtitle_text":dash_element.subtitle_text,
            "title":dash_element.title,
            "title_hidden":dash_element.title_hidden,
            "title_text":dash_element.title_text,
            "type":dash_element.type,
            "alert_count":dash_element.alert_count,
            "title_text_as_html":dash_element.title_text_as_html,
            "subtitle_text_as_html":dash_element.subtitle_text_as_html,
            "filterables":None if dash_element.result_maker is None else dash_element.result_maker.filterables,
        }
        dashElementDictList.append(data_groupsDict)
        total_dashboard_elements = total_dashboard_elements + 1
        if dash_element.result_maker is not None and dash_element.result_maker.query is not None:
        
            if str(dash_element.result_maker.query.dynamic_fields) is not None and str(dash_element.result_maker.query.dynamic_fields) != '{}' and str(dash_element.result_maker.query.dynamic_fields) != '[]' and str(dash_element.result_maker.query.dynamic_fields) != 'None':
                total_dynamic_fields = total_dynamic_fields + 1

#----------------------------------------- Dashboard Element ~End ---------------------------------------------------------

#----------------------------------------- Connection  ~Start ---------------------------------------------------------

con = sdk.all_connections()

for cItem in con:
    conDict = {
        "name" : cItem.name,
        "pdts_enabled" : cItem.pdts_enabled,
        "host" : cItem.host,
        "port" : cItem.port,
        "username" : cItem.username,
        "password" : cItem.password,
        "uses_oauth" : cItem.uses_oauth,
        "certificate" : cItem.certificate,
        "file_type" : cItem.file_type,
        "database" : cItem.database,
        "db_timezone" : cItem.db_timezone,
        "query_timezone" : cItem.query_timezone,
        "schema" : cItem.schema,
        "max_connections" : cItem.max_connections,
        "max_billing_gigabytes" : cItem.max_billing_gigabytes,
        "ssl" : cItem.ssl,
        "verify_ssl" : cItem.verify_ssl,
        "tmp_db_name" : cItem.tmp_db_name,
        "jdbc_additional_params" : cItem.jdbc_additional_params,
        "pool_timeout" : cItem.pool_timeout,
        "dialect_name" : cItem.dialect_name,
        "created_at" : cItem.created_at,
        "user_id" : cItem.user_id,
        "example" : cItem.example,
        "user_db_credentials" : cItem.user_db_credentials,
        "user_attribute_fields" : cItem.user_attribute_fields,
        "maintenance_cron" : cItem.maintenance_cron,
        "last_regen_at" : cItem.last_regen_at,
        "last_reap_at" : cItem.last_reap_at,
        "sql_runner_precache_tables" : cItem.sql_runner_precache_tables,
        "after_connect_statements" : cItem.after_connect_statements,
        "pdt_context_override" : cItem.pdt_context_override,
        "managed" : cItem.managed,

        "can_index" : None if 'index' not in cItem.can else cItem.can["index"],
        "can_index_limited" : None if 'index_limited' not in cItem.can else cItem.can["index_limited"],
        "can_show" : None if 'show' not in cItem.can else cItem.can["show"],
        "can_cost_estimate" : None if 'cost_estimate' not in cItem.can else cItem.can["cost_estimate"],
        "can_access_data" : None if 'access_data' not in cItem.can else cItem.can["access_data"],
        "can_explore" : None if 'explore' not in cItem.can else cItem.can["explore"],
        "can_refresh_schemas" : None if 'refresh_schemas' not in cItem.can else cItem.can["refresh_schemas"],
        "can_destroy" : None if 'destroy' not in cItem.can else cItem.can["destroy"],
        "can_test" : None if 'test' not in cItem.can else cItem.can["test"],
        "can_create" : None if 'create' not in cItem.can else cItem.can["create"],
        "can_update" : None if 'update' not in cItem.can else cItem.can["update"],

        "dialect_label" : cItem.dialect.label,
        "dialect_supports_cost_estimate" : cItem.dialect.supports_cost_estimate,
        "dialect_persistent_table_indexes" : cItem.dialect.persistent_table_indexes,
        "dialect_persistent_table_sortkeys" : cItem.dialect.persistent_table_sortkeys,
        "dialect_persistent_table_distkey" : cItem.dialect.persistent_table_distkey,
        "dialect_supports_streaming" : cItem.dialect.supports_streaming,
        "dialect_automatically_run_sql_runner_snippets" : cItem.dialect.automatically_run_sql_runner_snippets,
        "dialect_connection_tests" : cItem.dialect.connection_tests,
        "dialect_supports_inducer" : cItem.dialect.supports_inducer,
        "dialect_supports_multiple_databases" : cItem.dialect.supports_multiple_databases,
        "dialect_supports_persistent_derived_tables" : cItem.dialect.supports_persistent_derived_tables,
        "dialect_has_ssl_support" : cItem.dialect.has_ssl_support,
    }
    conDictList.append(conDict)
    total_connections = total_connections + 1

#----------------------------------------- Connection  ~End ---------------------------------------------------------

#----------------------------------------- All Dashboard  ~Start ---------------------------------------------------------


dashboards = sdk.dashboard(dashboard_id="1")

dashboards = sdk.all_dashboards()
for dashboard in dashboards:
    logging.info(dashboard)
    dashboardDict = {
        "id" : dashboard.id,
        "title" : dashboard.title,
        "content_favorite_id" : dashboard.content_favorite_id,
        "content_metadata_id" : dashboard.content_metadata_id,
        "description" : dashboard.description,
        "hidden" : dashboard.hidden,
        "model" : dashboard.model,
        "query_timezone" : str(dashboard.query_timezone),
        "readonly" : dashboard.readonly,
        "refresh_interval" : dashboard.refresh_interval,
        "refresh_interval_to_i" : dashboard.refresh_interval_to_i,
        "user_id" : dashboard.user_id,

        "can_download" : None if 'download' not in dashboard.can else dashboard.can["download"],
        "can_see_aggregate_table_lookml" : None if 'see_aggregate_table_lookml' not in dashboard.can else dashboard.can["see_aggregate_table_lookml"],
        "can_index" : None if 'index' not in dashboard.can else dashboard.can["index"],
        "can_show" : None if 'show' not in dashboard.can else dashboard.can["show"],
        "can_copy" : None if 'copy' not in dashboard.can else dashboard.can["copy"],
        "can_run" : None if 'run' not in dashboard.can else dashboard.can["run"],
        "can_create" : None if 'create' not in dashboard.can else dashboard.can["create"],
        "can_move" : None if 'move' not in dashboard.can else dashboard.can["move"],
        "can_update" : None if 'update' not in dashboard.can else dashboard.can["update"],
        "can_destroy" : None if 'destroy' not in dashboard.can else dashboard.can["destroy"],
        "can_recover" : None if 'recover' not in dashboard.can else dashboard.can["recover"],
        "can_see_lookml" : None if 'see_lookml' not in dashboard.can else dashboard.can["see_lookml"],
        "can_schedule" : None if 'schedule' not in dashboard.can else dashboard.can["schedule"],
        "can_render" : None if 'render' not in dashboard.can else dashboard.can["render"],

        "folder_name" : dashboard.folder.name,
        "folder_parent_id" : dashboard.folder.parent_id,
        "folder_id" : dashboard.folder.id,
        "folder_content_metadata_id" : dashboard.folder.content_metadata_id,
        "folder_created_at" : str(dashboard.folder.created_at),
        "folder_creator_id" : dashboard.folder.creator_id,
        "folder_child_count" : dashboard.folder.child_count,
        "folder_external_id" : dashboard.folder.external_id,
        "folder_is_embed" : dashboard.folder.is_embed,
        "folder_is_embed_shared_root" : dashboard.folder.is_embed_shared_root,
        "folder_is_embed_users_root" : dashboard.folder.is_embed_users_root,
        "folder_is_personal" : dashboard.folder.is_personal,
        "folder_is_personal_descendant" : dashboard.folder.is_personal_descendant,
        "folder_is_shared_root" : dashboard.folder.is_shared_root,
        "folder_is_users_root" : dashboard.folder.is_users_root,

        "folder_can_index" : None if 'index' not in dashboard.folder.can else dashboard.folder.can["index"],
        "folder_can_show" : None if 'show' not in dashboard.folder.can else dashboard.folder.can["show"],
        "folder_can_create" : None if 'create' not in dashboard.folder.can else dashboard.folder.can["create"],
        "folder_can_see_admin_spaces" : None if 'see_admin_spaces' not in dashboard.folder.can else dashboard.folder.can["see_admin_spaces"],
        "folder_can_update" : None if 'update' not in dashboard.folder.can else dashboard.folder.can["update"],
        "folder_can_destroy" : None if 'destroy' not in dashboard.folder.can else dashboard.folder.can["destroy"],
        "folder_can_move_content" : None if 'move_content' not in dashboard.folder.can else dashboard.folder.can["move_content"],
        "folder_can_edit_content" : None if 'edit_content' not in dashboard.folder.can else dashboard.folder.can["edit_content"],

        "space_name" : dashboard.space.name,
        "space_parent_id" : dashboard.space.parent_id,
        "space_id" : dashboard.space.id,
        "space_content_metadata_id" : dashboard.space.content_metadata_id,
        "space_created_at" : str(dashboard.space.created_at),
        "space_creator_id" : dashboard.space.creator_id,
        "space_child_count" : dashboard.space.child_count,
        "space_external_id" : dashboard.space.external_id,
        "space_is_embed" : dashboard.space.is_embed,
        "space_is_embed_shared_root" : dashboard.space.is_embed_shared_root,
        "space_is_embed_users_root" : dashboard.space.is_embed_users_root,
        "space_is_personal" : dashboard.space.is_personal,
        "space_is_personal_descendant" : dashboard.space.is_personal_descendant,
        "space_is_shared_root" : dashboard.space.is_shared_root,
        "space_is_users_root" : dashboard.space.is_users_root,

        "space_can_name" : None if 'name' not in dashboard.space.can else dashboard.space.can["name"],
        "space_can_parent_id" : None if 'parent_id' not in dashboard.space.can else dashboard.space.can["parent_id"],
        "space_can_id" : None if 'id' not in dashboard.space.can else dashboard.space.can["id"],
        "space_can_content_metadata_id" : None if 'content_metadata_id' not in dashboard.space.can else dashboard.space.can["content_metadata_id"],
        "space_can_created_at" : None if 'created_at' not in dashboard.space.can else str(dashboard.space.can["created_at"]),
        "space_can_creator_id" : None if 'creator_id' not in dashboard.space.can else dashboard.space.can["creator_id"],
        "space_can_child_count" : None if 'child_count' not in dashboard.space.can else dashboard.space.can["child_count"],
        "space_can_external_id" : None if 'external_id' not in dashboard.space.can else dashboard.space.can["external_id"],
        "space_can_is_embed" : None if 'is_embed' not in dashboard.space.can else dashboard.space.can["is_embed"],
        "space_can_is_embed_shared_root" : None if 'is_embed_shared_root' not in dashboard.space.can else dashboard.space.can["is_embed_shared_root"],
        "space_can_is_embed_users_root" : None if 'is_embed_users_root' not in dashboard.space.can else dashboard.space.can["is_embed_users_root"],
        "space_can_is_personal" : None if 'is_personal' not in dashboard.space.can else dashboard.space.can["is_personal"],
        "space_can_is_personal_descendant" : None if 'is_personal_descendant' not in dashboard.space.can else dashboard.space.can["is_personal_descendant"],
        "space_can_is_shared_root" : None if 'is_shared_root' not in dashboard.space.can else dashboard.space.can["is_shared_root"],
        "space_can_is_users_root" : None if 'is_users_root' not in dashboard.space.can else dashboard.space.can["is_users_root"],
    }
    dashboardDictList.append(dashboardDict)
    total_dashboards = total_dashboards + 1

#----------------------------------------- All Dashboard  ~End ---------------------------------------------------------


#-----------------------------------------Users Group  ~Start -------------------------------------------------------

groups = sdk.all_groups()


for group in groups:
    s_group = sdk.all_group_users(group.id)
    for user in s_group:

        groupsDict = {
            "can_show" : None if 'show' not in user.can else user.can["show"],
            "can_index" : None if 'index' not in user.can else user.can["index"],
            "can_show_details" : None if 'show_details' not in user.can else user.can["show_details"],
            "can_index_details" : None if 'index_details' not in user.can else user.can["index_details"],
            "can_sudo" : None if 'sudo' not in user.can else user.can["sudo"],

            "avatar_url" : user.avatar_url,
            "avatar_url_without_sizing" : user.avatar_url_without_sizing,
            "credentials_api3" : user.credentials_api3,

            "credentials_email_can_show_password_reset_url" : {} if user.credentials_email is None else user.credentials_email.can["show_password_reset_url"],
            "credentials_email_created_at" : {} if user.credentials_email is None else user.credentials_email.created_at,
            "credentials_email_email" : {} if user.credentials_email is None else user.credentials_email.email,
            "credentials_email_forced_password_reset_at_next_login" : {} if user.credentials_email is None else user.credentials_email.forced_password_reset_at_next_login,
            "credentials_email_is_disabled" : {} if user.credentials_email is None else user.credentials_email.is_disabled,
            "credentials_email_logged_in_at" : {} if user.credentials_email is None else user.credentials_email.logged_in_at,
            "credentials_email_password_reset_url" : {} if user.credentials_email is None else user.credentials_email.password_reset_url,
            "credentials_email_type" : {} if user.credentials_email is None else user.credentials_email.type,
            "credentials_email_url" : {} if user.credentials_email is None else user.credentials_email.url,
            "credentials_email_user_url" : {} if user.credentials_email is None else user.credentials_email.user_url,

            "credentials_embed" : user.credentials_embed,
            "credentials_google" : user.credentials_google,
            "credentials_ldap" : user.credentials_ldap,
            "credentials_looker_openid" : user.credentials_looker_openid,
            "credentials_oidc" : user.credentials_oidc,
            "credentials_saml" : user.credentials_saml,
            "credentials_totp" : user.credentials_totp,
            "display_name" : user.display_name,
            "email" : user.email,
            "embed_group_space_id" : user.embed_group_space_id,
            "first_name" : user.first_name,
            "group_ids" : user.group_ids,
            "home_space_id" : user.home_space_id,
            "home_folder_id" : user.home_folder_id,
            "id" : user.id,
            "is_disabled" : user.is_disabled,
            "last_name" : user.last_name,
            "locale" : user.locale,
            "looker_versions" : user.looker_versions,
            "models_dir_validated" : user.models_dir_validated,
            "personal_space_id" : user.personal_space_id,
            "personal_folder_id" : user.personal_folder_id,
            "presumed_looker_employee" : user.presumed_looker_employee,
            "role_ids" : user.role_ids,
            "sessions" : user.sessions,

            "ui_state_homepageGroupIdPreference" : {} if user.ui_state is None or 'homepageGroupIdPreference' not in user.ui_state else user.ui_state["homepageGroupIdPreference"],

            "verified_looker_employee" : user.verified_looker_employee,
            "roles_externally_managed" : user.roles_externally_managed,
            "allow_direct_roles" : user.allow_direct_roles,
            "allow_normal_group_membership" : user.allow_normal_group_membership,
            "allow_roles_from_normal_groups" : user.allow_roles_from_normal_groups,
            "url" : user.url,
        }
          
    groups_DictList.append(groupsDict)
    total_users_in_groups = total_users_in_groups + 1
 

#-----------------------------------------Users Group  ~End ---------------------------------------------------------
#-----------------------------------------All Data Groups ~Start -------------------------------------------------------

data_groups = sdk.all_datagroups()

for data_group in data_groups:
    data_groupsDict = {
        
        "can_index" : None if 'index' not in data_group.can else data_group.can["index"],
        "can_update" : None if 'update' not in data_group.can else data_group.can["update"],
        "can_show" : None if 'show' not in data_group.can else data_group.can["show"],

        "created_at" : data_group.created_at,
        "id" : data_group.id,
        "model_name" : data_group.model_name,
        "name" : data_group.name,
        "stale_before" : data_group.stale_before,
        "trigger_check_at" : data_group.trigger_check_at,
        "trigger_error" : data_group.trigger_error,
        "trigger_value" : data_group.trigger_value,
        "triggered_at" : data_group.triggered_at,

    }
    data_groups_DictList.append(data_groupsDict)
    total_datagroups = total_datagroups + 1
    
#-----------------------------------------All Data Groups ~End -------------------------------------------------------

#-----------------------------------------All Roles ~Start -------------------------------------------------------

roles = sdk.all_roles()
for role in roles:
    roleDict = {
        "can_show" : None if 'show' not in role.can else role.can["show"],
        "can_index" : None if 'index' not in role.can else role.can["index"],
        "can_update" : None if 'update' not in role.can else role.can["update"],

        "id" : role.id,
        "name" : role.name,

        "permission_set_can_show" : None if 'show' not in role.permission_set.can else role.permission_set.can["show"],
        "permission_set_can_index" : None if 'index' not in role.permission_set.can else role.permission_set.can["index"],
        "permission_set_can_update" : None if 'update' not in role.permission_set.can else role.permission_set.can["update"],

        "permission_set_all_access" : role.permission_set.all_access,
        "permission_set_built_in" : role.permission_set.built_in,
        "permission_set_name" : role.permission_set.name,
        "permission_set_permissions" : role.permission_set.permissions,
        "permission_set_url" : role.permission_set.url,
        "permission_set_id" : role.permission_set_id,

        "model_set_can_show" : None if 'show' not in role.model_set.can else role.model_set.can["show"],
        "model_set_can_index" : None if 'index' not in role.model_set.can else role.model_set.can["index"],
        "model_set_can_update" : None if 'update' not in role.model_set.can else role.model_set.can["update"],

        "model_set_all_access" : role.model_set.all_access,
        "model_set_built_in" : role.model_set.built_in,
        "model_set_id" : role.model_set.id,
        "model_set_models" : role.model_set.models,
        "model_set_name" : role.model_set.name,
        "model_set_url" : role.model_set.url,

        "model_set_id_model_set_id" : role.model_set_id,
        "model_set_id_url" : role.url,
        "model_set_id_users_url" : role.users_url,
    }
    roles_DictList.append(roleDict)
    total_roles = total_roles + 1


#-----------------------------------------All Roles ~End ---------------------------------------------------------



#-----------------------------------------All Locales ~Start -------------------------------------------------------


locales = sdk.all_locales()

for locale in locales:
    localeDict = {
        "code" : locale.code,
        "native_name" : locale.native_name,
        "english_name" : locale.english_name,
    }
    locales_DictList.append(localeDict)
    total_locales = total_locales + 1
    
#-----------------------------------------All Locales ~End ---------------------------------------------------------


#print(data_groups_DataFrame)

#-----------------------------------------Single Look ~Start -------------------------------------------------------



looks = sdk.all_looks()
for look in looks:
    s_look = sdk.look(look_id= look.id)

    look_Dict = {
        "can_download" : None if 'download' not in s_look.can else s_look.can["download"],
        "can_download_unlimited" : None if 'download_unlimited' not in s_look.can else s_look.can["download_unlimited"],
        "can_index" : None if 'index' not in s_look.can else s_look.can["index"],
        "can_show" : None if 'show' not in s_look.can else s_look.can["show"],
        "can_copy" : None if 'copy' not in s_look.can else s_look.can["copy"],
        "can_run" : None if 'run' not in s_look.can else s_look.can["run"],
        "can_create" : None if 'create' not in s_look.can else s_look.can["create"],
        "can_explore" : None if 'explore' not in s_look.can else s_look.can["explore"],
        "can_move" : None if 'move' not in s_look.can else s_look.can["move"],
        "can_update" : None if 'update' not in s_look.can else s_look.can["update"],
        "can_destroy" : None if 'destroy' not in s_look.can else s_look.can["destroy"],
        "can_recover" : None if 'recover' not in s_look.can else s_look.can["recover"],
        "can_show_errors" : None if 'show_errors' not in s_look.can else s_look.can["show_errors"],
        "can_find_and_replace" : None if 'find_and_replace' not in s_look.can else s_look.can["find_and_replace"],
        "can_schedule" : None if 'schedule' not in s_look.can else s_look.can["schedule"],
        "can_render" : None if 'render' not in s_look.can else s_look.can["render"],
 
        "content_metadata_id" : s_look.content_metadata_id,
        "id" : s_look.id,
        "title" : s_look.title,
        "content_favorite_id" : s_look.content_favorite_id,
        "created_at" : str(s_look.created_at),
        "deleted_at" : str(s_look.deleted_at),
        "deleter_id" : s_look.deleter_id,
        "description" : s_look.description,
        "embed_url" : s_look.embed_url,
        "excel_file_url" : s_look.excel_file_url,
        "favorite_count" : s_look.favorite_count,
        "google_spreadsheet_formula" : s_look.google_spreadsheet_formula,
        "image_embed_url" : s_look.image_embed_url,
        "is_run_on_load" : s_look.is_run_on_load,
        "last_accessed_at" : str(s_look.last_accessed_at),
        "last_updater_id" : s_look.last_updater_id,
        "last_viewed_at" : str(s_look.last_viewed_at),
        "model" : s_look.model,
        "public" : s_look.public,
        "public_slug" : s_look.public_slug,
        "public_url" : s_look.public_url,
        "query_id" : s_look.query_id,
        "short_url" : s_look.short_url,

        "folder_name" : s_look.folder.name,
        "folder_parent_id" : s_look.folder.parent_id,
        "folder_id" : s_look.folder.id,
        "folder_content_metadata_id" : s_look.folder.content_metadata_id,
        "folder_created_at" : str(s_look.folder.created_at),
        "folder_creator_id" : s_look.folder.creator_id,
        "folder_child_count" : s_look.folder.child_count,
        "folder_external_id" : s_look.folder.external_id,
        "folder_is_embed" : s_look.folder.is_embed,
        "folder_is_embed_shared_root" : s_look.folder.is_embed_shared_root,
        "folder_is_embed_users_root" : s_look.folder.is_embed_users_root,
        "folder_is_personal" : s_look.folder.is_personal,
        "folder_is_personal_descendant" : s_look.folder.is_personal_descendant,
        "folder_is_shared_root" : s_look.folder.is_shared_root,
        "folder_is_users_root" : s_look.folder.is_users_root,

        "folder_can_index" : None if 'index' not in s_look.folder.can else s_look.folder.can["index"],
        "folder_can_show" : None if 'show' not in s_look.folder.can else s_look.folder.can["show"],
        "folder_can_create" : None if 'create' not in s_look.folder.can else s_look.folder.can["create"],
        "folder_can_see_admin_spaces" : None if 'see_admin_spaces' not in s_look.folder.can else s_look.folder.can["see_admin_spaces"],
        "folder_can_update" : None if 'update' not in s_look.folder.can else s_look.folder.can["update"],
        "folder_can_destroy" : None if 'destroy' not in s_look.folder.can else s_look.folder.can["destroy"],
        "folder_can_move_content" : None if 'move_content' not in s_look.folder.can else s_look.folder.can["move_content"],
        "folder_can_edit_content" : None if 'edit_content' not in s_look.folder.can else s_look.folder.can["edit_content"],

        "updated_at" : str(s_look.updated_at),
        "user_id" : s_look.user_id,
        "view_count" : s_look.view_count,
        "user" : s_look.user,  

        "space_name" : s_look.space.name,
        "space_parent_id" : s_look.space.parent_id,
        "space_id" : s_look.space.id,
        "space_content_metadata_id" : s_look.space.content_metadata_id,
        "space_created_at" : str(s_look.space.created_at),
        "space_creator_id" : s_look.space.creator_id,
        "space_child_count" : s_look.space.child_count,
        "space_external_id" : s_look.space.external_id,
        "space_is_embed" : s_look.space.is_embed,
        "space_is_embed_shared_root" : s_look.space.is_embed_shared_root,
        "space_is_embed_users_root" : s_look.space.is_embed_users_root,
        "space_is_personal" : s_look.space.is_personal,
        "space_is_personal_descendant" : s_look.space.is_personal_descendant,
        "space_is_shared_root" : s_look.space.is_shared_root,
        "space_is_users_root" : s_look.space.is_users_root,

        "space_can_index" : None if 'index' not in s_look.space.can else s_look.space.can["index"],
        "space_can_show" : None if 'show' not in s_look.space.can else s_look.space.can["show"],
        "space_can_create" : None if 'create' not in s_look.space.can else s_look.space.can["create"],
        "space_can_see_admin_spaces" : None if 'see_admin_spaces' not in s_look.space.can else s_look.space.can["see_admin_spaces"],
        "space_can_update" : None if 'update' not in s_look.space.can else s_look.space.can["update"],
        "space_can_destroy" : None if 'destroy' not in s_look.space.can else s_look.space.can["destroy"],
        "space_can_move_content" : None if 'move_content' not in s_look.space.can else s_look.space.can["move_content"],
        "space_can_edit_content" : None if 'edit_content' not in s_look.space.can else s_look.space.can["edit_content"],

        "query_model": s_look.query.model,
        "query_view_": s_look.query.view,

        "query_can_run" : None if 'run' not in s_look.query.can else s_look.query.can["run"],
        "query_can_see_results" : None if 'see_results' not in s_look.query.can else s_look.query.can["see_results"],
        "query_can_explore" : None if 'explore' not in s_look.query.can else s_look.query.can["explore"],
        "query_can_create" : None if 'create' not in s_look.query.can else s_look.query.can["create"],
        "query_can_show" : None if 'show' not in s_look.query.can else s_look.query.can["show"],
        "query_can_True," : None if 'True,' not in s_look.query.can else s_look.query.can["True,"],
        "query_can_cost_estimate" : None if 'cost_estimate' not in s_look.query.can else s_look.query.can["cost_estimate"],
        "query_can_index" : None if 'index' not in s_look.query.can else s_look.query.can["index"],
        "query_can_see_lookml" : None if 'see_lookml' not in s_look.query.can else s_look.query.can["see_lookml"],
        "query_can_see_aggregate_table_lookml" : None if 'see_aggregate_table_lookml' not in s_look.query.can else s_look.query.can["see_aggregate_table_lookml"],
        "query_can_see_derived_table_lookml" : None if 'see_derived_table_lookml' not in s_look.query.can else s_look.query.can["see_derived_table_lookml"],
        "query_can_see_sql" : None if 'see_sql' not in s_look.query.can else s_look.query.can["see_sql"],
        "query_can_save" : None if 'save' not in s_look.query.can else s_look.query.can["save"],
        "query_can_generate_drill_links" : None if 'generate_drill_links' not in s_look.query.can else s_look.query.can["generate_drill_links"],
        "query_can_download" : None if 'download' not in s_look.query.can else s_look.query.can["download"],
        "query_can_download_unlimited" : None if 'download_unlimited' not in s_look.query.can else s_look.query.can["download_unlimited"],
        "query_can_schedule" : None if 'schedule' not in s_look.query.can else s_look.query.can["schedule"],
        "query_can_render" : None if 'render' not in s_look.query.can else s_look.query.can["render"],

        "query_fields" : s_look.query.fields,
        "query_pivots" : s_look.query.pivots,
        "query_fill_fields" : s_look.query.fill_fields,
        "query_filters": s_look.query.filters,

        "query_filter_expression" : s_look.query.filter_expression,
        "query_sorts" : s_look.query.sorts,
        "query_limit" : s_look.query.limit,
        "query_column_limit" : s_look.query.column_limit,
        "query_row_total" : s_look.query.row_total,
        "query_subtotals" : s_look.query.subtotals,

        "query_vis_config_custom_color_enabled" : None if 'custom_color_enabled' not in s_look.query.vis_config else s_look.query.vis_config["custom_color_enabled"],
        "query_vis_config_show_single_value_title" : None if 'show_single_value_title' not in s_look.query.vis_config else s_look.query.vis_config["show_single_value_title"],
        "query_vis_config_show_comparison" : None if 'show_comparison' not in s_look.query.vis_config else s_look.query.vis_config["show_comparison"],
        "query_vis_config_comparison_type" : None if 'comparison_type' not in s_look.query.vis_config else s_look.query.vis_config["comparison_type"],
        
        "query_vis_config_comparison_reverse_colors" : None if 'comparison_reverse_colors' not in s_look.query.vis_config else s_look.query.vis_config["comparison_reverse_colors"],
        "query_vis_config_show_comparison_label" : None if 'show_comparison_label' not in s_look.query.vis_config else s_look.query.vis_config["show_comparison_label"],
        "query_vis_config_enable_conditional_formatting" : None if 'enable_conditional_formatting' not in s_look.query.vis_config else s_look.query.vis_config["enable_conditional_formatting"],
        "query_vis_config_conditional_formatting_include_totals" : None if 'conditional_formatting_include_totals' not in s_look.query.vis_config else s_look.query.vis_config["conditional_formatting_include_totals"],
        "query_vis_config_conditional_formatting_include_nulls" : None if 'conditional_formatting_include_nulls' not in s_look.query.vis_config else s_look.query.vis_config["conditional_formatting_include_nulls"],
        "query_vis_config_type" : None if 'type' not in s_look.query.vis_config else s_look.query.vis_config["type"],
        
        "query_vis_config_query_timezone" : None if 'query_timezone' not in s_look.query.vis_config else str(s_look.query.vis_config["query_timezone"]),
        
        "query_vis_config_custom_color" : None if 'custom_color' not in s_look.query.vis_config else s_look.query.vis_config["custom_color"],
        
        "query_vis_config_show_view_names" : None if 'show_view_names' not in s_look.query.vis_config else s_look.query.vis_config["show_view_names"],
        "query_vis_config_font_size" : None if 'font_size' not in s_look.query.vis_config else s_look.query.vis_config["font_size"],
        
        "query_vis_config_text_color" : None if 'text_color' not in s_look.query.vis_config else s_look.query.vis_config["text_color"],
        "query_vis_config_colors" : None if 'colors' not in s_look.query.vis_config else s_look.query.vis_config["colors"],
        "query_vis_config_series_types" : None if 'series_types' not in s_look.query.vis_config else s_look.query.vis_config["series_types"],
        "query_vis_config_note_state" : None if 'note_state' not in s_look.query.vis_config else s_look.query.vis_config["note_state"],
        "query_vis_config_note_display" : None if 'note_display' not in s_look.query.vis_config else s_look.query.vis_config["note_display"],
        "query_vis_config_note_text" : None if 'note_text' not in s_look.query.vis_config else s_look.query.vis_config["note_text"],
        "query_vis_config_defaults_version" : None if 'defaults_version' not in s_look.query.vis_config else s_look.query.vis_config["defaults_version"],
        
        
        "query_filter_config_order_items.created_date" : None if 'order_items.created_date' not in s_look.query.filter_config else str(s_look.query.filter_config["order_items.created_date"]),
        "query_filter_config_users.country" : None if 'users.country' not in s_look.query.filter_config else s_look.query.filter_config["users.country"],
        "query_filter_config_users.state" : None if 'users.state' not in s_look.query.filter_config else s_look.query.filter_config["users.state"],
        "query_filter_config_users.city" : None if 'users.city' not in s_look.query.filter_config else s_look.query.filter_config["users.city"],

        "query_visible_ui_sections" : s_look.query.visible_ui_sections,
        "query_slug" : s_look.query.slug,
        "query_dynamic_fields" : s_look.query.dynamic_fields,
        "query_client_id" : s_look.query.client_id,
        "query_share_url" : s_look.query.share_url,
        "query_expanded_share_url" : s_look.query.expanded_share_url,
        "query_url" : s_look.query.url,
        "query_query_timezone" : str(s_look.query.query_timezone),
        "query_has_table_calculations" : s_look.query.has_table_calculations,
        "query_runtime" : str(s_look.query.runtime),
        "url" : s_look.url,

    }
    s_looks_DictList.append(look_Dict)
    



#-----------------------------------------Single Look ~End ---------------------------------------------------------

#-----------------------------------------All looks ~Start -------------------------------------------------------

looks = sdk.all_looks()
for look in looks:
    looks_Dict = {
        "look_can_download" : None if 'download' not in look.can else look.can["download"],
        "look_can_download_unlimited" : None if 'download_unlimited' not in look.can else look.can["download_unlimited"],
        "look_can_index" : None if 'index' not in look.can else look.can["index"],
        "look_can_show" : None if 'show' not in look.can else look.can["show"],
        "look_can_copy" : None if 'copy' not in look.can else look.can["copy"],
        "look_can_run" : None if 'run' not in look.can else look.can["run"],
        "look_can_create" : None if 'create' not in look.can else look.can["create"],
        "look_can_explore" : None if 'explore' not in look.can else look.can["explore"],
        "look_can_move" : None if 'move' not in look.can else look.can["move"],
        "look_can_update" : None if 'update' not in look.can else look.can["update"],
        "look_can_destroy" : None if 'destroy' not in look.can else look.can["destroy"],
        "look_can_recover" : None if 'recover' not in look.can else look.can["recover"],
        "look_can_show_errors" : None if 'show_errors' not in look.can else look.can["show_errors"],
        "look_can_find_and_replace" : None if 'find_and_replace' not in look.can else look.can["find_and_replace"],
        "look_can_schedule" : None if 'schedule' not in look.can else look.can["schedule"],
        "look_can_render" : None if 'render' not in look.can else look.can["render"],

        "look_content_metadata_id" : look.content_metadata_id,
        "look_id" : look.id,
        "look_title" : look.title,
        "look_content_favorite_id" : look.content_favorite_id,
        "look_created_at" : str(look.created_at),
        "look_deleted" : look.deleted,
        "look_deleted_at" : str(look.deleted_at),
        "look_deleter_id" : look.deleter_id,
        "look_description" : look.description,
        "look_embed_url" : look.embed_url,
        "look_excel_file_url" : look.excel_file_url,
        "look_favorite_count" : look.favorite_count,
        "look_google_spreadsheet_formula" : look.google_spreadsheet_formula,
        "look_image_embed_url" : look.image_embed_url,
        "look_is_run_on_load" : look.is_run_on_load,
        "look_last_accessed_at" : str(look.last_accessed_at),
        "look_last_updater_id" : look.last_updater_id,
        "look_last_viewed_at" : str(look.last_viewed_at),
        "look_model" : look.model,
        "look_public" : look.public,
        "look_public_slug" : look.public_slug,
        "look_public_url" : look.public_url,
        "look_query_id" : look.query_id,
        "look_short_url" : look.short_url,

        "look_space_name" : look.space.name,
        "look_space_parent_id" : look.space.parent_id,
        "look_space_id" : look.space.id,
        "look_space_content_metadata_id" : look.space.content_metadata_id,
        "look_space_created_at" : str(look.space.created_at),
        "look_space_creator_id" : look.space.creator_id,
        "look_space_child_count" : look.space.child_count,
        "look_space_external_id" : look.space.external_id,
        "look_space_is_embed" : look.space.is_embed,
        "look_space_is_embed_shared_root" : look.space.is_embed_shared_root,
        "look_space_is_embed_users_root" : look.space.is_embed_users_root,
        "look_space_is_personal" : look.space.is_personal,
        "look_space_is_personal_descendant" : look.space.is_personal_descendant,
        "look_space_is_shared_root" : look.space.is_shared_root,
        "look_space_is_users_root" : look.space.is_users_root,

        "look_folder_can_index" : None if 'index' not in look.folder.can else look.folder.can["index"],
        "look_folder_can_show" : None if 'show' not in look.folder.can else look.folder.can["show"],
        "look_folder_can_create" : None if 'create' not in look.folder.can else look.folder.can["create"],
        "look_folder_can_see_admin_spaces" : None if 'see_admin_spaces' not in look.folder.can else look.folder.can["see_admin_spaces"],
        "look_folder_can_update" : None if 'update' not in look.folder.can else look.folder.can["update"],
        "look_folder_can_destroy" : None if 'destroy' not in look.folder.can else look.folder.can["destroy"],
        "look_folder_can_move_content" : None if 'move_content' not in look.folder.can else look.folder.can["move_content"],
        "look_folder_can_edit_content" : None if 'edit_content' not in look.folder.can else look.folder.can["edit_content"],

        "look_folder_id" : look.folder_id,
        "look_updated_at" : str(look.updated_at),
        "look_user_id" : look.user_id,
        "look_view_count" : look.view_count,
        "look_user" : look.user,

        "look_space_can_index" : None if 'index' not in look.space.can else look.space.can["index"],
        "look_space_can_show" : None if 'show' not in look.space.can else look.space.can["show"],
        "look_space_can_create" : None if 'create' not in look.space.can else look.space.can["create"],
        "look_space_can_see_admin_spaces" : None if 'see_admin_spaces' not in look.space.can else look.space.can["see_admin_spaces"],
        "look_space_can_update" : None if 'update' not in look.space.can else look.space.can["update"],
        "look_space_can_destroy" : None if 'destroy' not in look.space.can else look.space.can["destroy"],
        "look_space_can_move_content" : None if 'move_content' not in look.space.can else look.space.can["move_content"],
        "look_space_can_edit_content" : None if 'edit_content' not in look.space.can else look.space.can["edit_content"],

    }
    looks_DictList.append(looks_Dict)
    total_looks = total_looks + 1
           

#-----------------------------------------All looks ~End ---------------------------------------------------------



#-----------------------------------------All Projects ~Start -------------------------------------------------------

projects = sdk.all_projects()
for project in projects:
    projectsDict = {
        "project_can_webhook_deploy" : None if 'webhook_deploy' not in project.can else project.can["webhook_deploy"],
        "project_can_show_manifest" : None if 'show_manifest' not in project.can else project.can["show_manifest"],
        "project_can_index" : None if 'index' not in project.can else project.can["index"],
        "project_can_show" : None if 'show' not in project.can else project.can["show"],
        "project_can_validate" : None if 'validate' not in project.can else project.can["validate"],
        "project_can_link_to_service" : None if 'link_to_service' not in project.can else project.can["link_to_service"],
        "project_can_update" : None if 'update' not in project.can else project.can["update"],
        "project_can_view_git_deploy_key" : None if 'view_git_deploy_key' not in project.can else project.can["view_git_deploy_key"],
        "project_can_show_branches" : None if 'show_branches' not in project.can else project.can["show_branches"],
        "project_can_deploy_ref_to_production" : None if 'deploy_ref_to_production' not in project.can else project.can["deploy_ref_to_production"],
       
        "project_id" : project.id,
        "project_name" : project.name,
        "project_uses_git" : project.uses_git,
        "project_git_remote_url" : project.git_remote_url,
        "project_git_service_name" : project.git_service_name,
        "project_pull_request_mode" : project.pull_request_mode,
        "project_validation_required" : project.validation_required,
        "project_git_release_mgmt_enabled" : project.git_release_mgmt_enabled,
        "project_allow_warnings" : project.allow_warnings,
        "project_is_example" : project.is_example,
    }
    projects_DictList.append(projectsDict)
    total_projects = total_projects + 1


#-----------------------------------------All Projects ~End ---------------------------------------------------------

#-----------------------------------------All Group ~Start -------------------------------------------------------

groups = sdk.all_groups()
for group in groups:
    groupDict = {
        "group_can_show" : None if 'show' not in group.can else group.can["show"],
        "group_can_create" : None if 'create' not in group.can else group.can["create"],
        "group_can_index" : None if 'index' not in group.can else group.can["index"],
        "group_can_update" : None if 'update' not in group.can else group.can["update"],
        "group_can_delete" : None if 'delete' not in group.can else group.can["delete"],
        "group_can_edit_in_ui" : None if 'edit_in_ui' not in group.can else group.can["edit_in_ui"],
        "group_can_add_to_content_metadata" : None if 'add_to_content_metadata' not in group.can else group.can["add_to_content_metadata"],

        
        "group_contains_current_user" : group.contains_current_user,
        "group_external_group_id" : group.external_group_id,
        "group_externally_managed" : group.externally_managed,
        "group_id" : group.id,
        "group_include_by_default" : group.include_by_default,
        "group_name" : group.name,
        "group_user_count" : group.user_count,
    }
    group_DictList.append(groupDict)

    total_groups = total_groups + 1
       


#-----------------------------------------All Group ~End ---------------------------------------------------------


#-----------------------------------------All Spaces -Start --------------------------------------------------------

spaces = sdk.all_spaces()

for space in spaces:
    userDict = {
        "space_name":space.name,
        "space_parent_id":str(space.parent_id),
        "space_content_metadata_id":str(space.parent_id),
        "space_created_at":str(space.created_at),
        
        "space_creator_id" : str(space.creator_id),
        "space_child_count" : space.child_count,
        "space_external_id" : space.external_id,
        "space_is_embed" : space.is_embed,
        "space_is_embed_shared_root" : space.is_embed_shared_root,
        "space_is_embed_users_root" : space.is_embed_users_root,
        "space_is_personal" : space.is_personal,
        "space_is_personal_descendant" : space.is_personal_descendant,
        "space_is_shared_root" : space.is_shared_root,
        "space_is_users_root" : space.is_users_root,

        "space_can_index" : None if 'index' not in space.can else space.can["index"],
        "space_can_show" : None if 'show' not in space.can else space.can["show"],
        "space_can_create" : None if 'create' not in space.can else space.can["create"],
        "space_can_see_admin_spaces" : None if 'see_admin_spaces' not in space.can else space.can["see_admin_spaces"],
        "space_can_update" : None if 'update' not in space.can else space.can["update"],
        "space_can_destroy" : None if 'destroy' not in space.can else space.can["destroy"],
        "space_can_move_content" : None if 'move_content' not in space.can else space.can["move_content"],
        "space_can_edit_content" : None if 'edit_content' not in space.can else space.can["edit_content"],
    }
    spacesDictList.append(userDict)
    total_spaces = total_spaces + 1
#print(spacesDataFrame)

#-----------------------------------------All Spaces -End ----------------------------------------------------------


#-----------------------------------------All User Attributes ~Start -----------------------------------------------


user_attributes = sdk.all_user_attributes()
for user_attribute in user_attributes:
    userDict = {
        "user_attribute_can_show" : None if 'show' not in user_attribute.can else user_attribute.can["show"],
        "user_attribute_can_index" : None if 'index' not in user_attribute.can else user_attribute.can["index"],
        "user_attribute_can_create" : None if 'create' not in user_attribute.can else user_attribute.can["create"],
        "user_attribute_can_show_value" : None if 'show_value' not in user_attribute.can else user_attribute.can["show_value"],
        "user_attribute_can_update" : None if 'update' not in user_attribute.can else user_attribute.can["update"],
        "user_attribute_can_destroy" : None if 'destroy' not in user_attribute.can else user_attribute.can["destroy"],
        "user_attribute_can_set_value" : None if 'set_value' not in user_attribute.can else user_attribute.can["set_value"],

        "user_attribute_id" : user_attribute.id,
        "user_attribute_name" : user_attribute.name,
        "user_attribute_label" : user_attribute.label,
        "user_attribute_type" : user_attribute.type,
        "user_attribute_is_system" : user_attribute.is_system,
        "user_attribute_value_is_hidden" : user_attribute.value_is_hidden,
        "user_attribute_user_can_view" : user_attribute.user_can_view,
        "user_attribute_user_can_edit" : user_attribute.user_can_edit,

    }
    user_attr_DictList.append(userDict)
    total_user_attributes = total_user_attributes + 1

#print(user_attr_DataFrame)


#-----------------------------------------All User Attributes ~End -----------------------------------------------
#--------------------------------------------All Users-Start---#-----------------------------------------


users = sdk.all_users()
for user in users:
    userDict = {
        "can_show" : None if 'show' not in user.can else user.can["show"],
        "can_index" : None if 'index' not in user.can else user.can["index"],
        "can_show_details" : None if 'show_details' not in user.can else user.can["show_details"],
        "can_index_details" : None if 'index_details' not in user.can else user.can["index_details"],
        "can_sudo" : None if 'sudo' not in user.can else user.can["sudo"],

        #"credentials_email_can" : None if 'show_password_reset_url' not in user.credentials_email.can else user.credentials_email.can["show_password_reset_url"],
        #"credentials_email_can" : user.credentials_email.can["show_password_reset_url"],
       
        "credentials_email_created_at" : {} if user.credentials_email is None else user.credentials_email.created_at,
        "credentials_email_email" : {} if user.credentials_email is None else user.credentials_email.email,
        "credentials_email_forced_password_reset_at_next_login" : {} if user.credentials_email is None else user.credentials_email.forced_password_reset_at_next_login,
        
        "credentials_email_is_disabled" : {} if user.credentials_email is None else user.credentials_email.forced_password_reset_at_next_login,

        "credentials_email_logged_in_at" : {} if user.credentials_email is None else user.credentials_email.logged_in_at,
        "credentials_email_password_reset_url" : {} if user.credentials_email is None else user.credentials_email.password_reset_url,
        "credentials_email_type" : {} if user.credentials_email is None else user.credentials_email.type,
        "credentials_email_url" : {} if user.credentials_email is None else user.credentials_email.url,
        "credentials_email_user_url" : {} if user.credentials_email is None else user.credentials_email.user_url,

        "credentials_embed" : user.credentials_embed,
        "credentials_google" : user.credentials_google,
        "credentials_ldap" : user.credentials_ldap,
        "credentials_looker_openid" : user.credentials_looker_openid,
        "credentials_oidc" : user.credentials_oidc,
        "credentials_saml" : user.credentials_saml,
        "credentials_totp" : user.credentials_totp,
        
        "display_name" : user.display_name,
        "email" : user.email,
        "embed_group_space_id" : user.embed_group_space_id,
        "first_name" : user.first_name,
        "group_ids" : user.group_ids,
        "home_space_id" : user.home_space_id,
        "home_folder_id" : user.home_folder_id,
        "id" : user.id,
        "is_disabled" : user.is_disabled,
        "last_name" : user.last_name,
        "locale" : user.locale,
        "looker_versions" : user.looker_versions,

        "models_dir_validated" : user.models_dir_validated,
        "personal_space_id" : user.personal_space_id,
        "personal_folder_id" : user.personal_folder_id,
        "presumed_looker_employee" : user.presumed_looker_employee,
        "role_ids" : user.role_ids,
        "sessions" : user.sessions,
        "verified_looker_employee" : user.verified_looker_employee,
        "roles_externally_managed" : user.roles_externally_managed,
        "allow_direct_roles" : user.allow_direct_roles,
        "allow_normal_group_membership" : user.allow_normal_group_membership,
        "allow_roles_from_normal_groups" : user.allow_roles_from_normal_groups,
        "url" : user.url,
        "ui_state_homepageGroupIdPreference" : {} if user.ui_state is None or 'homepageGroupIdPreference' not in user.ui_state else user.ui_state["homepageGroupIdPreference"],       
    }
    userDictList.append(userDict)
  
    total_users = total_users + 1


#########################################--All Users-End--##################################


'''
#                                               OLD ONE.
stats = []
stats.append(['Artifact Name',               'Count',                  'complexity'     ,'Estimate (Hours)'                     ])
stats.append(['Projects',               	(total_projects),               1           , 4 * (total_projects) * 1              ])
stats.append(['Spaces',                 	(total_spaces),                 1           , 4 * (total_spaces) * 1                ])
stats.append(['Models',               	    (total_models),                 1           , 4 * (total_models) * 1                ])
stats.append(['Explores',             	    (total_explores),               7           , 0.25 * (total_explores) * 7              ])
stats.append(['Dimensions',           	    (total_dimensions),				1			, 0.01 * (total_dimensions) * 1          ])
stats.append(['Liquid Dimensions',          (total_liquid_dimensions),		10			, 0 * (total_liquid_dimensions) * 10    ])
stats.append(['Measures', 				    (total_measures),				7			, 0.02 * (total_measures) * 7           ]) 
stats.append(['Sets', 					    (total_sets),					0			, 0 * (total_sets) * 0                  ])
stats.append(['Connections', 				(total_connections),			0			, 0 * (total_connections) * 0           ])
stats.append(['Looks', 					    (total_looks),					8			, 0.15 * (total_looks) * 7                 ])
stats.append(['Look Dynamic Fields', 		(total_dynamic_fields),			10			, 0 * (total_dynamic_fields) * 10       ])
stats.append(['Dashboards', 				(total_dashboards),				7			, 0.25 * (total_dashboards) * 7            ])
stats.append(['Dashboards Elements', 		(total_dashboard_elements),		7			, 0.15 * (total_dashboard_elements) * 7 ])
stats.append(['DataGroups', 				(total_datagroups),				2			, 1 * (total_datagroups) * 2            ])
stats.append(['Groups', 					(total_groups),					1			, 0.01 * (total_groups) * 1                ])
stats.append(['Users Groups', 		        (total_users_in_groups),		2			, 0.01 * (total_users_in_groups) * 2       ])
stats.append(['Users', 					    (total_users),					2			, 0.01 * (total_users) * 2                 ])
stats.append(['Roles', 					    (total_roles),					2			, 0.01 * (total_roles) * 2                 ])
stats.append(['User Attributes', 			(total_user_attributes),		10			, 0 * (total_user_attributes) * 10      ])
stats.append(['total Views', 			    (len(distinct_views)),			2			, 0.3 * (len(distinct_views)) * 2       ])
stats.append(['Always Filters', 			(total_always_filter),			2			, 0.5 * (total_always_filter) * 2       ])
stats.append(['Locales', 					(total_locales),				10			, 0 * (total_locales) * 10              ])

path = 'data_dictionary.xlsx'

n = stats
i = 0
m = []
for i in range(0, len(n)- 1):
    if i > 0:
        m.append(n[i]) 

rez = [[m[j][i] for j in range(len(m))] for i in range(len(m[0]))]

writer = pd.ExcelWriter(path, engine='xlsxwriter')

df = pd.DataFrame({'': []})

workbook  = writer.book
df.to_excel(writer, sheet_name='Sheet1')

worksheet = writer.sheets['Sheet1']
worksheet.name = "Stats"

bold = workbook.add_format({'bold': True})
headings = ['Artifacts', 'Count', 'Complexity','Estimate (Hours)', "Estimated (Weeks)"]

data = rez

hours = ["=ROUND(SUM(D2:D22) / (8 * 5), 0)"]

worksheet.write_row('A1', headings, bold)
worksheet.write_column('A2', data[0])
worksheet.write_column('B2', data[1])
worksheet.write_column('C2', data[2])
worksheet.write_column('D2', data[3])
worksheet.write_column('E2', hours)

column_chart2 = workbook.add_chart({'type': 'column'})
column_chart2.add_series({
    'name':       '=Stats!$B$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$B$2:$B$22',
    
})

line_chart2 = workbook.add_chart({'type': 'line'})
line_chart2.add_series({
    'name':       '=Stats!$C$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$C$2:$C$22',
    'y2_axis':    True,
})

column_chart2.combine(line_chart2)

column_chart2.set_title({  'name': 'Looker Sigma Complexity'})
column_chart2.set_x_axis({ 'name': 'Artifacts'})
column_chart2.set_y_axis({ 'name': 'Counts'})

column_chart2.set_size({'width': 900, 'height': 500})
line_chart2.set_y2_axis({'name': 'Complexity'})

worksheet.insert_chart('F1', column_chart2)

modelDict = pd.DataFrame(model_DictList)
exploreDict = pd.DataFrame(explore_DictList)
aliasesDict = pd.DataFrame(aliases_DictList)
setsDict = pd.DataFrame(sets_DictList)
dimensionDict = pd.DataFrame(dimension_DictList)
parameterDict = pd.DataFrame(parameter_DictList)
joinDict = pd.DataFrame(joins_DictList)
measureTypeList = pd.DataFrame(measure_type_DictList)
filterDict = pd.DataFrame(filters_DictList)
alwaysFilterDict = pd.DataFrame(always_filter_DictList)
connectionList = pd.DataFrame(conDictList) 
allDashboardList = pd.DataFrame(dashboardDictList)
dashelementList = pd.DataFrame(dashElementDictList)
dimensionList = pd.DataFrame(dimension_DictList)
liquidDimensionList = pd.DataFrame(liquid_dimension_DictList)
groupUserList = pd.DataFrame(groups_DictList)
datagroupsList = pd.DataFrame(data_groups_DictList)
rolesList = pd.DataFrame(roles_DictList)
localesList = pd.DataFrame(locales_DictList)
singlelooksList = pd.DataFrame(s_looks_DictList)
looksList = pd.DataFrame(looks_DictList)
projectsList = pd.DataFrame(projects_DictList)
allGroupsList = pd.DataFrame(group_DictList)
spacesList = pd.DataFrame(spacesDictList)
userAttributeList = pd.DataFrame(user_attr_DictList)
userList = pd.DataFrame(userDictList)
viewsList = pd.DataFrame({"Views": distinct_views})
measureList = pd.DataFrame(measure_DictList)


projectsList.to_excel(writer,sheet_name='Projects',index=False)
looksList.to_excel(writer,sheet_name='Looks',index=False)
modelDict.to_excel(writer,sheet_name='Models',index=False)
allGroupsList.to_excel(writer,sheet_name='Groups',index=False)
userList.to_excel(writer,sheet_name='Users',index=False)
spacesList.to_excel(writer,sheet_name='Spaces',index=False)
userAttributeList.to_excel(writer,sheet_name='User Attribute',index=False)
exploreDict.to_excel(writer,sheet_name='Explore',index=False)
aliasesDict.to_excel(writer,sheet_name='Aliases',index=False)
setsDict.to_excel(writer,sheet_name='Sets',index=False)
dimensionDict.to_excel(writer,sheet_name='Dimension',index=False)
liquidDimensionList.to_excel(writer,sheet_name='Liquid Dimension',index=False)
parameterDict.to_excel(writer,sheet_name='Parameter',index=False)
joinDict.to_excel(writer,sheet_name='Joins',index=False)
measureTypeList.to_excel(writer,sheet_name='Measure_Type',index=False)
filterDict.to_excel(writer,sheet_name='Filters',index=False)
alwaysFilterDict.to_excel(writer,sheet_name='Always Filter',index=False)
allDashboardList.to_excel(writer,sheet_name='Dashboards',index=False)
dashelementList.to_excel(writer,sheet_name='Dashboard Element',index=False)
connectionList.to_excel(writer,sheet_name='Connections',index=False)
dimensionList.to_excel(writer,sheet_name='Dimension List',index=False)
datagroupsList.to_excel(writer,sheet_name='Data Groups',index=False)
groupUserList.to_excel(writer,sheet_name='User Group',index=False)
rolesList.to_excel(writer,sheet_name='Roles',index=False)
localesList.to_excel(writer,sheet_name='Locales',index=False)
singlelooksList.to_excel(writer,sheet_name='Single look',index=False)
viewsList.to_excel(writer,sheet_name='Views',index=False)
measureList.to_excel(writer,sheet_name="Measure",index=False)

writer.save()

'''


#----------------------- Data ------------------------
stats = []
stats.append(['Artifact Name',               'Count',                  'complexity'     ,'Estimate (Hours)'                    , 'After complexity' , 'After Estimate (Hours)' ,])
stats.append(['Projects',               	(total_projects),               1           , 4 * (total_projects) * 1              ,       1           ,       4 * (total_projects) * 1             ])
stats.append(['Spaces',                 	(total_spaces),                 1           , 4 * (total_spaces) * 1                ,       1           ,       4 * (total_spaces) * 1              ])
stats.append(['Models',               	    (total_models),                 1           , 4 * (total_models) * 1                ,       1           ,       4 * (total_models) * 1               ])
stats.append(['Explores',             	    (total_explores),               7           , 0.25 * (total_explores) * 7           ,       7           ,       0.25 * (total_explores) * 7              ])
stats.append(['Dimensions',           	    (total_dimensions),				1			, 0.01 * (total_dimensions) * 1         ,       7           ,       0.01 * (total_dimensions) * 7               ])
stats.append(['Liquid Dimensions',          (total_liquid_dimensions),		10			, 0 * (total_liquid_dimensions) * 10    ,       10          ,       0 * (total_liquid_dimensions) * 10               ])
stats.append(['Measures', 				    (total_measures),				7			, 0.02 * (total_measures) * 7           ,       7           ,       0.02 * (total_measures) * 7               ]) 
stats.append(['Sets', 					    (total_sets),					0			, 0 * (total_sets) * 0                  ,       0           ,       0 * (total_sets) * 0                ])
stats.append(['Connections', 				(total_connections),			0			, 0 * (total_connections) * 0           ,       0           ,       0 * (total_connections) * 0               ])
stats.append(['Looks', 					    (total_looks),					8			, 0.15 * (total_looks) * 7          ,       8               ,       0.15 * (total_looks) * 7                 ])
stats.append(['Look Dynamic Fields', 		(total_dynamic_fields),			10			, 0 * (total_dynamic_fields) * 10       ,       10          ,       0 * (total_dynamic_fields) * 10                ])
stats.append(['Dashboards', 				(total_dashboards),				7			, 0.25 * (total_dashboards) * 7         ,       7           ,       0.25 * (total_dashboards) * 7                ])
stats.append(['Dashboards Elements', 		(total_dashboard_elements),		7			, 0.15 * (total_dashboard_elements) * 7 ,       7           ,       0.15 * (total_dashboard_elements) * 7               ])
stats.append(['DataGroups', 				(total_datagroups),				2			, 1 * (total_datagroups) * 2            ,       2           ,       1 * (total_datagroups) * 2              ])
stats.append(['Groups', 					(total_groups),					1			, 0.01 * (total_groups) * 1             ,       1           ,       0.01 * (total_groups) * 1              ])
stats.append(['Users Groups', 		        (total_users_in_groups),		2			, 0.01 * (total_users_in_groups) * 2    ,       2           ,       0.01 * (total_users_in_groups) * 2               ])
stats.append(['Users', 					    (total_users),					2			, 0.01 * (total_users) * 2          ,       2               ,       0.01 * (total_users) * 2               ])
stats.append(['Roles', 					    (total_roles),					2			, 0.01 * (total_roles) * 2          ,       2               ,       0.01 * (total_roles) * 2               ])
stats.append(['User Attributes', 			(total_user_attributes),		10			, 0 * (total_user_attributes) * 10      ,       10          ,       0 * (total_user_attributes) * 10                ])
stats.append(['total Views', 			    (len(distinct_views)),			2			, 0.3 * (len(distinct_views)) * 2       ,       2           ,       0.3 * (len(distinct_views)) * 2               ])
stats.append(['Always Filters', 			(total_always_filter),			2			, 0.5 * (total_always_filter) * 2       ,       2           ,       0.5 * (total_always_filter) * 2               ])
stats.append(['Locales', 					(total_locales),				10			, 0 * (total_locales) * 10              ,       10          ,       0 * (total_locales) * 10               ])


#------------------------ Data End --------------------

n = stats
i = 0
m = []
for i in range(0, len(n)- 1):
    if i > 0:
        m.append(n[i]) 

rez = [[m[j][i] for j in range(len(m))] for i in range(len(m[0]))]

path = 'data_dictionary.xlsx'

n = stats
i = 0
m = []
for i in range(0, len(n)- 1):
    if i > 0:
        m.append(n[i]) 

rez = [[m[j][i] for j in range(len(m))] for i in range(len(m[0]))]

writer = pd.ExcelWriter(path, engine='xlsxwriter')

df = pd.DataFrame({'': []})

workbook  = writer.book
df.to_excel(writer, sheet_name='Sheet1')

worksheet = writer.sheets['Sheet1']
worksheet.name = "Stats"

bold = workbook.add_format({'bold': True})
headings = ['Artifacts', 'Count', 'Complexity','Estimate (Hours)', 'After Complexity','After Estimate (Hours)',"Estimated (Weeks)" ]

data = rez

hours = ["=ROUND(SUM(D2:D22) / (8 * 5), 0)"]

#---------------------------------------Writing data to Excel Column ------------------------------------------- 

worksheet.write_row('A1', headings, bold)
worksheet.write_column('A2', data[0])
worksheet.write_column('B2', data[1])
worksheet.write_column('C2', data[2])
worksheet.write_column('D2', data[3])
worksheet.write_column('E2', data[4])
worksheet.write_column('F2', data[5])
worksheet.write_column('G2', hours)

#------------------------------------------1st Line and  Column Char ------------------------------------


#-----------------------------Add x-axis 'Count' in Chart Column----------------------------------

column_chart2 = workbook.add_chart({'type': 'column'})
column_chart2.add_series({
    'name':       '=Stats!$B$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$B$2:$B$22',
    
})
#--------------------------Add y-axis 'Complexity' in Chart Line ---------------------------------

line_chart2 = workbook.add_chart({'type': 'line'})
line_chart2.add_series({
    'name':       '=Stats!$C$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$C$2:$C$22',
    'y2_axis':    True,
})

column_chart2.combine(line_chart2)

column_chart2.set_title({  'name': 'Looker Sigma Complexity'})
column_chart2.set_x_axis({ 'name': 'Artifacts'})
column_chart2.set_y_axis({ 'name': 'Counts'})

column_chart2.set_size({'width': 900, 'height': 500})
line_chart2.set_y2_axis({'name': 'Complexity'})

worksheet.insert_chart('H1', column_chart2)


#------------------------------------------2nd Line and Column Chart -----------------------------------

#-----------------------------Add x-axis 'Count' in Chart Column----------------------------------

column_chart3 = workbook.add_chart({'type': 'column'})
column_chart3.add_series({
    'name':       '=Stats!$B$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$B$2:$B$22',
    
})



#--------------------------Add y-axis 'Complexity' in Chart Line ---------------------------------

line_chart3 = workbook.add_chart({'type': 'line'})
line_chart3.add_series({
    'name':       '=Stats!$C$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$C$2:$C$22',
    'y2_axis':    True,
   
})
#--------------------------Add y-axis 'After Complexity' in Chart Line ----------------------------

line_chart3.add_series({
    'name':       '=Stats!$E$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$E$2:$E$22',
    'y2_axis':    True,
  
})

column_chart3.combine(line_chart3)


column_chart3.set_title({  'name': 'Compare before and after complexity'})
column_chart3.set_x_axis({ 'name': 'Artifacts'})
column_chart3.set_y_axis({ 'name': 'Counts'})

column_chart3.set_size({'width': 900, 'height': 500})

line_chart3.set_y2_axis({'name': 'After Complexity'})

worksheet.insert_chart('F30', column_chart3)

#------------------------------------------3rd Line Chart -----------------------------------




#--------------------------Add y-axis 'Estimate (Hours)' in Chart Line ----------------------------
column_chart5 = workbook.add_chart({'type': 'column'})
column_chart5.add_series({
    'name':       '=Stats!$B$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$B$2:$B$22',
    
})
#--------------------------Add y-axis 'Estimate (Hours)' in Chart Line ----------------------------
line_chart5 = workbook.add_chart({'type': 'line'})
line_chart5.add_series({
    'name':       '=Stats!$D$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$D$2:$D$22',
    'y2_axis':    True,
  
})
#--------------------------Add y-axis 'After Estimate (Hours)' in Chart Line ----------------------------
line_chart5.add_series({
    'name':       '=Stats!$F$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$F$2:$F$22',
    'y2_axis':    True,
  
})

column_chart5.combine(line_chart5)

column_chart5.set_title({  'name': 'Before and After Hours Estimater'})
column_chart5.set_x_axis({ 'name': 'Artifacts'})
column_chart5.set_y_axis({ 'name': 'Counts'})

column_chart5.set_size({'width': 900, 'height': 500})

column_chart5.set_y2_axis({'name': 'Hours'})

worksheet.insert_chart('F57', column_chart5)
#--------------------------------------------------------------------------------------------
modelDict = pd.DataFrame(model_DictList)
exploreDict = pd.DataFrame(explore_DictList)
aliasesDict = pd.DataFrame(aliases_DictList)
setsDict = pd.DataFrame(sets_DictList)
dimensionDict = pd.DataFrame(dimension_DictList)
parameterDict = pd.DataFrame(parameter_DictList)
joinDict = pd.DataFrame(joins_DictList)
measureTypeList = pd.DataFrame(measure_type_DictList)
filterDict = pd.DataFrame(filters_DictList)
alwaysFilterDict = pd.DataFrame(always_filter_DictList)
connectionList = pd.DataFrame(conDictList) 
allDashboardList = pd.DataFrame(dashboardDictList)
dashelementList = pd.DataFrame(dashElementDictList)
dimensionList = pd.DataFrame(dimension_DictList)
liquidDimensionList = pd.DataFrame(liquid_dimension_DictList)
groupUserList = pd.DataFrame(groups_DictList)
datagroupsList = pd.DataFrame(data_groups_DictList)
rolesList = pd.DataFrame(roles_DictList)
localesList = pd.DataFrame(locales_DictList)
singlelooksList = pd.DataFrame(s_looks_DictList)
looksList = pd.DataFrame(looks_DictList)
projectsList = pd.DataFrame(projects_DictList)
allGroupsList = pd.DataFrame(group_DictList)
spacesList = pd.DataFrame(spacesDictList)
userAttributeList = pd.DataFrame(user_attr_DictList)
userList = pd.DataFrame(userDictList)
viewsList = pd.DataFrame({"Views": distinct_views})
measureList = pd.DataFrame(measure_DictList)


projectsList.to_excel(writer,sheet_name='Projects',index=False)
looksList.to_excel(writer,sheet_name='Looks',index=False)
modelDict.to_excel(writer,sheet_name='Models',index=False)
allGroupsList.to_excel(writer,sheet_name='Groups',index=False)
userList.to_excel(writer,sheet_name='Users',index=False)
spacesList.to_excel(writer,sheet_name='Spaces',index=False)
userAttributeList.to_excel(writer,sheet_name='User Attribute',index=False)
exploreDict.to_excel(writer,sheet_name='Explore',index=False)
aliasesDict.to_excel(writer,sheet_name='Aliases',index=False)
setsDict.to_excel(writer,sheet_name='Sets',index=False)
dimensionDict.to_excel(writer,sheet_name='Dimension',index=False)
liquidDimensionList.to_excel(writer,sheet_name='Liquid Dimension',index=False)
parameterDict.to_excel(writer,sheet_name='Parameter',index=False)
joinDict.to_excel(writer,sheet_name='Joins',index=False)
measureTypeList.to_excel(writer,sheet_name='Measure_Type',index=False)
filterDict.to_excel(writer,sheet_name='Filters',index=False)
alwaysFilterDict.to_excel(writer,sheet_name='Always Filter',index=False)
allDashboardList.to_excel(writer,sheet_name='Dashboards',index=False)
dashelementList.to_excel(writer,sheet_name='Dashboard Element',index=False)
connectionList.to_excel(writer,sheet_name='Connections',index=False)
dimensionList.to_excel(writer,sheet_name='Dimension List',index=False)
datagroupsList.to_excel(writer,sheet_name='Data Groups',index=False)
groupUserList.to_excel(writer,sheet_name='User Group',index=False)
rolesList.to_excel(writer,sheet_name='Roles',index=False)
localesList.to_excel(writer,sheet_name='Locales',index=False)
singlelooksList.to_excel(writer,sheet_name='Single look',index=False)
viewsList.to_excel(writer,sheet_name='Views',index=False)
measureList.to_excel(writer,sheet_name="Measure",index=False)

writer.save()