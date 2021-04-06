import looker_sdk
import logging
import json
import re
import pandas as pd
from pprint import pprint
from pprint import pformat
from yapf.yapflib.yapf_api import FormatCode  
from openpyxl import Workbook

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

sdk = looker_sdk.init31('looker.ini')
#----------------------------------------- Explore Model ~Start -------------------------------------------------------
models = sdk.all_lookml_models(fields= 'name, label, project_name, allowed_db_connection_names,has_content,can, unlimited_db_connections, explores')
ModelDictList = []
ExploreDictList = []
AliasesDictList = []
SetsDictList = []
DimensionDictList = []
ParameterDictList = []
JoinsDictList = []
MeasureTypeDictList = []
AlwaysFilterDictList = []
FiltersDictList = []
for model in models:

    if model.name == "system__activity":
        continue

    str_ = pformat(model)
    formatted_string = FormatCode(str_)
    #logging.info(formatted_string)
    
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
    ModelDictList.append(modelDic)



    for explore in model.explores:
        print("Tryging ... {}:{}".format(model.name, explore.name))
        exploreObj = sdk.lookml_model_explore(lookml_model_name=model.name,explore_name=explore.name)
        exploreStr = pformat(str(exploreObj))
        exploreFormattedString = FormatCode(exploreStr)
        #logging.info(exploreFormattedString)
        #logging.info(exploreObj)

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
        ExploreDictList.append(exploredict)

        

        for alias in exploreObj.aliases:
            aliasesdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id,   
                "name":alias.name,
                "value":alias.value,
            }
            AliasesDictList.append(aliasesdict)

        for set in exploreObj.sets:
            setsdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id,
                "name":set.name,
                "value":set.value,
            }
            SetsDictList.append(setsdict)
                
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
            DimensionDictList.append(dimensionsdict)
        for always_filter_item in exploreObj.always_filter:
            alwaysfilterdict = {
                "model_name":model.name,
                "explore_id":exploreObj.id,   
                "name":always_filter_item.name,
                "value":always_filter_item.value,
            }
            AlwaysFilterDictList.append(alwaysfilterdict)

        logging.info(exploreObj.fields.filters)
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
            FiltersDictList.append(filterdict)
        for parameter in exploreObj.fields.parameters:
            #print(parameter)
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
            ParameterDictList.append(parametersdict)
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
            JoinsDictList.append(joinsdict)
        for supported_measure_type in exploreObj.supported_measure_types:
            measuretypesdict =    {
                "measure_types":supported_measure_type.measure_types,
            }
            MeasureTypeDictList.append(measuretypesdict)
#----------------------------------------- Explore Model ~End -------------------------------------------------------

#-----------------------------------------Single Dashboard  ~Start -------------------------------------------------------

dash_DictList = []
s_dashboard = sdk.all_dashboards()

for dash in s_dashboard:
    dashboard = sdk.dashboard(dashboard_id=dash.id)  
    dash_Dict = {
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

        "content_favorite_id" : dashboard.content_favorite_id,
        "content_metadata_id" : dashboard.content_metadata_id,
        "description" : dashboard.description,
        "hidden" : dashboard.hidden,
        "id" : dashboard.id,
        "model" : dashboard.model,
        "query_timezone" : str(dashboard.query_timezone),
        "readonly" : dashboard.readonly,
        "refresh_interval" : dashboard.refresh_interval,
        "refresh_interval_to_i" : dashboard.refresh_interval_to_i,
        "title" : dashboard.title,
        "user_id" : dashboard.user_id,

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

        "title" : dashboard.title,
        "user_id" : dashboard.user_id,

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

        "space_can_index" : None if 'index' not in dashboard.space.can else dashboard.space.can["index"],
        "space_can_show" : None if 'show' not in dashboard.space.can else dashboard.space.can["show"],
        "space_can_create" : None if 'create' not in dashboard.space.can else dashboard.space.can["create"],
        "space_can_see_admin_spaces" : None if 'see_admin_spaces' not in dashboard.space.can else dashboard.space.can["see_admin_spaces"],
        "space_can_update" : None if 'update' not in dashboard.space.can else dashboard.space.can["update"],
        "space_can_destroy" : None if 'destroy' not in dashboard.space.can else dashboard.space.can["destroy"],
        "space_can_move_content" : None if 'move_content' not in dashboard.space.can else dashboard.space.can["move_content"],
        "space_can_edit_content" : None if 'edit_content' not in dashboard.space.can else dashboard.space.can["edit_content"],
         
        "background_color" : dashboard.background_color,
        "created_at" : str(dashboard.created_at),
        "crossfilter_enabled" : dashboard.crossfilter_enabled,

       
         
                }
    dash_DictList.append(dash_Dict)


#print(dash_DataFrame)

#-----------------------------------------Single Dashboard  ~End ---------------------------------------------------------
#----------------------------------------- Dashboard Element ~Start -------------------------------------------------------

dashElementDictList = []
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

            "query_id" : None if dash_element.query is None else dash_element.query.id,
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
            "result_maker_id":dash_element.result_maker.id,
            "result_maker_dynamic_fields":dash_element.result_maker.dynamic_fields,
            "result_maker_sorts":dash_element.result_maker.sorts,
            "result_maker_merge_result_id":dash_element.result_maker.merge_result_id,
            "result_maker_total":dash_element.result_maker.total,
            "result_maker_query_id":dash_element.result_maker.query_id,
            "result_maker_sql_query_id":dash_element.result_maker.sql_query_id,
            
            "result_maker_query_model":dash_element.result_maker.query.model,
            "result_maker_query_view":dash_element.result_maker.query.view,
            "result_maker_query_can":dash_element.result_maker.query.can,

            "result_maker_query_id" : dash_element.result_maker.query.id,
            "result_maker_query_fields" : dash_element.result_maker.query.fields,

            "result_maker_query_pivots" : dash_element.result_maker.query.pivots,
            "result_maker_query_fill_fields" : dash_element.result_maker.query.fill_fields,
            "result_maker_query_filters" : dash_element.result_maker.query.filters,
            "result_maker_query_filter_expression" : dash_element.result_maker.query.filter_expression,
            "result_maker_query_sorts" : dash_element.result_maker.query.sorts,
            "result_maker_query_limit" : dash_element.result_maker.query.limit,
            "result_maker_query_column_limit" : dash_element.result_maker.query.column_limit,
            "result_maker_query_total" : dash_element.result_maker.query.total,
            "result_maker_query_row_total" : dash_element.result_maker.query.row_total,
            "result_maker_query_subtotals" : dash_element.result_maker.query.subtotals,
            "result_maker_query_vis_config" : dash_element.result_maker.query.vis_config,
            "result_maker_query_filter_config" : dash_element.result_maker.query.filter_config,
            "result_maker_query_visible_ui_sections" : dash_element.result_maker.query.visible_ui_sections,
            "result_maker_query_slug" : dash_element.result_maker.query.slug,
            "result_maker_query_dynamic_fields" : dash_element.result_maker.query.dynamic_fields,
            "result_maker_query_client_id" : dash_element.result_maker.query.client_id,
            "result_maker_query_share_url" : dash_element.result_maker.query.share_url,
            "result_maker_query_expanded_share_url" : dash_element.result_maker.query.expanded_share_url,
            "result_maker_query_url" : dash_element.result_maker.query.url,
            "result_maker_query_query_timezone" : dash_element.result_maker.query.query_timezone,
            "result_maker_query_has_table_calculations" : dash_element.result_maker.query.has_table_calculations,
            "result_maker_query_runtime" : dash_element.result_maker.query.runtime,
            
            "result_maker_sql_vis_config":dash_element.result_maker.vis_config,

            "result_maker_id":dash_element.result_maker_id,
            "subtitle_text":dash_element.subtitle_text,
            "title":dash_element.title,
            "title_hidden":dash_element.title_hidden,
            "title_text":dash_element.title_text,
            "type":dash_element.type,
            "alert_count":dash_element.alert_count,
            "title_text_as_html":dash_element.title_text_as_html,
            "subtitle_text_as_html":dash_element.subtitle_text_as_html,
            
            "filterables":dash_element.result_maker.filterables,


         }
    dashElementDictList.append(data_groupsDict)


#print(dash_element_DataFrame)
#----------------------------------------- Dashboard Element ~End ---------------------------------------------------------

#----------------------------------------- Connection  ~Start ---------------------------------------------------------

con = sdk.all_connections()
conDictList = []
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

    
#df = pd.DataFrame(conDictList)
#df.to_csv("connections.csv")

dashboardDictList = []
dashboards = sdk.dashboard(dashboard_id="1")
#logging.info(dashboards)

dashboards = sdk.all_dashboards()
for dashboard in dashboards:
    logging.info(dashboard)
    #print(dashboard)
    #logging.info(dashboard.can)
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

#----------------------------------------- All Dashboard  ~End ---------------------------------------------------------


#-----------------------------------------All Dimensions ~Start -------------------------------------------------------


########################################################################################################
models = sdk.all_lookml_models(fields= 'name, label, project_name, allowed_db_connection_names,has_content,can, unlimited_db_connections, explores')
for model in models:
    logging.info(model.name)
    if model.name == 'system__activity':
        continue
    if model.name != 'its_sig':
        continue
      
    lookml_model = sdk.lookml_model(lookml_model_name = model.name)
    modelDict = {
        "name": lookml_model.name,
        "label": lookml_model.label,
        "project_name": lookml_model.project_name,
        "allowed_db_connection_names": lookml_model.allowed_db_connection_names,
        "has_content": lookml_model.has_content,
        "can": lookml_model.can,
        "unlimited_db_connections": lookml_model.unlimited_db_connections
    }

    
    for explore in lookml_model.explores:

        if explore.name != 'order_items':
            continue

        exploreObj = sdk.lookml_model_explore(
            lookml_model_name=model.name,
            explore_name=explore.name
        )
        dim_list = []
        
        for dimension in exploreObj.fields.dimensions:
            dim_def = {
                "name":dimension.name,
                "primary_key": dimension.primary_key,
                "type":dimension.type,
                "view":dimension.view,
                "view_label": dimension.view_label,
                "description": dimension.description,
                "sql": dimension.sql,
                "label_short":dimension.label_short,
            }
            dim_list.append(dim_def)
      

        dimensions = json.dumps(dim_list)
################################################################################################################


dimension_DictList = []

for dimension in dim_list:
    dimensionDict = {
        "description" : None if 'description' not in dimension else dimension["description"],
        "label_short" : None if 'label_short' not in dimension else dimension["label_short"],
        "name" : None if 'name' not in dimension else dimension["name"],
        "primary_key" : None if 'primary_key' not in dimension else dimension["primary_key"],
        "sql" : None if 'sql' not in dimension else dimension["sql"],
        "type" : None if 'type' not in dimension else dimension["type"],
        "view" : None if 'view' not in dimension else dimension["view"],
        "view_label" : None if 'view_label' not in dimension else dimension["view_label"],

    }
    dimension_DictList.append(dimensionDict)
#-----------------------------------------All Dimensions ~End ---------------------------------------------------------

#-----------------------------------------Single Group Users ~Start -------------------------------------------------------

groups_DictList = []
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

            "ui_state_homepageGroupIdPreference" : {} if user.ui_state is None else user.ui_state["homepageGroupIdPreference"],

            "verified_looker_employee" : user.verified_looker_employee,
            "roles_externally_managed" : user.roles_externally_managed,
            "allow_direct_roles" : user.allow_direct_roles,
            "allow_normal_group_membership" : user.allow_normal_group_membership,
            "allow_roles_from_normal_groups" : user.allow_roles_from_normal_groups,
            "url" : user.url,

        }
          
    groups_DictList.append(groupsDict)


#print(user_groups_DataFrame)

#-----------------------------------------Single Group Users ~End ---------------------------------------------------------
#-----------------------------------------All Data Groups ~Start -------------------------------------------------------

data_groups_DictList = []
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
#-----------------------------------------All Data Groups ~End -------------------------------------------------------

#-----------------------------------------All Roles ~Start -------------------------------------------------------

roles_DictList = []
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
        "permission_set_id" : role.permission_set.id,
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

roles_DataFrame = pd.DataFrame(roles_DictList)
#print(roles_DataFrame)

#-----------------------------------------All Roles ~End ---------------------------------------------------------



#-----------------------------------------All Locales ~Start -------------------------------------------------------


locales_DictList = []
locales = sdk.all_locales()

for locale in locales:
    localeDict = {
        "code" : locale.code,
        "native_name" : locale.native_name,
        "english_name" : locale.english_name,


    }
    locales_DictList.append(localeDict)


#print(locales_DataFrame)

#-----------------------------------------All Locales ~End ---------------------------------------------------------


#print(data_groups_DataFrame)

#-----------------------------------------Single Look ~Start -------------------------------------------------------

s_looks_DictList = []
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

        "folder_id" : s_look.folder_id,
        "updated_at" : str(s_look.updated_at),
        "user_id" : s_look.user_id,
        "view_count" : s_look.view_count,
        "user" : s_look.user,
        "space_id" : s_look.space_id,   

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
        "query_can_True," : None if 'True,' not in s_look.query.can else s_look.query.can["True,"],
        "query_can_generate_drill_links" : None if 'generate_drill_links' not in s_look.query.can else s_look.query.can["generate_drill_links"],
        "query_can_download" : None if 'download' not in s_look.query.can else s_look.query.can["download"],
        "query_can_download_unlimited" : None if 'download_unlimited' not in s_look.query.can else s_look.query.can["download_unlimited"],
        "query_can_schedule" : None if 'schedule' not in s_look.query.can else s_look.query.can["schedule"],
        "query_can_render" : None if 'render' not in s_look.query.can else s_look.query.can["render"],

        "query_id" : s_look.query.id,
        "query_fields" : s_look.query.fields,
        "query_pivots" : s_look.query.pivots,
        "query_fill_fields" : s_look.query.fill_fields,

        "query_filters_order_items.created_date" : None if 'order_items.created_date' not in s_look.query.filters else str(s_look.query.filters["order_items.created_date"]),
        "query_filters_users.country" : None if 'users.country' not in s_look.query.filters else s_look.query.filters["users.country"],
        "query_filters_users.state" : None if 'users.state' not in s_look.query.filters else s_look.query.filters["users.state"],
        "query_filters_users.city" : None if 'users.city' not in s_look.query.filters else s_look.query.filters["users.city"],

        "query_filter_expression" : s_look.query.filter_expression,
        "query_sorts" : s_look.query.sorts,
        "query_limit" : s_look.query.limit,
        "query_column_limit" : s_look.query.column_limit,
        "query_row_total" : s_look.query.row_total,
        "query_subtotals" : s_look.query.subtotals,
        "query_sorts" : s_look.query.sorts,

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


#print(s_looks_DataFrame)

#-----------------------------------------Single Look ~End ---------------------------------------------------------

#-----------------------------------------All looks ~Start -------------------------------------------------------

looks_DictList = []
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
        "look_space_id" : look.space_id,
'''
        #"look_space_certificate" : look.space.certificate,
        "look_space_file_type" : look.space.file_type,
        "look_space_database" : look.space.database,
        "look_space_db_timezone" : str(look.space.db_timezone),
        "look_space_query_timezone" : str(look.space.query_timezone),
        "look_space_schema" : look.space.schema,
        "look_space_max_connections" : look.space.max_connections,
        "look_space_max_billing_gigabytes" : look.space.max_billing_gigabytes,
        "look_space_ssl" : look.space.ssl,
        "look_space_verify_ssl" : look.space.verify_ssl,
        "look_space_tmp_db_name" : look.space.tmp_db_name,
        "look_space_jdbc_additional_params" : look.space.jdbc_additional_params,
        "look_space_pool_timeout" : look.space.pool_timeout,
        "look_space_dialect_name" : look.space.dialect_name,
        "look_space_created_at" : str(look.space.created_at),
'''
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


#print(looks_DataFrame)

#-----------------------------------------All looks ~End ---------------------------------------------------------



#-----------------------------------------All Projects ~Start -------------------------------------------------------

projects_DictList = []
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

#print(projects_DataFrame)

#-----------------------------------------All Projects ~End ---------------------------------------------------------

#-----------------------------------------All Group ~Start -------------------------------------------------------

group_DictList = []
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

        "group_can_add_to_content_metadata" : group.can_add_to_content_metadata,
        "group_contains_current_user" : group.contains_current_user,
        "group_external_group_id" : group.external_group_id,
        "group_externally_managed" : group.externally_managed,
        "group_id" : group.id,
        "group_include_by_default" : group.include_by_default,
        "group_name" : group.name,
        "group_user_count" : group.user_count,
    }
    group_DictList.append(groupDict)


#print(group_DataFrame)

#-----------------------------------------All Group ~End ---------------------------------------------------------




#-----------------------------------------Single Role Users ~Start -------------------------------------------------------


r_users_DictList = []
role_users = sdk.role_users(role_id=2)
for user in role_users:
    roles_Dict = {
        "user_can_show" : None if 'show' not in user.can else user.can["show"],
        "user_can_index" : None if 'index' not in user.can else user.can["index"],
        "user_can_show_details" : None if 'show_details' not in user.can else user.can["show_details"],
        "user_can_index_details" : None if 'index_details' not in user.can else user.can["index_details"],
        "user_can_sudo" : None if 'sudo' not in user.can else user.can["sudo"],

        "user_avatar_url" : user.avatar_url,
        "user_avatar_url_without_sizing" : user.avatar_url_without_sizing,
        "user_credentials_api3" : user.credentials_api3,
        "user_credentials_email" : user.credentials_email,
        "user_credentials_embed" : user.credentials_embed,
        "user_credentials_google" : user.credentials_google,
        "user_credentials_ldap" : user.credentials_ldap,
        "user_credentials_looker_openid" : user.credentials_looker_openid,
        "user_credentials_oidc" : user.credentials_oidc,
        "user_credentials_saml" : user.credentials_saml,
        "user_credentials_totp" : user.credentials_totp,
        "user_display_name" : user.display_name,
        "user_email" : user.email,
        "user_embed_group_space_id" : user.embed_group_space_id,
        "user_first_name" : user.first_name,
        "user_group_ids" : user.group_ids,
        "user_home_space_id" : user.home_space_id,
        "user_home_folder_id" : user.home_folder_id,
        "user_id" : user.id,
        "user_is_disabled" : user.is_disabled,
        "user_last_name" : user.last_name,
        "user_locale" : user.locale,
        "user_looker_versions" : user.looker_versions,
        "user_models_dir_validated" : user.models_dir_validated,
        "user_personal_space_id" : user.personal_space_id,
        "user_personal_folder_id" : user.personal_folder_id,
        "user_presumed_looker_employee" : user.presumed_looker_employee,
        "user_role_ids" : user.role_ids,
        "user_sessions" : user.sessions,
        "user_ui_state" : user.ui_state,
        "user_verified_looker_employee" : user.verified_looker_employee,
        "user_roles_externally_managed" : user.roles_externally_managed,
        "user_allow_direct_roles" : user.allow_direct_roles,
        "user_allow_normal_group_membership" : user.allow_normal_group_membership,
        "user_allow_roles_from_normal_groups" : user.allow_roles_from_normal_groups,
        "user_url" : user.url,
    }
    r_users_DictList.append(roles_Dict)

#print(roles_DataFrame)

#-----------------------------------------Single Role Users ~End ---------------------------------------------------------


#-----------------------------------------All Spaces -Start --------------------------------------------------------

spacesDictList = []
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

#print(spacesDataFrame)

#-----------------------------------------All Spaces -End ----------------------------------------------------------


#-----------------------------------------All User Attributes ~Start -----------------------------------------------

user_attr_DictList = []
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


#print(user_attr_DataFrame)


#-----------------------------------------All User Attributes ~End -----------------------------------------------
#--------------------------------------------All Users-Start---#-----------------------------------------


userDictList = []
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
        "ui_state_homepageGroupIdPreference" : None if user.ui_state is None else user.ui_state["homepageGroupIdPreference"],

       
    }
    userDictList.append(userDict)


#print(userDataFrame)


#########################################--All Users-End--##################################



modelDict = pd.DataFrame(ModelDictList)
exploreDict = pd.DataFrame(ExploreDictList)
aliasesDict = pd.DataFrame(AliasesDictList)
setsDict = pd.DataFrame(SetsDictList)
dimensionDict = pd.DataFrame(DimensionDictList)
parameterDict = pd.DataFrame(ParameterDictList)
joinDict = pd.DataFrame(JoinsDictList)
measureTypeList = pd.DataFrame(MeasureTypeDictList)
filterDict = pd.DataFrame(FiltersDictList)
alwaysFilterDict = pd.DataFrame(AlwaysFilterDictList)
connectionList = pd.DataFrame(conDictList) 
allDashboardList = pd.DataFrame(dashboardDictList)
dashboardList = pd.DataFrame(dash_DictList)
dashelementList = pd.DataFrame(dashElementDictList)
dimlist = pd.DataFrame(dim_list)
dimensionList = pd.DataFrame(dimension_DictList)
groupUserList = pd.DataFrame(groups_DictList)
datagroupsList = pd.DataFrame(data_groups_DictList)
rolesList = pd.DataFrame(roles_DictList)
localesList = pd.DataFrame(locales_DictList)
singlelooksList = pd.DataFrame(s_looks_DictList)
looksList = pd.DataFrame(looks_DictList)
projectsList = pd.DataFrame(projects_DictList)
allGroupsList = pd.DataFrame(group_DictList)
singleRoleUserList = pd.DataFrame(r_users_DictList)
spacesList = pd.DataFrame(spacesDictList)
userAttributeList = pd.DataFrame(user_attr_DictList)
userList = pd.DataFrame(userDictList)
with pd.ExcelWriter('data_dictionary.xlsx') as writer: 

    projectsList.to_excel(writer,sheet_name='projects')
    looksList.to_excel(writer,sheet_name='Looks')
    modelDict.to_excel(writer,sheet_name='models')
    allGroupsList.to_excel(writer,sheet_name='all_groups')
    userList.to_excel(writer,sheet_name='all_users')
    singleRoleUserList.to_excel(writer,sheet_name='single_user')
    spacesList.to_excel(writer,sheet_name='spaces')
    userAttributeList.to_excel(writer,sheet_name='user_attribute')
    exploreDict.to_excel(writer,sheet_name='explore')
    aliasesDict.to_excel(writer,sheet_name='aliases')
    setsDict.to_excel(writer,sheet_name='sets')
    dimensionDict.to_excel(writer,sheet_name='dimension')
    parameterDict.to_excel(writer,sheet_name='parameter')
    joinDict.to_excel(writer,sheet_name='joins')
    measureTypeList.to_excel(writer,sheet_name='measure_type')
    filterDict.to_excel(writer,sheet_name='filters')
    alwaysFilterDict.to_excel(writer,sheet_name='always_filter')
    allDashboardList.to_excel(writer,sheet_name='all_Dashboards')
    dashboardList.to_excel(writer,sheet_name='dashboard')
    dashelementList.to_excel(writer,sheet_name='dashboard_element')
    connectionList.to_excel(writer,sheet_name='connections')
    dimlist.to_excel(writer,sheet_name='dim_list')
    dimensionList.to_excel(writer,sheet_name='dimension_List')
    datagroupsList.to_excel(writer,sheet_name='all_data_groups')
    groupUserList.to_excel(writer,sheet_name='single_group_User')
    rolesList.to_excel(writer,sheet_name='roles')
    localesList.to_excel(writer,sheet_name='locales')
    singlelooksList.to_excel(writer,sheet_name='single_look')

''' 
    dashboard_elements = sdk.dashboard_dashboard_elements(dashboard_id = dashboard.id)

dash1 = sdk.all_dashboards()
for q in dash1:
    total_dashboards = total_dashboards + 1
    can = q.can
    content_favorite_id = q.content_favorite_id
    description = q.description
    hidden = q.hidden
    Id = q.id
    model = q.model
    query_timezone = q.query_timezone
    readonly = q.readonly
    refresh_interval = q.refresh_interval
    refresh_interval_to_i = q.refresh_interval_to_i 
    folder = q.folder
    title = q.title
    user_id = q.user_id
    space = q.space
        
    print("\nDashboardBase \n can =")
    for c,r in can.items():
        print("       {} : {}".format(c,r))
    print("\n content_favorite_id : {} \n description: {} \n hidden : {} \n Id : {} \n model : {} \n query_timezone: {} \n readonly : {} \n refresh_interval : {} \n refresh_interval_to_i : {} \n"
    .format(content_favorite_id,description,hidden, Id, model, query_timezone, readonly, refresh_interval ,refresh_interval_to_i))
    print("folder \n FolderBase =")
    for f in folder:
                
        if type(folder[f]) != dict :
            print("       {} : {}".format(f,folder[f]))
        
        if type(folder[f]) == dict :
            print("\n       can =")
            for i,b in folder[f].items():
                print("          {} : {} ".format(i,b))
    print("\n title : {} \n user_id : {} \n".format(title, user_id))
    print("space \n SpaceBase =")
    for s in space:
       
        if type(space[s]) != dict :
            print("       {} : {}".format(s,space[s]))
        
        if type(space[s]) == dict :
            print("\n       can =")
            for i,b in space[s].items():
                print("          {} : {} ".format(i,b))

    dash_element = sdk.dashboard_dashboard_elements(dashboard_id = q.id)

    #logging.info(dash_element)

    for element in dash_element:
        total_dashboard_elements = total_dashboard_elements + 1

a_looks = sdk.all_looks()
#logging.info(a_looks)
for a in a_looks:
    total_looks = total_looks + 1
    look = sdk.look(look_id = a.id)
    if look.query.dynamic_fields is not None:
        total_dynamic_fields = total_dynamic_fields + 1

all_user_attributes = sdk.all_user_attributes()
print(all_user_attributes)
for user_attribute in all_user_attributes:
    print(user_attribute)
    total_user_attributes = total_user_attributes + 1

all_roles = sdk.all_roles()
print(all_roles)
for role in all_roles:
    total_roles = total_roles + 1

all_users = sdk.all_users()
#print(all_users)
for user in all_users:
    print(user)
    total_users = total_users + 1

g_group = sdk.all_groups()
print(g_group)
for group in g_group:
    groupId = group.id
    total_groups = total_groups + 1
    group_users = sdk.all_group_users(group_id=groupId)
    for group_user in group_users:
        print(group_user)
        total_users_in_groups = total_users_in_groups + 1

data_groups = sdk.all_datagroups()
for data_group in data_groups:
    total_datagroups = total_datagroups + 1
    print(data_group.id)

models = sdk.all_lookml_models(fields= 'name, label, project_name, allowed_db_connection_names,has_content,can, unlimited_db_connections, explores')
for model in models:
    if model.name == 'system__activity':
        continue
    total_models = total_models + 1
    name = model.name
    label = model.label
    project_name = model.project_name
    has_content = model.has_content
    allowed_db_connection_names = model.allowed_db_connection_names
    can = model.can
    unlimited_db_connections = model.unlimited_db_connections
    explores = model.explores
    print("Name: {}\n Label: {}\n Project Name: {}\n allowed_db_connection_names: {}\n has_content: {}\n unlimited_db_connections: {}\n "
          .format(name, label,project_name,allowed_db_connection_names,has_content,unlimited_db_connections))
    print("Can:")
    for x, y in can.items():
        print("   {}:{}".format(x,y))

    
    for explore in model.explores:
        print(explore.name)
        exploreObj = sdk.lookml_model_explore(
            lookml_model_name=model.name,
            explore_name=explore.name
        )
        total_explores = total_explores + 1

        logging.info(exploreObj)

        for dimension in exploreObj.fields.dimensions:
            #logging.info(dimension)
            dim_def = {
                "name":dimension.name,
                "primary_key": dimension.primary_key,
                "type":dimension.type,
                "view":dimension.view,
                "view_label": dimension.view_label,
                "description": dimension.description,
                "sql": dimension.sql,
                "label_short":dimension.label_short,
            }
            total_dimensions = total_dimensions + 1

            views.append(dimension.source_file_path)

            liquidCondition = re.search(r'\{%.*%\}', dimension.sql)        
            if liquidCondition:
                total_liquid_dimensions = total_liquid_dimensions + 1


            liquidTemplate = re.search(r'\{\{.*\}\}', dimension.sql)        
            if liquidTemplate:
                total_liquid_dimensions = total_liquid_dimensions + 1


        e_measure = exploreObj.fields.measures
        for q in e_measure:
            print("\n")
            for x,y in q.items():
                print("{} : {}".format(x,y))
                total_measures = total_measures + 1 

        s = exploreObj.sets

        for q in s:
            print("\n")
            print(s)
            for t in q:
                print(t)
                total_sets = total_sets + 1

        if exploreObj.always_filter != []:
            total_always_filter = total_always_filter + 1


a_locales = sdk.all_locales()
for locale in a_locales:
    total_locales = total_locales + 1


a_project = sdk.all_projects()

for project in a_project:
    total_projects = total_projects + 1

distinct_views = []

for viewItem in views:
    found = False
    for view in distinct_views:
        if viewItem == view:
            found = True
    if found == False:
        distinct_views.append(viewItem) 
'''

a_space = sdk.all_spaces()
for space in a_space:
    total_spaces = total_spaces + 1



stats = []
stats.append(['Artifact Name', 'Count'])
stats.append(['All Projects', str(total_projects)])
stats.append(['All Spaces', str(total_spaces)])
stats.append(['Total Models', str(total_models)])
stats.append(['Total Explores', str(total_explores)])
stats.append(['Total Dimensions', str(total_dimensions)])
stats.append(['Total Liquid Dimensions', str(total_liquid_dimensions)])
stats.append(['Total Measures', str(total_measures)])
stats.append(['Total Sets', str(total_sets)])
stats.append(['Total Connections', str(total_connections)])
stats.append(['Total Looks', str(total_looks)])
stats.append(['Total Look Dynamic Fields', str(total_dynamic_fields)])
stats.append(['Total Dashboards', str(total_dashboards)])
stats.append(['Total Dashboards Elements', str(total_dashboard_elements)])
stats.append(['Total DataGroups', str(total_datagroups)])
stats.append(['Total Groups', str(total_groups)])
stats.append(['Total Users in all groups', str(total_users_in_groups)])
stats.append(['Total Users', str(total_users)])
stats.append(['Total Roles', str(total_roles)])
stats.append(['Total User Attributes', str(total_user_attributes)])
stats.append(['Total Views', str(len(distinct_views))])
stats.append(['Total Always Filters', str(total_always_filter)])
stats.append(['All Locales', str(total_locales)])

effort_types = []
effort_types.append(['Artifact Name', 'Complexity'])
effort_types.append(['All Projects', 1])
effort_types.append(['All Spaces', 1])
effort_types.append(['Total Models', 1])
effort_types.append(['Total Explores', 5])
effort_types.append(['Total Dimensions', 2])
effort_types.append(['Total Liquid Dimensions', 10])
effort_types.append(['Total Measures', 7])
effort_types.append(['Total Sets', 0])
effort_types.append(['Total Connections', 0])
effort_types.append(['Total Looks', 8])
effort_types.append(['Total Look Dynamic Fields', 10])
effort_types.append(['Total Dashboards', 9])
effort_types.append(['Total Dashboards Elements', 9])
effort_types.append(['Total DataGroups', 2])
effort_types.append(['Total Groups', 2])
effort_types.append(['Total Users in all groups', 2])
effort_types.append(['Total Users', 2])
effort_types.append(['Total Roles', 2])
effort_types.append(['Total User Attributes', 10])
effort_types.append(['Total Views', 2])
effort_types.append(['Total Always Filters', 2])
effort_types.append(['All Locales', 2])

new_stats = []
for i in range(0, len(effort_types) - 1):
    new_stats.append([str(stats[i][0]), str(stats[i][1]), str(effort_types[i][1])])
print(new_stats)
content = ''
for item in new_stats:
    content = content + '{},{},{}\n'.format(item[0], item[1], item[2]) 
f = open('data_dictionary_stats.csv', 'w+')
f.write(content)

