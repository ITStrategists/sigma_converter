import looker_sdk
import logging
import json
import re
import pandas as pd
from pprint import pprint
from pprint import pformat
from yapf.yapflib.yapf_api import FormatCode  
#from openpyxl import Workbook

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

models = sdk.all_lookml_models(fields= 'name, label, project_name, allowed_db_connection_names,has_content,can, unlimited_db_connections, explores')
ModelDictList = []
for model in models:

    if model.name == "system__activity":
        continue

    str_ = pformat(model)
    formatted_string = FormatCode(str_)
    logging.info(formatted_string)
    
    modelDic = {
    "name":model.name,
    "label":model.label,
    "has_content":model.has_content,
    "name":model.name,
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
        logging.info(exploreFormattedString)

        print(exploreObj.id)
        exploreObj.aliases
        exploredict = {   
        "id":exploreObj.id,
        "modelid":exploreObj.modelid,
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
        "model_name":exploreObj.model_name,
        "view_name":exploreObj.view_name,
        "hidden":exploreObj.hidden,
        "sql_table_name":exploreObj.sql_table_name,
        "access_filter_fields":exploreObj.access_filter_fields,
        "access_filters":exploreObj.access_filters,
        "group_label":exploreObj.group_label,

        
        }    
        for aliases in exploreObj.aliases:
        aliasesdict =    {

        }
        for dimensions in exploreObj.fields.dimensions:
        dimensionsdict =    {

        }
        for parameters in exploreObj.fields.parameters:
        parametersdict = {

        }
        for joins in exploreObj.fields.joins:
        joinsdict =   {

        }
        for supported_measure_types in exploreObj.supported_measure_types:
        measuretypesdict=    {

        }

df = pd.DataFrame(ModelDictList)
df.to_csv("models.csv")

'''
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

        "dialect_name" : cItem.dialect.name,
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


df = pd.DataFrame(conDictList)
df.to_csv("connections.csv")

dashboardDictList = []

dashboards = sdk.all_dashboards()
for dashboard in dashboards:
    #logging.info(dashboard)
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
        "query_timezone" : dashboard.query_timezone,
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
        "folder_created_at" : dashboard.folder.created_at,
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
        "space_created_at" : dashboard.space.created_at,
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
        "space_can_created_at" : None if 'created_at' not in dashboard.space.can else dashboard.space.can["created_at"],
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

    dashboard_elements = sdk.dashboard_dashboard_elements(dashboard_id = dashboard.id)
    for dashboard_element in dashboard_elements:
        logging.info(dashboard_element)

dashboardDataFrame = pd.DataFrame(dashboardDictList)
#print(dashboardDataFrame)
dashboardDataFrame.to_csv("dashboards.csv")

    
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

    logging.info(dash_element)

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

'''
