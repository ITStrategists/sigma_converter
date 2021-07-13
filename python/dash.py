import looker_sdk
import logging
import pandas as pd

sdk = looker_sdk.init31('looker.ini')

logging.basicConfig(filename='data_dictionary.log',level=logging.INFO, filemode='w', format = '%(asctime)s:%(levelname)s:%(message)s')
spaces = sdk.all_spaces()
spacesDictList = []
total_spaces = 0
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
'''

s_dashboard = sdk.all_dashboards()
dashboardDictList = []
total_dashboards = 0
deleted_dashboards = 0
for dash in s_dashboard:
    dashboardResponse = sdk.search_dashboards(id = dash.id)
    if len(dashboardResponse) == 0:
        continue
    dashboard = dashboardResponse[0]
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
        "deleted": dashboard.deleted,
        "deleted_at": dashboard.deleted_at,
        "deleter_id": dashboard.deleter_id,
        "view_count": dashboard.view_count
    }
    dashboardDictList.append(dashboardDict)
    total_dashboards = total_dashboards + 1
    if dashboard.deleted == True:
        deleted_dashboards = deleted_dashboards + 1

logging.info("Total Dashboards: {}".format(total_dashboards))
logging.info("Deleted Dashboards: {}".format(deleted_dashboards))
logging.info("To be Migrated Dashboards: {}".format(total_dashboards - deleted_dashboards))



'''

path = 'dashboards.xlsx'
writer = pd.ExcelWriter(path, engine='xlsxwriter')

'''
allDashboardList = pd.DataFrame(dashboardDictList)
allDashboardList.to_excel(writer,sheet_name='Dashboards',index=False)

'''

allSpacesList = pd.DataFrame(spacesDictList)
allSpacesList.to_excel(writer,sheet_name='Spaces',index=False)
writer.save()