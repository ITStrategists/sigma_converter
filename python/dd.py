
import logging
import json
import re
import pandas as pd
from itertools import chain

logging.basicConfig(filename='data_dictionary1.log',level=logging.INFO, filemode='w', format = '%(asctime)s:%(levelname)s:%(message)s')

total_models = 62
total_explores = 443
total_dimensions = 28362
total_measures = 4064
total_sets = 1722
total_filteres = 0
total_joins = 0
total_connections = 12
total_dashboards = 640
total_datagroups = 75
total_groups = 12
total_users_in_groups = 12
total_users = 221
total_roles = 5
total_user_attributes = 11
total_views = 0
total_always_filter = 34
views = []
distinct_views = []
total_locales = 0
total_projects = 17
total_spaces = 152
total_liquid_dimensions = 57
total_dashboard_elements = 7104
total_dynamic_fields = 2842
total_looks = 732
total_aliases = 0


#Lenth
total_views = (len(distinct_views))
total_views = 1056
stats = []
stats.append(['Artifact Name',               'Count',                  'complexity'     ,'Estimate (Hours)'                    , 'After complexity' , 'After Estimate (Hours)' ,])
stats.append(['Projects',               	(total_projects),               1           , 2 * (total_projects) * 1              ,       1           ,       2 * (total_projects) * 1             ])
stats.append(['Spaces',                 	(total_spaces),                 1           , 0.25 * (total_spaces) * 1                ,       1           ,       0.25 * (total_spaces) * 1              ])
stats.append(['Models',               	    (total_models),                 1           , 4 * (total_models) * 1                ,       1           ,       4 * (total_models) * 1               ])
stats.append(['Explores',             	    (total_explores),               7           , 0.14 * (total_explores) * 7           ,       7           ,       0.14 * (total_explores) * 7              ])
stats.append(['Dimensions',           	    (total_dimensions),				1			, 0.004 * (total_dimensions) * 1         ,       7           ,       0.004 * (total_dimensions) * 7               ])
stats.append(['Liquid Dimensions',          (total_liquid_dimensions),		10			, 0 * (total_liquid_dimensions) * 10    ,       10          ,       0 * (total_liquid_dimensions) * 10               ])
stats.append(['Measures', 				    (total_measures),				7			, 0.012 * (total_measures) * 7           ,       7           ,       0.012 * (total_measures) * 7               ]) 
stats.append(['Sets', 					    (total_sets),					0			, 0 * (total_sets) * 0                  ,       0           ,       0 * (total_sets) * 0                ])
stats.append(['Connections', 				(total_connections),			0			, 0 * (total_connections) * 0           ,       0           ,       0 * (total_connections) * 0               ])
stats.append(['Looks', 					    (total_looks),					8			, 0.075 * (total_looks) * 7          ,       8               ,       0.075 * (total_looks) * 7                 ])
stats.append(['Look Dynamic Fields', 		(total_dynamic_fields),			10			, 0 * (total_dynamic_fields) * 10       ,       10          ,       0 * (total_dynamic_fields) * 10                ])
stats.append(['Dashboards', 				(total_dashboards),				7			, 0.05 * (total_dashboards) * 7         ,       7           ,       0.05 * (total_dashboards) * 7                ])
stats.append(['Dashboards Elements', 		(total_dashboard_elements),		7			, 0.01 * (total_dashboard_elements) * 7 ,       7           ,       0.01 * (total_dashboard_elements) * 7               ])
stats.append(['DataGroups', 				(total_datagroups),				2			, 0.5 * (total_datagroups) * 2            ,       2           ,       0.5 * (total_datagroups) * 2              ])
stats.append(['Groups', 					(total_groups),					1			, 20                                    ,       1           ,       20              ])
stats.append(['Users Groups', 		        (total_users_in_groups),		2			, 20                                    ,       2           ,       20               ])
stats.append(['Users', 					    (total_users),					2			, 20                                    ,       2               ,       20              ])
stats.append(['Roles', 					    (total_roles),					2			, 0.01 * (total_roles) * 2          ,       2               ,       0.01 * (total_roles) * 2               ])
stats.append(['User Attributes', 			(total_user_attributes),		10			, 0 * (total_user_attributes) * 10      ,       10          ,       0 * (total_user_attributes) * 10                ])
stats.append(['Views', 			            total_views,	        		2			, 0.05 * total_views * 2                ,       8           ,       0.05 * total_views * 8                ])
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

path = 'data_dictionary1.xlsx'

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
headings = ['Artifacts', 'Count', 'Complexity after SigmaMe','Hours after SigmaMe', 'Comlexity before SigmaMe','Hours before SigmaMe']

data = rez

#hours = ["=ROUND(SUM(D2:D22),0)"]


#---------------------------------------Writing data to Excel Column ------------------------------------------- 

worksheet.write_row('A1', headings, bold)
worksheet.write_column('A2', data[0])
worksheet.write_column('B2', data[1])
worksheet.write_column('C2', data[2])
worksheet.write_column('D2', data[3])
worksheet.write_column('E2', data[4])
worksheet.write_column('F2', data[5])


worksheet.write('C24', "Total Hours")
worksheet.write('C25', "Total Weeks")
worksheet.write_formula('D24', '=ROUND(SUM(D2:D22),0)')
worksheet.write_formula('D25', '=ROUND(SUM(D2:D22)/40,0)')

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

#worksheet.insert_chart('H1', column_chart2)


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

#worksheet.insert_chart('F30', column_chart3)

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

worksheet.insert_chart('H1', column_chart5)
#--------------------------------------------------------------------------------------------
# modelDict = pd.DataFrame(model_DictList)
# exploreDict = pd.DataFrame(explore_DictList)
# aliasesDict = pd.DataFrame(aliases_DictList)
# setsDict = pd.DataFrame(sets_DictList)
# dimensionDict = pd.DataFrame(dimension_DictList)
# parameterDict = pd.DataFrame(parameter_DictList)
# joinDict = pd.DataFrame(joins_DictList)
# measureTypeList = pd.DataFrame(measure_type_DictList)
# filterDict = pd.DataFrame(filters_DictList)
# alwaysFilterDict = pd.DataFrame(always_filter_DictList)
# connectionList = pd.DataFrame(conDictList) 
# allDashboardList = pd.DataFrame(dashboardDictList)
# dashelementList = pd.DataFrame(dashElementDictList)
# dimensionList = pd.DataFrame(dimension_DictList)
# liquidDimensionList = pd.DataFrame(liquid_dimension_DictList)
# groupUserList = pd.DataFrame(groups_DictList)
# datagroupsList = pd.DataFrame(data_groups_DictList)
# rolesList = pd.DataFrame(roles_DictList)
# localesList = pd.DataFrame(locales_DictList)
# singlelooksList = pd.DataFrame(s_looks_DictList)
# looksList = pd.DataFrame(looks_DictList)
# projectsList = pd.DataFrame(projects_DictList)
# allGroupsList = pd.DataFrame(group_DictList)
# spacesList = pd.DataFrame(spacesDictList)
# userAttributeList = pd.DataFrame(user_attr_DictList)
# userList = pd.DataFrame(userDictList)
# viewsList = pd.DataFrame({"Views": distinct_views})
# measureList = pd.DataFrame(measure_DictList)


# projectsList.to_excel(writer,sheet_name='Projects',index=False)
# looksList.to_excel(writer,sheet_name='Looks',index=False)
# modelDict.to_excel(writer,sheet_name='Models',index=False)
# allGroupsList.to_excel(writer,sheet_name='Groups',index=False)
# userList.to_excel(writer,sheet_name='Users',index=False)
# spacesList.to_excel(writer,sheet_name='Spaces',index=False)
# userAttributeList.to_excel(writer,sheet_name='User Attribute',index=False)
# exploreDict.to_excel(writer,sheet_name='Explore',index=False)
# aliasesDict.to_excel(writer,sheet_name='Aliases',index=False)
# setsDict.to_excel(writer,sheet_name='Sets',index=False)
# dimensionDict.to_excel(writer,sheet_name='Dimension',index=False)
# liquidDimensionList.to_excel(writer,sheet_name='Liquid Dimension',index=False)
# parameterDict.to_excel(writer,sheet_name='Parameter',index=False)
# joinDict.to_excel(writer,sheet_name='Joins',index=False)
# measureTypeList.to_excel(writer,sheet_name='Measure_Type',index=False)
# filterDict.to_excel(writer,sheet_name='Filters',index=False)
# alwaysFilterDict.to_excel(writer,sheet_name='Always Filter',index=False)
# allDashboardList.to_excel(writer,sheet_name='Dashboards',index=False)
# dashelementList.to_excel(writer,sheet_name='Dashboard Element',index=False)
# connectionList.to_excel(writer,sheet_name='Connections',index=False)
# dimensionList.to_excel(writer,sheet_name='Dimension List',index=False)
# datagroupsList.to_excel(writer,sheet_name='Data Groups',index=False)
# groupUserList.to_excel(writer,sheet_name='User Group',index=False)
# rolesList.to_excel(writer,sheet_name='Roles',index=False)
# localesList.to_excel(writer,sheet_name='Locales',index=False)
# singlelooksList.to_excel(writer,sheet_name='Single look',index=False)
# viewsList.to_excel(writer,sheet_name='Views',index=False)
# measureList.to_excel(writer,sheet_name="Measure",index=False)

writer.save()