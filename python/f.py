from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from xlsxwriter.workbook import Workbook as ChartWorkBook
import pandas as pd


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

stats = []
stats.append(['Artifact Name',                  'Count',                        'complexity'    ])
stats.append(['All Projects',               	(total_projects),                1           ])
stats.append(['All Spaces',                 	(total_spaces),                  1           ])
stats.append(['Total Models',               	(total_models),                  1           ])
stats.append(['Total Explores',             	(total_explores),                5           ])
stats.append(['Total Dimensions',           	(total_dimensions),				2			])
stats.append(['Total Liquid Dimensions',        (total_liquid_dimensions),		10			])
stats.append(['Total Measures', 				(total_measures),				7			])
stats.append(['Total Sets', 					(total_sets),					0			])
stats.append(['Total Connections', 				(total_connections),				0			])
stats.append(['Total Looks', 					(total_looks),					8			])
stats.append(['Total Look Dynamic Fields', 		(total_dynamic_fields),			10			])
stats.append(['Total Dashboards', 				(total_dashboards),				9			])
stats.append(['Total Dashboards Elements', 		(total_dashboard_elements),		9			])
stats.append(['Total DataGroups', 				(total_datagroups),				2			])
stats.append(['Total Groups', 					(total_groups),					2			])
stats.append(['Total Users in all groups', 		(total_users_in_groups),			2			])
stats.append(['Total Users', 					(total_users),					2			])
stats.append(['Total Roles', 					(total_roles),					2			])
stats.append(['Total User Attributes', 			(total_user_attributes),			10			])
stats.append(['Total Views', 					(len(distinct_views)),			2			])
stats.append(['Total Always Filters', 			(total_always_filter),			2			])
stats.append(['All Locales', 					(total_locales),					2			])

path = 'data_dictionary.xlsx'

measureList=pd.DataFrame([{"DAta":"asdfa"}])


n = stats
i = 0
m = []
for i in range(0, len(n)- 1):
    if i > 0:
        m.append(n[i]) 
#for row in m :

    #print(row)
rez = [[m[j][i] for j in range(len(m))] for i in range(len(m[0]))]
print("\n")

workbook  = ChartWorkBook(path)
worksheet = workbook.add_worksheet()
worksheet.name = "Stats"

# Add a format for the headings.
bold = workbook.add_format({'bold': True})

# Add the worksheet data that the charts will refer to.
#["Art", "Projects", "Models"],
headings = ['Artifacts', 'Count', 'Complexity']


data = rez

worksheet.write_row('A1', headings, bold)
worksheet.write_column('A2', data[0])
worksheet.write_column('B2', data[1])
worksheet.write_column('C2', data[2])

# Create a new column chart. This will use this as the primary chart.
column_chart2 = workbook.add_chart({'type': 'column'})

# Configure the data series for the primary chart.
column_chart2.add_series({
    'name':       '=Stats!$B$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$B$2:$B$22',
})

# Create a new column chart. This will use this as the secondary chart.
line_chart2 = workbook.add_chart({'type': 'line'})

# Configure the data series for the secondary chart. We also set a
# secondary Y axis via (y2_axis). This is the only difference between
# this and the first example, apart from the axis label below.
line_chart2.add_series({
    'name':       '=Stats!$C$1',
    'categories': '=Stats!$A$2:$A$22',
    'values':     '=Stats!$C$2:$C$22',
    'y2_axis':    True,
})

# Combine the charts.
column_chart2.combine(line_chart2)

# Add a chart title and some axis labels.
column_chart2.set_title({  'name': 'Looker Sigma Complexity'})
column_chart2.set_x_axis({ 'name': 'Artifacts'})
column_chart2.set_y_axis({ 'name': 'Counts'})

column_chart2.set_size({'width': 900, 'height': 500})
# Note: the y2 properties are on the secondary chart.
line_chart2.set_y2_axis({'name': 'Complexity'})

# Insert the chart into the worksheet
worksheet.insert_chart('F1', column_chart2)


worksheet = workbook.add_worksheet()
worksheet.name = "Data"
worksheet.write(stats)
