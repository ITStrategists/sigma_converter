from openpyxl import Workbook
from openpyxl.chart import (
    LineChart,
    BarChart,
    Reference,
    Series,
)

total_models = 20
total_explores = 10
total_dimensions = 10
total_measures = 10
total_sets = 10
total_filteres = 10
total_joins = 10
total_connections = 1
total_dashboards = 10
total_datagroups = 10
total_groups = 10
total_users_in_groups = 10
total_users = 0
total_roles = 0
total_user_attributes = 0
total_views = 10
total_always_filter = 0
views = []
distinct_views = []
total_locales = 0
total_projects = 0
total_spaces = 10
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




m = stats
for row in m :
    print(row)
rez = [[m[j][i] for j in range(len(m))] for i in range(len(m[0]))]
print("\n")
for row in rez:
    print(row)

print(rez)


rows = [
        ['Count'  ,            	total_projects,         	total_spaces,           	total_models ,          	total_explores,         	total_dimensions,	total_liquid_dimensions	,total_measures,	total_sets,	total_connections,	total_looks,	total_dynamic_fields,	total_dashboards,	total_dashboard_elements,	total_datagroups,	total_groups,	total_users_in_groups,	total_users,	total_roles	,total_user_attributes	,len(distinct_views),	total_always_filter,	total_locales	],
        ['Complexity',	1,	5,	2,	10,	7,	0,	0,	8,	10,	9,	9,	2,	2,	2,	2,	2,	10,	2,	2,	2,0,1]
]

wb = Workbook()
ws = wb.active

row1 = [
    ["Art", "Projects", "Models"],
    ["Count", 1, 2]
]

for row in row1:
    ws.append(row)

c1 = BarChart()
v2 = Reference(ws, min_col=1, max_col=3, min_row=1, max_row=1)
v1 = Reference(ws, min_col=1, min_row=1, max_col=3)
c1.add_data(v2, titles_from_data= True, from_rows=True)
#c1.add_data(v1, titles_from_data=True, from_rows=True)

c1.x_axis.title = 'Days'
c1.y_axis.title = 'Count'
c1.y_axis.majorGridlines = None
c1.title = 'Conversion Model'
'''

# Create a second chart
c2 = LineChart()
v2 = Reference(ws, min_col=1, min_row=2, max_col=20)
c2.add_data(v2, titles_from_data=True, from_rows=True)
c2.y_axis.axId = 200
c2.y_axis.title = "Complexity"

# Display y-axis of the second chart on the right by setting it to cross the x-axis at its maximum
c1.y_axis.crosses = "max"
c1 += c2



'''
ws.add_chart(c1, "D4")
wb.save("secondary.xlsx")
