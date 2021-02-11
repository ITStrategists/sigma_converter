import lkml
import re

class Column:

    def __init__(self):
        self.name = ''
        self.sql = ''
        self.field = ''
        self.view = ''
        self.columnType = ''

    def setColumn(self, column, type):
    	self.columnType = type
    	if 'name' in column:
    		self.name = column['name']
    	if 'field' in column:
    		self.field = column['field']
    	if 'sql' in column:
    		self.sql = column['sql']    		

    def __str__(self):
        return """
            Column: --------------------------------------------------------------------------------------------------------
            Name:               {name}
            Field:              {field}
            Column Type: 		{columnType}
            SQL: 				{sql}
            """.format(name = self.name, field = self.field, columnType = self.columnType, sql = self.sql)