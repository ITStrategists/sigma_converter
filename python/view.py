import lkml
import re

from dimension import Dimension
from column import Column

class View:
    def __init__(self):
        self.name = ''
        self.sql = ''
        self.databaseName = ''
        self.schemaName = ''
        self.dimenions = []
        self.targetSchema = ''
        self.viewType = ''
        self.persistedType = ''
        self.persistedSQL = ''
        self.dependencies = ''
        self.dbtModelName = ''
        self.sql_table_name = None
        self.allDimensions = []
        self.validDimensions = []
        self.excludedDimensions = []
        self.exploreSourceName = None
        self.columns = []
        self.exploreSourceView = None

    def getViewByName(self, viewName, viewList):
        view = None
        for viewItem in viewList:
            if view.name == viewItem.name:
                view = viewItem
                break
        return view

    def processNDTColumns(self, exploreSourceView, columns):

        columns_ = []
        dimension_ = Dimension()
        column = []

        for column_ in columns:
            if column_.columnType == 'COLUMN':
                fieldName = column_.transformExploreField(self.exploreSourceName, column_.field)
                dimension_ = dimension_.getDimensionByName(fieldName, exploreSourceView.allDimensions)
                column_.sql = dimension_.sql
                column_.dimensionType = dimension_.dimensionType

        for column_ in columns:
            if column_.columnType == 'DERIVED_COLUMN':
                processedSQL = column_.sql
                possibleColumns = column_.sql.split(' ')
                isMeasure = False
                for possibleColumn in possibleColumns:
                    baseColumn = column_.getColumnByName(possibleColumn, columns)
                    if baseColumn:
                        if baseColumn.dimensionType == "MEASURE":
                            isMeasure = True
                        processedSQL = processedSQL.replace(baseColumn.name, baseColumn.sql)

                column_.sql = processedSQL

                if isMeasure:
                    column_.dimensionType = "MEASURE"
                else:
                    column_.dimensionType = "DIMENSION"

            print(column_)

        for dimension_ in self.allDimensions:
            column_ = column_.getColumnByName(dimension_.name, columns)
            dimension_.sql = column_.sql
            dimension_.dimensionType = column_.dimensionType
            
    def processNDT(self, viewList):
        view_ = self.getViewByName(self.exploreSourceName, viewList)
        self.exploreSourceView = view_
        processedColumns = self.processNDTColumns(self.exploreSourceView, self.columns)

    
    def getNDTViewSQL(self):

        viewSQL = ''

        dimList = []

        for dimension in self.allDimensions:

            if dimension.name.upper().strip() != dimension.sql.upper().strip():
                row = "{} AS {}".format(dimension.sql.strip(), dimension.name.upper().strip())
                dimList.append(row)

            else:
                row = "{}".format(dimension.name.upper().strip())
                dimList.append(row)

        cols =  ',\n'.join(dimList)

        exploreSourceSQL = self.exploreSourceView.sql

        if cols == None or cols.strip() == '':
            cols = '*'

        groupByColumnsList = []

        for index in range(len(dimList)):
            dimension_ = self.allDimensions[index]
            if dimension_.dimensionType == 'DIMENSION':
                groupByColumnsList.append(str(index + 1))

        groupByColumns = ','.join(groupByColumnsList)

        if self.viewType == 'NDT':
            viewSQL = """
            SELECT
            {cols}
            FROM ({sql})
            GROUP BY {groupByColumns}
            """.format(cols = cols, sql = exploreSourceSQL, groupByColumns = groupByColumns)

        self.sql = viewSQL
        print(self.sql)    
        return viewSQL


    def setDBTModelName(self):
        self.dbtModelName = self.targetSchema.lower().strip().replace(' ', '_') + '_' + self.name.lower().strip().replace(' ', '_')

    def getDBTModelName(self):
        return self.dbtModelName



    def setView(self, view):

        if 'derived_table' in view:

            if 'explore_source' in view['derived_table']:
                self.viewType = 'NDT'
                exploreSource = view['derived_table']['explore_source']
                if 'name' in exploreSource:
                    self.exploreSourceName = view['derived_table']['explore_source']['name']

                if 'columns' in exploreSource:
                    columns = exploreSource['columns']
                    for column_ in columns:
                        columnObj = Column()
                        columnObj.setColumn(column_, "COLUMN")
                        self.columns.append(columnObj)
                if 'derived_columns' in exploreSource:
                    columns = exploreSource['derived_columns']
                    for column_ in columns:
                        columnObj = Column()
                        columnObj.setColumn(column_, "DERIVED_COLUMN")
                        self.columns.append(columnObj)
            else:        
                if 'sql' in view['derived_table']:
                    self.sql = view['derived_table']['sql']
                    self.sql = self.sql.replace('"','\"')

                if 'persist_for' in view['derived_table']:
                    self.persistedSQL = view['derived_table']['persist_for']
                    self.persistedSQL = self.persistedSQL.replace('"', "\"")
                    self.persistedType = 'PERSIST_FOR'

                if 'sql_trigger_value' in view['derived_table']:
                    self.persistedSQL = view['derived_table']['sql_trigger_value']
                    self.persistedSQL = self.persistedSQL.replace('"', "\"")
                    self.persistedType = 'SQL_TRIGGER_VALUE'

                self.viewType = 'PDT'

        elif 'sql_table_name' in view:
            self.viewType = 'VIEW'
            self.sql_table_name = view['sql_table_name']

        if 'name' in view:
            self.name = view['name']


        if self.viewType == 'VIEW' or self.viewType == 'PDT' or self.viewType == 'NDT':

            dimensions_ = []
            viewDimensions = []
            viewMeasures = []
            viewDimensionGroups = []

            if 'dimensions' in view:
                viewDimensions = view['dimensions']
                print(viewDimensions)

            if 'measures' in view:
                viewMeasures = view['measures']

            if 'dimension_groups' in view:
                viewDimensionGroups = view['dimension_groups']

            dimensions_ = Dimension().processDimensions(viewDimensions, viewMeasures, viewDimensionGroups)

            allDimensions = Dimension().getProcessedSubstituteDimensions(dimensions_)

            validDimensions = []
            excludedDimensions = []

            for dimension_ in allDimensions:
                if not dimension_.isExcluded:
                    validDimensions.append(dimension_)
                else:
                    excludedDimensions.append(dimension_)

            self.allDimensions = allDimensions
            self.dimensions = validDimensions
            self.validDimensions = validDimensions
            self.excludedDimensions = excludedDimensions

            for v in self.excludedDimensions:
                print(v)

    def checkKeyExists(self, key, dictionary):
        found = False

        if key in dictionary:
            found = True

        return found

    def getKeyValue(self, key, dictionary):
        val = {}

        for item in dictionary:
            if key in item:
                val = item
        return val

    def injectViewSchema(self):

        if self.sql is not None and self.sql != '':

            processedSQL = re.sub(r'\s+',' ', self.sql.replace('\n', ' ').replace('\t', ' '))
            print("Source SQL: {}".format(processedSQL))
            dependencies = []

            keywords = ['lateral']

            rx = re.compile(r'FROM\s+(\w+\s*\w*,\s*\w+\s*\w*)\s+',re.IGNORECASE)

            for match in rx.finditer(processedSQL):
                group = match.group(1)
                print("-------------------------------------")
                print(group)
                found = False
                for keyword in keywords:
                    if keyword in group:
                        found = True

                if not found:
                    from_ = 'FROM {}'.format(group)
                    list_ = group.split(',')
                    transformedList = []
                    dictList = []
                    for item in list_:
                        itemStripped = item.strip()
                        if '.' not in itemStripped:
                            transformItem = "{}.{}".format(self.schemaName, itemStripped)
                            dictObj = {itemStripped:transformItem}
                            check = self.checkKeyExists(itemStripped, dictList) 
                            if not check:
                                dictList.append(dictObj)

                        else:
                            dictObj = {itemStripped:itemStripped}
                            found = self.checkKeyExists(itemStripped, dictList)
                            if not check:
                                dictList.append(dictObj)

                    schemaConcatenatedList = []

                    for item in list_:
                        key = item.strip()
                        schemaConcatenatedItem = self.getKeyValue(key, dictList)
                        value = schemaConcatenatedItem[key]
                        schemaConcatenatedList.append(value)

                    to_ = "FROM {}".format(' , '.join(schemaConcatenatedList))

                    processedSQL = re.sub(r'{}'.format(from_), to_, processedSQL, flags=re.I)            


            processedSQL = re.sub(r'\s+',' ', processedSQL)
            
            rx = re.compile(r'(\w+\(*\w+\s+)FROM\s+(\w+)', re.IGNORECASE)
            substitued = []
            for match in rx.finditer(processedSQL):
                rxExtract = re.compile(r'EXTRACT', re.IGNORECASE)
                group1 = match.group(1)
                extractFound = rxExtract.search(group1)
                if extractFound:
                    print("Skipping Extract: {}".format(group1))
                else:
                    group = match.group(2)
                    if group not in substitued:
                        itemStripped = group.strip()
                        from_ = 'FROM {}'.format(group)
                        schemaConcatenatedValue ='{}.{}'.format(self.schemaName, itemStripped)
                        to_ = 'FROM {}'.format(schemaConcatenatedValue)
                        processedSQL = re.sub(from_, to_, processedSQL, flags=re.I)

                        substitued.append(itemStripped)

            processedSQL = re.sub(r'\s+',' ', processedSQL)


            rx = re.compile(r'JOIN\s+(\w+)', re.IGNORECASE)
            substitued = []
            for match in rx.finditer(processedSQL):
                group = match.group(1)
                
                if group not in substitued:
                    itemStripped = group.strip()
                    from_ = 'JOIN {}'.format(group)
                    schemaConcatenatedValue ='{}.{}'.format(self.schemaName, itemStripped)
                    to_ = 'JOIN {}'.format(schemaConcatenatedValue)
                    processedSQL = re.sub(from_, to_, processedSQL, flags=re.I)

                    substitued.append(itemStripped)

            processedSQL = re.sub(r'\s+',' ', processedSQL)

            self.sql = processedSQL

            print(self.sql)

    def __str__(self):
        return """
            View: ---------------------------------------------------------------------------------------------------------------
            View Name       :     {name}
            View Type       :     {viewType}
            View Source     :     {exploreSourceName}
            Persisted Type  :     {persistedType}
            Persisted SQL   :     {persistedSQL}
            SQL             :     {sql}
            """.format(name = self.name, persistedType = self.persistedType, sql = self.sql, persistedSQL = self.persistedSQL, viewType = self.viewType, exploreSourceName = self.exploreSourceName)

    
    def getViewInfomationFromFile(self, fileName):

        views = []

        with open(fileName, 'r') as file:
            parsed = lkml.load(file)
            print(parsed)
            #logging.info(parsed)

            for view in parsed['views']:

                viewObj = View()

                viewObj.setView(view)
                views.append(viewObj)

        return views

    def injectSqlTableName(self, views):
        rx = re.compile(r'\$\{(\w+)\.SQL_TABLE_NAME\}',re.IGNORECASE)
        for match in rx.finditer(self.sql):
            group = match.group(1)
            
            view = self.getViewByName(group.lower().strip(), views)
            dbtModelName = view.getDBTModelName() 

            ref = r"{{ref('" + dbtModelName +r"')}}"

            processedSQL = re.sub(r'\$\{\w+\.SQL_TABLE_NAME\}',ref, self.sql)

            self.sql = processedSQL

    def injectSqlTableNameInSQLTriggerValue(self, views):
        rx = re.compile(r'\$\{(\w+)\.SQL_TABLE_NAME\}',re.IGNORECASE)
        for match in rx.finditer(self.persistedSQL):
            group = match.group(1)
            
            view = self.getViewByName(group.lower().strip(), views)
            dbtModelName = view.getDBTModelName() 

            #ref = r"{{ref('" + dbtModelName +r"')}}"

            ref = "{}.{}".format(view.targetSchema, view.name) 

            processedSQL = re.sub(r'\$\{\w+\.SQL_TABLE_NAME\}',ref, self.persistedSQL)

            self.persistedSQL = processedSQL


    
    def getViewByName(self, name, views):
        view = None

        for view_ in views:
            if view_.name == name:
                view = view_
                break
        return view

    def writedbtModel(self):

        if self.viewType == 'PDT':
            placeholder = 'pdt_placeholder.ddl'
        elif self.viewType == 'NDT':
            placeholder = 'view_placeholder.ddl'
        else:
            placeholder = 'view_placeholder.ddl'

        f = open(placeholder, "r")
        placeholder = f.read()
        dbtModelName = self.dbtModelName

        fileName = dbtModelName + '.sql'

        filePath = "../models/" + fileName

        dbtrunModelsPath = "run_models.sh" 
        dbtrunPresistedModelsPath = "run_presisted_models.sh"

        sql = self.sql

        content = placeholder \
                    .replace("@@SCHEMA@@",self.targetSchema.lower().strip()) \
                    .replace("@@ALIAS@@", self.name.lower().strip()) \
                    .replace("@@SQL@@", sql) \
                    .replace("@@PERSISTED_TYPE@@", self.persistedType) \
                    .replace("@@PERSISTED_SQL@@", self.persistedSQL) \
                    .replace("@@VIEWTYPE@@", self.viewType)

        with open(filePath, 'w') as file:
            file.write(content)

        if self.viewType == 'PDT':
            content = 'dbt run --models {}\n'.format(dbtModelName)
            with open(dbtrunPresistedModelsPath, 'a') as file:
                file.write(content)

        content = 'dbt run --models {}\n'.format(dbtModelName)
        with open(dbtrunModelsPath, 'a') as file:
            file.write(content)

    def getViewSQL(self):

        viewSQL = ''

        dimList = []

        for dimension in self.validDimensions:

            if dimension.name.upper().strip() != dimension.sql.upper().strip():
                row = "{} AS {}".format(dimension.sql.strip(), dimension.name.upper().strip())
                dimList.append(row)

            else:
                row = "{}".format(dimension.name.upper().strip())
                dimList.append(row)

        cols =  ',\n'.join(dimList)

        if cols == None or cols.strip() == '':
            cols = '*'

        if self.viewType == 'PDT':
            viewSQL = """
            SELECT
            {cols}
            FROM ({sql})
            """.format(cols = cols, sql = self.sql)
        elif self.viewType == 'VIEW':
            viewSQL = """
            SELECT
            {cols}
            FROM {sql}
            """.format(cols = cols, sql = self.sql_table_name)
        else:
            viewSQL = ''

        self.sql = viewSQL    
        return viewSQL




