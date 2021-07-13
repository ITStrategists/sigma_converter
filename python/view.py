import os
import lkml
import re
import glob

from dimension import Dimension
from column import Column
from logger import Logger
from sql_formatter.core import format_sql

class View:
    def __init__(self):
        self.name = ''
        self.sql = ''
        self.parsedView = None
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
        self.extends = None
        self.extendedView = None
        self.includes = []
        self.viewDimensions = []
        self.parsedDimensions = []

    def getViewByName(self, viewName, viewList, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        logging.info("Checking View: {}".format(viewName))
        view = None
        for viewItem in viewList:
            logging.info("{}:{}".format(viewName, viewItem.name))
            if viewName == viewItem.name:
                view = viewItem
                logging.info("Found View: {}".format(viewName))
                break
        return view

    def processNDTColumns(self, exploreSourceView, columns, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        columns_ = []
        column = []
        
        for column_ in columns:
            logging.ino("Processing Column.")
            logging.info(column_)
            print(column_)
            if column_.columnType == 'COLUMN':
                fieldName = column_.transformExploreField(self.exploreSourceName, column_.field)
                dimension_temp = Dimension()
                dimension_temp = dimension_temp.getDimensionByName(fieldName, exploreSourceView.allDimensions)
                if dimension_temp is not None: 
                    column_.sql = dimension_temp.sql
                    column_.dimensionType = dimension_temp.dimensionType()

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
            logging.info(dimension_)
            print(dimension_)
            column_ = Column().getColumnByName(dimension_.name, columns)
            if column_ is not None:
                dimension_.sql = column_.sql    
                dimension_.dimensionType = column_.dimensionType
            
    def processNDT(self, viewList, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        view_ = self.getViewByName(self.exploreSourceName, viewList)
        self.exploreSourceView = view_
        self.processNDTColumns(self.exploreSourceView, self.columns, logging)

    
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


    def setDBTModelName(self, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        #self.dbtModelName = self.targetSchema.lower().strip().replace(' ', '_').replace('&', '_') + '_' + self.name.lower().strip().replace(' ', '_').replace('&', '_')
        self.dbtModelName = self.name

    def getDBTModelName(self):
        return self.dbtModelName

    def processExtendedView(self, rootDir, logging = None):
        if logging is None:
            logging = Logger().getLogger()

        if self.extends is not None:
            msg = """
            Processing Extended View: {}
            """.format(self.extends)
            logging.info(msg)

            vFileName = '{}.view.lkml'.format(self.extends)

            vFilePath = self.getFilePath(rootDir, vFileName, logging)[0]
            print(vFilePath)

            views = self.getViewInformationFromFile(vFilePath, True, logging)            
            
            extendedView = views[0]

            msg = """
            Processed Extended View: {}
            """.format(views[0])
            logging.info(msg)

            extendedView.parseDimensions(logging)

            self.extendedView = extendedView

    def getFilePath(self, rootDir, includeItem, logging = None):
        
        if logging is None:
            logging = Logger().getLogger()

        viewFileList = []
        viewFileName = os.path.join(rootDir, includeItem.split('/', 1)[-1])
        logging.info(viewFileName)
        if viewFileName.endswith('view'):
            viewFileName = "{}.lkml".format(viewFileName)
        #logging.info("Checking: {}".format(viewFileName))
        for name in glob.glob(viewFileName):
            #logging.info("ViewFile: {}".format(name))
            viewFileList.append(name)
        return viewFileList

    def parseDimensions(self, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        
        if self.viewType == 'VIEW' or self.viewType == 'PDT' or self.viewType == 'NDT' or self.viewType == 'EXTENDED':
            viewDimensions = []
            viewMeasures = []
            viewDimensionGroups = []

            if 'dimensions' in self.parsedView:
                viewDimensions = self.parsedView['dimensions']
                logging.info("Dimensions in parsed View")
                logging.info(viewDimensions)

            if 'measures' in self.parsedView:
                viewMeasures = self.parsedView['measures']
                logging.info("Dimensions in parsed View")
                logging.info(viewMeasures)

            if 'dimension_groups' in self.parsedView:
                viewDimensionGroups = self.parsedView['dimension_groups']
                logging.info("Dimension Groups in parsed View")
                logging.info(viewDimensionGroups)

            self.parsedDimensions = Dimension().processDimensions(viewDimensions, viewMeasures, viewDimensionGroups, logging)
            msg = """
            View : {}
            Info : Parsed Dimensions
            """.format(self.name)
            logging.info(msg)
            for dim in self.parsedDimensions:
                logging.info(dim)

    def processDimensions(self, logging = None):
        if logging is None:
            logging = Logger().getLogger()

        if self.extendedView is not None:
            msg = """
            View : {}
            Info : Append extended dimensions from base view {}
            """.format(self.name, self.extendedView.name)
            logging.info(msg)
            for dimensionFromExtendedDimensions in self.extendedView.parsedDimensions:
                self.parsedDimensions.append(dimensionFromExtendedDimensions)

        self.allDimensions = Dimension().getProcessedSubstituteDimensions(self.parsedDimensions)

        for dimension in self.allDimensions:
            msg = """
            View : {}
            Info : All dimensions {}
            """.format(self.name, dimension)
            logging.info(msg)

    def validateDimensions(self, views, logging = None):
        if logging is None:
            logging = Logger().getLogger()

        if self.viewType == 'VIEW':
            allDimensions = Dimension().getProcessedSubstituteDimensions(self.parsedDimensions)

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


        '''
        viewAndExtendedViewDimensions = []

        for viewDimension in self.parsedDimensions:
            viewAndExtendedViewDimensions.append(viewDimension)
        
        if self.extends is not None:
            extendedView = self.getViewByName(self.extends, views, logging)
            self.sql_table_name = extendedView.sql_table_name
            for extendedViewDimension in extendedView.parsedDimensions:
                viewAndExtendedViewDimensions.append(extendedView)

        allDimensions = Dimension().getProcessedSubstituteDimensions(self.parsedDimensions)

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

        logging.info("---------------Validated View ------------------")
        '''

    def setView(self, view, logging = None):
        self.parsedView = view
        if 'derived_table' in self.parsedView:
            if 'extends__all' in self.parsedView:
                self.extends = self.parsedView['extends__all'][0][0]
            if 'explore_source' in self.parsedView['derived_table']:
                self.viewType = 'NDT'
                exploreSource = self.parsedView['derived_table']['explore_source']
                if 'name' in exploreSource:
                    self.exploreSourceName = self.parsedView['derived_table']['explore_source']['name']

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
                if 'sql' in self.parsedView['derived_table']:
                    self.sql = self.parsedView['derived_table']['sql']
                    self.sql = self.sql.replace('"','\"')

                if 'persist_for' in self.parsedView['derived_table']:
                    self.persistedSQL = self.parsedView['derived_table']['persist_for']
                    self.persistedSQL = self.persistedSQL.replace('"', "\"")
                    self.persistedType = 'PERSIST_FOR'

                if 'sql_trigger_value' in self.parsedView['derived_table']:
                    self.persistedSQL = self.parsedView['derived_table']['sql_trigger_value']
                    self.persistedSQL = self.persistedSQL.replace('"', "\"")
                    self.persistedType = 'SQL_TRIGGER_VALUE'

                self.viewType = 'PDT'

        elif 'sql_table_name' in view:
            self.viewType = 'VIEW'
            self.sql_table_name = self.parsedView['sql_table_name']
        if 'name' in self.parsedView:
            self.name = self.parsedView['name']
        if 'extends__all' in self.parsedView:
                self.extends = self.parsedView['extends__all'][0][0]

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

    def injectViewSchema(self, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        if self.sql is not None and self.sql != '':
            logging.info("--------Prv View")
            logging.info(self.sql)
            processedSQL = re.sub(r'\s+',' ', self.sql.replace('\n', ' ').replace('\t', ' '))
            #print("Source SQL: {}".format(processedSQL))
            logging.info("----Source SQL")
            logging.info(processedSQL)
            #print("{}".format(processedSQL))
            dependencies = []

            keywords = ['lateral']

            rx = re.compile(r'FROM\s+(\w+\s*\w*,\s*\w+\s*\w*)\s+',re.IGNORECASE)

            for match in rx.finditer(processedSQL):
                group = match.group(1)
                #print("-------------------------------------")
                #print(group)
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
            logging.info("kk: {}".format(self.name))
            logging.info(processedSQL)
            rx = re.compile(r'(\w+\(*\w+\s+|\*|\SELECT\s\*)\s*FROM\s+(\w+\.*\w+\.*\w+|\"{0,}\w+\"{0,}\.\"{0,}\w+\"{0,}|\w+)', re.IGNORECASE)
            substitued = []
            for match in rx.finditer(processedSQL):
                rxExtract = re.compile(r'EXTRACT', re.IGNORECASE)
                group1 = match.group(1)

                if group1 is not None:
                    logging.info("Grp: {}".format(group1))
                    extractFound = rxExtract.search(group1)
                    if extractFound:
                        logging.info("Skipping Table Name because of Extract: {}".format(group1))
                    else:
                        group = match.group(2)
                        logging.info("G2: {}".format(group))
                        rxCte = re.compile(r'^cte', re.IGNORECASE)
                        group2 = match.group(1)
                        cteFound = rxCte.search(group2)

                        if group not in substitued:
                            logging.info("---Skipping Table Name bacasue of Substituted: {}".format(group))
                        elif '.' in group:
                            logging.info("---Skipping Table Name already have schema: {}".format(group))
                        elif group.lower().startswith('cte'):
                            logging.info("---Skipping Table Name because of cte: {}".format(group))
                        else:
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
            processedSQL = format_sql(processedSQL)
            self.sql = processedSQL
            logging.info("---------------------Injected ViewSchema: {}".format(self.name))
            logging.info(self.sql)

    def __str__(self):
        return """
            View: ---------------------------------------------------------------------------------------------------------------
            View Name       :     {name}
            View Type       :     {viewType}
            View Source     :     {exploreSourceName}
            Persisted Type  :     {persistedType}
            Persisted SQL   :     {persistedSQL}
            Extends         :     {extends}
            SQL             :     {sql}
            SQL Table Name  :     {sqlTableName}
            Includes        :     {includes}
            """.format(name = self.name, persistedType = self.persistedType, sql = self.sql, persistedSQL = self.persistedSQL, viewType = self.viewType, exploreSourceName = self.exploreSourceName, extends = self.extends, sqlTableName = self.sql_table_name, includes=self.includes)

    
    def getViewInformationFromFile(self, fileName, isExtended = False, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        views = []

        with open(fileName, 'r') as file:
            parsed = lkml.load(file)
            #print(parsed)
            logging.info(parsed)
            includes = []
            if 'includes' in parsed:
                includes = parsed['includes']
            for view in parsed['views']:

                viewObj = View()
                viewObj.setView(view)
                viewObj.includes = includes
                if isExtended:
                    viewObj.viewType = 'EXTENDED'
                views.append(viewObj)

        return views

    def injectSqlTableName(self, views, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        rx = re.compile(r'\$\{(\w+)\.(SQL_TABLE_NAME)\}',re.IGNORECASE)
        for match in rx.finditer(self.sql):
            group = match.group(1)
            group2 = match.group(2)
            view = self.getViewByName(group.lower().strip(), views)
            dbtModelName = view.getDBTModelName() 

            ref = r"{{ref('" + dbtModelName +r"')}}"
            sub = r"\$\{\w+\."+ group2 +"\}"
            processedSQL = re.sub(sub,ref, self.sql)

            self.sql = processedSQL

    def injectSqlTableNameInSQLTriggerValue(self, views, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        rx = re.compile(r'\$\{(\w+)\.SQL_TABLE_NAME\}',re.IGNORECASE)
        for match in rx.finditer(self.persistedSQL):
            group = match.group(1)
            
            view = self.getViewByName(group.lower().strip(), views)
            dbtModelName = view.getDBTModelName() 

            #ref = r"{{ref('" + dbtModelName +r"')}}"

            ref = "{}.{}".format(view.targetSchema, view.name) 

            processedSQL = re.sub(r'\$\{\w+\.SQL_TABLE_NAME\}',ref, self.persistedSQL)

            self.persistedSQL = processedSQL

    def writedbtModel(self, connectionName, schemaName, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        if self.viewType == 'PDT':
            placeholder = 'pdt_placeholder.ddl'
        elif self.viewType == 'NDT':
            placeholder = 'view_placeholder.ddl'
        else:
            placeholder = 'view_placeholder.ddl'

        f = open(placeholder, "r")
        placeholder = f.read()
        dbtModelName = self.dbtModelName

        fileName = self.name + '.sql'
        dirName = "../models/{}".format(connectionName)
        if not os.path.exists(dirName):
            os.makedirs(dirName)
        filePath = os.path.join(dirName,fileName)

        dbtrunModelsPath = "run_models.sh" 
        dbtrunPresistedModelsPath = "run_presisted_models.sh"

        sql = self.sql
        logging.info("------------Final SQL for {}".format(self.name))
        logging.info(sql)
        content = placeholder \
                    .replace("@@SCHEMA@@",schemaName) \
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

    def getViewSQL(self, logging = None):
        if logging is None:
            logging = Logger().getLogger()
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
            rx = re.compile(r'(--.*\n)',re.IGNORECASE)
            tempSQL = self.sql
            for match in rx.finditer(tempSQL):
                group = match.group(1)
                print("-------------------------------------")
                logging.info("---Found Comment: {}".format(group))
                newGroup = '/*{}*/'.format(group.strip('\n'))
                tempSQL = tempSQL.replace(group, newGroup)
            logging.info("PDTtmpSQL-----------{}--------------------------".format(self.name))
            logging.info(tempSQL)

            viewSQL = """
            SELECT
            {cols}
            FROM ({sql})
            """.format(cols = cols, sql = tempSQL)
        elif self.viewType == 'VIEW':
            
            viewSQL = """
            SELECT
            {cols}
            FROM {sql}
            """.format(cols = cols, sql = self.sql_table_name)
        elif self.viewType == 'NDT':
            tempSQL = self.sql
            logging.info("NDT: SQL")
            logging.info(tempSQL)
            

            logging.info("NDT: SQL")
            logging.info(cols)

            viewSQL = """
            SELECT
                {cols}
            FROM {sql}
            """.format(cols = cols, sql = tempSQL)
        else:
            viewSQL = ''
        viewSQL = format_sql(viewSQL)
        self.sql = viewSQL
        logging.info("-------------FORMATED VIEW-----------------")
        logging.info(viewSQL)
        return viewSQL