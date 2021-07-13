import lkml
from connection import Connection
from logger import Logger
from view import View
import os
import re
import glob



class Model:
    def __init__(self):
        self.name = None
        self.connectionName = None
        self.connection = None
        self.label = None
        self.includes = None
        self.modelFileInformation = None
        self.schemaName = None

    def setModel(self, model):
        source_schema_initial = os.getenv('SIGMA_ME_SCHEMA')
        self.schemaName = source_schema_initial
        if 'label' in model:
            self.label = model['label']
            self.name = "{}_{}".format(source_schema_initial.upper(), self.label.upper().replace(' ', '_'))

        if 'includes' in model:
            self.includes = model['includes']

        if 'connection' in model:
            self.connectionName = model['connection']

            self.connection = Connection(self.connectionName)

    def __str__(self):
        return """
            Model: ---------------------------------------------------------------------------------------------------------------
            Label               :        {label}
            Name                :        {name}
            Connection Name     :        {connectionName}
            Database Name       :        {databaseName}
            SchemaName          :        {schemaName}
            includes            :        {includes}
            Model File          :        {modelFileInformation}
            """.format(label = self.label, connectionName = self.connectionName, databaseName = self.connection.getDatabaseName(), schemaName = self.connection.getSchemaName(), name = self.name, includes = self.includes, modelFileInformation = self.modelFileInformation)

logging = Logger().getLogger()


def getFiles(dir, filesIncluded):
    filesList = []
    for dirName, subdirList, fileList in os.walk(dir):
        for subdir in subdirList:
            for fname in fileList:
                for fileIncluded in filesIncluded:
                    fileIncluded = fileIncluded.replace('*.', '.*.')
                    rx = re.compile(fileIncluded)
                    filePath = '{}{}'.format(dirName, fname)
                    if rx.match(filePath):    
                        modelFilesDict = {
                            "FileName" : fname,
                            "DirName"  : dirName,
                        }
                        logging.info('File:{}={}:{}:{}'.format(fname, dirName, subdir, fname))
                        filesList.append(modelFilesDict)

    return filesList

def getFilePath(rootDir, includeItem):
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

def main():
    source_repo = os.getenv('SIMGA_ME_SOURCE_REPO')
    rootDir = '../data/{}/'.format(source_repo)

    dirs = os.walk(rootDir)
    models_regex = '.*.model.lkml'

    modelFilesList = []

    for dir, sub_dir, files in dirs:
        for file in files:
            rx = re.compile(models_regex)
            filePath = os.path.join(dir, file)
            if rx.match(filePath):
                modelFilesList.append(
                    {
                        "DirName": dir,
                        "FileName" : file
                    }
                )
    logging.info(modelFilesList)

    for modelFileItem in modelFilesList:
        if modelFileItem['FileName'] != 'adthrive_ds_athena.model.lkml':
            print(modelFileItem['FileName'])
            continue
        
        logging.info("Model To Be Parsed. {}".format(modelFileItem['FileName']))

        modelFilePath = os.path.join(modelFileItem["DirName"], modelFileItem["FileName"]) 
        with open(modelFilePath, 'r') as modelFile:
            modelParsed = lkml.load(modelFile)
            logging.info(modelParsed)
            model = Model()
            model.setModel(modelParsed)
            model.modelFileInformation = modelFileItem
            logging.info("-----------------------MODEL-------------------")
            logging.info(model)
            #print(model)

            viewList = []
            viewFileList = []
            filesNotFound = []

            for includeItem in model.includes:
                filesToBeIncluded = getFilePath(rootDir, includeItem)
                found = False
                for fileToBeIncluded in filesToBeIncluded:
                    found = True
                    viewFileList.append(fileToBeIncluded)
                if not found:
                    filesNotFound.append(includeItem)
            logging.info("Views to be Parsed: -------------")
            for viewItem in viewFileList:
                logging.info(viewItem)
                
            logging.info("Views Not found: -------------")
            for viewItem in filesNotFound:
                logging.info(viewItem)
            
            for viewFileItem in viewFileList:
                if viewFileItem != os.path.join(rootDir,'_earnings_and_analytics.view.lkml'):
                    continue
                msg = "Parsing View: {}".format(viewFileItem)
                logging.info(msg)
                print(msg)
                
                viewObj = View()
                views = viewObj.getViewInformationFromFile(viewFileItem, False, logging)
                for view in views:
                    view.schemaName = model.connection.schemaName
                    view.databaseName = model.connection.databaseName
                    view.targetSchema = model.name
                    view.processExtendedView(rootDir, logging)
                    view.parseDimensions()
                    logging.info('Parsed Views: -------------')
                    logging.info(view)
                    viewList.append(view)

            for view in viewList:
                view.processDimensions(logging)

            for view in viewList:
                view.validateDimensions(viewList, logging)

            
            

            '''
            extendedViewNamesRaw = []
            extendedViewNames = []
            for view_ in viewList:
                for includeView in view_.includes:
                    logging.info("IncludedView: ")
                    logging.info(includeView)
                    if includeView is not None:
                        for alreadyParsedView in model.includes:
                            #logging.info("Already Parsed View Compare: {}: {}".format(alreadyParsedView, includeView))
                            found = False
                            if alreadyParsedView == includeView:
                                found = True
                                break
                        if not found:
                            extendedViewNamesRaw.append(includeView)        

            for extendedViewNameRaw in extendedViewNamesRaw:
                found = False
                for extendedViewName in extendedViewNames:
                    if extendedViewName == extendedViewNameRaw:
                        found = True
                        break
                if not found:
                    extendedViewNames.append(extendedViewNameRaw)

            extendViewFiles = []
            extendedFilesNotFound = []
            for extendedViewName in extendedViewNames:
                filesToBeIncluded = getFilePath(rootDir, extendedViewName)
                found = False
                for fileToBeIncluded in filesToBeIncluded:
                    found = True
                    extendViewFiles.append(fileToBeIncluded)
                if not found:
                    extendedFilesNotFound.append(extendedViewName)

            for viewFileItem in extendViewFiles:
                msg = "Parsing Extended View: {}".format(viewFileItem)
                logging.info(msg)
                print(msg)
                
                viewObj = View()
                views = viewObj.getViewInfomationFromFile(viewFileItem, logging)
                for view in views:
                    view.schemaName = model.connection.schemaName
                    view.databaseName = model.connection.databaseName
                    view.targetSchema = model.name
                    view.processDimensions(logging)
                    viewList.append(view)
            
            for view_ in viewList:
                logging.info("Extends Views: -------------------")
                logging.info(view_)
                view_.processDimensions()
                view_.validateDimensions(viewList)
                logging.info("------------------------- Extends Views All Dimensions---------------------------------------------")
                logging.info(view_)
                for dimension_ in view_.allDimensions:
                    logging.info(dimension_)

                logging.info("-------------------------Valid Dimensions---------------------------------------------")

                for dimension_ in view_.validDimensions:
                    logging.info(dimension_)
                
                logging.info("-------------------------Invalid Dimensions---------------------------------------------")
                for dimension_ in view_.excludedDimensions:
                    logging.info(dimension_)
            '''
            
            #
            #Process VIEWS AND PDTS
            #
            logging.info('-----------------------Process VIEWS for model {}---------------------------------------------- '.format(model.name))
            for view in viewList:
                if view.viewType == 'VIEW':
                    logging.info("Processing VIEW: {}".format(view.name) )
                    view.getViewSQL(logging)
                    view.injectViewSchema(logging)
                    view.setDBTModelName(logging)
                    view.injectSqlTableName(viewList, logging)
                    view.injectSqlTableNameInSQLTriggerValue(viewList, logging)
                    view.writedbtModel(model.connectionName, model.schemaName, logging)
            '''
            logging.info('-----------------------Process PDTs for model {}---------------------------------------------- '.format(model.name))
            for view in viewList:
                if  view.viewType == 'PDT':
                    logging.info("Processing PDT: {}".format(view.name) )
                    view.getViewSQL(logging)
                    view.injectViewSchema(logging)
                    view.setDBTModelName(logging)
                    view.injectSqlTableName(viewList, logging)
                    view.injectSqlTableNameInSQLTriggerValue(viewList, logging)
                    view.writedbtModel(model.connectionName, model.schemaName, logging)
            logging.info('-----------------------Process NDTs for model {}---------------------------------------------- '.format(model.name))
            for view in viewList:
                if view.viewType == 'NDT':
                    logging.info("Processing NDT: {}".format(view.name) )
                    view.getViewSQL(logging)
                    view.injectViewSchema(logging)
                    view.setDBTModelName(logging)
                    view.injectSqlTableName(viewList, logging)
                    view.injectSqlTableNameInSQLTriggerValue(viewList, logging)
                    view.writedbtModel(model.connectionName, model.schemaName, logging)
            '''  

if __name__ == "__main__":
    main()