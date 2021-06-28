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

    def setModel(self, model):
        source_schema_initial = os.getenv('SIGMA_ME_SCHEMA')
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
            #print("adthrive_ds_athena.model.lkml")
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
                dir = model.modelFileInformation["DirName"]
                viewFileName = os.path.join(rootDir, includeItem.split('/', 1)[-1])
                logging.info(viewFileName)
                if viewFileName.endswith('view'):
                    viewFileName = "{}.lkml".format(viewFileName)
                #logging.info("Checking: {}".format(viewFileName))
                found = False
                for name in glob.glob(viewFileName):
                    #logging.info("ViewFile: {}".format(name))
                    found = True
                    viewFileList.append(name)

                if not found:
                    filesNotFound.append(includeItem)
            logging.info("Views to be Parsed: -------------")
            for viewItem in viewFileList:
                logging.info(viewItem)

            logging.info("Views Not found: -------------")
            for viewItem in filesNotFound:
                logging.info(viewItem)
            
            for viewFileItem in viewFileList:
                #if viewFileItem['FileName'] != 'order_items.view.lkml' and viewFileItem['FileName'] != 'user_order_facts.view.lkml':
                #    print('')
                    #continue
                msg = "Parsing View: {}".format(viewFileItem)
                logging.info(msg)
                print(msg)
                
                viewObj = View()
                views = viewObj.getViewInfomationFromFile(viewFileItem, logging)
                for view in views:
                    logging.info("Viewinfo")
                    logging.info(view)
                    print(view)
                    
                    view.schemaName = model.connection.schemaName
                    view.databaseName = model.connection.databaseName
                    view.targetSchema = model.name

                    logging.info("-------------------------All Dimensions---------------------------------------------")

                    for dimension_ in view.allDimensions:
                        logging.info(dimension_)

                    logging.info("-------------------------Valid Dimensions---------------------------------------------")

                    for dimension_ in view.validDimensions:
                        logging.info(dimension_)
                    
                    logging.info("-------------------------Invalid Dimensions---------------------------------------------")
                    for dimension_ in view.excludedDimensions:
                        logging.info(dimension_)

                    viewList.append(view)


            #
            #Process VIEWS AND PDTS
            #

            for view in viewList:
                if view.viewType == 'VIEW' or view.viewType == 'PDT':
                    view.getViewSQL()
                    view.injectViewSchema()
                    view.setDBTModelName()
                    view.injectSqlTableName(viewList)
                    view.injectSqlTableNameInSQLTriggerValue(viewList)
                    view.writedbtModel()
            
            print('-----------------------Process NDTs---------------------------------------------- ')
            for view in viewList:
                if view.viewType == 'NDT':
                    logging.info("Processing NDT: {}".format(view.name) )
                    logging.info(view)
                    view.processNDT(viewList)
                    view.getNDTViewSQL()
                    view.setDBTModelName()                    
                    view.writedbtModel()


if __name__ == "__main__":
    main()