import lkml
import re
from logger import Logger

class Dimension:
    def __init__(self):
        self.name = ''
        self.sql_raw = ''
        self.sql = ''
        self.type = ''
        self.hidden = ''
        self.primaryKey = ''
        self.dependencies= []
        self.column = ''
        self.isSubstituded = False
        self.dependenciesByPath = []
        self.isExcluded = False
        self.excludedReason = None
        self.startLocationField = None
        self.endLocationField = None
        self.distanceUnits = None
        self.sqlLongitude = None
        self.sqlLatitute = None
        self.dimensionType = None

    def setDistanceDimensionSQL(self, startDimLongitude, startDimLatitute, endDimLongitude ,endDimLatitute):
        distanceUnits=self.distanceUnits


        if 'miles' in distanceUnits:
            self.sql = '((CASE WHEN round({endDimLatitute},1)  = {endDimLatitute}  AND round({endDimLongitude},1)  = {endDimLongitude}  THEN 0 ELSE ACOS(SIN(RADIANS(round({endDimLatitute},1) )) * SIN(RADIANS({startDimLatitute})) + COS(RADIANS(round({endDimLatitute},1) )) * COS(RADIANS({startDimLatitute})) * COS(RADIANS({startDimLongitude}  - round({endDimLongitude},1) ))) * 6371 END) / 1.60934)'.format(startDimLongitude=startDimLongitude, startDimLatitute=startDimLatitute, endDimLongitude=endDimLongitude ,endDimLatitute=endDimLatitute)
            self.transformTableDimensions()
        elif 'meters' in distanceUnits:
            self.sql = '((CASE WHEN round({endDimLatitute},1)  = {endDimLatitute}  AND round({endDimLongitude},1)  = {endDimLongitude}  THEN 0 ELSE ACOS(SIN(RADIANS(round({endDimLatitute},1) )) * SIN(RADIANS({startDimLatitute})) + COS(RADIANS(round({endDimLatitute},1) )) * COS(RADIANS({startDimLatitute})) * COS(RADIANS({startDimLongitude}  - round({endDimLongitude},1) ))) * 6371 END) / 0.001)'.format(startDimLongitude=startDimLongitude, startDimLatitute=startDimLatitute, endDimLongitude=endDimLongitude ,endDimLatitute=endDimLatitute)
            self.transformTableDimensions()
        elif 'feet' in distanceUnits:
            self.sql = '((CASE WHEN round({endDimLatitute},1)  = {endDimLatitute}  AND round({endDimLongitude},1)  = {endDimLongitude}  THEN 0 ELSE ACOS(SIN(RADIANS(round({endDimLatitute},1) )) * SIN(RADIANS({startDimLatitute})) + COS(RADIANS(round({endDimLatitute},1) )) * COS(RADIANS({startDimLatitute})) * COS(RADIANS({startDimLongitude}  - round({endDimLongitude},1) ))) * 6371 END) / 0.0003048)'.format(startDimLongitude=startDimLongitude, startDimLatitute=startDimLatitute, endDimLongitude=endDimLongitude ,endDimLatitute=endDimLatitute)
            self.transformTableDimensions()
        elif 'nautical_miles' in distanceUnits:
            self.sql = '((CASE WHEN round({endDimLatitute},1)  = {endDimLatitute}  AND round({endDimLongitude},1)  = {endDimLongitude}  THEN 0 ELSE ACOS(SIN(RADIANS(round({endDimLatitute},1) )) * SIN(RADIANS({startDimLatitute})) + COS(RADIANS(round({endDimLatitute},1) )) * COS(RADIANS({startDimLatitute})) * COS(RADIANS({startDimLongitude}  - round({endDimLongitude},1) ))) * 6371 END) / 1.852)'.format(startDimLongitude=startDimLongitude, startDimLatitute=startDimLatitute, endDimLongitude=endDimLongitude ,endDimLatitute=endDimLatitute)
            self.transformTableDimensions()
        elif 'yards' in distanceUnits:
            self.sql = '((CASE WHEN round({endDimLatitute},1)  = {endDimLatitute}  AND round({endDimLongitude},1)  = {endDimLongitude}  THEN 0 ELSE ACOS(SIN(RADIANS(round({endDimLatitute},1) )) * SIN(RADIANS({startDimLatitute})) + COS(RADIANS(round({endDimLatitute},1) )) * COS(RADIANS({startDimLatitute})) * COS(RADIANS({startDimLongitude}  - round({endDimLongitude},1) ))) * 6371 END) / 0.0009144)'.format(startDimLongitude=startDimLongitude, startDimLatitute=startDimLatitute, endDimLongitude=endDimLongitude ,endDimLatitute=endDimLatitute)
            self.transformTableDimensions()
        elif 'kilometers' in distanceUnits:
            self.sql = '((CASE WHEN round({endDimLatitute},1)  = {endDimLatitute}  AND round({endDimLongitude},1)  = {endDimLongitude}  THEN 0 ELSE ACOS(SIN(RADIANS(round({endDimLatitute},1) )) * SIN(RADIANS({startDimLatitute})) + COS(RADIANS(round({endDimLatitute},1) )) * COS(RADIANS({startDimLatitute})) * COS(RADIANS({startDimLongitude}  - round({endDimLongitude},1) ))) * 6371 END))'.format(startDimLongitude=startDimLongitude, startDimLatitute=startDimLatitute, endDimLongitude=endDimLongitude ,endDimLatitute=endDimLatitute)
            self.transformTableDimensions()                   

    def setExcludedDimension(self):

        if self.dimensionType == 'MEASURE':
            self.isExcluded = True
            if self.excludedReason == '' or self.excludedReason == None:
                self.excludedReason = "Dimension is a Measure."
            else:
                self.excludedReason = self.excludedReason + " And Dimension is a Measure."

        otherViewCheck = re.search(r'\$\{\s*\w+\s*\.\s*\w+\s*\}', self.sql)
        if otherViewCheck:
            self.isExcluded = True
            if self.excludedReason == '' or self.excludedReason == None:
                self.excludedReason = "Dimension References other views."
            else:
                self.excludedReason = self.excludedReason + " And Dimension References other views."

        liquidCondition = re.search(r'\{%.*%\}', self.sql)        
        if liquidCondition:
            self.isExcluded = True
            if self.excludedReason == '' or self.excludedReason == None:
                self.excludedReason = "Liquid Condition"
            else:
                self.excludedReason = self.excludedReason + " And Dimension is Liquid Condition."

        liquidTemplate = re.search(r'\{\{.*\}\}', self.sql)        
        if liquidTemplate:
            self.isExcluded = True
            if self.excludedReason == '' or self.excludedReason == None:
                self.excludedReason = "Liquid Template"
            else:
                self.excludedReason = self.excludedReason + " And Dimension is Liquid Template."            

    def getDependencies(self):

        rx = re.compile(r'\$\{(\w+)\}')

        matches_raw = [match.group(1) for match in rx.finditer(self.sql)]

        dependencies = []

        for item in matches_raw:
            if item not in dependencies:
                if item != 'TABLE':
                    dependencies.append(item)

        return dependencies

    def transformLocationDimension(self, sqlLogitute, sqlLatitute):
        return "{}||','||{}".format(sqlLogitute, sqlLatitute)

    def transformYesNoDiemension(self):
        self.sql = """
        (CASE 
            WHEN {} THEN TRUE 
            ELSE FALSE 
        END)""".format(self.sql)

    def transformTableDimensions(self):
        if '${TABLE}.' in self.sql:
            self.sql = self.sql.replace('${TABLE}.', '') 

    def transformZipCodeDimension(self):
        self.transformTableDimensions()
   
   
    def transformTierDimension(self, tiers ,style, logging = None):
        if tiers is not None:
            tiers_data = []
            for tier in tiers:
                tiers_data.append(
                {
                    "origional_tier_value": tier,
                    "tier_float_value": float(tier)
                })
            logging.info("Prev Tier")
            logging.info(tiers_data)

            tiers_data.sort(key=lambda x: x.get('tier_float_value'))
            logging.info("Sorted Tier")
            logging.info(tiers_data)
            queryList = []
            if style == 'integer':
                below = ''
            if len(tiers) >= 2:
                for index in range(len(tiers_data)):
                    query = ''
                    if index == 0:
                        sql = self.sql
                        value = tiers_data[index]["origional_tier_value"]
                        if style=='integer':
                            query="CASE WHEN {}  < {} THEN 'Below {}'".format(sql, value, value)
                        elif style == 'relational':
                            query="CASE WHEN {}  < {} THEN '< {}'".format(sql, value, value)
                        elif style == 'classic':
                            query="CASE WHEN {}  < {} THEN 'T{:02d} (-inf,{})'".format(sql, value, value)
                        elif style == 'interval':
                            query="CASE WHEN {}  < {} THEN '(-inf,{})'".format(sql, value, value)
                    elif index == len(tiers_data) - 1:
                        sql = self.sql
                        value = tiers_data[index]["origional_tier_value"]
                        if style=='integer':
                            query="WHEN {}  >= {} THEN '{} or Above' ELSE 'Undefined' END".format(sql, value, value)
                        elif style == 'relational':
                            query="WHEN {}  >= {} THEN '>={}' ELSE 'Undefined' END".format(sql, value, value)
                        elif style == 'classic':
                            query="WHEN {}  >= {} THEN 'T{:02d} [{},inf)' ELSE 'TXX Undefined' END".format(sql, value, index, value)
                        elif style == 'interval':
                            query="WHEN {}  >= {} THEN '[{},inf)' ELSE 'Undefined' END".format(sql, value, value)
                    else:
                        first_value = tiers_data[index]["origional_tier_value"]
                        second_value = tiers_data[index + 1]["origional_tier_value"]
                        logging.info('Index: {}:{}:{}'.format(index, first_value, second_value))
                        if style=='integer':
                            query="WHEN {} >= {} AND {}  < {} THEN '{} to {}'".format(sql, first_value, sql, second_value, first_value, second_value)
                        elif style == 'relational':
                            query="WHEN {} >= {} AND {}  < {} THEN '>={} and <{}'".format(sql, first_value, sql, second_value, first_value, second_value)
                        elif style == 'classic':
                            query="WHEN {}  >= {} AND {}  < {} THEN 'T{:02d} [{},{})'".format(sql, first_value, sql, second_value, index + 1, first_value, second_value)
                        elif style == 'interval':
                            query="CASE WHEN {} >= {} AND {} < {} THEN '[{},{})".format(sql, first_value, sql, second_value, first_value, second_value)
                    queryList.append(query)
                self.sql = '\n'.join(queryList)
                logging.info("TierFinal---")
                logging.info(self.sql)

            '''
            


            difference=int(float(tiers[1]))-int(float(tiers[0]))
            floatdifference = float(tiers[1])-float(tiers[0])
            finalquery=''
            i=00
            for value in tiers:
                if style=='integer':
                    diff_from_value=int(value)-int(difference)
                    minus_one_value=int(value)-1
                    if int(value)==0:
                        query="CASE WHEN {}  < {} THEN 'Below {}'".format(self.sql,value,value)
                        finalquery = '{} {}'.format(finalquery, query)
                    if int(value)>0:
                        query="WHEN {}  >= {} AND {}  < {} THEN '{} to {}'".format(self.sql,diff_from_value,self.sql,value,diff_from_value,minus_one_value)
                        finalquery = '{} {}'.format(finalquery, query)
                    
                    
                if style=='relational':
                    diff_from_value=float(value)-float(floatdifference)
                    minus_one_value=float(value)
                    if float(value)==0:
                        query="CASE WHEN {}  < {} THEN '< {}'".format(self.sql,float(value),float(value))
                        finalquery = '{} {}'.format(finalquery, query)
                    
                    if float(value)>0:
                        query="WHEN {}  >= {} AND {}  < {} THEN '>={} and <{}'".format(self.sql,float(diff_from_value),self.sql,float(value),float(diff_from_value),float(minus_one_value))
                        finalquery = '{} {}'.format(finalquery, query)


                
                if style=='classic':
                    
                    diff_from_value=float(value)-float(difference)
                    minus_one_value=float(value)
                    if int(value)==0:
                        query="CASE WHEN {}  < {} THEN 'T{:02d} (-inf,{})'".format(self.sql,float(value),i,float(value))
                        finalquery = '{} {}'.format(finalquery, query)
                    if int(value)>0:
                        query="WHEN {}  >= {} AND {}  < {} THEN 'T{:02d} [{},{})'".format(self.sql,float(diff_from_value),self.sql,float(value),i,float(diff_from_value),float(minus_one_value))
                        finalquery = '{} {}'.format(finalquery, query)

                if style=='interval':
                    
                    diff_from_value=float(value)-float(difference)
                    minus_one_value=float(value)
                    if int(value)==0:
                        query="CASE WHEN {}  < {} THEN '(-inf,{})'".format(self.sql,float(value),float(value))
                        finalquery = '{} {}'.format(finalquery, query)
                    if int(value)>0:
                        query="CASE WHEN {}  >= {} AND {}  < {} THEN '[{},{})'".format(self.sql,float(diff_from_value),self.sql,float(value),float(diff_from_value),float(minus_one_value))
                        finalquery = '{} {}'.format(finalquery, query)

                i=i+1

            if style == 'integer':
                query="WHEN {}  >= {} THEN '{} or Above' ELSE 'Undefined' END".format(self.sql,value,value)
                finalquery = '{} {}'.format(finalquery, query)
            elif style == 'relational':
                query="WHEN {}  >= {} THEN '>={}' ELSE 'Undefined' END".format(self.sql,float(value),float(value))
                finalquery = '{} {}'.format(finalquery, query)
            elif style == 'classic':
                query="WHEN {}  >= {} THEN 'T{:02d} [{},inf)' ELSE 'TXX Undefined' END".format(self.sql,float(value),i,float(value))
                finalquery = '{} {}'.format(finalquery, query)
            elif style == 'interval':
                query="WHEN {}  >= {} THEN '[{},inf)' ELSE 'Undefined' END".format(self.sql,float(value),float(value))
                finalquery = '{} {}'.format(finalquery, query)
            self.sql = finalquery
            '''
            


    def duration_day(self, sql_start, sql_end):
        type = 'string'
        query = "(TIMESTAMPDIFF(DAY, {sql_start} , {sql_end}) + CASE WHEN TIMESTAMPDIFF(SECOND, TO_DATE({sql_start}), {sql_end}) = TIMESTAMPDIFF(SECOND, TO_DATE({sql_start} ), {sql_end} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, TO_DATE({sql_start}), {sql_end}) < TIMESTAMPDIFF(SECOND, TO_DATE({sql_start} ), {sql_end} ) THEN CASE WHEN {sql_start}  < {sql_end} THEN -1 ELSE 0 END ELSE CASE WHEN {sql_start}  > {sql_end} THEN 1 ELSE 0 END END)".format(sql_start = sql_start, sql_end = sql_end)
        self.sql = query

    def duration_hour(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        type = 'string'
        query = "CASE WHEN TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / (60*60) < 0 THEN CEIL(TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / (60*60)) ELSE FLOOR(TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / (60*60)) END".format(created_at=sql_start,delivered_at=sql_end)
        self.sql = query

    def duration_second(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        type = 'string'
        query = "TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at})".format(created_at=sql_start,delivered_at=sql_end)
        self.sql = query

    def duration_minute(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        type = 'string'
        query = "CASE WHEN TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / 60 < 0 THEN CEIL(TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / 60) ELSE FLOOR(TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / 60) END".format(created_at=sql_start,delivered_at=sql_end)
        self.sql = query

    def duration_month(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        type = 'string'
        query = "(TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END)".format(created_at=sql_start,delivered_at=sql_end)
        self.sql = query

    def duration_quarter(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        type = 'string'
        query = "CASE WHEN (TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 3 < 0 THEN CEIL((TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 3) ELSE FLOOR((TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 3) END".format(created_at=sql_start,delivered_at=sql_end)
        self.sql = query

    def duration_weeks(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        type = 'string'
        query = "CASE WHEN (TIMESTAMPDIFF(DAY, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 7 < 0 THEN CEIL((TIMESTAMPDIFF(DAY, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 7) ELSE FLOOR((TIMESTAMPDIFF(DAY, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 7) END".format(created_at=sql_start,delivered_at=sql_end)
        self.sql = query

    def duration_years(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        type = 'string'
        query = "CASE WHEN (TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 12 < 0 THEN CEIL((TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 12) ELSE FLOOR((TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 12) END".format(created_at=sql_start,delivered_at=sql_end)
        self.sql = query

    def processDistanceDimension(self,distanceUnits,startLocationField,endLocationField,latitude,longitude):
        self.distanceUnits = distanceUnits
        self.startLocationField = startLocationField
        self.endLocationField = endLocationField


    def setDimension(self, dimension, dimensionType,logging = None):

        self.dimensionType = dimensionType


        if 'name' in dimension:
            self.name = dimension['name']

        if 'sql' in dimension:
            self.sql_raw = dimension['sql']

        if 'type' in dimension:
            self.type = dimension['type']

        if 'hidden' in dimension:
            self.hidden = dimension['hidden']

        if 'primary_key' in dimension:
            self.primary_key = dimension['primary_key']

        
        if self.dimensionType == 'DIMENSION':

            self.sql = self.sql_raw

            if self.type == 'location':
                if 'sql_latitude' in dimension:
                    self.sqlLatitude = dimension['sql_latitude']
                if 'sql_longitude' in dimension:
                    self.sqlLongitude = dimension['sql_longitude']
                self.sql = self.transformLocationDimension(self.sqlLongitude, self.sqlLatitude)
                

            self.transformTableDimensions()

            if self.type == 'zipcode':
                self.transformZipCodeDimension()
            if self.type == 'tier':
                tiers = None
                if 'tiers' in dimension:
                    tiers = dimension['tiers']
                    style = dimension['style']
                    self.transformTierDimension(tiers,style,logging)

            if self.type == 'yesno':
                self.transformYesNoDiemension()

            if 'duration' in self.type:
                if 'day' in self.type:
                    sql_start = dimension['sql_start']
                    sql_end = dimension['sql_end']
                    self.duration_day(sql_start,sql_end)
                elif 'hour' in self.type:
                    sql_start = dimension['sql_start']
                    sql_end = dimension['sql_end']
                    self.duration_hour(sql_start,sql_end)
                elif 'minute' in self.type:
                    sql_start = dimension['sql_start']
                    sql_end = dimension['sql_end']
                    self.duration_minute(sql_start,sql_end)
                elif 'month' in self.type:
                    sql_start = dimension['sql_start']
                    sql_end = dimension['sql_end']
                    self.duration_month(sql_start,sql_end)
                elif 'quarter' in self.type:
                    sql_start = dimension['sql_start']
                    sql_end = dimension['sql_end']
                    self.duration_quarter(sql_start,sql_end)
                elif 'week' in self.type:
                    sql_start = dimension['sql_start']
                    sql_end = dimension['sql_end']
                    self.duration_weeks(sql_start,sql_end)
                elif 'year' in self.type:
                    sql_start = dimension['sql_start']
                    sql_end = dimension['sql_end']
                    self.duration_years(sql_start,sql_end)
                elif 'second' in self.type:
                    sql_start = dimension['sql_start']
                    sql_end = dimension['sql_end']
                    self.duration_second(sql_start,sql_end)
            if self.type=='distance':
                start_location_field=dimension['start_location_field']
                end_location_field=dimension['end_location_field']
                units=dimension['units']
                latitude=None
                longitude=None
                self.processDistanceDimension(units,start_location_field,end_location_field,latitude,longitude)

        elif self.dimensionType == 'MEASURE':
            self.sql = self.sql_raw
            self.transformTableDimensions()
            self.processMeasures()
        
        self.dependencies = self.getDependencies()

    def processMeasures(self):
        if self.type.lower() == 'count':
            if self.sql or self.sql.strip() == '':
                self.sql = "COUNT(1)"
            else:
                self.sql = "COUNT({})".format(self.sql)

        elif self.type.lower() == 'average':
            self.sql = "AVG({})".format(self.sql)

        elif self.type.lower() == 'sum':
            self.sql = "SUM({})".format(self.sql)
            
        elif self.type.lower() == 'count_distinct':
            self.sql = "COUNT(DISTINCT {})".format(self.sql)

    
    def getProcessedDistanceDimensions(self, dimensions):
        cleanedDimensions = dimensions
        for i in range(len(dimensions)):
            currentDimension = dimensions[i]
            if currentDimension.type == 'distance':
                name = currentDimension.name
                startLocationField = currentDimension.startLocationField
                endLocationField = currentDimension.endLocationField
                Units = currentDimension.distanceUnits
                startDim = currentDimension.getDimensionByName(startLocationField, dimensions)
                endDim = currentDimension.getDimensionByName(endLocationField, dimensions)
                startDimLongitude=startDim.sqlLongitude
                startDimLatitute=startDim.sqlLatitude
                endDimLongitude=endDim.sqlLongitude
                endDimLatitute=endDim.sqlLatitude
                currentDimension.setDistanceDimensionSQL(startDimLongitude, startDimLatitute, endDimLongitude ,endDimLatitute)
                cleanedDimensions[i] = currentDimension
                
        return cleanedDimensions            


    def processDimensions(self, viewDimensions, viewMeasures, viewDimensionGroups, logging = None):
        if logging is None:
            logging = Logger().getLogger()
        dimensions_ = []
        for dimensionRow in viewDimensions:
            dimensionObj = Dimension()
            dimensionObj.setDimension(dimensionRow, 'DIMENSION', logging)
            dimensions_.append(dimensionObj)

            
        for dimensionRow in viewMeasures:
            dimensionObj = Dimension()
            dimensionObj.setDimension(dimensionRow, 'MEASURE', logging)
            dimensions_.append(dimensionObj)

        dimensionGroupList = []
        logging.info("DIME_GROUP")
        logging.info(viewDimensionGroups)
        for dimension_groupRow in viewDimensionGroups:

            baseName = None

            if 'type' in dimension_groupRow:
                dimensionGroupType = dimension_groupRow['type']

                if dimensionGroupType == 'time':
                    #Add timeframes dimensions

                    if 'timeframes' in dimension_groupRow:

                        if 'sql' in dimension_groupRow:
                            if '${TABLE}.' in dimension_groupRow['sql']:
                                baseName = dimension_groupRow['sql'].replace('${TABLE}.', '')
                        dimensionGroupName = dimension_groupRow['name']

                        for timeframe in dimension_groupRow['timeframes']:
                            name = None
                            type = None
                            sql = None

                            name = '{}_{}'.format(dimensionGroupName, timeframe)

                            if timeframe == 'raw':
                                type = 'date'
                                sql="{}".format(baseName)
                            elif timeframe == 'date':
                                type = 'string'
                                #sql = "convert_timezone('UTC', 'America/New_York', cast('{}' as timestamp_ntz)) as timestamp)"
                                sql = """
                                    (TO_CHAR(TO_DATE({}), 'YYYY-MM-DD'))
                                    """.format(baseName)
                            elif timeframe == 'week':
                                type = 'string'
                                sql = """
                                (TO_CHAR(DATE_TRUNC('week', {}), 'YYYY-MM-DD'))
                                """.format(baseName)
                            elif timeframe == 'month':
                                type = 'string'
                                sql = """
                                (TO_CHAR(DATE_TRUNC('month', {}), 'YYYY-MM'))
                                """.format(baseName)
                            elif timeframe == 'time':
                                type = 'string'
                                sql = """
                                (TO_CHAR(DATE_TRUNC('second', {}), 'YYYY-MM-DD HH24:MI:SS'))
                                """.format(baseName)
                            elif timeframe == 'year':
                                type = 'number'
                                sql = """
                                (EXTRACT(YEAR FROM {})::integer)
                                """.format(baseName)
                            elif timeframe == 'yesno':
                                type = 'string'
                                sql="(CASE WHEN {}  IS NOT NULL THEN 'Yes' ELSE 'No' END)".format(baseName)
                            elif timeframe == 'day_of_month':
                                type = 'string'
                                sql="(EXTRACT(DAY FROM {})::integer)".format(baseName)
                            elif timeframe == 'day_of_year':
                                type = 'string'
                                sql="(EXTRACT(DOY FROM {})::integer)".format(baseName)

                            elif timeframe == 'hour_of_day':
                                type = 'string'
                                sql="(CAST(EXTRACT(HOUR FROM CAST({} AS TIMESTAMP)) AS INT))".format(baseName)
                            elif timeframe == 'time_of_day':
                                type = 'string'
                                sql = """
                                (TO_CHAR({}, 'HH24:MI'))
                                """.format(baseName)

                            elif timeframe == 'week_of_year':
                                type = 'string'
                                sql="(EXTRACT(WEEK FROM {})::int)".format(baseName)

                            elif timeframe == 'fiscal_quarter_of_year':
                                type = 'string'
                                #sql="(CAST('Q' AS VARCHAR) || CAST(CEIL(EXTRACT(MONTH FROM convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz)) as timestamp) )::integer / 3) AS VARCHAR))".format(baseName)
                                sql = """
                                ((CAST('Q' AS VARCHAR) || CAST(CEIL(EXTRACT(MONTH FROM {})::integer / 3) AS VARCHAR)))
                                """.format(baseName)
                            elif timeframe == 'quarter_of_year':
                                type = 'string'
                                #sql="(CAST('Q' AS VARCHAR) || CAST(CEIL(EXTRACT(MONTH FROM convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz)) as timestamp) )::integer / 3) AS VARCHAR))".format(baseName)
                                sql = """
                                ((CAST('Q' AS VARCHAR) || CAST(CEIL(EXTRACT(MONTH FROM {})::integer / 3) AS VARCHAR)))
                                """.format(baseName)
                            elif timeframe == 'day_of_week_index':
                                type = 'string'
                                sql="(MOD(EXTRACT(DOW FROM {})::integer - 1 + 7, 7))".format(baseName)
                            elif timeframe == 'fiscal_month_num':
                                type = 'string'
                                sql="(EXTRACT(MONTH FROM {})::integer)".format(baseName)

                            elif timeframe == 'fiscal_quarter':
                                type = 'string'
                                sql = """
                                (TO_CHAR(DATE_TRUNC('month', CAST(DATE_TRUNC('quarter', {}) AS DATE)), 'YYYY-MM'))
                                """.format(baseName)
                            elif timeframe == 'fiscal_year':
                                type = 'string'
                                sql="(EXTRACT(YEAR FROM {})::integer)".format(baseName)

                            elif timeframe == 'hour':
                                type = 'string'
                                sql="(TO_CHAR(DATE_TRUNC('hour', {}), 'YYYY-MM-DD HH24'))".format(baseName)
                            elif timeframe == 'microsecond':
                                type = 'string'
                                sql="(LEFT(TO_CHAR({} , 'YYYY-MM-DD HH24:MI:SS.FF'), 26))".format(baseName)
                            elif timeframe == 'millisecond':
                                type = 'string'
                                sql="(LEFT(TO_CHAR({}, 'YYYY-MM-DD HH24:MI:SS.FF'), 23))".format(baseName)
                            elif timeframe == 'minute':
                                type = 'string'
                                sql="(TO_CHAR(DATE_TRUNC('minute', {}), 'YYYY-MM-DD HH24:MI'))".format(baseName)
                            elif timeframe == 'month_num':
                                type = 'string'
                                sql="(EXTRACT(MONTH FROM {})::integer)".format(baseName)
                            elif timeframe == 'quarter':
                                type = 'string'
                                sql = "(TO_CHAR(DATE_TRUNC('month', CAST(DATE_TRUNC('quarter', {}) AS DATE)), 'YYYY-MM'))".format(baseName)
                            elif timeframe == 'second':
                                type = 'string'
                                sql="(TO_CHAR(DATE_TRUNC('second', {}), 'YYYY-MM-DD HH24:MI:SS'))".format(baseName)
                            elif timeframe == 'day_of_week':
                                type = 'string'
                                sql="""
                                (CASE TO_CHAR({baseName}, 'DY')
                                    WHEN 'Tue' THEN 'Tuesday'
                                    WHEN 'Wed' THEN 'Wednesday'
                                    WHEN 'Thu' THEN 'Thursday'
                                    WHEN 'Sat' THEN 'Saturday'
                                    ELSE TO_CHAR({baseName}, 'DY') || 'day'
                                    END)
                                """.format(baseName=baseName)
                            elif timeframe == 'month_name':
                                type = 'string'
                                sql="(DECODE(EXTRACT('month', {}), 1, 'January', 2, 'February', 3, 'March', 4, 'April', 5, 'May', 6, 'June', 7, 'July', 8, 'August', 9, 'September', 10, 'October', 11, 'November', 12, 'December'))".format(baseName)
                            elif re.search(r'hour\d+', timeframe):
                                rx = re.compile(r'hour(\d+)')
                                matches_raw = [match.group(1) for match in rx.finditer(timeframe)]
                                hourValue = None
                                for item in matches_raw:
                                    hourValue = item
                                type = 'string'
                                sql="""
                                (TO_CHAR(DATE_TRUNC('hour', DATE_TRUNC('hour', DATEADD('HOURS', -1 * (CAST(DATE_PART('HOUR', CAST({baseName}  AS TIMESTAMP)) AS INT) % {hourValue}), {baseName}))), 'YYYY-MM-DD HH24'))
                                """.format(baseName=baseName, hourValue=hourValue)
                            elif re.search(r'millisecond\d+', timeframe):
                                rx = re.compile(r'millisecond(\d+)')
                                matches_raw = [match.group(1) for match in rx.finditer(timeframe)]
                                milisecondValue = None
                                for item in matches_raw:
                                    milisecondValue = item
                                type = 'string'
                                sql="""
                                (LEFT(TO_CHAR(TO_TIMESTAMP(LEFT(TO_CHAR(DATEADD('NANOSECOND', -1 * (CAST(DATE_PART('NANOSECOND', CAST({baseName}  AS TIMESTAMP)) AS INT) % ({milisecondValue} * 1000000)), {baseName} ), 'YYYY-MM-DD HH24:MI:SS.FF'), 23)), 'YYYY-MM-DD HH24:MI:SS.FF'), 23))
                                """.format(baseName=baseName, milisecondValue=milisecondValue)

                            dimension_ = Dimension()
                            dict_ = {"name": name, "type":type, "sql": sql}
                            dimension_.setDimension(dict_, 'DIMENSION')
                            dimensionGroupList.append(dimension_)

                elif dimensionGroupType == 'duration':
                    #Add duration timeframes

                    name = None
                    type = None
                    sql = None
                    sql_start = None
                    sql_end = None

                    if 'intervals' in dimension_groupRow:
                        for intervals in dimension_groupRow['intervals']:

                            dimensionGroupName = dimension_groupRow['name']
                            name = '{}_{}'.format(dimensionGroupName, intervals)
                            type = '{}_{}'.format(dimensionGroupType, intervals)
                            sql = ''
                            sql_start = dimension_groupRow['sql_start']
                            sql_end = dimension_groupRow['sql_end']
                            dict_ = {"name":name, "type":type, "sql":sql, "sql_start":sql_start, "sql_end":sql_end}
                            dimension_ = Dimension()
                            dict_ = {"name": name, "type":type, "sql": sql, "sql_start":sql_start, "sql_end":sql_end}
                            dimension_.setDimension(dict_, 'DIMENSION')
                            dimensionGroupList.append(dimension_)

        for dimensionItem in dimensionGroupList:
            dimensions_.append(dimensionItem)

        dimensions_ = self.getProcessedDistanceDimensions(dimensions_)

        return dimensions_

    def getIndex(self, dimensions):
        
        index = 0
        
        for dimension_ in dimensions:
            if self.getDimensionName() == dimension_.getDimensionName():
                break
            else:
                index = index + 1
        return index

    def updateDimensionAtIndex(self, dimension, dimensions, index):

        dimensions_ = dimensions
        
        if index >= 0 and index < len(dimensions):
            dimensions_[index] = dimension

        return dimensions_


    def __str__(self):
        return """
            Dimension: --------------------------------------------------------------------------------------------------------
            Name:               {name}
            Type:               {type}
            Hidden:             {hidden}
            Primary Key:        {primary_key}
            Dependencies:       {dependencies}
            DependenciesByPath: {dependenciesByPath}
            SQL RAW:            {sql_raw}
            SQL:                {sql}
            IsExcluded :        {isExcluded}
            ExcludedReason :    {excludedReason}
            dimensionType  :    {dimensionType}  
            """.format(name = self.name, type = self.type, sql = self.sql, primary_key = self.primaryKey, hidden = self.hidden, dependencies = self.dependencies, sql_raw = self.sql_raw, dependenciesByPath = self.dependenciesByPath, isExcluded = self.isExcluded, excludedReason = self.excludedReason, dimensionType = self.dimensionType)

    def getDimensionName(self):
        return self.name

    def getDimensionByName(self, name, dimensions):

        dimension_ = None
        for dimension in dimensions:
            if dimension.getDimensionName() == name:
                dimension_ = dimension
        return dimension_

    def setDependenciesByPath(self, dependenciesByPath):

        self.dependenciesByPath = dependenciesByPath

    def getDependenciesByPath(self):
        return self.dependenciesByPath

    def getSQL(self):
        return self.sql

    def substituteDimension(self, sourceDimension, dimensions):
        sourceName = sourceDimension.getDimensionName()
        sourceSQL = sourceDimension.getSQL()

        sourceDimensionPlaceHolder = r'${' + sourceName +'}'


        self.sql = self.sql.replace(sourceDimensionPlaceHolder, sourceSQL)

    def getProcessedSubstituteDimensions(self, dimenions):
        graph = dict()

        dimensionsObjs = dimenions

        for dimension_ in dimensionsObjs:
            dimName_ = dimension_.getDimensionName()
            dimDep_ = dimension_.getDependencies()

            item = {
                dimName_ : dimDep_
            }

            graph.update(item)


        for dimension_ in dimensionsObjs:
            dimName_ = dimension_.getDimensionName()
            path = findPath(graph, dimName_)

            path.reverse()

            dimension_.setDependenciesByPath(path)

        for i in range(0, len(dimensionsObjs)):
            dimension_ = dimensionsObjs[i]
            dimName = dimension_.getDimensionName()
            dependenciesByPath = dimension_.getDependenciesByPath()

            if len(dependenciesByPath) == 1 and dimName == dependenciesByPath[0]:
                #logging.info("Only I dimension")
                continue
            else:
                #logging.info("All Dimensions")
                dimension_.getDependenciesByPath()
                for j in range(0, len(dependenciesByPath) - 2):
                    #logging.info("Sub Dimenions")
                    sourceDimensionName = dependenciesByPath[j]
                    targetDimensionName = dependenciesByPath[j + 1]

                    targetDimension = dimension_.getDimensionByName(targetDimensionName, dimensionsObjs)
                    sourceDimension = dimension_.getDimensionByName(sourceDimensionName, dimensionsObjs)

                    targetDimension.substituteDimension(sourceDimension, dimensionsObjs)
                    index = targetDimension.getIndex(dimensionsObjs)
                    dimensionsObjs[index] = targetDimension



                    #logging.info("substituteDimension: {}".format(targetDimension))

                #REPLACE ALL IN TARGET DIMENSION

                for name in dimension_.getDependencies():
                    sourceDimension = dimension_.getDimensionByName(name, dimensionsObjs)

                    dimension_.substituteDimension(sourceDimension, dimensionsObjs)
                    index = dimension_.getIndex(dimensionsObjs)
                    dimensionsObjs[index] = dimension_


        for dimension in dimensionsObjs:
            dimension.setExcludedDimension()

        for dimension in dimensionsObjs:
            dependencies = dimension.getDependencies()
            for dependencyItem in dependencies:
                dimension_ = dimension.getDimensionByName(dependencyItem, dimensionsObjs)
                if dimension_.isExcluded:
                    dimension.isExcluded = True
                    dimension.excludedReason = "Referencing to a excluded dimension."

        return dimensionsObjs





def findPath(graph, start, path=[]): 
    path = path + [start]

    if len(graph[start]) == 0: 
        return path
    else: 
        for node in graph[start]: 
            if node not in path: 
                newpath = findPath(graph, node, path) 
                if newpath: 
                    return newpath