import csv
import subprocess
import re

import xml.etree.ElementTree as ET

def main(yamlFileName):
    if yamlFileName is None:
        yamlFileName = r'C:\Users\sichez\PycharmProjects\SQLParser\configuration.yaml'

    # parse configuration yaml file, get constant dictionary
    constantDictionary = parseYaml(yamlFileName)

    # pass configuration dictionary to runExe function to run exe fille and get output
    if not constantDictionary['skipExe']:
        runExe(constantDictionary)

    # consuming the output file, put xml into ElementTree structure and return the structure to xml file
    parsingXML(constantDictionary)


def parseYaml(yamlFileName):
    import yaml
    stream = open(yamlFileName, 'r')
    doc = yaml.load(stream)
    retDictionary = {}
    for k, v in doc.items():
        retDictionary[k] = v
    return retDictionary


# get rid of blank lines in input string
# we need to do this for getting neat Element Tree elements
def deleteExtraNewLinesInString(inputString):
    inputString = re.sub(r'[\r\n][\r\n]{2,}', '\n\n', inputString)
    inputString = re.sub(r'\n\s*\n', '\n', inputString)
    inputString = re.sub(r'\n\s*\r', '\n', inputString)
    inputString = re.sub(r'\r\s*\n', '\n', inputString)
    inputString = re.sub(r'\r\s*\r', '\n', inputString)
    return inputString


def runExe(constantDictionary):
    fileCounter = 0
    validCounter = 0

    # run exe and generate file combing all XMLs results
    outputfile = open(constantDictionary['exeOutputFilePath'], 'w')
    with open(constantDictionary['csvFilePath'], 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(csvreader)
        for row in csvreader:
            outputfile.write('statementId = ' + row[0] + '\n')
            outputfile.write('statementInXML.sichen - BEGIN\n')
            try:
                outputxml = deleteExtraNewLinesInString(subprocess.check_output(
                    constantDictionary['exeCommand'] + ' ' + constantDictionary['exeCommandConsumingString'].format(
                        '"' + row[1] + '"'), shell=True).decode("utf-8"))
                validCounter = validCounter + 1
            except subprocess.CalledProcessError:
                outputxml = 'INVALID RETURN FOR EXE\n'
            outputfile.write(outputxml)
            outputfile.write('statementInXML.sichen - END\n')
            outputfile.write('\n')
            fileCounter = fileCounter + 1

    outputfile.write('SUMMARY: # of Scripts is ' + str(fileCounter) + ', ' + str(validCounter) + ' is valid')
    outputfile.close()


# go in XMLs results, parse XML and put all parse results in a new file
def parsingXML(constantDictionary):
    parsingOutputPath = constantDictionary['parseOutputFilePath']
    parsingOutputFile = open(parsingOutputPath, 'wb')

    outputfile = open(constantDictionary['exeOutputFilePath'], 'r')

    def ParseXmlStripNs(xmlStr):
        import io
        # instead of ET.fromstring(xml)
        it = ET.iterparse(io.StringIO(xmlStr))
        for _, el in it:
            if '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
        return it.root

    def isSelectStatement(statement):
        return statement.items()[0][1] == 'sstselect';

    def containInRegex(inputString, regexList):
        import re
        return re.match(("(" + ")|(".join(regexList) + ")"), inputString) != None

    # delete elements in xml in elemNameList
    # e.g. <a><b><c></c></b></a> with ['b'] will get <a><c></c></a>
    def XmlStripElements(parent, elemNameList):
        index = 0
        for child in parent.getchildren():
            if containInRegex(child.tag, elemNameList):
                # do replace recusrively until insert everything correctly
                XmlStripElements(child, elemNameList)
                parent.remove(child)
                for element in child.getchildren():
                    parent.insert(index, element)
                    index = index + 1
                    XmlStripElements(element, elemNameList)
            else:
                index = index + 1
                XmlStripElements(child, elemNameList)

    def hasTableAttribute(parent, tableAttribute):
        tableName = tableAttribute[0]
        attribute = tableAttribute[1]
        hasTable = False
        hasAttribute = False
        for it in parent.iter():
            if (it.tag == tableName) or (it.text == tableName):
                hasTable = True
            if (it.tag == tableName) or (it.text == tableName):
                hasAttribute = True
        return (hasTable and hasAttribute)

    def FilterTableAttributes(parent, tableAttriTuples):
        if tableAttriTuples is None:
            return parent
        else:
            result = False
            for ta in tableAttriTuples:
                result = result or hasTableAttribute(parent, ta)
            if result:
                return parent
            else:
                return None

    def parseXml(tmpStr):
        import zlib
        # root = ET.fromstring(tmpStr)# this parsing will include xml namespace in front of all tags, will strip NS instead
        root = ParseXmlStripNs(tmpStr)
        root = root.find('statement')

        if isSelectStatement(root):
            XmlStripElements(root, constantDictionary['tagsToDiscard'])
            root = FilterTableAttributes(root, constantDictionary['tableAttributeFilter'])
        else:
            return 'NOT SELECT STATEMENT'
        # add whatever we want to do with the dictionary

        return ET.tostring(root).decode("utf-8")

    def writeStr(tmpString):
        import zlib
        if constantDictionary['compress']:
            parsingOutputFile.write(zlib.compress(bytes(tmpString, 'UTF-8'), 9))
        else:
            parsingOutputFile.write(bytes(tmpString, 'UTF-8'))
        return


    flag = False
    tmpStr = ''
    inputStringNumber = 0
    finalXmlNumber = 0
    finalWhereClauseNumber = 0
    for row in outputfile:
        if row == 'statementInXML.sichen - BEGIN\n':
            flag = True
            continue
        elif row == 'statementInXML.sichen - END\n':
            inputStringNumber = inputStringNumber + 1
            try:
                parsingResult = parseXml(tmpStr)
                if constantDictionary['includeNoWhere']:
                    writeStr(parsingResult)
                else:
                    if parsingResult.find('where_clause') > 0:
                        writeStr(parsingResult)
                finalXmlNumber = finalXmlNumber + 1
                finalWhereClauseNumber = finalWhereClauseNumber + (parsingResult.find('where_clause')>0)
            except:
                writeStr('CANNOT BE PARSED \n')
            # initialize flags
            flag = False
            tmpStr = ''
            continue
        elif row == 'INVALID RETURN FOR EXE\n':
            continue
        if flag:
            tmpStr = tmpStr + row

    parsingOutputFile.write(
        bytes('Finally done: {} inputs, {} valid output, {} with where clause'.format(inputStringNumber, finalXmlNumber,
                                                                                finalWhereClauseNumber), 'UTF-8'))
    parsingOutputFile.close


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Given YAML configuration parse csv file with SQL Queries into AST-Trees.")
    parser.add_argument('-y', '--yaml', type=str, required=False)
    args = parser.parse_args()
    main(args.yaml)
