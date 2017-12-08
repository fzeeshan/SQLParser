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
    parsingOutputFile = open(parsingOutputPath, 'w')

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

    # delete elements in xml in elemNameList
    # e.g. <a><b><c></c></b></a> with ['b'] will get <a><c></c></a>
    def XmlStripElements(parent, elemNameList):
        index = 0
        for child in parent.getchildren():
            if child.tag in elemNameList:
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

    def parseXml(tmpStr):
        # root = ET.fromstring(tmpStr)# this parsing will include xml namespace in front of all tags, will strip NS instead
        root = ParseXmlStripNs(tmpStr)
        root = root.find('statement')
        if isSelectStatement(root):
            XmlStripElements(root, constantDictionary['tagsToDiscard'])
        else:
            return 'NOT SELECT STATEMENT'
        # add whatever we want to do with the dictionary
        return ET.tostring(root).decode("utf-8")

    flag = False
    tmpStr = ''
    for row in outputfile:
        if row == 'statementInXML.sichen - BEGIN\n':
            flag = True
            continue
        elif row == 'statementInXML.sichen - END\n':
            try:
                parsingOutputFile.write(parseXml(tmpStr))
            except:
                parsingOutputFile.write('CANNOT BE PARSED \n')
            # initialize flags
            flag = False
            tmpStr = ''
            continue
        elif row == 'INVALID RETURN FOR EXE\n':
            continue
        if flag:
            tmpStr = tmpStr + row

    parsingOutputFile.close


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Given YAML configuration parse csv file with SQL Queries into AST-Trees.")
    parser.add_argument('-y', '--yaml', type=str, required=False)
    args = parser.parse_args()
    main(args.yaml)
