
import glob
import argparse
import tarfile
import xmltodict
from collections import Counter
from functools import reduce
import operator
from operator import concat
import time
import json

class tedDict ():
    __data              = {}
    __formKey           = None



    def __init__ ( self, data ):
        self.__data = data

    def extracFormKey (self):
        self.__formKey = [key for key in self.__data['TED_EXPORT']['FORM_SECTION'].keys()][0]

        return self.__formKey

    def extracFormValues (self):

        if not (self.__formKey):
            self.extracFormKey ()

        try:
            return [key for key in self.__data['TED_EXPORT']['FORM_SECTION'][self.__formKey][0].keys()]
        except:
            #patata = self.__data
            #frkey = self.__formKey
            #import ipdb ; ipdb.set_trace()
            return self.__data['TED_EXPORT']['FORM_SECTION'][self.__formKey]


def openTarFile ( filename ):

    with tarfile.open(filename, "r:gz") as file:
        # don't use file.members as it's 
        # not giving nested files and folders
        
        dictdata = []

        for member in file:        
            if member.name.endswith ('xml'):
                data = file.extractfile(member.name).read()
                dictdata.append(xmltodict.parse(data))
        return dictdata



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Script en python para leer ficheros XML de Ted')
    parser.add_argument('-p','--path', help='Ruta donde se encuentran los ficheros tar.gz con los xmls', required=True )
    parser.add_argument('-y','--year', help='tratar solo los de ese año', required=False )
    arg = parser.parse_args()

    if arg.year:
        filePattern = arg.path + '*' + str(arg.year) + '*.tar.gz'
    else:
        filePattern = arg.path + '*.tar.gz'

    keys    = {}
    stats   = {}
    listVal = []

    langsDict = {}
    paisesDict = {}

    counter = Counter()

    for file in glob.glob( filePattern ):
        print (file)
        dataFile = openTarFile (file)

        tmpList = ''

        '''
            Estadisticas idiomas 
                '''

        '''
        langs = [ doc['TED_EXPORT']['TECHNICAL_SECTION']['FORM_LG_LIST'].split(' ') for doc in dataFile]
        try:
            langs = reduce(operator.concat, langs)
            transStats = {lang:langs.count(lang) for lang in set(langs)}
            langsDict = dict(Counter(langsDict)+Counter(transStats))

            with open(time.strftime("%Y%m%d-%H%M%S"), 'w') as fout:
                json.dump (transStats, fout)
        except:
            pass

        '''
        paises = [doc['TED_EXPORT']['CODED_DATA_SECTION']['NOTICE_DATA']['ISO_COUNTRY']['@VALUE']  for doc in dataFile]
        counter.update (Counter (paises))


        #listContractAward = [ data['TED_EXPORT']  if 'CONTRACT_AWARD' in data ['TED_EXPORT']['FORM_SECTION'].keys() for data in dataFile]

    paisesDict = dict (counter)

    with open('paises.json', 'w') as fout:
        json.dump (paisesDict, fout)

    import ipdb ; ipdb.set_trace()



       


















'''
        import ipdb ; ipdb.set_trace()   
        for data in dataFile:

            mydictData = tedDict ( data )            
            formkey = mydictData.extracFormKey ()

            valor = stats.get (formkey, 0) + 1
            stats.update ({formkey : valor })

            try:
                listVal =  listVal + mydictData.extracFormValues () 
            except:
                if type(mydictData.extracFormValues ())==str:
                    listval = listVal + [mydictData.extracFormValues ()]
                else:
                    listval = listVal + [key for key in  (mydictData.extracFormValues ()).keys()]


            keys[ formkey ] = keys.get(formkey, mydictData.extracFormValues () )
            

    for tmpk in keys:
        values = [key for key in keys[tmpk]]
        values.sort()
        datastr = ",".join(values)
        print ('%s,%s'% (tmpk, datastr) )


    listValSort = [dato for dato in set(listval)]
    listValSort.sort()
    print (listValSort)

    for tmpk in keys.keys():
        strtmp = ''
        for val in listValSort:
            if val in keys[tmpk]:
                valor = 1
            else:
                valor = 0
            strtmp = strtmp + ',' + str(valor)
        print ('%s%s' % (tmpk,strtmp))





    for k in stats.keys():
        print ('%s,%s' % (k, stats[k]))

    import ipdb ; ipdb.set_trace()

'''

    


