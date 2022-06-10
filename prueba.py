import glob
import json

import sys
sys.path.append('./pySpark/lib')
from utils import openTarFile
from utils import TranslateMachine

if __name__ == "__main__":

    #file ='///export/data_ml4ds/IntelComp/Datasets/ted/pruebas/test.tgz'
    file = '/home/sblanco/Downloads/readTED/2022-03.tar.gz'
    #file = './datosprueba/test.tgz'
    #file = '/export/data_ml4ds/IntelComp/Datasets/ted/pruebas/test.tgz'
    #file = '/export/data_ml4ds/IntelComp/Datasets/ted/xmls/2022-01.tar.gz'
    totalResult = []

    print ('abriendo fichero')
    dataFile = openTarFile (file)

    tm = TranslateMachine ()

    print ('Filtrando formularios, en el original hay %s datos' % len(dataFile))
    validForm = ['F','F']
    dataFile = list(filter (lambda d: list (d['TED_EXPORT']['FORM_SECTION'].keys())[0][:1] in validForm, dataFile))
    print ('nos quedamos con %s datos' % len (dataFile))

    print ('convirtiendo datos')
    totalResult += [tm.translateKeys (data) for data in dataFile]

    import ipdb ; ipdb.set_trace()
