import tarfile
import xmltodict


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




class TranslateMachine:

	__keysToSaveMain = ['@DOC_ID']
	__keysTranslateMain = {'@DOC_ID':'doc_id'}

	__keysToSaveMainTechnicalSection = ['RECEPTION_ID']
	__keysTranslateTechnicalSection = {'RECEPTION_ID':'reception_id'}


	def translateMainSection ( self, data ):
		return {self.__keysTranslateMain[key] : data[key] for key in self.__keysToSaveMain }

	def translateTechnicalSection ( self, data ):
	    #si queremos un diccionarion con otro diccionario para cada sección:
	    #data['TECHNICAL_SECTION'] = {keysTranslateTechnicalSection[key]: tedDoc['TED_EXPORT']['TECHNICAL_SECTION'][key] for key in keysTranslateTechnicalSection }
	    #si queremos un sólo nivel:
		return {self.__keysTranslateTechnicalSection[key]: data[key] for key in self.__keysTranslateTechnicalSection }    



	def __getLote (self, lote):

		if 'TITLE' in lote:
			title = lote['TITLE']['P']
		else:
			title = 'NONE'

		if 'VAL_ESTIMATED_TOTAL' in lote:
			val_est = lote['VAL_ESTIMATED_TOTAL']['#text']
		else:
			val_est = 'NONE'

		if 'SHORT_DESCR' in lote:
			short_d = lote['SHORT_DESCR']['P']
		else:
			short_d = 'NONE'

		if 'TYPE_CONTRACT' in lote:
			type_cont = lote['TYPE_CONTRACT']['@CTYPE']
		else:
			type_cont = 'NONE'



		try:
			valor = {'TITLE': title,'REFERENCE_NUMBER':lote.get('REFERENCE_NUMBER','NONE'),\
						 'CPV_MAIN':lote['CPV_MAIN']['CPV_CODE']['@CODE'],'TYPE_CONTRACT':type_cont,\
						  'SHORT_DESCR':short_d, 'VAL_ESTIMATED_TOTAL':val_est,\
						   'LOT_DIVISION':lote.get('LOT_DIVISION','NONE'),'OBJECT_DESCR':lote.get('OBJECT_DESCR','NONE'),'DATE_PUBLICATION_NOTICE':lote.get('DATE_PUBLICATION_NOTICE','NONE')
					}
		except Exception as E:
			print (E)
			import ipdb ; ipdb.set_trace()

		return valor

	def procesarLote (self, data ):

		lotes = []

		if type(data) == list:
			for d in data:
				lotes.append (self.__getLote (d))
		else:			
			lotes.append (self.__getLote (data))

		return lotes




	def translateFormSection ( self, data ):
		

		formData = {}
		formData['EN_TRANSLATION'] = False

		#si es una lista de valores es que se trata de una traduccion, un idioma en cada uno. Buscamos el original y 
		#la traducción al ingles si existe, tambien guardamos el original :

		if type (data) == list:
			for value in data:
				if value['@LG'] == 'EN' and value['@CATEGORY'] == 'TRANSLATION':
					formData['EN_TRANSLATION'] = True
					formData['LOT_TRANSLATION'] = self.procesarLote ( value['OBJECT_CONTRACT'] )

				if  value['@CATEGORY'] == 'ORIGINAL':
					formData['ORIGINAL_LG'] = value['@LG']
					formData['LOT_ORIGINAL'] = self.procesarLote ( value['OBJECT_CONTRACT'] )					


		#solo tiene un idioma:
		else:
			formData['ORIGINAL_LG'] = data['@LG']
			formData['LOT_ORIGINAL'] = self.procesarLote ( data['OBJECT_CONTRACT'] )

		formData['LOT_NUMBER'] = len (formData['LOT_ORIGINAL'])



		return formData

	def translateKeys ( self, tedDoc ):

	    data = {}
	    
	    formdata = {}
	    formdata['FORM_ID'] = list (tedDoc['TED_EXPORT']['FORM_SECTION'].keys())[0]
	        

	    data = self.translateMainSection ( tedDoc['TED_EXPORT'] )	    

	    techData = self.translateTechnicalSection (tedDoc['TED_EXPORT']['TECHNICAL_SECTION'])
	    data.update ( techData )

	    data.update ( formdata )

	    formdata2 = self.translateFormSection ( tedDoc['TED_EXPORT']['FORM_SECTION'][formdata['FORM_ID']] ) 

	    data.update ( formdata2 )


	    return data

