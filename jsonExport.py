'''
Python:
	accept epsg as argument

	copy into new DB
	create structure for 3nf
	add geometry columns
	Call Ruby:
		Write responses into 3nf
	write geometries into 3nf tables

	call shapefile tool


	TODO:
		convert ruby calls into rbenv or system ruby calls
		figure out how shell script wrapper needs to work for exporter


'''


import sqlite3
import csv, codecs, cStringIO
from xml.dom import minidom
import sys
import pprint
import glob
import json
import os
import shutil
import re
import zipfile
import subprocess
import glob
import tempfile
import xml.etree.ElementTree as ET
import re
import lsb_release
import tarfile
import traceback

from collections import defaultdict

from collections import namedtuple
from itertools import izip
fileNameType = "Identifier"

pp = pprint.PrettyPrinter(indent=2)

def namedtuple_factory(cursor, row):
	"""
	Usage:
	con.row_factory = namedtuple_factory
	"""
	fields = [col[0] for col in cursor.description]
	Row = namedtuple("Row", fields)
	return Row(*row)

def cleanWithUnder(str):
     out = re.sub("[^a-zA-Z0-9]+", "_", str)     
     return out  

def upper_repl(match):
	if (match.group(1) == None):
		return ""
	return match.group(1).upper()

def clean(str):
	 out = re.sub(" ([a-z])|[^A-Za-z0-9]+", upper_repl, str)	 
	 return out

def indent(elem, level=0):
	i = "\n" + level*"  "
	if len(elem):
		if not elem.text or not elem.text.strip():
			elem.text = i + "  "
		if not elem.tail or not elem.tail.strip():
			elem.tail = i
		for elem in elem:
			indent(elem, level+1)
		if not elem.tail or not elem.tail.strip():
			elem.tail = i
	else:
		if level and (not elem.tail or not elem.tail.strip()):
			elem.tail = i

def makeSurePathExists(path):
	try:
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise

pprinterr = pprint.PrettyPrinter(indent=2, stream=sys.stderr)
pprinterr.pprint(sys.argv)

originalDir = sys.argv[1]
finalExportDir = sys.argv[2]+"/"

exportDir = tempfile.mkdtemp()+"/"

importDB = originalDir+"db.sqlite3"
exportDB = exportDir+"json.sqlite3"
files = ['json.sqlite3']

pprinterr.pprint(originalDir)
pprinterr.pprint(finalExportDir)

with open(originalDir+'/module.settings') as settings:
	jsondata = json.load(settings)

srid = jsondata['srid']
arch16nFile = glob.glob(originalDir+"*.0.properties")[0]
print jsondata
moduleName = clean(jsondata['name'])



images = None
overrideFormattedIdentifiers = None
try:
	foo= json.load(open(sys.argv[3],"r"))
	# print foo["Export Images and Files?"]
	if (foo["Export Images and Files?"] != []):
		images = True
	else:
		images = False
except:
	sys.stderr.write("Json input failed")
	images = True

print "Exporting Files %s" % (images)
print foo
def zipdir(path, zip):
	for root, dirs, files in os.walk(path):
		for file in files:
			zip.write(os.path.join(root, file))



if lsb_release.get_lsb_information()['RELEASE'] == '16.04':
	LIBSPATIALITE = 'mod_spatialite.so'
else:
	LIBSPATIALITE = 'libspatialite.so.5'



def zipdir(path, zip):
	for root, dirs, files in os.walk(path):
		for file in files:
			zip.write(os.path.join(root, file))


try:
	os.remove(exportDB)
except OSError:
	pass

importCon = sqlite3.connect(importDB)
importCon.enable_load_extension(True)
importCon.load_extension(LIBSPATIALITE)
exportCon = sqlite3.connect(exportDB)
exportCon.enable_load_extension(True)
exportCon.load_extension(LIBSPATIALITE)
exportCon.row_factory = namedtuple_factory


exportCon.execute("select initSpatialMetaData(1)")

for line in importCon.iterdump():
	try:
		exportCon.execute(line)
	except sqlite3.Error:		
		pass


 
exportCon.executescript("drop table if exists keyval; create table keyval (key text, val text);")
arch16nDict = {}

f = open(arch16nFile, 'r')
for line in f:
	
	keyval = line.replace("\n","").replace("\r","").decode("utf-8").split('=')
	keyval[0] = '{'+keyval[0]+'}'
	exportCon.execute("insert into keyval(key, val) VALUES(?, ?)", keyval)
	arch16nDict[keyval[0]] = keyval[1]
f.close()

#pattern = re.compile(r'\b(' + '|'.join(arch16nDict.keys()) + r')\b')
pattern = re.compile('|'.join(re.escape(key) for key in arch16nDict.keys()))
#pp.pprint(arch16nDict)

'''
Attributes + Vocab
Archents
Relationships
'''

attributeXML = ET.Element("attributes")

for row in exportCon.execute("select * from attributekey;"):
	attrib = ET.SubElement(attributeXML,"attribute")

	'''	AttributeID           	INTEGER PRIMARY KEY,
	AttributeType		 	TEXT, -- this is typing for external tools. It has no bearing internally
	AttributeName         	TEXT NOT NULL, -- effectively column name
	AttributeDescription  	TEXT, -- human-entered description for the "column"
	FormatString			TEXT,
	AppendCharacterString	TEXT,
	SemanticMapURL			TEXT

	name = ET.SubElement(attrib, "name")
	name.text = unicode(row[2])
	atrid = ET.SubElement(attrib,"id")
	atrid.text = unicode(row[0])
	desc = ET.SubElement(attrib,"description")
	desc.text = unicode(row[3])
	typeE = ET.SubElement(attrib,"type")
	typeE.text = unicode(row[1])
	formatString = ET.SubElement(attrib,"formatString")
	formatString.text = unicode(row[4])
	appendCharacterString = ET.SubElement(attrib,"appendCharacterString")
	appendCharacterString.text = unicode(row[5])
	semanticMapURL = ET.SubElement(attrib,"semanticMapURL")
	semanticMapURL.text = unicode(row[6])
'''

	for value, key in izip(row, row._fields):		
		if value:
			sublEle = ET.SubElement(attrib,key)
			sublEle.text = unicode(value)


	vocabList = ET.SubElement(attrib,"controlledVocabs")
	for vocab in exportCon.execute("select * from vocabulary where attributeid = ?", [row[0]]):
		vocabEle = ET.SubElement(vocabList,"vocabulary")

		for value, key in izip(vocab, vocab._fields):			
			if value:
				sublEle = ET.SubElement(vocabEle,key)
				sublEle.text = unicode(value)
		for arch16n in exportCon.execute("select trim(val) as val from keyval where key = ?", [vocab[2]]):
			archEle = ET.SubElement(vocabEle,'Arch16n')
			archEle.text = unicode(arch16n[0])

		'''
		VocabID          INTEGER PRIMARY KEY,
  AttributeID      INTEGER NOT NULL REFERENCES AttributeKey,
  VocabName        TEXT    NOT NULL, -- This is the human-visible part of vocab that forms lookup tables. It is likely to be Arch16nized.
  SemanticMapURL   TEXT,
  PictureURL       TEXT, -- relative path.
  VocabDescription TEXT,
  VocabCountOrder  INTEGER,
  VocabDeleted     TEXT,
  ParentVocabID    INTEGER REFERENCES Vocabulary (VocabID),
	VocabDateFrom			TEXT,
	VocabDateTo				TEXT,
	VocabDateAnnotation		TEXT,
	VocabProvenance			TEXT,
	VocabDateCertainty		TEXT, -- this is effectively whether or not your dates are circa or not. It can be covered in note from in the Annotation, but might be nice to separate out.
	VocabType				TEXT, -- This would be for associations other than 'ParentVocab'. I can't think of too many examples where this is relevant, but it's basically an option giving you access to a meaningful analytic group or type, that you may not want to have to step through in a picture gallery. Eg for transfer prints, I might like to see a summary of % of Chinoiserie vs European landscape patterns as I work, but I don't want to have to have to choose 'Chinoiserie>Landscape>Landscape with Border>Willow' each time I record something. In some respects this is stretching bow for 'data entry' fused with analysis, but if we're tinkering with the schema it may not hurt. It might?
	VocabMaker				TEXT -- This is standard in typologies for historical archaeology but not as important to other archaeologies. As above, it may be nice to see these sorts of analyses as you work, but likely better suited to post-ex analysis than data recording.
 
 '''
root = ET.ElementTree(attributeXML)
indent(attributeXML)
root.write(exportDir+"attributes.xml")
files = []
files.append("attributes.xml")

aentXML = ET.Element("archents")



subprocess.call(["bash", "./format.sh", originalDir, exportDir, exportDir])

updateArray = []
formattedIdentifiers = {}



f= open(exportDir+'shape.out', 'r')
for line in f.readlines():	
	out = line.replace("\n","").replace("\\r","").split("\t")
	#print "!!%s -- %s!!" %(line, out)
	if (len(out) ==4):		
		data = unicode(out[3].replace("\\n","\n").replace("'","''"), errors="replace")
		if str(out[0]) not in formattedIdentifiers:
			formattedIdentifiers[str(out[0])] = {}
		formattedIdentifiers[str(out[0])][out[2]] = data

		# print out[2]
		# pp.pprint(formattedIdentifiers[str(out[0])])
		# exportCon.execute(update, data)

#pp.pprint(formattedIdentifiers)



fileReplacelist={}
if images:
#	for directory in importCon.execute("select distinct aenttypename, attributename from latestnondeletedaentvalue join attributekey using (attributeid) join latestnondeletedarchent using (uuid) join aenttype using (aenttypeid) where attributeisfile is not null and measure is not null"):
#		makeSurePathExists("%s/%s/%s" % (exportDir,clean(directory[0]), clean(directory[1])))

	filehash = defaultdict(int)



	print "* File list exported:"
	for filename in importCon.execute("select uuid, measure, freetext, certainty, attributename, aenttypename from latestnondeletedaentvalue join attributekey using (attributeid) join latestnondeletedarchent using (uuid) join aenttype using (aenttypeid) where attributeisfile is not null and measure is not null"):
		try:        
			oldPath = filename[1].split("/")
			oldFilename = oldPath[2]
			aenttypename = clean(filename[5])
			attributename = clean(filename[4])
			newFilename = "%s/%s/%s" % (aenttypename, attributename, oldFilename)
			if os.path.isfile(originalDir+filename[1]):
					# print filename[0]
					
				filehash["%s%s" % (filename[0], attributename)] += 1
				
				#pp.pprint(formattedIdentifiers )
				#foo = exportCon.execute("select identifier from %s where uuid = %s" % (aenttypename, filename[0]))
				identifier=cleanWithUnder(formattedIdentifiers[str(filename[0])]['identifier'])

				r= re.search("(\.[^.]*)$",oldFilename)

				delimiter = ""
				
				if filename[2]:
					delimiter = "a"

				newPath = "%s/%s" % (aenttypename, identifier)
				newFilename =  "%s/%s-%s_%s%s%s" % (newPath, attributename, identifier, filehash["%s%s" % (filename[0], attributename)],delimiter, r.group(0))
				

				#pp.pprint(formattedIdentifiers[filename[0]])
				#exifdata = exifCon.execute("select * from %s where uuid = %s" % (aenttypename, filename[0])).fetchone()
				# iddata = [] 
				# for id in importCon.execute("select coalesce(measure, vocabname, freetext) from latestnondeletedarchentidentifiers where uuid = %s union select aenttypename from latestnondeletedarchent join aenttype using (aenttypeid) where uuid = %s" % (filename[0], filename[0])):
				# 	iddata.append(id[0])

				#print("```From {0} to {1}```".format(originalDir+filename[1], exportDir+newFilename))
				if not os.path.exists(exportDir+newPath):
					os.makedirs(exportDir+newPath)
				shutil.copyfile(originalDir+filename[1], exportDir+newFilename)
				fileReplacelist[filename[1]] = newFilename

				# mergedata = exifdata.copy()
				# mergedata.update(jsondata)
				# mergedata.pop("geospatialcolumn", None)
				# exifjson = {"SourceFile":exportDir+newFilename, 
				# 			"UserComment": [json.dumps(mergedata)], 
				# 			"ImageDescription": formattedIdentifiers[str(filename[0])]['identifier'],
				# 			"XPSubject": "Annotation: %s" % (filename[2]),
				# 			"Keywords": iddata,
				# 			"Software": "FAIMS Project",
				# 			"ImageID":  formattedIdentifiers[str(filename[0])]['uuid'],
				# 			"Copyright": jsondata['name']


				# 			}
				# with open(exportDir+newFilename+".json", "w") as outfile:
				# 	json.dump(exifjson, outfile)    

				# if imghdr.what(exportDir+newFilename):
					
				# 	subprocess.call(["exiftool", "-m", "-q", "-sep", "\"; \"", "-overwrite_original", "-j=%s" % (exportDir+newFilename+".json"), exportDir+newFilename])

				#TODO Figure out how to write this back to something?

				#exportCon.execute("update %s set %s = ? where uuid = ?" % (aenttypename, attributename), (newFilename, filename[0]))
				print "    * %s" % (newFilename)
				#files.append(newFilename+".json")
				files.append(newFilename)
			else:
				print "```Unable to find file %s, from uuid: %s```" % (originalDir+filename[1], filename[0]) 
		except Exception as e:
				pp.pprint(e)
				traceback.print_exc(file=sys.stdout)
				print "```Unable to find file (exception thrown) %s, from uuid: %s```" % (originalDir+filename[1], filename[0])  


for aenttype in exportCon.execute("select aenttypeid, aenttypename from aenttype"):
	aentTypeEnt = ET.SubElement(aentXML, "aenttype")
	aentTypeEnt.set("aentTypeID", unicode(aenttype[0]))
	aentTypeEnt.set("aentTypeName", unicode(aenttype[1]))

	for row in exportCon.execute("select uuid,createdAt,createdBy,modifiedAt,modifiedBy, astext(geospatialcolumn) as WKT,aenttypename from latestnondeletedarchent join createdModifiedAtBy using (uuid) join aenttype using (aenttypeid) where aenttypename =?", [aenttype[1]]):
		aent = ET.SubElement(aentTypeEnt, "archentity")
		for value, key in izip(row, row._fields):		
			if value:
				sublEle = ET.SubElement(aent,key)
				sublEle.text = unicode(value)

			


		idents = ET.SubElement(aent, "identifiers")
		properties = ET.SubElement(aent, "properties")	
		for ident in exportCon.execute( '''select vocabname, measure, freetext, certainty, attributename, aentcountorder, vocabcountorder, formatString, appendCharacterString, vocabid
	from latestNonDeletedArchent 
	JOIN aenttype using (aenttypeid) 
	JOIN (select * from idealaent where isIdentifier='true') using (aenttypeid) 
	join attributekey  using (attributeid) 
	join latestNonDeletedAentValue using (uuid, attributeid) 
	left outer join vocabulary using (attributeid, vocabid) 
	where uuid = ? 
	order by uuid, aentcountorder, vocabcountorder;''', [row[0]]):
			identEle = ET.SubElement(idents, "identifier")
			for value, key in izip(ident, ident._fields):		
				if value:
					sublEle = ET.SubElement(identEle,key)
					sublEle.text = unicode(value)
		

		formattedIdents = ET.SubElement(idents, "formattedIdentifier")
		if str(row[0]) in formattedIdentifiers and "identifier" in formattedIdentifiers[str(row[0])]:
			formattedIdents.text = unicode(formattedIdentifiers[str(row[0])]['identifier'])	
		else:
			formattedIdents.text = "ERROR-NoIdentifier!"


		for prop in exportCon.execute( '''select vocabname, devocab as resolvedvocabname, measure, freetext, certainty, attributename, aentcountorder, vocabcountorder, formatString, appendCharacterString, attributeid, vocabid
	from latestNonDeletedArchent 
	JOIN aenttype using (aenttypeid) 
	JOIN (select * from idealaent) using (aenttypeid) 
	join attributekey  using (attributeid) 
	join latestNonDeletedAentValue using (uuid, attributeid) 
	left outer join vocabulary using (attributeid, vocabid)
	left outer join (select vocabid, attributeid, coalesce(val, vocabname) as devocab
								  from vocabulary 
								  left outer join keyval on (vocabulary.vocabname = keyval.key))
								  using (attributeid, vocabid)      
	where uuid = ? 
	order by uuid, aentcountorder, vocabcountorder;''', [row[0]]):
			propEle = ET.SubElement(properties, "property")
			for value, key in izip(prop, prop._fields):		
				if value:
					sublEle = ET.SubElement(propEle,key)
					if value in fileReplacelist:
						print("Replaced {0} with {1}".format(value, fileReplacelist[value]))
						sublEle.text = unicode(fileReplacelist[value])	
					else:
						sublEle.text = unicode(value)

			if str(row[0]) in formattedIdentifiers:
				#print str(	)
				if prop[5] in formattedIdentifiers[str(row[0])]:
					formattedProp = ET.SubElement(propEle, "formattedAttribute")
					#print "Good: %s %s" % (prop[5], formattedIdentifiers[str(row[0])][prop[5]])
					formattedProp.text = pattern.sub(lambda x: arch16nDict[x.group()], unicode(formattedIdentifiers[str(row[0])][prop[5]]))
				else:
					print "Formatted identifier Notfound" 
					print(prop[5], row[0])
					pp.pprint(prop)
					pp.pprint(formattedIdentifiers[str(row[0])])





root = ET.ElementTree(aentXML)
indent(aentXML)
root.write(exportDir+"archents.xml")
files.append("archents.xml")



relnXML = ET.Element("relationships")

relntypequery = '''select distinct relntypeid, relntypename from relntype join latestnondeletedrelationship using (relntypeid);'''

relnquery = '''select parent.uuid as fromuuid, child.uuid as touuid, fname || ' ' || lname as username, parent.aentrelntimestamp as aentrelntimestamp, parent.participatesverb as participatesverb from (select * from latestnondeletedaentreln join relationship using (relationshipid)  where relationshipid in (select relationshipid from relationship join relntype using (relntypeid) where relntypename = ?)) parent join (latestnondeletedaentreln join relationship using (relationshipid)) child on (parent.relationshipid = child.relationshipid and parent.uuid != child.uuid)  join user using (userid)'''

for relntype in exportCon.execute(relntypequery):
	relnTypeEle = ET.SubElement(relnXML, "relationshipType")
	relnTypeEle.set("relntypeid", unicode(relntype[0]))
	relnTypeEle.set("relntypename", unicode(relntype[1]))
	for reln in exportCon.execute(relnquery, [relntype[1]]):
		relnEle = ET.SubElement(relnTypeEle, "relationship")
		for value, key in izip(reln, reln._fields):		
			if value:
				sublEle = ET.SubElement(relnEle,key)
				sublEle.text = unicode(value)


root = ET.ElementTree(relnXML)
indent(relnXML)
root.write(exportDir+"relns.xml")
files.append("relns.xml")

shutil.copyfile(originalDir+'module.settings', exportDir+"module.settings")

files.append("module.settings")


shutil.copyfile(originalDir+'data_schema.xml', exportDir+"data_schema.xml")

files.append("data_schema.xml")

shutil.copyfile(arch16nFile, "%sprimaryArch16n.properties"%(exportDir))
files.append("primaryArch16n.properties")

  





tarf = tarfile.open("%s/%s-xmlrepo-export.tar.bz2" % (finalExportDir,moduleName), 'w:bz2')
try:
	for file in files:
		tarf.add(exportDir+file, arcname=moduleName+'/'+file)
finally:
	tarf.close()




#zipf = zipfile.ZipFile("%s/%s-export.zip" % (finalExportDir,moduleName), 'w')
#for file in files:
#    zipf.write(exportDir+file, moduleName+'/'+file)
#zipf.close()

try:
	os.remove(exportDir)
except OSError:
	pass

