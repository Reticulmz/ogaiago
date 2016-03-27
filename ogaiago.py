import struct
import sys
import os
import configparser
import pymysql
import glob
import datetime

VERSION = "1.0.0"

class db:
	connection = None
	disconnected = False

	def __init__(self, __host, __username, __password, __database):
		self.connection = pymysql.connect(host=__host, user=__username, password=__password, db=__database, cursorclass=pymysql.cursors.DictCursor, autocommit=True)

	def execute(self, __query):
		with self.connection.cursor() as cursor:
			try:
				cursor.execute(__query)
			finally:
				cursor.close()

	def lastInsertID(self):
		return self.connection.insert_id()

	def fetch(self, __query):
		with self.connection.cursor() as cursor:
			try:
				cursor.execute(__query)
				return cursor.fetchone()
			finally:
				cursor.close()

class config:
	config = configparser.ConfigParser()
	fileName = ""
	mysql = True

	def __init__(self, __file):
		self.fileName = __file
		if (os.path.isfile(self.fileName)):
			self.config.read(self.fileName)

	def loadConfig(self):
		try:
			self.config.get("db","host")
			self.config.get("db","username")
			self.config.get("db","password")
			self.config.get("db","database")
			self.mysql = True
		except:
			print("Invalid syntax in config.ini. Working in local mode.")
			self.mysql = False


class dataTypes:
	byte 	= 0
	uInt16 	= 1
	sInt16 	= 2
	uInt32 	= 3
	sInt32 	= 4
	uInt64 	= 5
	sInt64 	= 6
	string 	= 7
	ffloat	= 8
	bbytes 	= 9
	rawReplay = 10

def clamp(value, low, high):
	return min(max(value,low),high)

def calcAcc(c300, c100, c50, cMiss):
	totalHits = c300+c100+c50+cMiss
	return clamp(float(c50 * 50 + c100 * 100 + c300 * 300) / (totalHits * 300), 0.0, 1.0)

def uleb128Decode(num):
	shift = 0
	arr = [0,0]
	while True:
		b = num[arr[1]]
		arr[1]+=1
		arr[0] = arr[0] | (int(b & 127) << shift)
		if (b & 128 == 0):
			break
		shift += 7
	return arr

def unpackData(__data, __dataType):
	if (__dataType == dataTypes.uInt16):
		unpackType = "<H"
	elif (__dataType == dataTypes.sInt16):
		unpackType = "<h"
	elif (__dataType == dataTypes.uInt32):
		unpackType = "<L"
	elif (__dataType == dataTypes.sInt32):
		unpackType = "<l"
	elif (__dataType == dataTypes.uInt64):
		unpackType = "<Q"
	elif (__dataType == dataTypes.sInt64):
		unpackType = "<q"
	elif (__dataType == dataTypes.string):
		unpackType = "<s"
	elif (__dataType == dataTypes.ffloat):
		unpackType = "<f"
	else:
		unpackType = "<B"
	return struct.unpack(unpackType, bytes(__data))[0]

def readBinData(__stream, __structure = []):
	data = {}
	end = 0
	start = 0
	for i in __structure:
		start = end
		unpack = True
		if (i[1] == dataTypes.string):
			unpack = False
			if (__stream[start] == 0):
				data[i[0]] = ""
				end = start+1
			else:
				length = uleb128Decode(__stream[start+1:])
				end = start+length[0]+length[1]+1
				data[i[0]] = ''.join(chr(j) for j in __stream[start+1+length[1]:end])
		elif (i[1] == dataTypes.byte):
			end = start+1
		elif (i[1] == dataTypes.uInt16 or i[1] == dataTypes.sInt16):
			end = start+2
		elif (i[1] == dataTypes.uInt32 or i[1] == dataTypes.sInt32):
			end = start+4
		elif (i[1] == dataTypes.uInt64 or i[1] == dataTypes.sInt64):
			end = start+8
		elif (i[1] == dataTypes.rawReplay):
			unpack = False
			length = unpackData(__stream[start:start+4], dataTypes.uInt32)
			end = start+4+length
			data[i[0]] = __stream[start+4:end]

		if (unpack == True):
			data[i[0]] = unpackData(__stream[start:end], i[1])
	return data

def printVerbose(s):
	if (verbose):
		print(s)

def getTimeStamp():
	dt = datetime.datetime.now()
	return int("{:02d}{:02d}{:02d}{:02d}{:02d}{:02d}".format(dt.year-2000, dt.month, dt.day, dt.hour, dt.minute, dt.second))

# Help argument
if ("-h" in sys.argv or "--help" in sys.argv):
	print("""NAME:
   ogaiago - Submit scores and replays to ripple from replays

USAGE:
   ogaiago [arguments...]

VERSION:
   {}

ARGUMENTS:
   -i                show some replay info about the osr file that's being imported
   -v                run in verbose mode
   -l                force local mode (mysql will be ignored even if configured properly)
   -h, --help        show help
   """.format(VERSION))
	sys.exit()

print("OGAIAGO v{}".format(VERSION))

# Reset argument
if ("-r" in sys.argv):
	print("Resetting replays/")
	if (os.path.exists("replays/")):
		for i in glob.glob("replays/*"):
			os.remove(i)

	print("Resetting output/")
	if (os.path.exists("output/")):
		for i in glob.glob("output/*"):
			os.remove(i)

	print("Done")
	sys.exit()

# Make sure replays folder exists
if (not os.path.exists("replays/")):
	os.makedirs("replays/")

# Make sure output folder exists
if (not os.path.exists("output/")):
	os.makedirs("output/")

# Make sure the folder is not empty
if (os.listdir("replays/") == []):
	print("Put your .osr replays inside \"replays\" folder and run ogaiago again.")
	sys.exit()

# Load config
conf = config("config.ini")
conf.loadConfig()

# Defult CLI options
showInfo = False
verbose = False

# CLI arguments
# TODO: Single replay mode
# TODO: Generate config.ini
# TODO: Reset replays and output
if ("-i" in sys.argv):
	showInfo = True

if ("-v" in sys.argv):
	verbose = True

if ("-l" in sys.argv):
	conf.mysql = False

# Connect to db if needed
if (conf.mysql):
	try:
		print("Working in remote mode")
		printVerbose("Connecting to db...")
		conn = db(conf.config["db"]["host"], conf.config["db"]["username"], conf.config["db"]["password"], conf.config["db"]["database"])
	except:
		print("Error while connecting to db.\nWorking in local mode.")
		conf.mysql = False
else:
	print("Working in local mode")

# Get files in replays folder
fileList = glob.glob("replays/*.osr")

# Loop though all files
for i in fileList:
	# Read data from file
	with open(i, "rb") as f:
		printVerbose("Reading {}...".format(i))
		fileData = f.read()

	# Process data
	replayData = readBinData(fileData, [
		["gameMode", dataTypes.byte],
		["osuVersion", dataTypes.uInt32],
		["beatmapHash", dataTypes.string],
		["playerName", dataTypes.string],
		["magicHash", dataTypes.string],
		["count300", dataTypes.uInt16],
		["count100", dataTypes.uInt16],
		["count50", dataTypes.uInt16],
		["countGeki", dataTypes.uInt16],
		["countKatu", dataTypes.uInt16],
		["countMiss", dataTypes.uInt16],
		["score", dataTypes.uInt32],
		["maxCombo", dataTypes.uInt16],
		["fullCombo", dataTypes.byte],
		["mods", dataTypes.uInt32],
		["lifeBarGraph", dataTypes.string],
		["timeStamp", dataTypes.uInt64],
		["rawReplay", dataTypes.rawReplay],
	])
	acc = calcAcc(replayData["count300"], replayData["count100"], replayData["count50"], replayData["countMiss"])*100

	printVerbose("Replay read!")

	if (showInfo):
		print("----------\nReplay data:")
		print("> game mode: {}".format(replayData["gameMode"]))
		print("> osu version: {}".format(replayData["osuVersion"]))
		print("> beatmap hash: {}".format(replayData["beatmapHash"]))
		print("> player: {}".format(replayData["playerName"]))
		print("> magic hash: {}".format(replayData["magicHash"]))
		print("> 300s: {}".format(replayData["count300"]))
		print("> 100s: {}".format(replayData["count100"]))
		print("> 50s: {}".format(replayData["count50"]))
		print("> gekis: {}".format(replayData["countGeki"]))
		print("> katus: {}".format(replayData["countKatu"]))
		print("> misses: {}".format(replayData["countMiss"]))
		print("> score: {}".format(replayData["score"]))
		print("> max combo: {}".format(replayData["maxCombo"]))
		print("> full combo: {}".format(replayData["fullCombo"]))
		print("> mods: {}".format(replayData["mods"]))
		print("> time stamp: {}".format(replayData["timeStamp"]))

	if (conf.mysql):
		topScore = conn.fetch("SELECT * FROM scores WHERE username = '{}' AND beatmap_md5 = '{}' AND completed = 3".format(replayData["playerName"], replayData["beatmapHash"]))
		if (topScore == None or replayData["score"] > topScore["score"]):
			completed = 3
		else:
			completed = 2
	else:
		completed = -1

	if (completed == 3):
		printVerbose("This is a top score")
	elif (completed == 2):
		printVerbose("This is NOT a top score")
	else:
		printVerbose("Working on local mode. I don't know if this is a top score. Let's assume it is.")

	# Build query
	query = "INSERT INTO scores (id, beatmap_md5, username, score, max_combo, full_combo, mods, 300_count, 100_count, 50_count, katus_count, gekis_count, misses_count, time, play_mode, completed, accuracy) VALUES (NULL, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(replayData["beatmapHash"], replayData["playerName"], replayData["score"], replayData["maxCombo"], replayData["fullCombo"], replayData["mods"], replayData["count300"], replayData["count100"], replayData["count50"], replayData["countKatu"], replayData["countGeki"], replayData["countMiss"], int(getTimeStamp()), replayData["gameMode"], completed, acc)
	if (conf.mysql):
		try:
			conn.execute(query)
			outputFileName = "output/replay_{}.osr".format(conn.lastInsertID())
		except:
			print("Error while executing query!")
			raise
	else:
		fileName = os.path.basename(i)
		outputFileName = "output/{}_raw.osr".format(fileName.replace(".osr", ""))
		printVerbose("----------\nQuery:")
		printVerbose(query)
		with open("output/queries.sql", "a") as f:
			f.write(query)

	# Raw replay output
	with open(outputFileName, "wb") as f:
		printVerbose("Writing {}...".format(outputFileName))
		f.write(bytearray(replayData["rawReplay"]))

	# Done
	print("{} imported!".format(os.path.basename(i)))

if (conf.mysql):
	# TODO: Auto copy files
	print("Done! Please copy all the files inside the \"output\" folder in your \"osu.ppy.sh/replays\" folder.")
else:
	print("Done! Please execute \"output/queries.sql\" on your MySQL server and copy all the .osr files inside the \"output\" folder in your \"osu.ppy.sh/replays\" folder. You also have to rename them with this format: replay_SCOREID.osr")
