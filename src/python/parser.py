import gzip
import re

IMAGE_RELATED_INFO_FILE_MODE = {
								'image_id': 0, 
								'subreddit': 1, 
								'number_of_upvotes': 2, 
								'number_of_downvotes': 3,
								'total_votes': 4, 
								'score': 5
								}

SUBREDDIT_WITH_IDS_FILE_COLUMN_POSITIONS = {"id": 0, "name": 1}
SUBREDDIT_WITH_IDS_FILE_COLUMN_SEPARATOR = "\t"

REDDIT_GZIP_COLUMN_INFO = {
							'image_id': 0, 
							'unixtime': 1, 
							'rawtime': 2, 
							'title': 3,
							'total_votes': 4, 
							'reddit_id': 5, 
							'number_of_upvotes': 6, 
							'subreddit': 7, 
							'number_of_downvotes': 8,
							'localtime': 9, 
							'score': 10, 
							'number_of_comments': 11, 
							'username': 12
							}
REDDIT_GZIP_COLUMN_SEPARATOR = ","
#REDDIT_GZIP_FILE_PATH = "/home/yigit/Doktora/social_network_analysis/project/dataset/redditSubmissions.csv.gz"
REDDIT_GZIP_FILE_PATH = "/home/arcelik/projects/MiniProjects/social-network-analysis-spring2015/excluded/dataset/redditSubmissions.csv.gz"
#DEFAULT_OUTPUT_FOLDER = "/home/yigit/Project/social-network-analysis-spring2015/excluded/output/"
DEFAULT_OUTPUT_FOLDER = "/home/arcelik/projects/MiniProjects/social-network-analysis-spring2015/excluded/output/"

def readSubredditFromGzip(file_path, column_separator, output_file):
	separated_line = []
	subreddit_list = []
	double_quote_positions = []
	double_quote_positions_index = 0

	with gzip.open(file_path, 'rb') as fin:
		for line in fin:
			double_quote_positions = [m.start() for m in re.finditer('"', line)]
			# Delete contents of double quotations
			while double_quote_positions_index < len(double_quote_positions):
				line = line[ : double_quote_positions[double_quote_positions_index]+1] + (double_quote_positions[double_quote_positions_index + 1] - double_quote_positions[double_quote_positions_index]-1) * "*" + line[double_quote_positions[double_quote_positions_index+1] : ]
				double_quote_positions_index += 2
			double_quote_positions_index = 0
			
	 		separated_line = line.split(column_separator)
			try:
				if separated_line[REDDIT_GZIP_COLUMN_INFO['subreddit']] not in subreddit_list:
					subreddit_list.append(separated_line[REDDIT_GZIP_COLUMN_INFO['subreddit']].strip())
			except:
	 			print line

	with open(output_file, 'w') as fout:
		fout.write("Subreddit ID\tSubreddit Name\n")
		for (subreddit_id,subreddit) in enumerate(sorted(subreddit_list)):
			fout.write("{0}\t{1}\n".format(subreddit_id+1, subreddit))
#readSubredditFromGzip(REDDIT_GZIP_FILE_PATH, REDDIT_GZIP_COLUMN_SEPARATOR, DEFAULT_OUTPUT_FOLDER + "subreddits_with_ids.txt")

def loadSubredditsFromFile(subreddit_file):
	subreddit_list = []
	with open(subreddit_file, "r") as fin:
		for line in fin:
			try:
				subreddit_list.append(line.strip.split(SUBREDDIT_WITH_IDS_FILE_COLUMN_SEPARATOR))[SUBREDDIT_WITH_IDS_FILE_COLUMN_POSITIONS["name"]]
			except:
				print "loadSubredditsFromFile ERROR " + line
	return subreddit_list

def readImageidRelatedInfoFromGzip(file_path, column_separator, output_file):
	separated_line = []
	subreddit_list = []
	double_quote_positions = []
	double_quote_positions_index = 0
	image_id = ""
	subreddit_name = ""
	num_of_upvotes = ""
	num_of_downvotes = ""
	total_votes = ""
	score = ""
	fout = open(output_file, "w")
	#fout.write("image_id\tsubreddit_name\tnumber_of_upvotes\tnumber_of_downvotes\tnumber_of_total_votes\tnet_score\n")

	with gzip.open(file_path, 'rb') as fin:
		for line in fin:
			double_quote_positions = [m.start() for m in re.finditer('"', line)]
			# Delete contents of double quotations
			while double_quote_positions_index < len(double_quote_positions):
				line = line[ : double_quote_positions[double_quote_positions_index]+1] + (double_quote_positions[double_quote_positions_index + 1] - double_quote_positions[double_quote_positions_index]-1) * "*" + line[double_quote_positions[double_quote_positions_index+1] : ]
				double_quote_positions_index += 2
			double_quote_positions_index = 0
			
	 		separated_line = line.split(column_separator)
			try:
				image_id = separated_line[REDDIT_GZIP_COLUMN_INFO['image_id']].strip()
				subreddit_name = separated_line[REDDIT_GZIP_COLUMN_INFO['subreddit']].strip()
				num_of_upvotes = separated_line[REDDIT_GZIP_COLUMN_INFO['number_of_upvotes']].strip()
				num_of_downvotes = separated_line[REDDIT_GZIP_COLUMN_INFO['number_of_downvotes']].strip()
				total_votes = separated_line[REDDIT_GZIP_COLUMN_INFO['total_votes']].strip()
				score = separated_line[REDDIT_GZIP_COLUMN_INFO['score']].strip()
				fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(image_id,subreddit_name,num_of_upvotes,num_of_downvotes,total_votes,score))
			except:
	 			print line
	fout.close()
#readImageidRelatedInfoFromGzip(REDDIT_GZIP_FILE_PATH, REDDIT_GZIP_COLUMN_SEPARATOR, DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt")

def extractSingleInfoFromImageAllInOneFile(image_related_comprehensive_file, info_to_be_extracted, output_file):
	image_info_dictionary = {}
	sum_of_values = 0
	if info_to_be_extracted not in IMAGE_RELATED_INFO_FILE_MODE.keys():
		print "Requested info does not exist"
		return

	with open(image_related_comprehensive_file, "r") as fin:
		# Skip the first line because it contains explanation about columns
		fin.readline()
		for line in fin:
			line = line.strip().split("\t")
			try:
				image_info_dictionary[int(line[IMAGE_RELATED_INFO_FILE_MODE['image_id']])].append(line[IMAGE_RELATED_INFO_FILE_MODE[info_to_be_extracted]])
			except:
				image_info_dictionary[int(line[IMAGE_RELATED_INFO_FILE_MODE['image_id']])] = [line[IMAGE_RELATED_INFO_FILE_MODE[info_to_be_extracted]]]

	with open(output_file, "w") as fout:
		fout.write("image_id\t{0}\n".format(info_to_be_extracted))
		if info_to_be_extracted not in ['image_id', 'subreddit']:
			for key in sorted(image_info_dictionary.keys()):
				sum_of_values = reduce(lambda x,y:int(x)+int(y), image_info_dictionary[key])
				fout.write("{0}\t{1}\n".format(key, sum_of_values))
		else:
			for key in sorted(image_info_dictionary.keys()):
				for elem in image_info_dictionary[key]:
					fout.write("{0}\t{1}\n".format(key, elem))
#xtractSingleInfoFromImageAllInOneFile(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", "number_of_upvotes", DEFAULT_OUTPUT_FOLDER + "images_with_number_of_upvotes.txt")
extractSingleInfoFromImageAllInOneFile(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", "number_of_downvotes", DEFAULT_OUTPUT_FOLDER + "images_with_number_of_downvotes.txt")
extractSingleInfoFromImageAllInOneFile(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", "total_votes", DEFAULT_OUTPUT_FOLDER + "images_with_total_votes.txt")
extractSingleInfoFromImageAllInOneFile(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", "score", DEFAULT_OUTPUT_FOLDER + "images_with_score.txt")