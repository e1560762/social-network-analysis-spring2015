import gzip
import re

from collections import OrderedDict

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

IMAGE_SUBREDDIT_OCCURENCES_FILE_POSITIONS = {'image_id' : 0, 'subreddit' : 1, 'number_of_occurences' : 2}
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

DEFAULT_PAJEK_RELATED_FOLDER = "/home/arcelik/projects/MiniProjects/social-network-analysis-spring2015/excluded/pajek/"
SAMPLE_DATAFILE_FOR_PAJEK_POSITIONS = {"image_id" : 0, "occurence_count" : 1}
SORT_MODE = {'not_sorted' : 0, 'ascending' : 1, 'descending' : 2}
COMPREHENSIVE_PAJEK_RELATED_FILE_POSITIONS = {
												"real_image_id" : 0,
												"pajek_image_id" : 1,
												"subreddit_name" : 2,
												"pajek_subreddit_id" : 3,
												"number_of_upvotes" : 4,
												"number_of_downvotes" : 5,
												"total_votes" : 6,
												"score" : 7
											}

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
# readSubredditFromGzip(REDDIT_GZIP_FILE_PATH, REDDIT_GZIP_COLUMN_SEPARATOR, DEFAULT_OUTPUT_FOLDER + "subreddits_with_ids.txt")

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
# readImageidRelatedInfoFromGzip(REDDIT_GZIP_FILE_PATH, REDDIT_GZIP_COLUMN_SEPARATOR, DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt")

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
# extractSingleInfoFromImageAllInOneFile(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", "number_of_upvotes", DEFAULT_OUTPUT_FOLDER + "images_with_number_of_upvotes.txt")
# extractSingleInfoFromImageAllInOneFile(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", "number_of_downvotes", DEFAULT_OUTPUT_FOLDER + "images_with_number_of_downvotes.txt")
# extractSingleInfoFromImageAllInOneFile(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", "total_votes", DEFAULT_OUTPUT_FOLDER + "images_with_total_votes.txt")
# extractSingleInfoFromImageAllInOneFile(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", "score", DEFAULT_OUTPUT_FOLDER + "images_with_score.txt")

def extractNumberOfTiesPerSubreddit(image_related_comprehensive_file, output_file):
	images_with_number_of_ties_dict = {}
	splitted_line = []
	image_id = -1
	subreddit_name = ""
	with open(image_related_comprehensive_file, "r") as fin:
		fin.readline()
		for line in fin:
			splitted_line = line.strip().split("\t")
			try:
				image_id = int(splitted_line[IMAGE_RELATED_INFO_FILE_MODE['image_id']])
				subreddit_name = splitted_line[IMAGE_RELATED_INFO_FILE_MODE['subreddit']]
				images_with_number_of_ties_dict[image_id][subreddit_name] = images_with_number_of_ties_dict[image_id].get(subreddit_name, 0) + 1
			except:
				try:
					images_with_number_of_ties_dict[image_id] = {subreddit_name : 1}
				except:
					print line.strip()

	with open(output_file, "w") as fout:
		for image in sorted(images_with_number_of_ties_dict.keys()):
			for subr, count in images_with_number_of_ties_dict[image].iteritems():
				fout.write("{0}\t{1}\t{2}\n".format(image, subr, count))
# extractNumberOfTiesPerSubreddit(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", DEFAULT_OUTPUT_FOLDER + "image_subreddit_occurences.txt")

def extractNumberOfTiesPerImage(image_related_comprehensive_file, output_file):
	images_with_number_of_ties_dict = {}
	splitted_line = []
	image_id = -1

	with open(image_related_comprehensive_file, "r") as fin:
		fin.readline()
		for line in fin:
			splitted_line = line.strip().split("\t")
			try:
				image_id = int(splitted_line[IMAGE_RELATED_INFO_FILE_MODE['image_id']])
				images_with_number_of_ties_dict[image_id] = images_with_number_of_ties_dict.get(image_id, 0) + 1
			except:
				print line.strip()

	with open(output_file, "w") as fout:
		for image in sorted(images_with_number_of_ties_dict.keys()):
			fout.write("{0}\t{1}\n".format(image, images_with_number_of_ties_dict[image]))
# extractNumberOfTiesPerImage(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", DEFAULT_OUTPUT_FOLDER + "image_total_occurences.txt")
def getSubredditTiesToImages(image_subreddit_occurences_file, output_file):
	subreddit_to_image_tie_dict = {}
	splitted_line = []
	subreddit_name = ""
	image_id = -1
	number_of_occurrences = -1

	with open(image_subreddit_occurences_file, "r") as fin:
		fin.readline()
		for line in fin:
			splitted_line = line.strip().split("\t")
			image_id = splitted_line[IMAGE_SUBREDDIT_OCCURENCES_FILE_POSITIONS['image_id']]
			subreddit_name = splitted_line[IMAGE_SUBREDDIT_OCCURENCES_FILE_POSITIONS['subreddit']]
			number_of_occurrences = int(splitted_line[IMAGE_SUBREDDIT_OCCURENCES_FILE_POSITIONS['number_of_occurences']])
			try:
				subreddit_to_image_tie_dict[subreddit_name][image_id] = subreddit_to_image_tie_dict[subreddit_name].get(image_id, 0) + number_of_occurrences
				subreddit_to_image_tie_dict[subreddit_name]['total'] = subreddit_to_image_tie_dict[subreddit_name].get('total', 0) + number_of_occurrences
			except:
				subreddit_to_image_tie_dict[subreddit_name] = {'total' : number_of_occurrences, image_id : number_of_occurrences}
	
	print subreddit_to_image_tie_dict

	with open(output_file, "w") as fout:
		fout.write("#subreddit_name\ttotal_number_of_ties\tImage:Occurance\n")
		for subr, image_occurence_dict in OrderedDict(reversed(sorted(subreddit_to_image_tie_dict.iteritems(), key=lambda x: x[1]['total']))).iteritems():
			fout.write("{0}\t{1}".format(subr, image_occurence_dict['total']))
			for img, cnt in image_occurence_dict.iteritems():
				if img != 'total':
					fout.write("\t{0}\t{1}".format(img, cnt))
			fout.write("\n")

# getSubredditTiesToImages(DEFAULT_OUTPUT_FOLDER + "image_subreddit_occurences.txt", DEFAULT_OUTPUT_FOLDER + "subreddit_images_occurences.txt")

############# Pajek project file related methods are defined below. #############
def getSelectedImagesFromSampleFile(selected_sample_file, sort_mode):
	image_id_list = []
	images_with_pajek_id = {}
	image_id = -1
	with open(selected_sample_file, "r") as fin:
		for line in fin:
			image_id = int(line.strip().split("\t")[SAMPLE_DATAFILE_FOR_PAJEK_POSITIONS['image_id']])
			if image_id not in image_id_list:
				image_id_list.append(image_id)
	if sort_mode == SORT_MODE['ascending']:
		for ind, img in enumerate(sorted(image_id_list)):
			images_with_pajek_id[img] = ind+1
	elif sort_mode == SORT_MODE['descending']:
		 for ind, img in enumerate(reversed(sorted(image_id_list))):
		 	images_with_pajek_id[img] = ind+1
	else:
		for ind, img in enumerate(image_id_list):
			images_with_pajek_id[img] = ind+1

	return (image_id_list, images_with_pajek_id)

def extractComprehensiveDataForPajekFromSelected(selected_sample_file, image_related_comprehensive_file, comprehensive_pajek_data_file, sort_mode):
	real_image_ids,pajek_image_id_dict = getSelectedImagesFromSampleFile(selected_sample_file, sort_mode)
	splitted_line = []
	subreddit_list = []
	subreddits_with_ids = {}
	image_id = -1
	subreddit_name = ""
	number_of_images = len(real_image_ids)
	fout = open(comprehensive_pajek_data_file, 'w+')
	fout.write("real_image_id\tpajek_image_id\tsubreddit_name\tpajek_subreddit_id\tnumber_of_upvotes\tnumber_of_downvotes\ttotal_votes\tscore\n")

	with open(image_related_comprehensive_file, "r") as fin:
		fin.readline()
		for line in fin:
			splitted_line = line.strip().split("\t")
			try:
				image_id = int(splitted_line[IMAGE_RELATED_INFO_FILE_MODE['image_id']])
				subreddit_name = splitted_line[IMAGE_RELATED_INFO_FILE_MODE['subreddit']]
				if image_id in real_image_ids and subreddit_name not in subreddit_list:
					subreddit_list.append(subreddit_name)
			except:
				print line.strip()

	if sort_mode == SORT_MODE['ascending']:
		for ind,subr in enumerate(sorted(subreddit_list)):
			subreddits_with_ids[subr] = ind + 1 + number_of_images
	elif sort_mode == SORT_MODE['descending']:
		for ind,subr in enumerate(reversed(sorted(subreddit_list))):
			subreddits_with_ids[subr] = ind + 1 + number_of_images
	else:
		for ind,subr in subreddit_list:
			subreddits_with_ids[subr] = ind + 1 + number_of_images

	with open(image_related_comprehensive_file, "r") as fin:
		fin.readline()
		for line in fin:
			splitted_line = line.strip().split("\t")
			try:
				image_id = int(splitted_line[IMAGE_RELATED_INFO_FILE_MODE['image_id']])
				subreddit_name = splitted_line[IMAGE_RELATED_INFO_FILE_MODE['subreddit']]
				if image_id in real_image_ids:
					fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format(image_id,
																				pajek_image_id_dict[image_id],
																				subreddit_name,
																				subreddits_with_ids[subreddit_name],
																				splitted_line[IMAGE_RELATED_INFO_FILE_MODE['number_of_upvotes']],
																				splitted_line[IMAGE_RELATED_INFO_FILE_MODE['number_of_downvotes']],
																				splitted_line[IMAGE_RELATED_INFO_FILE_MODE['total_votes']],
																				splitted_line[IMAGE_RELATED_INFO_FILE_MODE['score']]
																				))
			except:
				print line.strip()
	return (number_of_images, len(subreddit_list))

#img_count, subr_count = extractComprehensiveDataForPajekFromSelected(DEFAULT_PAJEK_RELATED_FOLDER + "images_with_number_of_ties_2.txt", DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", DEFAULT_PAJEK_RELATED_FOLDER + "comprehensive_pajek_data_2.txt", SORT_MODE['ascending'])
#print img_count, subr_count # 295 154

def calculatePajekSubredditIdsFromImageComprehensive(image_related_comprehensive_file, number_of_images, sort_mode):
	splitted_line = []
	subreddit_list = []
	subreddits_with_ids = {}
	image_id = -1
	subreddit_name = ""

	with open(image_related_comprehensive_file, "r") as fin:
		fin.readline()
		for line in fin:
			splitted_line = line.strip().split("\t")
			try:
				image_id = int(splitted_line[IMAGE_RELATED_INFO_FILE_MODE['image_id']])
				subreddit_name = splitted_line[IMAGE_RELATED_INFO_FILE_MODE['subreddit']]
				if image_id in real_image_ids and subreddit_name not in subreddit_list:
					subreddit_list.append(subreddit_name)
			except:
				print line.strip()

	if sort_mode == SORT_MODE['ascending']:
		for ind,subr in enumerate(sorted(subreddit_list)):
			subreddits_with_ids[subr] = ind + 1 + number_of_images
	elif sort_mode == SORT_MODE['descending']:
		for ind,subr in enumerate(reversed(sorted(subreddit_list))):
			subreddits_with_ids[subr] = ind + 1 + number_of_images
	else:
		for ind,subr in subreddit_list:
			subreddits_with_ids[subr] = ind + 1 + number_of_images
	return subreddits_with_ids

# subreddits_with_pajek_ids = calculatePajekSubredditIdsFromImageComprehensive(DEFAULT_OUTPUT_FOLDER + "image_related_info_all_in_one.txt", 295, SORT_MODE['ascending'])
def createPajekProjectFileFromPajekComprehensive(comprehensive_pajek_data_file, number_of_images, number_of_subreddits, vote_field, output_file):
	splitted_line = []
	subreddits_with_pajek_ids_dict = {}

	with open(comprehensive_pajek_data_file, 'r') as fin:
			fin.readline()
			for line in fin:
				splitted_line = line.strip().split('\t')
				subreddits_with_pajek_ids_dict[int(splitted_line[COMPREHENSIVE_PAJEK_RELATED_FILE_POSITIONS['pajek_subreddit_id']])] = splitted_line[COMPREHENSIVE_PAJEK_RELATED_FILE_POSITIONS['subreddit_name']]
	
	with open(output_file, 'w') as fout:
		fout.write("*Network Images.net [2-mode]\n")
		fout.write("*Vertices\t{0}\t{1}\n".format(number_of_images+number_of_subreddits, number_of_images))
		image_id_counter = 1
		
		while image_id_counter <= number_of_images:
			fout.write("\t{0} \"Image_{1}\"\n".format(image_id_counter, image_id_counter))
			image_id_counter += 1

		for subreddit_id in sorted(subreddits_with_pajek_ids_dict.keys()):
			fout.write("\t{0} \"{1}\"\n".format(subreddit_id, subreddits_with_pajek_ids_dict[subreddit_id]))
		
		fout.write("*Arcs\n*Edges\n")

		with open(comprehensive_pajek_data_file, 'r') as fin:
			fin.readline()
			for line in fin:
				splitted_line = line.strip().split('\t')
				fout.write("\t{0} {1} {2}\n".format(splitted_line[COMPREHENSIVE_PAJEK_RELATED_FILE_POSITIONS['pajek_image_id']],
													splitted_line[COMPREHENSIVE_PAJEK_RELATED_FILE_POSITIONS['pajek_subreddit_id']],
													splitted_line[COMPREHENSIVE_PAJEK_RELATED_FILE_POSITIONS[vote_field]]
													))

#createPajekProjectFileFromPajekComprehensive(DEFAULT_PAJEK_RELATED_FOLDER + "comprehensive_pajek_data_2.txt", img_count, subr_count, 'number_of_upvotes', DEFAULT_PAJEK_RELATED_FOLDER + "Images_with_65_img.paj")

def loadNumberOfSubscribers(subscriber_count_file):
	subscriber_count_dict = {}
	separated_line = []

	with open(subscriber_count_file, 'r') as fin:
		for line in fin:
			separated_line = line.strip().split('\t')
			subscriber_count_dict[separated_line[0]] = int(separated_line[1])

	return subscriber_count_dict

def extractNumberOfSubscribersOfSubredditsFromPajekComprehensive(comprehensive_pajek_data_file, subscriber_count_file, output_file):
	subscriber_info_dict = loadNumberOfSubscribers(subscriber_count_file)
	separated_line = []
	distinct_subreddit_names = []

	fin = open(comprehensive_pajek_data_file, 'r')

	fin.readline()
	for line in fin:
		separated_line = line.strip().split('\t')
		if separated_line[COMPREHENSIVE_PAJEK_RELATED_FILE_POSITIONS['subreddit_name']] not in distinct_subreddit_names:
			distinct_subreddit_names.append(separated_line[COMPREHENSIVE_PAJEK_RELATED_FILE_POSITIONS['subreddit_name']])
	fin.close()
	
	fout = open(output_file, 'w')
	for subr in distinct_subreddit_names:
		fout.write("{0}\t{1}\n".format(subr, subscriber_info_dict[subr]))
	fout.close()

#extractNumberOfSubscribersOfSubredditsFromPajekComprehensive(DEFAULT_PAJEK_RELATED_FOLDER + "comprehensive_pajek_data_2.txt", DEFAULT_OUTPUT_FOLDER + "subreddits_with_subscribers.txt", DEFAULT_PAJEK_RELATED_FOLDER + "subscriber_info_2.txt")

def loadImageIdsFromPajekSample(pajek_distinct_image_id_file):
	splitted_line = []
	image_set = []

	with open(pajek_distinct_image_id_file, 'r') as fin:
		for line in fin:
			splitted_line = line.strip().split('\t')
			if splitted_line[0] not in image_set:
				image_set.append(splitted_line[0])
	return image_set

def loadOverallVotingStatistics(overall_statistics_file):
	splitted_line = []
	overall_statistics_dict = {}

	with open(overall_statistics_file, 'r') as fin:
		fin.readline()
		for line in fin:
			splitted_line = line.strip().split('\t')
			overall_statistics_dict[splitted_line[0]] = int(splitted_line[1])

	return overall_statistics_dict


def extractOverallVotingStatisticsForPajekSample(image_set, overall_statistics_dict, output_file):
	with open(output_file, 'w') as fout:
		for img in image_set:
			fout.write("{0}\t{1}\n".format(img, overall_statistics_dict[img]))

image_list = loadImageIdsFromPajekSample(DEFAULT_PAJEK_RELATED_FOLDER + "images_with_number_of_ties_2.txt")
overall_statistics_map = loadOverallVotingStatistics(DEFAULT_OUTPUT_FOLDER + "images_with_total_votes.txt")
extractOverallVotingStatisticsForPajekSample(image_list, overall_statistics_map, DEFAULT_PAJEK_RELATED_FOLDER + "total_votes_stats_sample_2.txt")
