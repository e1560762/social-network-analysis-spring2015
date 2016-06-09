import gzip

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
REDDIT_GZIP_FILE_PATH = "/home/yigit/Doktora/social_network_analysis/project/dataset/redditSubmissions.csv.gz"
DEFAULT_OUTPUT_FOLDER = "/home/yigit/Project/social-network-analysis-spring2015/excluded/output/"

def readSubredditFromGzip(file_path, column_separator, output_file):
	separated_line = []
	subreddit_list = []
	subreddit_name = None
	double_quote_positions = []

	with gzip.open(file_path, 'rb') as fin:
		for line in fin:
			double_quote_positions = [m.start() for m in re.finditer('"', line)]
			
			# ToDo: Delete contents of double quotations

			separated_line = line.split(column_separator)
			try:
				if separated_line[REDDIT_GZIP_COLUMN_INFO['title']].find("\"") > -1 and separated_line[REDDIT_GZIP_COLUMN_INFO['subreddit']+2] not in subreddit_list:
					subreddit_name = separated_line[REDDIT_GZIP_COLUMN_INFO['subreddit']+2].strip()
				elif separated_line[REDDIT_GZIP_COLUMN_INFO['subreddit']] not in subreddit_list:
					subreddit_name = separated_line[REDDIT_GZIP_COLUMN_INFO['subreddit']].strip()
				
				if subreddit_name != None:
					subreddit_list.append(subreddit_name)
			except:
				print line
			subreddit_name = None

	with open(output_file, 'w') as fout:
		fout.write("Subreddit ID\tSubreddit Name\n")
		for (subreddit_id,subreddit) in enumerate(sorted(subreddit_list)):
			fout.write("{0}\t{1}\n".format(subreddit_id+1, subreddit))

readSubredditFromGzip(REDDIT_GZIP_FILE_PATH, REDDIT_GZIP_COLUMN_SEPARATOR, DEFAULT_OUTPUT_FOLDER + "subreddits_with_ids.txt")