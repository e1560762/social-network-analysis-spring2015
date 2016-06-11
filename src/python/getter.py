import praw
import threading
import time

USER_AGENT = "desktop:network_of_subreddits:v1.0 by /u/yrenugli"
SUBREDDIT_FILE_PATH = "/home/arcelik/projects/MiniProjects/social-network-analysis-spring2015/excluded/output/subreddits_with_ids.txt"
SUBREDDIT_FILE_COLUMN_POSITIONS = {'id':0, 'name':1}
DEFAULT_OUTPUT_FOLDER = "/home/arcelik/projects/MiniProjects/social-network-analysis-spring2015/excluded/output/"
NUMBER_OF_THREADS = 4

class redditCrawler(threading.Thread):
	def __init__(self, thread_id, name, amount_of_sleep, subreddit_names):
		threading.Thread.__init__(self)
		self.thread_id = thread_id
		self.name = name
		self.amount_of_sleep = amount_of_sleep
		self.subreddit_names = subreddit_names

	def getSubscriberAmount(self):
		for ind, subreddit_name in enumerate(self.subreddit_names):
			try:
				number_of_subscribers = int(reddit_object.get_subreddit(subreddit_name)._get_json_dict()['subscribers'])
			except:
				number_of_subscribers = 0
			try:
				threadLock.acquire()
				fout.write("{0}\t{1}\n".format(subreddit_name, number_of_subscribers))
			finally:
				threadLock.release()
			
			if ind+1 % self.amount_of_sleep == 0:
				time.sleep(1)

	def run(self):
		self.getSubscriberAmount()

def extractSubredditsFromFile(subreddit_file):
	subreddits_list = []
	splitted_line = []
	list_length = 0
	with open(subreddit_file, "r") as fin:
		fin.readline() # first line explains columns, so skipped
		for line in fin:
			splitted_line = line.strip().split("\t")
			subreddits_list.append(splitted_line[SUBREDDIT_FILE_COLUMN_POSITIONS['name']])
			list_length += 1
	return (list_length, subreddits_list)

size_of_list, subreddit_list = extractSubredditsFromFile(SUBREDDIT_FILE_PATH)
print subreddit_list
global reddit_object
global fout
sth = size_of_list / NUMBER_OF_THREADS
threadLock = threading.Lock()
redditCrawlerThreads = []
reddit_object = praw.Reddit(user_agent=USER_AGENT)
reddit_object.set_oauth_app_info(client_id="wv5A-eBFuOCDkA",client_secret="pY0TTKzzlFJOEODt5xWrVXxGyRk", redirect_uri="https://www.reddit.com/user/yrenugli/")
fout = open(DEFAULT_OUTPUT_FOLDER + "/subreddits_with_subscribers.txt", "w")

redditC1 = redditCrawler(1, "Crawler-1", 20, subreddit_list[ : sth])
redditC2 = redditCrawler(2, "Crawler-2", 20, subreddit_list[sth : 2*sth])
redditC3 = redditCrawler(3, "Crawler-3", 20, subreddit_list[2*sth : 3*sth])
redditC4 = redditCrawler(4, "Crawler-4", 20, subreddit_list[3*sth : ])

redditC1.start()
redditC2.start()
redditC3.start()
redditC4.start()

redditCrawlerThreads.append(redditC1)
redditCrawlerThreads.append(redditC2)
redditCrawlerThreads.append(redditC3)
redditCrawlerThreads.append(redditC4)

for t in redditCrawlerThreads:
	t.join()
print "Exiting main thread"
fout.close()