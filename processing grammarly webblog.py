import os
# running headless
from pyvirtualdisplay import Display
Display(visible=0, size=(1024, 768)).start()

from selenium import webdriver
driver = webdriver.Firefox()

from nltk import clean_html
from nltk.stem import PorterStemmer
import json

from boilerpipe.extract import Extractor
from BeautifulSoup import BeautifulSoup

from socket import error as SocketError
import errno

from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
import collections
from pprint import pprint

shared_folder= "vagrant"
blogs_file = 'blogs.json'
blogs2_file = 'feeds_2.json'
url_string = "http://www.grammarly.com/blog/page/{0}/"

stemmer = PorterStemmer()

stop_words = nltk.corpus.stopwords.words('english') + [
    '.',
    ',',
    '--',
    '\'s',
    '?',
    ')',
    '(',
    ':',
    '\'',
    '\'re',
    '"',
    '-',
    '}',
    '{',
    u'—',
    '!',
    u'•',
    u'”',
    u';'
    ]

def extract_blog_posts(url_string, PAGES = 48):
    blog_posts = []
    page_count = 0
    
    while(page_count<=PAGES):
        page_count+=1
        url = url_string.format(page_count) # create url
        driver.get(url)
        
        try:        
            article = driver.find_elements_by_tag_name('article')        
            articles_size = len(article)
            print 'processing ', url
        except SocketError as e:
            if e.errno != errno.ECONNRESET:
                raise # Not error we are looking for
            continue
            
        for i in xrange(articles_size):
            headers = article[i].find_elements_by_tag_name("header")
        for header in headers:
            article_a = header.find_elements_by_xpath("//h1/a[@title]")
        print 'extracting ...'             
        for e in article_a:
            extractor = Extractor(extractor = 'ArticleExtractor', url = e.get_attribute('href'))
            texts = extractor.getText()    
            
            blog_posts.append({'title': e.text, 'content': clean_html(texts), 'link': e.get_attribute('href')})
            return blog_posts

def save_blog_posts_to(file_name, blog_posts):
    #save to a dict
    out_file = os.path.join(shared_folder, file_name)
    f = open(out_file, 'w')
    f.write(json.dumps(blog_posts, indent=1))
    f.close()
    print 'Wrote to %s' % ( f.name, )

def words_distribution_of_text(posts):    
    texts = u",".join([post_block['content'] for post_block in posts])
    
    sentences = nltk.tokenize.sent_tokenize(texts)
    
    words = [stemmer.stem(w.lower()) for sentence in sentences for w in
             nltk.tokenize.word_tokenize(sentence)]
    
    fdist = nltk.FreqDist(words)
    return (fdist,words, texts)

# main functions call
blog_posts = extract_blog_posts(url_string)  
save_blog_posts_to(shared_folder, blogs_file)

#save to a list
blog_data = json.loads(os.path.join(shared_folder, blogs_file).read())
list_blogs = []
for post in  blog_data:
    list_blogs.append(post['content'])
out_file = os.path.join(shared_folder, blogs2_file)
f = open(out_file, 'w')
f.write(json.dumps(list_blogs, indent=1))
f.close()
print 'Wrote to %s' % ( f.name, )

#words distribution
fdist, words, texts = words_distribution_of_text(blog_data)
num_words = sum([i[1] for i in fdist.items()])
num_unique_words = len(fdist.keys())

# Hapaxes are words that appear only once
num_hapaxes = len(fdist.hapaxes())

top_10_non_stop_words = [w for w in fdist.items() if w[0]
                                not in stop_words][:10]
#print prettify 
print '\tNum Words:'.ljust(25), num_words
print '\tNum Unique Words:'.ljust(25), num_unique_words
print '\tNum Hapaxes:'.ljust(25), num_hapaxes
print '\tTop 10 Most Frequent Words (without stop words):\n\t\t', \
        '\n\t\t'.join(['%s (%s)'
        % (w[0], w[1]) for w in top_10_non_stop_words])
print

def process_text(text, stem=True):
    #Tokenize text and stem words removing punctuation 
    tokens = nltk.word_tokenize(text)
     
    if stem:
        tokens = [stemmer.stem(t) for t in tokens]
         
    return tokens

def cluster_texts(texts, clusters=3):
    # Transform texts to Tf-Idf coordinates and cluster texts using K-Means
    vectorizer = TfidfVectorizer(tokenizer=process_text,
        stop_words=stop_words,
        max_df=0.5,
        min_df=0.1,
        strip_accents='unicode')
     
    tfidf_model = vectorizer.fit_transform(texts)
    km_model = KMeans(n_clusters=clusters)
    km_model.fit(tfidf_model)
     
    clustering = collections.defaultdict(list)
     
    for idx, label in enumerate(km_model.labels_):
        clustering[label].append(idx)
     
    return clustering
    
#cluster into 5 chunks  
clusters = cluster_texts(posts, 5)
#print out the title of blog entry within each cluster group
for cluster in clusters :
    print "\n************************\n"
    for blog_key in clusters[cluster] :
        print blog_data[blog_key]['title']
#pprint(dict(clusters))
