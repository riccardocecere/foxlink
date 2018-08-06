from pyspark import SparkContext, SparkConf
from web_discovery_aux import parsing_aux
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
import nltk


# Input spark context, path to the training set, number of features for the vectors
def create_naive_bayes_model(sc,training_path,number_of_features):
    input = sc.textFile(training_path)

    try:
        nltk.download('punkt')
        nltk.download('stopwords')
    except:
        print 'error to take nltk packages:'


    training_set = input.map(lambda line: line.split('\t')) \
    .map(lambda (site,label):(site,label,parsing_aux.extract_clean_text_from_page(site)))\
        .filter(lambda (site,label,words):isinstance(words, list))\
        .filter(lambda (site,label,words):len(words)>2)

    # Split data into labels and features, transform
    # preservesPartitioning is not really required
    # since map without partitioner shouldn't trigger repartitiong
    labels = training_set.map(lambda (site,label,words):label)


    # Indicates the length of the vector to use, and apply for each element an Hash function
    # the result will be an RDD VECTOR
    tf = HashingTF(numFeatures=number_of_features).transform(
        training_set.map(lambda (site,label,words): words))

    tf.cache()

    # I creates the IDF based on the previous TF
    idf = IDF().fit(tf)

    # Trasform idf in our tfidf RDD vectors
    tfidf = idf.transform(tf)


    tfidf =tfidf.zipWithIndex()
    labels = labels.zipWithIndex()

    # Combine using join
    training = labels.map(lambda(label,index):(index,label)).join(tfidf.map(lambda (vector,index):(index,vector)))\
               .map(lambda (index,(label,vector)): LabeledPoint(label,vector))

    # Train
    model = NaiveBayes.train(training)

    return (model,idf)

# input spark context, naive model already trained, idf trained with the training set,
# an rdd containing all the sites to evaluate in the form (domain, {'pages': [(id1,page1),(id2,page2)...], 'home_page_clean_text': ['token1','token2',...]})
# number of features of the vector
# boolean for save on hdfs and path where to save
def classify_with_naive_bayes(sc, model, idf, number_of_features, sites, save, path_to_save_evaluation):

    # Calculate Tf based on the tokens extracted from the pages
    tf = HashingTF(numFeatures=number_of_features).transform(sites.map(lambda (domain, values): values['home_page_clean_text']))

    #Calculate tfidf
    tfidf = idf.transform(tf)

    #Makes predictions
    preds = model.predict(tfidf)

    #Zip with index necessary for the join
    preds = preds.zipWithIndex()

    # I creates a new rdd for bind the correct domain and values to each prediction
    domains = sites.map(lambda (domain,values): (domain, {'pages': values['pages']}))
    domains = domains.zipWithIndex()

    # Join predictions with domains
    result = domains.map(lambda ((domain,values),index): (index, (domain,values))).join(preds.map(lambda (pred, index): (index, pred)))
    result = result.map(lambda(index, ((domain,values),prediction)):(domain,{'pages':values['pages'], 'prediction':prediction}))\
            .filter(lambda (domain,values): values['prediction'] == 1.0)

    if(save):
        if path_to_save_evaluation!=None and path_to_save_evaluation!='':
            result.saveAsTextFile(path_to_save_evaluation)
    return result






