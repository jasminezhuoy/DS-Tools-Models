def predictLabel(k, predictCorpus):
    regex = re.compile('[^a-zA-Z]')
    Word = regex.sub(' ', predictCorpus).lower().split()
    
    # to {Word1: 3, Word2: 2, ...}
    WordCount = {}
    for w in Word:
        WordCount[w] = WordCount.get(w, 0) + 1
    
    # Words in Dictionary
    WordsAll = dictionary.keys().collect()
    
    # to ([1,2,0,...])
    WordCount20k = np.zeros(20000)
    for w in WordCount:
        if w in WordsAll:
            WordCount20k[WordsAll.index(w)] = WordCount[w]
    
    # TF
    predictTF = WordCount20k/len(Word)
    # TF-IDF
    predictTF_IDF = predictTF*IDF
    
    # kNN with L2 Distance
    # to [(dist1, label1), (dist2, label2), ...]
    NN = TF_IDF.map(lambda x: (np.linalg.norm(x[1]-predictTF_IDF, ord=2), x[0]))
    kNN = NN.sortByKey(ascending = True).take(k)
    
    # k labels
    kNN_labels = []
    for dist, lab in kNN:
        kNN_labels.append(lab.split('/')[1])
    
    # Labels' Freq
    labels = {}
    for label in kNN_labels:
        if label in labels:
            labels[label] += 1
        else:
            labels[label] = 1
    
    return max(labels, key = labels.get)
