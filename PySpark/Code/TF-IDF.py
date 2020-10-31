# to (docID1, 483) (docID2, 1340)...
WordsNum = keyAndListOfWords.map(lambda x: (x[0], len(x[1])))

# to (docID1, [[tf1,tf2,tf3,...],483]) (docID2, [[tf1,tf2,tf3,...],1340])...
TFAll = DocCounts.join(WordsNum)

# Term Frequency
# to (docID1, [tf1,tf2,tf3,...]) (docID2, [tf1,tf2,tf3,...])...
TF = TFAll.map(lambda x: (x[0], x[1][0]/x[1][1]))

# Number of Docs
# to (docID1, 1) (docID2, 1)...
DocAll = DocCounts.map(lambda x: (x[0], 1))
DocNum = DocAll.reduce(lambda x, y: ('', x[1]+y[1]))[1] # retrieve the values

# Number of Docs having Words i
# to (docID1, [1,1,0,...]) (docID2, [1,0,1,...])...
DocHaveWord = DocCounts.map(lambda x: (x[0], np.where(x[1] > 0, 1, 0)))
DocHaveWordNum = DocHaveWord.reduce(lambda x, y: ('', x[1]+y[1]))[1] # retrieve the values

# Inverse Document Frequency
# to [idf1,idf2,idf3,...]
IDF = np.log(DocNum/DocHaveWordNum)

TF_IDF = TF.map(lambda x: (x[0], x[1]*IDF))

# Result
# result2_1 = TF_IDF.lookup('20_newsgroups/comp.graphics/37261')
# result2_2 = TF_IDF.lookup('20_newsgroups/talk.politics.mideast/75944')
# result2_3 = TF_IDF.lookup('20_newsgroups/sci.med/58763')

# result2_1[0][result2_1[0].nonzero()]
# result2_2[0][result2_2[0].nonzero()]
# result2_3[0][result2_3[0].nonzero()]
