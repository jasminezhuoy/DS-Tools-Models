# to ("word1", docID) ("word2", docID)...
WordDoc = keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1]))

# dictionary join WordDoc on word
# to ("mostcommonword", [1, docID1]) ("nextmostcommon", [2, docID2])...
DictDoc = dictionary.join(WordDoc)

# to (docID1, 1) (docID1, 2)...
DocEntry = DictDoc.map(lambda x: (x[1][1], x[1][0]))

# to (docID1, [1,1,2,2,2,...]) (docID2, [1,2,2,3,3,...])...
Doc_dictionary = DocEntry.groupByKey()

# rdd to np.array
def rdd_to_nparray(rdd):
    nparray = np.zeros(20000)
    for i in rdd:
        nparray[i] += 1
    return nparray

DocCounts = Doc_dictionary.map(lambda x: (x[0], rdd_to_nparray(x[1])))

# Result
# result1_1 = DocCounts.lookup('20_newsgroups/comp.graphics/37261')
# result1_2 = DocCounts.lookup('20_newsgroups/talk.politics.mideast/75944')
# result1_3 = DocCounts.lookup('20_newsgroups/sci.med/58763')

# result1_1[0][result1_1[0].nonzero()]
# result1_2[0][result1_2[0].nonzero()]
# result1_3[0][result1_3[0].nonzero()]
