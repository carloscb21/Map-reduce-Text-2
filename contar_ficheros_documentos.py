from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
from mrjob.step import MRStep

import string  

#Devolemos dos listas, [palabra, documento] [numero de apariciones en el documento, ¿en cuantos documentos aparece?]
class Td_Idf(MRJob):
    def mapper(self, _, line):
        for x in string.punctuation:
            line = line.replace(x,' ')
        words = line.split()
        for word in words:
            yield (word.lower(),jobconf_from_env('map.input.file')), 1.
                    
    def tf_reducer(self, (word,doc), values):
        yield word, (doc, sum(values))
        
    def idf_reducer(self, word, values):
        docs_in = set() #para poder saber el numero de documentos en los que aparece la palabra.
        doc_freq = []
        for (doc, freq) in values:
            doc_freq.append((doc,freq))
            docs_in.add(doc)
        K = len(docs_in)
        for (doc, freq) in doc_freq:
            yield (word,doc), (freq, K)

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.tf_reducer),
            MRStep(reducer = self.idf_reducer) 
        ]

if __name__ == '__main__':
    Td_Idf.run()
    
    
