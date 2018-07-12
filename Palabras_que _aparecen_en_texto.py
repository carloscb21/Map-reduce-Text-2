from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import string


#lee las palabras de los textos, y devuelve una lista de los archivos en los que aparecen
class MRIvertedIndex(MRJob):
    
    def mapper(self,_,line):
        for x in string.punctuation:
            line = line.replace(x,' ')
        for word in line.split():
            yield word, jobconf_from_env('map.input.file')
    
    def reducer(self,word,files):
        file_index = []
        for l in files:
            if not l in file_index:
                file_index.append(l)
        yield word, file_index
    
if __name__ == '__main__':
    MRIvertedIndex.run()
    
