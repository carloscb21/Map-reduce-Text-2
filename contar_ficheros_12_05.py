from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env

#Veamos el numero de ficheros que me pasan
class ContDoc(MRJob):
    def mapper(self,_,line):
        yield None,jobconf_from_env('map.input.file')
    
    def reducer(self,_,values):
    	print(set(values))
        yield "numero-ficheros",len(set(values))

if __name__ == '__main__':
    ContDoc.run()