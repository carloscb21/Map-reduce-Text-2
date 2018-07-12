from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
from mrjob.step import MRStep

#hace lo mismo que mapReducA_10_05
class MRIvertedIndex(MRJob):
    SORT_VALUES = True
    def mapper(self,_,line):
        yield jobconf_from_env('map.input.file'), len(line)
        yield '.total.', len(line)
    
    def reducer(self,file,chars):
        yield None,(file,sum(chars))
        
    def total(self,_,values):
        contador_total = values.next()
        assert contador_total[0] == '.total.'
        for (file,chars) in values:
            yield file, (chars,contador_total[1])
    
    def step(self):
        return [MRStep(mapper = self.mapper,reducer= self.reducer),
                MRStep(reducer = self.total)
        ]
    
if __name__ == '__main__':
    MRIvertedIndex.run()