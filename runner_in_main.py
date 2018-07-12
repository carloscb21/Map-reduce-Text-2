from mrjob.job import MRJob

#para poder usarlo con otro codigo no Map-Reduc
class MrChart(MRJob):
    
    def mapper(self,_,line):
        yield "chars",len(line)
    
    def reducer(self,key,chars):
        yield key,sum(chars)

if __name__ == '__main__':
    print 'Starting map-reduced job'
    #...
    job = MrChart(args=['El_buscon-Quevedo.asc'])
    runner = job.make_runner()
    runner.run()
    
    #ahora viene nuestro codigo no Map-Reduc
    tmp_output = []
    for line in runner.stream_output():
        print "****parser",job.parse_output_line(line)
        tmp_output = tmp_output + [line]
        #...
    print 'Results: ',tmp_output
    #...