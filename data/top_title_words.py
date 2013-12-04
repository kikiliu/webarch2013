"""Find 10 most common title words in Vroot title.

This program will take a CSV data file and output tab-seperated lines of

    Vroot -> number of visits

To run:

    python top_title_words.py <anonymous-msweb.data

To store output:

    python top_title_words.py <anonymous-msweb.data >top_title_words.out
"""

from mrjob.job import MRJob
from combine_user_visits import csv_readline

class TopWords(MRJob):

    def mapper(self, line_no, line):
        """Extracts the words in Vroot"""
        cell = csv_readline(line)
        if cell[0] == 'A':
            for word in cell[3].split():
                yield word, 1

    def reducer(self, word, counts):
        """Sumarizes the wordcounts by adding them together."""
        total = sum(counts)
        yield word, total
        
if __name__ == '__main__':
    words_list=[]
    topwords = TopWords()
    with topwords.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            word, total = topwords.parse_output_line(line)
            words_list.append((word,total))
    sorted_list = sorted(words_list, key=lambda x: x[1], reverse=True)
    for pair in sorted_list[:10]:
        print pair[0], pair[1]