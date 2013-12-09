"""Find 10 most common titles in Vroot.

This program will take a CSV data file and output tab-seperated lines of

    Title -> number of visits

To run:

    python top_titles.py user-visits_msweb.data

To store output:

    python top_titles.py user-visits_msweb.data >top_titles.out
"""

from mrjob.job import MRJob
from combine_user_visits import csv_readline
import csv

title_ids_dict = {}

class TopTitleIDs(MRJob):

    def mapper_get_ids(self, line_no, line):
        """Extracts the title IDs in Vroot"""
        cell = csv_readline(line)
        if cell[0] == 'V':
            yield cell[1], 1

    def reducer_generate_pairs(self, title_id, counts):
        """Sumarizes the title counts by adding them together."""
        yield None, (sum(counts), title_id)

    def reducer_sorted_counts(self, _, title_pairs):
        """Sorts top visited title_ids"""
        sorted_pairs = sorted(title_pairs, reverse = True)
        for pair in sorted_pairs[:10]:
            title_ids_dict[pair[1]] = pair[0]
            # yield pair[1], pai
    def steps(self):
        return [
            self.mr(mapper = self.mapper_get_ids,
                    reducer = self.reducer_generate_pairs),
            self.mr(reducer = self.reducer_sorted_counts)
        ]

class TopTitles(MRJob):
    """Match top visited title id with title name"""
    # def __init__(self, title_ids_dict):
    #     super(MRJob, self).__init__()
    #     self.title_ids_dict= title_ids_dict
    def mapper_get_titles(self, _, line):
        """Extracts the titles in Vroot"""
        cell = csv_readline(line)
        if cell[0] == 'A':
            yield cell[1], cell[3]
    def reducer_generate_pairs(self, title_id, title):
        """Match top titles with their counts"""
        for each_title in title: #Title is generator (although with one element), cannot be retrieved otherwise
            if title_id in title_ids_dict.keys():
                yield None, (title_ids_dict[title_id], each_title)
    def reducer_sorted(self, _, title_pairs):
        for each in sorted(title_pairs, reverse = True):
            yield each[1], each[0]
    def steps(self):
        return [
            self.mr(mapper = self.mapper_get_titles,
                    reducer = self.reducer_generate_pairs),
            self.mr(reducer = self.reducer_sorted)
        ]

if __name__ == '__main__':
    TopTitleIDs.run()
    TopTitles.run()