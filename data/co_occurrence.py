"""Find 10 most frequently co_occurred URLs within one user session.

This program will take a CSV data file and output tab-seperated lines of

    (URL1, URL2) -> number of co_occurrences

To run:

    python co_occurrence.py user-visits_msweb.data

To store output:

    python co_occurrence.py user-visits_msweb.data >co_occurrence.out
"""

from mrjob.job import MRJob
from combine_user_visits import csv_readline
import csv


class TopCoocurrencesID(MRJob):

    def mapper_get_ids(self, line_no, line):
        """Extracts the title IDs for each user session in Vroot"""
        cell = csv_readline(line)
        if cell[0] == 'V':
            yield cell[3], cell[1]

    def reducer_generate_pairs(self, user_id, url_ids):
        """Gets the urls pair in each user session. Notice that url_ids is a generator and its length cannot be acquired"""
        temp_url_id = ""
        for item in url_ids:
            if temp_url_id != "":
                yield (temp_url_id, item), 1
            temp_url_id = item

    def reducer_counts(self, urls_pair, counts):
        """Counts co-occurences of title_ids"""
        yield None, (sum(counts), urls_pair)

    def reducer_sorted(self, _, co_occurences):
        """Sorts the urls_pairs by co-occurences"""
        sorted_co_occurences = sorted(co_occurences, reverse = True)
        for co_occurrence in sorted_co_occurences[:10]:
            yield co_occurrence[1], co_occurrence[0]

    def steps(self):
        return [
            self.mr(mapper = self.mapper_get_ids,
                    reducer = self.reducer_generate_pairs),
            self.mr(reducer = self.reducer_counts),
            self.mr(reducer = self.reducer_sorted)
        ]
        

if __name__ == '__main__':
   TopCoocurrencesID.run()