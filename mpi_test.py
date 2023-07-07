import unittest
import csv


# must run the mpi_simulator.py first to create the csv file
# containing the output
class MpiTest(unittest.TestCase):

    def test_strings(self):
        target_strings = ['8 is not prime', '19 is prime', '107 is prime',
                          '2037 is not prime', '10111 is prime']

        # test for inaccurate strings
        # target_strings = ['8 is not prime', '19 is prime', '107 is prime',
        #                   '2037 is prime', '10111 is not prime']

        found_strings = []

        with open('prime.csv', 'r') as file:
            reader = csv.reader(file)

            for row in reader:
                print(row)
                for string in target_strings:
                    if string in row:
                        found_strings.append(string)

        missing_strings = set(target_strings) - set(found_strings)

        message = f"Inaccurate Results: {', '.join(missing_strings)}"
        self.assertEqual(len(missing_strings), 0, message)


if __name__ == '__main__':
    unittest.main()
