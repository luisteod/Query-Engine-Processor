import csv
import os

def csv_import():
    #Define the path of csv to import
    print('Type the csv path to import')
    input_path = input('>> ')

    #Verify if the csv exists
    if not os.path.isfile(input_path):
        print("Csv not found!")
        return

    #Define the path to import the csv content
    print('Type the output path')
    output_path = input('>> ')

    #Verify if the file already exists, if exists allow to override or abort
    if os.path.isfile(output_path):
        confirm = input("The file already exist, do you wan't yo override? (y/n): ")
        if confirm.lower() != 'y':
            print("Canceled!")
            return

    #Creates the csv file
    open(output_path, 'w').close()

    #Write the content in csv
    try:
        with open(input_path, 'r') as input_file, open(output_path, 'w', newline='') as output_file:
            csv_reader = csv.reader(input_file)
            csv_writer = csv.writer(output_file)

            for line in csv_reader:
                csv_writer.writerow(line)

        print("Ended import!")
    except Exception as e:
        print("Error:", str(e))
