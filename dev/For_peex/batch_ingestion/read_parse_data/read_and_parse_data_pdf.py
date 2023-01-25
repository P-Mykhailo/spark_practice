import pdfplumber
import pdfrw
import csv

my_pdf = 'C:/Users/mpuga/PycharmProjects/spark_practice/dev/data_for_examples/pdf_example.pdf'
py_pdf = pdfrw.PdfReader(my_pdf)

#Find number of pages in PDF
print(len(py_pdf.pages))

#Write first and third pages from pdf as distinct PDF
one_page = pdfrw.PdfWriter()
one_page.addpage(py_pdf.pages[0])
one_page.addpage(py_pdf.pages[2])
one_page.write('C:/Users/mpuga/PycharmProjects/spark_practice/dev/data_for_examples/result1.pdf')

my_pdf_table = 'C:/Users/mpuga/PycharmProjects/spark_practice/dev/data_for_examples/ALFABET.pdf'
#Open PDF and extract table
with pdfplumber.open(my_pdf_table) as pdf:
    page = pdf.pages[1]
    table = page.extract_tables()
#Table before transformation
    print(table)
#We have some data in columms which use two rows and we need to union these rows
    for i in range(len(table)):
        for j in range(len(table[i])):
            for k in range(len(table[i][j])):
                table[i][j][k] = str(table[i][j][k]).replace("\n", " ")
#Table after transfromation
    print(table)
#Split data into separate tables
    table_1 = table[0]
    table_dwuznaki = table[1]
    table_uwaga = table[2]
    pdf.close()

#Write new table into csv file with other delimiter
with open('/dev/data_for_examples/table_from_pdf_alphabet.csv', 'w', encoding='utf-8') as f:
    writer = csv.writer(f,  delimiter=';')
    writer.writerows(table_1)
    f.close()

with open('/dev/data_for_examples/table_from_pdf_dwuznaki.csv', 'w', encoding='utf-8') as f:
    writer = csv.writer(f,  delimiter=';')
    writer.writerows(table_dwuznaki)
    f.close()

with open('/dev/data_for_examples/table_from_pdf_uwaga.csv', 'w', encoding='utf-8') as f:
    writer = csv.writer(f,  delimiter=';')
    writer.writerows(table_uwaga)
    f.close()
