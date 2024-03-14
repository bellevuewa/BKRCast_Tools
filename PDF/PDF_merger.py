import PyPDF2

file1 = r"I:\Modeling and Analysis Group\00_ModelCoordination\001InroLicense\Bentley invoices\2024\Dynameq\Bentley-dynameq_form.pdf"
file2 = r"I:\Modeling and Analysis Group\00_ModelCoordination\001InroLicense\Bentley invoices\2024\Dynameq\Dynameq_2024_Invoice.PDF"
outputfile = r'I:\Modeling and Analysis Group\00_ModelCoordination\001InroLicense\Bentley invoices\2024\Dynameq\2044_Dynameq_invoice_submittal.pdf'

def merge_pdfs(input_file1, input_file2, output_file):
    with open(input_file1, 'rb') as file1, open(input_file2, 'rb') as file2:
        pdf_reader1 = PyPDF2.PdfFileReader(file1)
        pdf_reader2 = PyPDF2.PdfFileReader(file2)

        pdf_writer = PyPDF2.PdfFileWriter()

        for page_num in range(pdf_reader1.numPages):
            page = pdf_reader1.getPage(page_num)
            pdf_writer.addPage(page)

        for page_num in range(pdf_reader2.numPages):
            page = pdf_reader2.getPage(page_num)
            pdf_writer.addPage(page)

        with open(output_file, 'wb') as output:
            pdf_writer.write(output)

# Example usage
merge_pdfs(file1, file2, outputfile)
print('Done')

