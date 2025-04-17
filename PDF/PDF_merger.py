import PyPDF2

file1 = r"I:\Modeling and Analysis Group\00_ModelCoordination\PTV license\2025\PTV-invoice_submission_form.pdf"
file2 = r"I:\Modeling and Analysis Group\00_ModelCoordination\PTV license\2025\Inv. #10713971.pdf"
outputfile = r'I:\Modeling and Analysis Group\00_ModelCoordination\PTV license\2025\2025_PTV_invoice_submittal.pdf'

def merge_pdfs(input_file1, input_file2, output_file):
    with open(input_file1, 'rb') as file1, open(input_file2, 'rb') as file2:
        pdf_reader1 = PyPDF2.PdfReader(file1)
        pdf_reader2 = PyPDF2.PdfReader(file2)

        pdf_writer = PyPDF2.PdfWriter()

        for page_num in range(len(pdf_reader1.pages)):
            page = pdf_reader1.pages[page_num]
            pdf_writer.add_page(page)

        for page_num in range(len(pdf_reader2.pages)):
            page = pdf_reader2.pages[page_num]
            pdf_writer.add_page(page)

        with open(output_file, 'wb') as output:
            pdf_writer.write(output)

# Example usage
merge_pdfs(file1, file2, outputfile)
print('Done')

