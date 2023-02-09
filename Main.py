from cgitb import text
import mysql.connector
import os
import time
import PyPDF2
import fitz
import urllib.request
import pytesseract
import sys
import pdf2image

from dotenv import load_dotenv
from PIL import Image
#from pdf2image import convert_from_path

load_dotenv()

from elasticsearch import Elasticsearch

es = Elasticsearch(os.getenv("ES_URL"))

mydb = mysql.connector.connect(
    host=os.getenv("HOST"),
    port=os.getenv("PORT"),
    user=os.getenv("USER"),
    passwd=os.getenv("PASSWORD"),
    database=os.getenv("DATABASE")
)
#=========================== unterminated looping
while True:
    cursorLoop = mydb.cursor()
    cursorLoop.execute("SELECT p.id, s.sys_val as tipe_dokumen, r.sys_val as peraturan_daerah, p.no_perda, p.tahun, p.user_id, p.file, p.judul, p.indexed, p.created_dt, p.updated_dt, p.created_by, p.updated_by, p.tipe_dokumen as code_tipe_dokumen, p.peraturan_daerah as code_peraturan_daerah, p.is_hidden, p.jml_download, p.perda_text, p.is_image FROM perdas p INNER JOIN systems s ON s.sys_cd = p.tipe_dokumen INNER JOIN systems r ON r.sys_cd= p.peraturan_daerah WHERE s.sys_sub_cat = 'DOC_TYPE' AND r.sys_sub_cat='JENIS_PERATURAN' AND p.indexed = 0")
    myresult = cursorLoop.fetchall()
    rowLoop = cursorLoop.rowcount
    mydb.commit()
    
    #============looping condition indexed 1
    if(rowLoop == 0):
        print("No Data Indexed, wait for 60 sec")
        time.sleep(60)
    elif(rowLoop > 0):
        for x in myresult:
            #========= variable edit
            idDb = x[0]
            tipe_dokumen = x[1]
            peraturan_daerah = x[2]
            no_perda = x[3]
            tahun = x[4]
            user_id = x[5]
            file = x[6]
            judul = x[7]
            idxed = x[8]
            created_dt = x[9]
            updated_dt = x[10]
            created_by = x[11]
            updated_by = x[12]
            code_tipe_dokumen = x[13]
            code_perda = x[14]
            is_hidden = x[15]
            jml_download = x[16]
            indexed = 1
            is_image = x[18]
            
            #========= Download API From document
            downloadPdf = os.getenv("URL")
            response = urllib.request.urlopen(downloadPdf+idDb)    
            filePdf = open(file, 'wb')
            filePdf.write(response.read())
            filePdf.close()
            
            #========= Text Extractor PYPDF
            # creating a pdf file object 
            #pdfFileObj = open(file, 'rb') 
            # creating a pdf reader object 
            #pdfReader = PyPDF2.PdfFileReader(pdfFileObj)  
            # creating a page object
            #textPdf = ''
            #get all page
            #count = pdfReader.numPages
            #looping page
            #for i in range(count):
            #    page = pdfReader.getPage(i)
                # extracting text from page
            #    textPdf += page.extractText()
            #replace \n 
            #textPdf = textPdf.replace('\n','') 
            # closing the pdf file object 
            #pdfFileObj.close()

            #======================= Text Extractor PyMuPDF    
            if(is_image == 0):
                docPdf = fitz.open(file)
                textPdf = ''
                #extract text pymupdf
                for pageNumber, page in enumerate(docPdf.pages(), start=1):  
                    
                    textPdf += page.get_text()
                    textPdf = textPdf.replace("'","")
                    textPdf = textPdf.replace("\n"," ")
                    textPdf = textPdf.replace("\r"," ")

                    txt = open(f'test_{pageNumber}.txt', 'a', encoding='utf-8')
                    txt.writelines(textPdf)
                    txt.close()
                    os.remove(f'test_{pageNumber}.txt')
                #extract image pymupdf
                for pageNumber, page in enumerate(docPdf.pages(), start=1):  
                    
                    for imgNumber, img in enumerate(page.getImageList(), start=1):
                        xref = img[0]
                        pix = fitz.Pixmap(docPdf, xref)

                        if pix.n > 4:
                            pix = fitz.Pixmap(fitz.csRGB, pix)

                        pix.writePNG(f'image_page{pageNumber}.png')
                        os.remove(f'image_page{pageNumber}.png')  

                docPdf.close()
                
            elif(is_image == 1):
                # declare Tesseract
                pytesseract.pytesseract.tesseract_cmd = os.getenv("TESSERACT")
                # Store all the pages of the PDF in a variable
                pages = pdf2image.convert_from_path(file, poppler_path=os.getenv("POPPLER"))

                # Counter to store images of each page of PDF to image
                image_counter = 1

                for page in pages:
                    #save image
                    filename = "page_"+str(image_counter)+".jpg"
                    page.save(filename, 'JPEG')
                    
                    # Increment the counter to update filename
                    image_counter = image_counter + 1

                # Variable to get count of total number of pages
                filelimit = image_counter-1
                
                # Creating a text file to write the output
                outfile = "out_text.txt"
                f = open(outfile, "a")
                
                textPdf = ''
                # Iterate from 1 to total number of pages
                for i in range(1, filelimit + 1):
                
                    filename = "page_"+str(i)+".jpg"
                        
                    # Recognize the text as string in image using pytesserct
                    textPdf += str(((pytesseract.image_to_string(Image.open(filename)))))
                    textPdf = textPdf.replace("\n", " ")
                    textPdf = textPdf.replace("'"," ")  
                
                    # Finally, write the processed text to the file.
                    f.write(textPdf)
                    os.remove(filename)
                # Close the file after writing all the text.
                f.close()
                #delete output text
                os.remove("out_text.txt")
                
            #========= Update DB
            cursorLoop.execute ("UPDATE perdas as p SET p.indexed = '%s', p.perda_text = '%s'  WHERE p.id='%s' " % (indexed, textPdf, idDb))
            mydb.commit()
            
            #========= Delete File
            os.remove(file)

            #========= Elasticsearch
            doc = {
                "id": idDb,
                "tipe_dokumen": tipe_dokumen,
                "peraturan_daerah": peraturan_daerah,
                "no_perda": no_perda,
                "tahun": tahun,
                "user_id": user_id,
                "file": file,
                "judul": judul,
                "indexed": idxed,
                "created_dt": created_dt,
                "updated_dt": updated_dt,
                "created_by": created_by,
                "updated_by": updated_by,
                "code_tipe_dokumen": code_tipe_dokumen,
                "code_peraturan_daerah": code_perda,
                "is_hidden": is_hidden,
                "jml_download": jml_download,
                'perda_text': textPdf,
            }
            #resp = es.index(index="perdas", id=idDb, document=doc)
            resp = es.index(index="perdas", doc_type="perdas", id=idDb, body=doc)
            print(resp['result'])
            print("Row Data Indexed")
