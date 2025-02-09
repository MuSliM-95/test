import pytesseract
from  pdf2image import  convert_from_bytes


# TRY https://github.com/loadlost/PP-Parser/blob/master/pdf_parser.py

def reverse_string_order(text):
    """Reverses the order of strings in a given text.

    Args:
        text: The input text containing multiple strings.

    Returns:
        The text with the order of strings reversed.
    """
    strings = text.splitlines()
    reversed_strings = strings[::-1]
    return "\n".join(reversed_strings)

def extract_text_from_pdf_images(file_bytes: bytes) -> str:
    """
    Extracts text from images within a PDF file.

    Args:
        file_bytes: The bytes of the PDF file.

    Returns:
        A string containing the extracted text from all images in the PDF.
    """
    images = convert_from_bytes(file_bytes)
    #images  = convert_from_path(temp_pdf.name)  # This will raise an exception if the file is not a valid PDF
    extracted_text = ''
    for i, image in enumerate(images):
        text = pytesseract.image_to_string(image, lang='rus')
        extracted_text += text
    return extracted_text
            