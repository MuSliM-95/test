import io
import tempfile
import fitz  # PyMuPDF
import pytesseract
from PIL import Image

def extract_text_from_pdf_images(file_bytes: bytes) -> str:
    """
    Extracts text from images within a PDF file.

    Args:
        file_bytes: The bytes of the PDF file.

    Returns:
        A string containing the extracted text from all images in the PDF.
    """
    try:
        with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_pdf:
            temp_pdf.write(file_bytes)
            temp_pdf.flush()  # Ensure data is written to disk

            # Open the PDF file
            pdf_document = fitz.open(temp_pdf.name)
            full_text = ""

            # Iterate through each page
            for page_number in range(len(pdf_document)):
                page = pdf_document[page_number]
                image_list = page.get_images(full=True)

                # Iterate through each image on the page
                for img_index, img in enumerate(image_list):
                    xref = img[0]  # Get the xref of the image
                    base_image = pdf_document.extract_image(xref)  # Extract the image
                    image_bytes = base_image["image"]  # Get the image bytes
                    image = Image.open(io.BytesIO(image_bytes))

                    # Recognize text in the image
                    text = pytesseract.image_to_string(image, lang="rus")
                    full_text += f"\n{text}\n\n"
            pdf_document.close()
            return full_text

    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return ""                    