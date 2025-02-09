import PyPDF2

def extract_text_from_pdf(pdf_path):
    # Open the PDF file
    admins = await bot.get_chat_administrators(chat_id)
    admin_list = [admin.user.id for admin in admins if not admin.user.is_bot]
    
    # Получаем информацию о файле
    file_id = message.document.file_id
    file = await bot.get_file(file_id)

    # Получаем URL для скачивания файла
    file_url = f'https://api.telegram.org/file/bot{bot.token}/{file.file_path}'
    s3_factory = S3ServiceFactory(
        s3_settings=S3SettingsModel(
            aws_access_key_id=os.getenv('S3_ACCESS'),
            aws_secret_access_key=os.getenv('S3_SECRET'),
            endpoint_url=os.getenv('S3_URL')
        )
    )
    
    # Скачиваем файл
    async with aiohttp.ClientSession() as session:
        async with session.get(file_url) as response:
            if response.status == 200:
                file_bytes = await response.read()
                s3_client = s3_factory()
                await s3_client.upload_file_object(file_bytes=file_bytes, bucket_name='tg-bills', file_key=file_id)
            else:
                await message.reply("Не удалось скачать файл.")
                return

    # Открываем PDF файл
    with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_pdf:
        temp_pdf.write(file_bytes)
        temp_pdf.flush()  # Убедитесь, что данные записаны на диск

        # Открываем PDF файл
        pdf_document = fitz.open(temp_pdf.name)

        # Итерация по каждой странице
        for page_number in range(len(pdf_document)):
            page = pdf_document[page_number]
            image_list = page.get_images(full=True)
            response_text = ''
            # Итерация по каждому изображению на странице
            for img_index, img in enumerate(image_list):
                xref = img[0]  # Получаем xref изображения
                base_image = pdf_document.extract_image(xref)  # Извлекаем изображение
                image_bytes = base_image["image"]  # Получаем байты изображения
                image = Image.open(io.BytesIO(image_bytes))
                # Распознаем текст на изображении
                text = pytesseract.image_to_string(image, lang="rus")
                response_text += f"\n{text}\n\n"

            # Отправляем текст пользователю и администраторам
            if response_text:
                await message.reply(response_text)
                #for admin_id in admin_list:
                    #await bot.send_message(admin_id, response_text)

        pdf_document.close()