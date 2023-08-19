export const TABLE_COLUMNS = Object.freeze([
  {
      title: 'ID',
      data: 'id',
      type: 'numeric',
      editor: false,
  },
  {
      title: 'Имя',
      data: 'name',
      type: 'text',
      editor: false
  },
  {
      title: 'Телефон',
      data: 'phone',
      type: 'text',
      editor: false
  },
  {
      title: 'ИНН',
      data: 'inn',
      type: 'text',
      editor: false
  },
  {
      title: 'Описание',
      data: 'description',
      type: 'text',
      editor: false
  },
  {
      title: 'Время создания',
      data: 'created_at',
      type: 'time',
      editor: false,
  },
  {
      title: 'Время изменения',
      data: 'updated_at',
      type: 'time',
      editor: false,
  },
  {
    title: 'Код ответа',
    data: 'response_code',
    type: 'numeric',
    editor: false,
  },
  {
    title: 'Ответ в формате JSON',
    data: 'response',
    type: 'text',
    editor: false,
  },
  {
    title: 'Действие',
    data: 'action',
    type: 'text',
    editor: false
  },
]);
