# PlatesMania scanner for Uzbek LPRNet dataset

Асинхронный сканер обходит `platesmania.com/uz/gallery-{page}` по сегментам `r + nomer + ctype`,
собирает прямые `image_url` и текст номера, а затем сохраняет их в CSV.

## Что делает

- Генерирует все сегменты `регион × ctype`
- Идет по страницам галереи асинхронно через `httpx`
- Меняет `User-Agent` каждые `N` запросов
- Делает случайную паузу между запросами
- Сохраняет нормализованный номер без пробелов в колонку `plate_text`
- Создает папку `dataset/images/` под будущий загрузчик

## Установка

```bash
python -m pip install -r requirements.txt
```

## Быстрый запуск

```bash
python scan_platesmania.py --save-debug-html
```

CSV по умолчанию сохраняется в `dataset/platesmania_links.csv`.

## Если сайт отдает KillBot

Скрипт умеет явно распознавать страницу антибота. В этом случае передайте cookies
из уже верифицированной браузерной сессии:

```bash
python scan_platesmania.py --cookies-file cookies.json --save-debug-html
```

Поддерживаются:

- JSON-экспорт cookies (`[{"name":"...", "value":"..."}]`)
- JSON вида `{"cookies":[...]}`
- обычная строка `Cookie` в файле
- `--cookie-header "a=1; b=2"`

## Полезные аргументы

```bash
python scan_platesmania.py \
  --regions tashkent,fergana,samarkand \
  --ctypes 1,2 \
  --max-pages 120 \
  --segment-concurrency 4 \
  --delay-min 1.5 \
  --delay-max 3.0 \
  --rotate-every 12 \
  --save-debug-html
```

## Колонки CSV

- `filename` - имя будущего файла изображения
- `image_url` - прямой URL картинки
- `plate_text` - номер без пробелов, формат для LPRNet
- `plate_display` - номер в человекочитаемом виде
- `source_page`, `source_url` - откуда была взята запись
- `region_*`, `mask`, `ctype`, `ctype_label` - метаданные сегмента
