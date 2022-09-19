# bikroy-scrapy-test

Скрипт, использующий фреймворк `Scrapy`, предназначенный для получения полной информации об объявлениях на
сайте https://bikroy.com


## Установка и настройка

Установите [Python 3.6+](https://www.python.org/)

Клонируйте проект, запустив в терминале команду:<br>
```bash
git clone https://github.com/Malkiz223/bikroy-scrapy-test.git && cd bikroy-scrapy
```
<hr>

Активируйте виртуальное окружение следующей командой (в зависимости от ОС и терминала):<br>
#### Linux 
```bash
source venv\Scripts\activate
```
#### Windows
```commandline
venv\Scripts\activate
```
<hr>

#### Установите зависимости:<br>
```bash
pip install -r requirements.txt
```

## Запуск

После установки и настройки мы готовы приступить к первому запуску.

Командой `cd bikroy` перейдите в родительскую папку проекта<br>
#### Для запуска паука по нескольким ссылкам укажите их через "|":
```bash
scrapy crawl bikroy.com -a start_urls="https://bikroy.com/en/ads/bangladesh/mobiles|https://bikroy.com/en/ads/bangladesh/vehicles" -o result.json
```
#### Если не указать стартовые ссылки, то паук запустится по главной категории:

```bash
scrapy crawl bikroy.com -o result.json
```

<hr>

## Пример собираемых данных

[example.json](example.json)
