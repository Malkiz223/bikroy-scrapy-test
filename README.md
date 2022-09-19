# bikroy-scrapy-test

Скрипт, использующий фреймворк `Scrapy`, предназначенный для получения полной информации о товарах на
сайте https://bikroy.com


## Установка и настройка

Установите [Python 3.6+](https://www.python.org/)

Клонируйте проект, запустив в терминале команду:<br>
```git clone https://github.com/Malkiz223/bikroy-scrapy-test.git && cd bikroy-scrapy```<br>

Активируйте виртуальное окружение следующей командой (в зависимости от ОС и терминала):<br>
`venv\Scripts\activate.bat` # Windows, командная строка<br>
`venv\Scripts\activate.ps1` # Windows, PowerShell<br>
`source venv\Scripts\activate` # Linux

Установите зависимости:<br>
`pip install -r requirements.txt`

## Запуск

После установки и настройки мы готовы приступить к первому запуску.

Командой `cd bikroy` перейдите в первую папку проекта:<br>
Следующей командой запустите скрипт:<br>
`scrapy crawl bikroy.com -o my_json_file_name.json`

Флаг `-o` позволяет сохранить полученные данные в файл.

## Пример собираемых данных

[example.json](example.json)
