import requests
from bs4 import BeautifulSoup
import database
from pymongo import MongoClient


def parse_news():
    page_link = "https://www.v102.ru"
    list_link = []
    client = MongoClient('localhost', 27017)
    # Подключение к бд MongoDB
    db = client['News']
    series_collection = db.collection
    news_count = series_collection.count_documents({}) + 1

    # Цикл получения новостей
    for k in range(1, 7):
        current_page = page_link + str(k)
        l = requests.get(current_page)
        page_parse = BeautifulSoup(l.text, 'html.parser')
        # Получение ссылок на конкретные новости
        for news_link in page_parse.find_all("div", {"class": "btc_block-1_1 btc_h"}):
            temp = news_link.find("a")
            current_link = "https://www.v102.ru/" + temp["href"]

            content_string = ""
            list_link.append(current_link)
            # Заполнение массива ссылками на новости текущей страницы сайта
            for i in range(len(list_link)):
                link = list_link[i]
            r = requests.get(link)
            soup = BeautifulSoup(r.text, 'html.parser')

            # Получение заголовка новости
            titles = soup.find_all('h1')
            for title in titles:
                cmts = soup.find_all("div", {"class": "btc_block-5_2"})

            # Получение текста новости
                para = soup.find("div", id="bt_center").find_all("p")

            for content in para:
                content_string = content_string + content.text

            # Получение даты и времени публикации
            date = soup.find('div', {'align': 'right'})
            if date == None:
                continue
            if len(date.text) > 40:
                date = soup.find('div', {'align': 'right'}).next_sibling


            # Добавление данных о новости в словарь для передачи в БД
            new_data = {
                "_id": news_count,
                "title": title.text,
                "date": date.text,
                "link": list_link[i],
                "text": content_string,
                "cmts": len(cmts),
                "people": '',
                "places": ''
            }

            data_to_update = {
                "title": title.text,
                "date": date.text,
                "link": list_link[i]
            }

            update = {
                "cmts": len(cmts)
            }

            if series_collection.find_one({"title": title.text, "date": date.text,"link": list_link[i]}) != None:
                database.update_document(series_collection, data_to_update, update)
            else:
                database.insert_document(series_collection, new_data)
                news_count = news_count + 1


def main():
    parse_news()


if __name__ == '__main__':
    main()
